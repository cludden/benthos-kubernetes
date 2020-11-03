package input

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	sess "github.com/Jeffail/benthos/v3/lib/util/aws/session"
	"github.com/Jeffail/gabs/v2"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sqs"
)

func init() {
	Constructors[TypeAWSS3] = TypeSpec{
		constructor: func(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
			r, err := newAmazonS3(conf.AWSS3, log, stats)
			if err != nil {
				return nil, err
			}
			return NewAsyncReader(
				TypeAWSS3,
				true,
				reader.NewAsyncBundleUnacks(
					reader.NewAsyncPreserver(r),
				),
				log, stats,
			)
		},
		Status: docs.StatusExperimental,
		Summary: `
This input is a refactor of the current stable (and shorter named) ` + "[`s3` input](/docs/components/inputs/s3)" + ` which is still the recommended one to use until this input is considered stable. However, this input has improved capabilities and will eventually replace it.`,
		Description: `
Downloads objects within an S3 bucket, optionally filtered by a prefix. If an SQS queue has been configured then only object keys read from the queue will be downloaded.

If an SQS queue is not specified the entire list of objects found when this input starts will be consumed.

## Downloading Objects on Upload with SQS

A common pattern for consuming S3 objects is to emit upload notification events from the bucket either directly to an SQS queue, or to an SNS topic that is consumed by an SQS queue, and then have your consumer listen for events which prompt it to download the newly uploaded objects. More information about this pattern and how to set it up can be found at: https://docs.aws.amazon.com/AmazonS3/latest/dev/ways-to-add-notification-config-to-bucket.html.

Benthos is able to follow this pattern when you configure an ` + "`sqs.url`" + `, where it consumes events from SQS and only downloads object keys received within those events. In order for this to work Benthos needs to know where within the event the key and bucket names can be found, specified as [dot paths](/docs/configuration/field_paths) with the fields ` + "`sqs.key_path` and `sqs.bucket_path`" + `. The default values for these fields should already be correct when following the guide above.

If your notification events are being routed to SQS via an SNS topic then the events will be enveloped by SNS, in which case you also need to specify the field ` + "`sqs.envelope_path`" + `, which in the case of SNS to SQS will usually be ` + "`Message`" + `.

When using SQS please make sure you have sensible values for ` + "`sqs.max_messages`" + ` and also the visibility timeout of the queue itself. When Benthos consumes an S3 object the SQS message that triggered it is not deleted until the S3 object has been sent onwards. This ensures at-least-once crash resiliency, but also means that if the S3 object takes longer to process than the visibility timeout of your queue then the same objects might be processed multiple times.

## Downloading Large Files

When downloading large files it's often necessary to process it in streamed parts in order to avoid loading the entire file in memory at a given time. In order to do this a ` + "[`codec`](#codec)" + ` can be specified that determines how to break the input into smaller individual messages.

## Credentials

By default Benthos will use a shared credentials file when connecting to AWS services. It's also possible to set them explicitly at the component level, allowing you to transfer data across accounts. You can find out more [in this document](/docs/guides/aws).

## Metadata

This input adds the following metadata fields to each message:

` + "```" + `
- s3_key
- s3_bucket
- s3_last_modified_unix
- s3_last_modified (RFC3339)
- s3_content_type
- s3_content_encoding
- All user defined metadata
` + "```" + `

You can access these metadata fields using [function interpolation](/docs/configuration/interpolation#metadata).`,
		FieldSpecs: append(
			append(docs.FieldSpecs{
				docs.FieldCommon("bucket", "The bucket to consume from. If the field `sqs.url` is specified this field is optional."),
				docs.FieldCommon("prefix", "An optional path prefix, if set only objects with the prefix are consumed."),
			}, sess.FieldSpecs()...),
			docs.FieldAdvanced("force_path_style_urls", "Forces the client API to use path style URLs for downloading keys, which is often required when connecting to custom endpoints."),
			docs.FieldAdvanced("delete_objects", "Whether to delete downloaded objects from the bucket once they are processed."),
			codecDocs,
			docs.FieldCommon("sqs", "Consume SQS messages in order to trigger key downloads.").WithChildren(
				docs.FieldCommon("url", "An optional SQS URL to connect to. When specified this queue will control which objects are downloaded."),
				docs.FieldAdvanced("endpoint", "A custom endpoint to use when connecting to SQS."),
				docs.FieldCommon("key_path", "A [dot path](/docs/configuration/field_paths) whereby object keys are found in SQS messages."),
				docs.FieldCommon("bucket_path", "A [dot path](/docs/configuration/field_paths) whereby the bucket name can be found in SQS messages."),
				docs.FieldCommon("envelope_path", "A [dot path](/docs/configuration/field_paths) of a field to extract an enveloped JSON payload for further extracting the key and bucket from SQS messages. This is specifically useful when subscribing an SQS queue to an SNS topic that receives bucket events.", "Message"),
				docs.FieldAdvanced("max_messages", "The maximum number of SQS messages to consume from each request."),
			),
		),
		Categories: []Category{
			CategoryServices,
			CategoryAWS,
		},
	}
}

//------------------------------------------------------------------------------

// AWSS3SQSConfig contains configuration for hooking up the S3 input with an SQS queue.
type AWSS3SQSConfig struct {
	URL          string `json:"url" yaml:"url"`
	Endpoint     string `json:"endpoint" yaml:"endpoint"`
	EnvelopePath string `json:"envelope_path" yaml:"envelope_path"`
	KeyPath      string `json:"key_path" yaml:"key_path"`
	BucketPath   string `json:"bucket_path" yaml:"bucket_path"`
	MaxMessages  int64  `json:"max_messages" yaml:"max_messages"`
}

// NewAWSS3SQSConfig creates a new AWSS3SQSConfig with default values.
func NewAWSS3SQSConfig() AWSS3SQSConfig {
	return AWSS3SQSConfig{
		URL:          "",
		Endpoint:     "",
		EnvelopePath: "",
		KeyPath:      "Records.*.s3.object.key",
		BucketPath:   "Records.*.s3.bucket.name",
		MaxMessages:  10,
	}
}

// AWSS3Config contains configuration values for the aws_s3 input type.
type AWSS3Config struct {
	sess.Config        `json:",inline" yaml:",inline"`
	Bucket             string         `json:"bucket" yaml:"bucket"`
	Codec              string         `json:"codec" yaml:"codec"`
	Prefix             string         `json:"prefix" yaml:"prefix"`
	ForcePathStyleURLs bool           `json:"force_path_style_urls" yaml:"force_path_style_urls"`
	DeleteObjects      bool           `json:"delete_objects" yaml:"delete_objects"`
	SQS                AWSS3SQSConfig `json:"sqs" yaml:"sqs"`
}

// NewAWSS3Config creates a new AWSS3Config with default values.
func NewAWSS3Config() AWSS3Config {
	return AWSS3Config{
		Config:             sess.NewConfig(),
		Bucket:             "",
		Prefix:             "",
		Codec:              "all-bytes",
		ForcePathStyleURLs: false,
		DeleteObjects:      false,
		SQS:                NewAWSS3SQSConfig(),
	}
}

//------------------------------------------------------------------------------

type objectTarget struct {
	key    string
	bucket string

	ackFn func(context.Context, error) error
}

func newObjectTarget(key, bucket string, ackFn codecAckFn) *objectTarget {
	if ackFn == nil {
		ackFn = func(context.Context, error) error {
			return nil
		}
	}
	return &objectTarget{key, bucket, ackFn}
}

type objectTargetReader interface {
	Pop(ctx context.Context) (*objectTarget, error)
	Close(ctx context.Context) error
}

//------------------------------------------------------------------------------

type staticTargetReader struct {
	pending    []*objectTarget
	s3         *s3.S3
	conf       AWSS3Config
	startAfter *string
}

func newStaticTargetReader(
	ctx context.Context,
	conf AWSS3Config,
	log log.Modular,
	s3Client *s3.S3,
) (*staticTargetReader, error) {
	listInput := &s3.ListObjectsV2Input{
		Bucket:  aws.String(conf.Bucket),
		MaxKeys: aws.Int64(100),
	}
	if len(conf.Prefix) > 0 {
		listInput.Prefix = aws.String(conf.Prefix)
	}
	output, err := s3Client.ListObjectsV2WithContext(ctx, listInput)
	if err != nil {
		return nil, fmt.Errorf("failed to list objects: %v", err)
	}
	staticKeys := staticTargetReader{
		s3:   s3Client,
		conf: conf,
	}
	for _, obj := range output.Contents {
		staticKeys.pending = append(staticKeys.pending, newObjectTarget(*obj.Key, conf.Bucket, nil))
	}
	if len(output.Contents) > 0 {
		staticKeys.startAfter = output.Contents[len(output.Contents)-1].Key
	}
	return &staticKeys, nil
}

func (s *staticTargetReader) Pop(ctx context.Context) (*objectTarget, error) {
	if len(s.pending) == 0 && s.startAfter != nil {
		s.pending = nil
		listInput := &s3.ListObjectsV2Input{
			Bucket:     aws.String(s.conf.Bucket),
			MaxKeys:    aws.Int64(100),
			StartAfter: s.startAfter,
		}
		if len(s.conf.Prefix) > 0 {
			listInput.Prefix = aws.String(s.conf.Prefix)
		}
		output, err := s.s3.ListObjectsV2WithContext(ctx, listInput)
		if err != nil {
			return nil, fmt.Errorf("failed to list objects: %v", err)
		}
		for _, obj := range output.Contents {
			s.pending = append(s.pending, newObjectTarget(*obj.Key, s.conf.Bucket, nil))
		}
		if len(output.Contents) > 0 {
			s.startAfter = output.Contents[len(output.Contents)-1].Key
		}
	}
	if len(s.pending) == 0 {
		return nil, io.EOF
	}
	obj := s.pending[0]
	s.pending = s.pending[1:]
	return obj, nil
}

func (s staticTargetReader) Close(context.Context) error {
	return nil
}

//------------------------------------------------------------------------------

type sqsTargetReader struct {
	conf AWSS3Config
	log  log.Modular
	sqs  *sqs.SQS

	pending []*objectTarget
}

func newSQSTargetReader(
	conf AWSS3Config,
	log log.Modular,
	sqs *sqs.SQS,
) *sqsTargetReader {
	return &sqsTargetReader{conf, log, sqs, nil}
}

func (s *sqsTargetReader) Pop(ctx context.Context) (*objectTarget, error) {
	if len(s.pending) > 0 {
		t := s.pending[0]
		s.pending = s.pending[1:]
		return t, nil
	}
	var err error
	if s.pending, err = s.readSQSEvents(ctx); err != nil {
		return nil, err
	}
	if len(s.pending) == 0 {
		return nil, types.ErrTimeout
	}
	t := s.pending[0]
	s.pending = s.pending[1:]
	return t, nil
}

func (s *sqsTargetReader) Close(ctx context.Context) error {
	var err error
	for _, p := range s.pending {
		if aerr := p.ackFn(ctx, errors.New("service shutting down")); aerr != nil {
			err = aerr
		}
	}
	return err
}

func digStrsFromSlices(slice []interface{}) []string {
	var strs []string
	for _, v := range slice {
		switch t := v.(type) {
		case []interface{}:
			strs = append(strs, digStrsFromSlices(t)...)
		case string:
			strs = append(strs, t)
		}
	}
	return strs
}

func (s *sqsTargetReader) parseObjectPaths(sqsMsg *string) ([]objectTarget, error) {
	gObj, err := gabs.ParseJSON([]byte(*sqsMsg))
	if err != nil {
		return nil, fmt.Errorf("failed to parse SQS message: %v", err)
	}

	if len(s.conf.SQS.EnvelopePath) > 0 {
		d := gObj.Path(s.conf.SQS.EnvelopePath).Data()
		if str, ok := d.(string); ok {
			if gObj, err = gabs.ParseJSON([]byte(str)); err != nil {
				return nil, fmt.Errorf("failed to parse enveloped message: %v", err)
			}
		} else {
			return nil, fmt.Errorf("expected string at envelope path, found %T", d)
		}
	}

	var keys []string
	var buckets []string

	switch t := gObj.Path(s.conf.SQS.KeyPath).Data().(type) {
	case string:
		keys = []string{t}
	case []interface{}:
		keys = digStrsFromSlices(t)
	}
	if len(s.conf.SQS.BucketPath) > 0 {
		switch t := gObj.Path(s.conf.SQS.BucketPath).Data().(type) {
		case string:
			buckets = []string{t}
		case []interface{}:
			buckets = digStrsFromSlices(t)
		}
	}

	objects := make([]objectTarget, 0, len(keys))
	for i, key := range keys {
		if key, err = url.QueryUnescape(key); err != nil {
			return nil, fmt.Errorf("failed to parse key from SQS message: %v", err)
		}
		bucket := s.conf.Bucket
		if len(buckets) > i {
			bucket = buckets[i]
		}
		if len(bucket) == 0 {
			return nil, errors.New("required bucket was not found in SQS message")
		}
		objects = append(objects, objectTarget{
			key:    key,
			bucket: bucket,
		})
	}

	return objects, nil
}

func (s *sqsTargetReader) readSQSEvents(ctx context.Context) ([]*objectTarget, error) {
	var dudMessageHandles []*sqs.ChangeMessageVisibilityBatchRequestEntry
	addDudFn := func(m *sqs.Message) {
		dudMessageHandles = append(dudMessageHandles, &sqs.ChangeMessageVisibilityBatchRequestEntry{
			Id:                m.MessageId,
			ReceiptHandle:     m.ReceiptHandle,
			VisibilityTimeout: aws.Int64(0),
		})
	}

	output, err := s.sqs.ReceiveMessageWithContext(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(s.conf.SQS.URL),
		MaxNumberOfMessages: aws.Int64(s.conf.SQS.MaxMessages),
	})
	if err != nil {
		return nil, err
	}

	var pendingObjects []*objectTarget

messageLoop:
	for _, sqsMsg := range output.Messages {
		sqsMsg := sqsMsg

		if sqsMsg.Body == nil {
			addDudFn(sqsMsg)
			s.log.Errorln("Received empty SQS message")
			continue messageLoop
		}

		objects, err := s.parseObjectPaths(sqsMsg.Body)
		if err != nil {
			addDudFn(sqsMsg)
			s.log.Errorf("SQS extract key error: %v\n", err)
			continue messageLoop
		}
		if len(objects) == 0 {
			addDudFn(sqsMsg)
			s.log.Debugln("Extracted zero target keys from SQS message")
			continue messageLoop
		}

		pendingAcks := int32(len(objects))
		var nackOnce sync.Once
		for _, object := range objects {
			ackOnce := sync.Once{}
			pendingObjects = append(pendingObjects, newObjectTarget(
				object.key, object.bucket,
				func(ctx context.Context, err error) (aerr error) {
					if err != nil {
						nackOnce.Do(func() {
							// Prevent future acks from triggering a delete.
							atomic.StoreInt32(&pendingAcks, -1)

							// It's possible that this is called for one message
							// at the _exact_ same time as another is acked, but
							// if the acked message triggers a full ack of the
							// origin message then even though it shouldn't be
							// possible, it's also harmless.
							aerr = s.nackSQSMessage(ctx, sqsMsg)
						})
					} else {
						ackOnce.Do(func() {
							if atomic.AddInt32(&pendingAcks, -1) == 0 {
								aerr = s.ackSQSMessage(ctx, sqsMsg)
							}
						})
					}
					return
				},
			))
		}
	}

	// Discard any SQS messages not associated with a target file.
	for len(dudMessageHandles) > 0 {
		input := sqs.ChangeMessageVisibilityBatchInput{
			QueueUrl: aws.String(s.conf.SQS.URL),
			Entries:  dudMessageHandles,
		}

		// trim input entries to max size
		if len(dudMessageHandles) > 10 {
			input.Entries, dudMessageHandles = dudMessageHandles[:10], dudMessageHandles[10:]
		} else {
			dudMessageHandles = nil
		}
		s.sqs.ChangeMessageVisibilityBatch(&input)
	}

	return pendingObjects, nil
}

func (s *sqsTargetReader) nackSQSMessage(ctx context.Context, msg *sqs.Message) error {
	_, err := s.sqs.ChangeMessageVisibilityWithContext(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(s.conf.SQS.URL),
		ReceiptHandle:     msg.ReceiptHandle,
		VisibilityTimeout: aws.Int64(0),
	})
	return err
}

func (s *sqsTargetReader) ackSQSMessage(ctx context.Context, msg *sqs.Message) error {
	_, err := s.sqs.DeleteMessageWithContext(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(s.conf.SQS.URL),
		ReceiptHandle: msg.ReceiptHandle,
	})
	return err
}

//------------------------------------------------------------------------------

// AmazonS3 is a benthos reader.Type implementation that reads messages from an
// Amazon S3 bucket.
type awsS3 struct {
	conf AWSS3Config

	objectScannerCtor partCodecCtor
	keyReader         objectTargetReader

	session *session.Session
	s3      *s3.S3
	sqs     *sqs.SQS

	objectMut sync.Mutex
	object    *pendingObject

	log   log.Modular
	stats metrics.Type
}

type pendingObject struct {
	target  *objectTarget
	obj     *s3.GetObjectOutput
	scanner partCodec
}

// NewAmazonS3 creates a new Amazon S3 bucket reader.Type.
func newAmazonS3(
	conf AWSS3Config,
	log log.Modular,
	stats metrics.Type,
) (*awsS3, error) {
	if len(conf.Bucket) == 0 && len(conf.SQS.URL) == 0 {
		return nil, errors.New("either a bucket or an sqs.url must be specified")
	}
	s := &awsS3{
		conf:  conf,
		log:   log,
		stats: stats,
	}
	var err error
	if s.objectScannerCtor, err = getPartCodec(conf.Codec, newCodecConfig()); err != nil {
		return nil, err
	}
	return s, nil
}

func (a *awsS3) getTargetReader(ctx context.Context) (objectTargetReader, error) {
	if a.sqs != nil {
		return newSQSTargetReader(a.conf, a.log, a.sqs), nil
	}
	return newStaticTargetReader(ctx, a.conf, a.log, a.s3)
}

// ConnectWithContext attempts to establish a connection to the target S3 bucket
// and any relevant queues used to traverse the objects (SQS, etc).
func (a *awsS3) ConnectWithContext(ctx context.Context) error {
	if a.session != nil {
		return nil
	}

	sess, err := a.conf.GetSession(func(c *aws.Config) {
		c.S3ForcePathStyle = aws.Bool(a.conf.ForcePathStyleURLs)
	})
	if err != nil {
		return err
	}

	a.session = sess
	a.s3 = s3.New(sess)
	if len(a.conf.SQS.URL) != 0 {
		sqsSess := sess.Copy()
		if len(a.conf.SQS.Endpoint) > 0 {
			sqsSess.Config.Endpoint = &a.conf.SQS.Endpoint
		}
		a.sqs = sqs.New(sqsSess)
	}

	if a.keyReader, err = a.getTargetReader(ctx); err != nil {
		a.session = nil
		a.s3 = nil
		a.sqs = nil
		return err
	}

	if len(a.conf.SQS.URL) == 0 {
		a.log.Infof("Downloading S3 objects from bucket: %s\n", a.conf.Bucket)
	} else {
		a.log.Infof("Downloading S3 objects found in messages from SQS: %s\n", a.conf.SQS.URL)
	}
	return nil
}

func s3MsgFromPart(p *pendingObject, part types.Part) types.Message {
	msg := message.New(nil)
	msg.Append(part)

	meta := msg.Get(0).Metadata()

	meta.Set("s3_key", p.target.key)
	meta.Set("s3_bucket", p.target.bucket)
	if p.obj.LastModified != nil {
		meta.Set("s3_last_modified", p.obj.LastModified.Format(time.RFC3339))
		meta.Set("s3_last_modified_unix", strconv.FormatInt(p.obj.LastModified.Unix(), 10))
	}
	if p.obj.ContentType != nil {
		meta.Set("s3_content_type", *p.obj.ContentType)
	}
	if p.obj.ContentEncoding != nil {
		meta.Set("s3_content_encoding", *p.obj.ContentEncoding)
	}
	return msg
}

func (a *awsS3) getObjectTarget(ctx context.Context) (*pendingObject, error) {
	if a.object != nil {
		return a.object, nil
	}

	target, err := a.keyReader.Pop(ctx)
	if err != nil {
		return nil, err
	}

	obj, err := a.s3.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: aws.String(target.bucket),
		Key:    aws.String(target.key),
	})
	if err != nil {
		target.ackFn(ctx, err)
		return nil, err
	}

	object := &pendingObject{
		target: target,
		obj:    obj,
	}
	if object.scanner, err = a.objectScannerCtor(obj.Body, target.ackFn); err != nil {
		target.ackFn(ctx, err)
		return nil, err
	}

	a.object = object
	return object, nil
}

// ReadWithContext attempts to read a new message from the target S3 bucket.
func (a *awsS3) ReadWithContext(ctx context.Context) (msg types.Message, ackFn reader.AsyncAckFn, err error) {
	a.objectMut.Lock()
	defer a.objectMut.Unlock()
	if a.session == nil {
		return nil, nil, types.ErrNotConnected
	}

	defer func() {
		if errors.Is(err, io.EOF) {
			err = types.ErrTypeClosed
		} else if errors.Is(err, context.Canceled) ||
			errors.Is(err, context.DeadlineExceeded) ||
			(err != nil && strings.HasSuffix(err.Error(), "context canceled")) {
			err = types.ErrTimeout
		}
	}()

	var object *pendingObject
	if object, err = a.getObjectTarget(ctx); err != nil {
		return
	}

	var p types.Part
	var scnAckFn codecAckFn

scanLoop:
	for {
		if p, scnAckFn, err = object.scanner.Next(ctx); err == nil {
			break scanLoop
		}
		a.object = nil
		if err != io.EOF {
			return
		}
		if err = object.scanner.Close(ctx); err != nil {
			a.log.Warnf("Failed to close bucket object scanner cleanly: %v\n", err)
		}
		if object, err = a.getObjectTarget(ctx); err != nil {
			return
		}
	}

	return s3MsgFromPart(object, p), func(rctx context.Context, res types.Response) error {
		return scnAckFn(rctx, res.Error())
	}, nil
}

// CloseAsync begins cleaning up resources used by this reader asynchronously.
func (a *awsS3) CloseAsync() {
	go func() {
		a.objectMut.Lock()
		if a.object != nil {
			a.object.scanner.Close(context.Background())
			a.object = nil
		}
		a.objectMut.Unlock()
	}()
}

// WaitForClose will block until either the reader is closed or a specified
// timeout occurs.
func (a *awsS3) WaitForClose(time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
