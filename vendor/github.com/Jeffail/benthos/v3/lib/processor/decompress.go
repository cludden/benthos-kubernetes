package processor

import (
	"bytes"
	"compress/bzip2"
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	"fmt"
	"io"
	"time"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/opentracing/opentracing-go"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeDecompress] = TypeSpec{
		constructor: NewDecompress,
		Categories: []Category{
			CategoryParsing,
		},
		Summary: `
Decompresses messages according to the selected algorithm. Supported
decompression types are: gzip, zlib, bzip2, flate.`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("algorithm", "The decompression algorithm to use.").HasOptions("gzip", "zlib", "bzip2", "flate"),
			partsFieldSpec,
		},
	}
}

//------------------------------------------------------------------------------

// DecompressConfig contains configuration fields for the Decompress processor.
type DecompressConfig struct {
	Algorithm string `json:"algorithm" yaml:"algorithm"`
	Parts     []int  `json:"parts" yaml:"parts"`
}

// NewDecompressConfig returns a DecompressConfig with default values.
func NewDecompressConfig() DecompressConfig {
	return DecompressConfig{
		Algorithm: "gzip",
		Parts:     []int{},
	}
}

//------------------------------------------------------------------------------

type decompressFunc func(bytes []byte) ([]byte, error)

func gzipDecompress(b []byte) ([]byte, error) {
	buf := bytes.NewBuffer(b)
	zr, err := gzip.NewReader(buf)
	if err != nil {
		return nil, err
	}

	outBuf := bytes.Buffer{}
	if _, err = outBuf.ReadFrom(zr); err != nil && err != io.EOF {
		return nil, err
	}
	zr.Close()
	return outBuf.Bytes(), nil
}

func zlibDecompress(b []byte) ([]byte, error) {
	buf := bytes.NewBuffer(b)
	zr, err := zlib.NewReader(buf)
	if err != nil {
		return nil, err
	}

	outBuf := bytes.Buffer{}
	if _, err = outBuf.ReadFrom(zr); err != nil && err != io.EOF {
		return nil, err
	}
	zr.Close()
	return outBuf.Bytes(), nil
}

func flateDecompress(b []byte) ([]byte, error) {
	buf := bytes.NewBuffer(b)
	zr := flate.NewReader(buf)

	outBuf := bytes.Buffer{}
	if _, err := outBuf.ReadFrom(zr); err != nil && err != io.EOF {
		return nil, err
	}
	zr.Close()
	return outBuf.Bytes(), nil
}

func bzip2Decompress(b []byte) ([]byte, error) {
	buf := bytes.NewBuffer(b)
	zr := bzip2.NewReader(buf)

	outBuf := bytes.Buffer{}
	if _, err := outBuf.ReadFrom(zr); err != nil && err != io.EOF {
		return nil, err
	}
	return outBuf.Bytes(), nil
}

func strToDecompressor(str string) (decompressFunc, error) {
	switch str {
	case "gzip":
		return gzipDecompress, nil
	case "zlib":
		return zlibDecompress, nil
	case "flate":
		return flateDecompress, nil
	case "bzip2":
		return bzip2Decompress, nil
	}
	return nil, fmt.Errorf("decompression type not recognised: %v", str)
}

//------------------------------------------------------------------------------

// Decompress is a processor that can decompress parts of a message following a
// chosen compression algorithm.
type Decompress struct {
	conf   DecompressConfig
	decomp decompressFunc

	log   log.Modular
	stats metrics.Type

	mCount     metrics.StatCounter
	mErr       metrics.StatCounter
	mSent      metrics.StatCounter
	mBatchSent metrics.StatCounter
}

// NewDecompress returns a Decompress processor.
func NewDecompress(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	dcor, err := strToDecompressor(conf.Decompress.Algorithm)
	if err != nil {
		return nil, err
	}
	return &Decompress{
		conf:   conf.Decompress,
		decomp: dcor,
		log:    log,
		stats:  stats,

		mCount:     stats.GetCounter("count"),
		mErr:       stats.GetCounter("error"),
		mSent:      stats.GetCounter("sent"),
		mBatchSent: stats.GetCounter("batch.sent"),
	}, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (d *Decompress) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	d.mCount.Incr(1)
	newMsg := msg.Copy()

	proc := func(i int, span opentracing.Span, part types.Part) error {
		newBytes, err := d.decomp(part.Get())
		if err != nil {
			d.mErr.Incr(1)
			d.log.Errorf("Failed to decompress message part: %v\n", err)
			return err
		}
		part.Set(newBytes)
		return nil
	}

	if newMsg.Len() == 0 {
		return nil, response.NewAck()
	}

	IteratePartsWithSpan(TypeDecompress, d.conf.Parts, newMsg, proc)

	d.mBatchSent.Incr(1)
	d.mSent.Incr(int64(newMsg.Len()))
	msgs := [1]types.Message{newMsg}
	return msgs[:], nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (d *Decompress) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (d *Decompress) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
