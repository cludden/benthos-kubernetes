package processor

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/processor"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/opentracing/opentracing-go"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
)

//------------------------------------------------------------------------------

func init() {
	processor.RegisterPlugin(
		"kubernetes",
		func() interface{} {
			return NewKubernetesConfig()
		},
		func(
			iconf interface{},
			mgr types.Manager,
			logger log.Modular,
			stats metrics.Type,
		) (types.Processor, error) {
			conf, ok := iconf.(*KubernetesConfig)
			if !ok {
				return nil, errors.New("failed to cast config")
			}
			return NewKubernetes(*conf, logger, stats)
		},
	)
	processor.DocumentPlugin(
		"kubernetes",
		`Performs operations against a Kubernetes cluster.`,
		nil,
	)
}

//------------------------------------------------------------------------------

// KubernetesConfig defines runtime configuration for a Kubernetes processor
type KubernetesConfig struct {
	Operator string `json:"operator" yaml:"operator"`
	Parts    []int  `json:"parts" yaml:"parts"`
}

// NewKubernetesConfig creates a new KubernetesConfig with default values
func NewKubernetesConfig() *KubernetesConfig {
	return &KubernetesConfig{}
}

//------------------------------------------------------------------------------

// Kubernetes is a processor that reverses all messages.
type Kubernetes struct {
	client   client.Client
	operator string
	parts    []int

	log   log.Modular
	stats metrics.Type
}

// NewKubernetes returns a Reverse processor.
func NewKubernetes(
	conf KubernetesConfig,
	log log.Modular,
	stats metrics.Type,
) (types.Processor, error) {
	k := &Kubernetes{
		operator: conf.Operator,
		parts:    conf.Parts,

		log:   log,
		stats: stats,
	}

	// initalize controller manager
	client, err := client.New(config.GetConfigOrDie(), client.Options{})
	if err != nil {
		return nil, fmt.Errorf("error initializing controller manager: %v", err)
	}
	k.client = client

	switch k.operator {
	case "get", "create", "update", "delete":
	default:
		return nil, fmt.Errorf("unsupported operator: %s", k.operator)
	}

	return k, nil
}

// ProcessMessage applies the processor to a message
func (k *Kubernetes) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	// Always create a new copy if we intend to mutate message contents.
	newMsg := msg.Copy()
	ctx := context.Background()

	proc := func(index int, span opentracing.Span, part types.Part) error {
		var u unstructured.Unstructured
		if err := u.UnmarshalJSON(part.Get()); err != nil {
			return fmt.Errorf("invalid message part, must be valid kubernetes runtime object: %v", err)
		}

		var err error
		switch k.operator {
		case "get":
			key, err := client.ObjectKeyFromObject(&u)
			if err != nil {
				err = fmt.Errorf("failed to get object: failed to get object key from object: %v", err)
				break
			}
			if err = k.client.Get(ctx, key, &u); err != nil {
				err = fmt.Errorf("failed to get object: %v", err)
			}
		case "create":
			if err = k.client.Create(ctx, &u); err != nil {
				err = fmt.Errorf("failed to create object: %v", err)
			}
		case "update":
			if err = k.client.Update(ctx, &u); err != nil {
				err = fmt.Errorf("failed to update object: %v", err)
			}
		case "delete":
			if err = k.client.Delete(ctx, &u); err != nil {
				err = fmt.Errorf("failed to update object: %v", err)
			}
		}
		if err != nil {
			k.log.Errorf("failed to process message: %v", err)
			return err
		}

		b, err := u.MarshalJSON()
		if err != nil {
			err = fmt.Errorf("failed to parse result object: %v", err)
			k.log.Errorln(err.Error())
		}

		part.Set(b)

		return nil
	}

	processor.IteratePartsWithSpan("kubernetes", k.parts, newMsg, proc)
	return []types.Message{newMsg}, nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (k *Kubernetes) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (k *Kubernetes) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
