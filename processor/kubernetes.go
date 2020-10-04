package processor

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Jeffail/benthos/v3/lib/bloblang"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/processor"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/opentracing/opentracing-go"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
		`Performs CRUD operations for Kubernetes objects.`,
		nil,
	)
}

//------------------------------------------------------------------------------

// KubernetesConfig defines runtime configuration for a Kubernetes processor
type KubernetesConfig struct {
	DeletionPropagation metav1.DeletionPropagation `json:"deletion_propagation" yaml:"deletion_propagation"`
	Operator            string                     `json:"operator" yaml:"operator"`
	OperatorMapping     string                     `json:"operator_mapping" yaml:"operator_mapping"`
	Parts               []int                      `json:"parts" yaml:"parts"`
}

// NewKubernetesConfig creates a new KubernetesConfig with default values
func NewKubernetesConfig() *KubernetesConfig {
	return &KubernetesConfig{
		Operator:            "get",
		DeletionPropagation: metav1.DeletePropagationBackground,
	}
}

//------------------------------------------------------------------------------

// Kubernetes is a processor that reverses all messages.
type Kubernetes struct {
	client client.Client

	deletionPropagation metav1.DeletionPropagation
	operator            string
	operatorMapping     bloblang.Mapping
	parts               []int

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
		deletionPropagation: conf.DeletionPropagation,
		operator:            conf.Operator,
		parts:               conf.Parts,

		log:   log,
		stats: stats,
	}

	switch k.deletionPropagation {
	case metav1.DeletePropagationBackground, metav1.DeletePropagationForeground, metav1.DeletePropagationOrphan:
	default:
		return nil, fmt.Errorf("invalid deletion propagation policy: %s", k.deletionPropagation)
	}

	if conf.OperatorMapping != "" {
		m, err := bloblang.NewMapping(conf.OperatorMapping)
		if err != nil {
			return nil, fmt.Errorf("error parsing operator field: %v", err)
		}
		k.operatorMapping = m
	}

	// initalize controller manager
	client, err := client.New(config.GetConfigOrDie(), client.Options{})
	if err != nil {
		return nil, fmt.Errorf("error initializing controller manager: %v", err)
	}
	k.client = client

	return k, nil
}

// ProcessMessage applies the processor to a message
func (k *Kubernetes) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	// Always create a new copy if we intend to mutate message contents.
	newMsg := msg.Copy()
	ctx := context.Background()

	proc := func(index int, span opentracing.Span, part types.Part) error {
		var err error
		var u unstructured.Unstructured
		if err := u.UnmarshalJSON(part.Get()); err != nil {
			return fmt.Errorf("invalid message part, must be valid kubernetes runtime object: %v", err)
		}
		id := fmt.Sprintf("%s Namespace=%s Name=%s", u.GetObjectKind().GroupVersionKind().String(), u.GetNamespace(), u.GetName())

		operator := k.operator
		if k.operatorMapping != nil {
			operatorB, err := k.operatorMapping.MapPart(index, msg)
			if err != nil {
				return fmt.Errorf("error evaluating operator mapping: %v", err)
			}
			operator = string(operatorB.Get())
		}

		switch operator {
		case "get":
			k.log.Debugf("getting kubernetes object: %s", id)
			key, perr := client.ObjectKeyFromObject(&u)
			if err != nil {
				err = fmt.Errorf("failed to get object: failed to get object key from object: %v", perr)
				break
			}
			if err = k.client.Get(ctx, key, &u); err != nil {
				err = fmt.Errorf("failed to get object: %v", err)
			}
		case "create":
			k.log.Debugf("creating kubernetes object: %s", id)
			if err = k.client.Create(ctx, &u); err != nil {
				err = fmt.Errorf("failed to create object: %v", err)
			}
		case "update":
			k.log.Debugf("updating kubernetes object: %s", id)
			if err = k.client.Update(ctx, &u); err != nil {
				err = fmt.Errorf("failed to update object: %v", err)
			}
		case "delete":
			k.log.Debugf("deleting kubernetes object: %s", id)
			var opts []client.DeleteOption

			policy := k.deletionPropagation
			if msgPolicy := metav1.DeletionPropagation(part.Metadata().Get("deletion_propagation")); string(msgPolicy) != "" {
				switch msgPolicy {
				case metav1.DeletePropagationBackground, metav1.DeletePropagationForeground, metav1.DeletePropagationOrphan:
					policy = msgPolicy
				default:
					return fmt.Errorf("invalid deletion propagation policy: %s", msgPolicy)
				}
			}

			opts = append(opts, &client.DeleteOptions{
				PropagationPolicy: &policy,
			})

			if err = k.client.Delete(ctx, &u, opts...); err != nil {
				err = fmt.Errorf("failed to delete object: %v", err)
			}
		case "status":
			k.log.Debugf("updating kubernetes object status: %s", id)
			if err = k.client.Status().Update(ctx, &u); err != nil {
				err = fmt.Errorf("failed to update object status: %v", err)
			}
		default:
			k.log.Errorf("unsupported operator: %s", operator)
			return fmt.Errorf("unsupported operator: %s", operator)
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
