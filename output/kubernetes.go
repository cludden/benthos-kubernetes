package output

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output"
	"github.com/Jeffail/benthos/v3/lib/types"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
)

func init() {
	output.RegisterPlugin(
		"kubernetes",
		NewKubernetesConfig,
		func(iconf interface{}, mgr types.Manager, logger log.Modular, stats metrics.Type) (types.Output, error) {
			conf, ok := iconf.(*KubernetesConfig)
			if !ok {
				return nil, errors.New("failed to cast config")
			}
			return NewKubernetes(*conf, mgr, logger, stats)
		},
	)

	output.DocumentPlugin(
		"kubernetes",
		`
This plugin creates, updates, or deletes kubernetes object.`,
		nil,
	)
}

//------------------------------------------------------------------------------

// KubernetesConfig defines runtime configuration for a kubernetes output
type KubernetesConfig struct {
	MaxInFlight int `json:"max_in_flight" yaml:"max_in_flight"`
}

// NewKubernetesConfig returns a new KubernetesConfig value with sensible defaults
func NewKubernetesConfig() interface{} {
	return &KubernetesConfig{
		MaxInFlight: 1,
	}
}

//------------------------------------------------------------------------------

// NewKubernetes creates a new kubernetes plugin output type.
func NewKubernetes(
	conf KubernetesConfig,
	mgr types.Manager,
	log log.Modular,
	stats metrics.Type,
) (output.Type, error) {
	k8sWriter, err := NewKubernetesWriter(conf, mgr, log, stats)
	if err != nil {
		return nil, err
	}

	var w output.Type
	if conf.MaxInFlight == 1 {
		w, err = output.NewWriter(
			"kubernetes", k8sWriter, log, stats,
		)
	} else {
		w, err = output.NewAsyncWriter(
			"kubernetes", conf.MaxInFlight, k8sWriter, log, stats,
		)
	}
	return w, err
}

//------------------------------------------------------------------------------

// Kubernetes output creates, updates, or deletes k8s objects
type Kubernetes struct {
	client client.Client

	log   log.Modular
	stats metrics.Type

	connMutex sync.Mutex
}

// NewKubernetesWriter creates a new kubernetes writer type.
func NewKubernetesWriter(
	conf KubernetesConfig,
	mgr types.Manager,
	log log.Modular,
	stats metrics.Type,
) (*Kubernetes, error) {
	k := &Kubernetes{
		log:   log,
		stats: stats,
	}
	return k, nil
}

//------------------------------------------------------------------------------

// ConnectWithContext establishes a connection to Kubernetes
func (k *Kubernetes) ConnectWithContext(context.Context) error {
	return k.Connect()
}

// Connect establishes a connection to Kubernetes
func (k *Kubernetes) Connect() error {
	k.connMutex.Lock()
	defer k.connMutex.Unlock()

	if k.client != nil {
		return nil
	}

	// initalize controller manager
	c, err := client.New(config.GetConfigOrDie(), client.Options{})
	if err != nil {
		return fmt.Errorf("error initializing controller manager: %v", err)
	}
	k.log.Infoln("Writing objects to kubernetes.")
	k.client = c

	return nil
}

// Write will attempt to write an object to Kubernetes, wait for
// acknowledgement, and returns an error if applicable.
func (k *Kubernetes) Write(msg types.Message) error {
	return k.WriteWithContext(context.Background(), msg)
}

// WriteWithContext will attempt to write an object to Kubernetes, wait for
// acknowledgement, and returns an error if applicable.
func (k *Kubernetes) WriteWithContext(ctx context.Context, msg types.Message) error {
	if k.client == nil {
		return types.ErrNotConnected
	}

	return msg.Iter(func(i int, p types.Part) error {
		var u unstructured.Unstructured
		if err := u.UnmarshalJSON(p.Get()); err != nil {
			return fmt.Errorf("error parsing object: %v", err)
		}

		switch {
		case p.Metadata().Get("deleted") != "":
			if err := k.client.Delete(ctx, &u); err != nil {
				return fmt.Errorf("error deleting object: %v", err)
			}
		case string(u.GetUID()) != "":
			if err := k.client.Update(ctx, &u); err != nil {
				return fmt.Errorf("error updating object: %v", err)
			}
		default:
			if err := k.client.Create(ctx, &u); err != nil {
				return fmt.Errorf("error creating object: %v", err)
			}
		}

		return nil
	})
}

// CloseAsync begins cleaning up resources used by this reader asynchronously.
func (k *Kubernetes) CloseAsync() {
}

// WaitForClose will block until either the reader is closed or a specified
// timeout occurs.
func (k *Kubernetes) WaitForClose(time.Duration) error {
	return nil
}
