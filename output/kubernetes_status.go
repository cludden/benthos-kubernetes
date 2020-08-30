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
		"kubernetes_status",
		NewKubernetesStatusConfig,
		func(iconf interface{}, mgr types.Manager, logger log.Modular, stats metrics.Type) (types.Output, error) {
			conf, ok := iconf.(*KubernetesStatusConfig)
			if !ok {
				return nil, errors.New("failed to cast config")
			}
			return NewKubernetesStatus(*conf, mgr, logger, stats)
		},
	)

	output.DocumentPlugin(
		"kubernetes_status",
		`
This plugin updates kubernetes objects' status.`,
		nil,
	)
}

//------------------------------------------------------------------------------

// KubernetesStatusConfig defines runtime configuration for a kubernetes output
type KubernetesStatusConfig struct {
	MaxInFlight int `json:"max_in_flight" yaml:"max_in_flight"`
}

// NewKubernetesStatusConfig returns a new KubernetesStatusConfig value with sensible defaults
func NewKubernetesStatusConfig() interface{} {
	return &KubernetesStatusConfig{
		MaxInFlight: 1,
	}
}

//------------------------------------------------------------------------------

// NewKubernetesStatus creates a new kubernetes plugin output type.
func NewKubernetesStatus(
	conf KubernetesStatusConfig,
	mgr types.Manager,
	log log.Modular,
	stats metrics.Type,
) (output.Type, error) {
	statusWriter, err := NewKubernetesStatusWriter(conf, mgr, log, stats)
	if err != nil {
		return nil, err
	}

	var w output.Type
	if conf.MaxInFlight == 1 {
		w, err = output.NewWriter(
			"kubernetes_status", statusWriter, log, stats,
		)
	} else {
		w, err = output.NewAsyncWriter(
			"kubernetes_status", conf.MaxInFlight, statusWriter, log, stats,
		)
	}
	return w, err
}

//------------------------------------------------------------------------------

// KubernetesStatus output creates, updates, or deletes k8s objects
type KubernetesStatus struct {
	client client.Client

	log   log.Modular
	stats metrics.Type

	connMutex sync.Mutex
}

// NewKubernetesStatusWriter creates a new kubernetes writer type.
func NewKubernetesStatusWriter(
	conf KubernetesStatusConfig,
	mgr types.Manager,
	log log.Modular,
	stats metrics.Type,
) (*KubernetesStatus, error) {
	k := &KubernetesStatus{
		log:   log,
		stats: stats,
	}
	return k, nil
}

//------------------------------------------------------------------------------

// ConnectWithContext establishes a connection to Kubernetes
func (k *KubernetesStatus) ConnectWithContext(context.Context) error {
	return k.Connect()
}

// Connect establishes a connection to Kubernetes
func (k *KubernetesStatus) Connect() error {
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
	k.log.Infoln("Writing object status to kubernetes.")
	k.client = c

	return nil
}

// Write will attempt to write an object to Kubernetes, wait for
// acknowledgement, and returns an error if applicable.
func (k *KubernetesStatus) Write(msg types.Message) error {
	return k.WriteWithContext(context.Background(), msg)
}

// WriteWithContext will attempt to write an object to Kubernetes, wait for
// acknowledgement, and returns an error if applicable.
func (k *KubernetesStatus) WriteWithContext(ctx context.Context, msg types.Message) error {
	if k.client == nil {
		return types.ErrNotConnected
	}

	return msg.Iter(func(i int, p types.Part) error {
		var u unstructured.Unstructured
		if err := u.UnmarshalJSON(p.Get()); err != nil {
			return fmt.Errorf("error parsing object: %v", err)
		}

		if err := k.client.Status().Update(ctx, &u); err != nil {
			return fmt.Errorf("error updating object status: %v", err)
		}
		return nil
	})
}

// CloseAsync begins cleaning up resources used by this reader asynchronously.
func (k *KubernetesStatus) CloseAsync() {
}

// WaitForClose will block until either the reader is closed or a specified
// timeout occurs.
func (k *KubernetesStatus) WaitForClose(time.Duration) error {
	return nil
}
