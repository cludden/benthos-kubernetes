package input

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/lib/input"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	bmeta "github.com/Jeffail/benthos/v3/lib/message/metadata"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

func init() {
	input.RegisterPlugin(
		"kubernetes",
		func() interface{} {
			return NewKubernetesConfig()
		},
		func(iconf interface{}, mgr types.Manager, logger log.Modular, stats metrics.Type) (types.Input, error) {
			conf, ok := iconf.(*KubernetesConfig)
			if !ok {
				return nil, errors.New("failed to cast config")
			}
			return NewKubernetes(*conf, mgr, logger, stats)
		},
	)

	input.DocumentPlugin(
		"kubernetes",
		`
This plugin streams changes to kubernetes objects from a given cluster.`,
		nil, // No need to sanitise the config.
	)
}

//------------------------------------------------------------------------------

var emptyobj = []byte("{}")

// KubernetesConfig defines runtime configuration for a kubernetes input
type KubernetesConfig struct {
	Group    string    `json:"group" yaml:"group"`
	Version  string    `json:"version" yaml:"version"`
	Kind     string    `json:"kind" yaml:"kind"`
	Selector *selector `json:"selector,omitempty" yaml:"selector,omitempty"`
}

type selector struct {
	MatchLabels      map[string]string     `json:"matchLabels,omitempty" yaml:"matchLabels,omitempty"`
	MatchExpressions []selectorRequirement `json:"matchExpressions,omitempty" yaml:"matchExpressions,omitempty"`
}

type selectorRequirement struct {
	Key      string                       `json:"key" yaml:"key"`
	Operator metav1.LabelSelectorOperator `json:"operator" yaml:"operator"`
	Values   []string                     `json:"values" yaml:"values"`
}

// NewKubernetesConfig creates a new KubernetesConfig with default values
func NewKubernetesConfig() *KubernetesConfig {
	return &KubernetesConfig{}
}

// GVK returns a GroupVersionKind value
func (c *KubernetesConfig) GVK() schema.GroupVersionKind {
	gvk := schema.GroupVersionKind{
		Group:   c.Group,
		Version: c.Version,
		Kind:    c.Kind,
	}
	return gvk
}

// Predicates returns a list of watch predicates using runtime config
func (c *KubernetesConfig) Predicates() ([]predicate.Predicate, error) {
	predicates := []predicate.Predicate{}

	if c.Selector != nil {
		selector := metav1.LabelSelector{
			MatchLabels: c.Selector.MatchLabels,
		}

		for i := 0; i < len(c.Selector.MatchExpressions); i++ {
			expr := c.Selector.MatchExpressions[i]
			selector.MatchExpressions = append(selector.MatchExpressions, metav1.LabelSelectorRequirement{
				Key:      expr.Key,
				Operator: expr.Operator,
				Values:   expr.Values,
			})
		}

		if selector.Size() > 0 {
			selector, err := metav1.LabelSelectorAsSelector(&selector)
			if err != nil {
				return predicates, fmt.Errorf("error parsing selector: %v", err)
			}

			predicates = append(predicates, predicate.Funcs{
				CreateFunc: func(e event.CreateEvent) bool {
					return selector.Matches(labels.Set(e.Meta.GetLabels()))
				},
				DeleteFunc: func(e event.DeleteEvent) bool {
					return selector.Matches(labels.Set(e.Meta.GetLabels()))
				},
				GenericFunc: func(e event.GenericEvent) bool {
					return selector.Matches(labels.Set(e.Meta.GetLabels()))
				},
				UpdateFunc: func(e event.UpdateEvent) bool {
					return selector.Matches(labels.Set(e.MetaNew.GetLabels()))
				},
			})

		}
	}

	return predicates, nil
}

//------------------------------------------------------------------------------

// Kubernetes input watches one or more k8s resources
type Kubernetes struct {
	gvk schema.GroupVersionKind
	mgr manager.Manager

	resChan          chan types.Response
	transactionsChan chan types.Transaction

	log   log.Modular
	stats metrics.Type

	closeOnce  sync.Once
	closeChan  chan struct{}
	closedChan chan struct{}
}

// NewKubernetes creates a new kubernetes input type
func NewKubernetes(
	conf KubernetesConfig,
	mgr types.Manager,
	log log.Modular,
	stats metrics.Type,
) (input.Type, error) {
	gvk := conf.GVK()

	// define input
	c := &Kubernetes{
		gvk: gvk,

		log: log.WithFields(map[string]string{
			"gvk": gvk.String(),
		}),

		stats: stats,

		resChan:          make(chan types.Response),
		transactionsChan: make(chan types.Transaction),
		closeChan:        make(chan struct{}),
		closedChan:       make(chan struct{}),
	}

	// initalize manager
	cmgr, err := manager.New(config.GetConfigOrDie(), manager.Options{})
	if err != nil {
		log.Errorf("error initializing controller manager: %v", err)
		return nil, err
	}

	// initialize controller
	ctlr, err := controller.New("component-controller", cmgr, controller.Options{
		Reconciler: c,
	})

	if err != nil {
		log.Errorf("unable to set up individual controller: %v", err)
		return nil, err

	}

	log.Debugln("initializing watch")
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(c.gvk)

	predicates, err := conf.Predicates()
	if err != nil {
		log.Errorf("error computing watch predicates: %v", err)
		return nil, err
	}

	if err := ctlr.Watch(&source.Kind{Type: u}, &handler.EnqueueRequestForObject{}, predicates...); err != nil {
		log.Errorf("error initializing watch: %v", err)
		return nil, err
	}

	c.mgr = cmgr

	go c.loop()
	return c, nil
}

// Connected returns true if this input is currently connected to its target.
func (k *Kubernetes) Connected() bool {
	return true
}

// TransactionChan returns a transactions channel for consuming messages from
// this input type.
func (k *Kubernetes) TransactionChan() <-chan types.Transaction {
	return k.transactionsChan
}

// CloseAsync shuts down the input and stops processing requests.
func (k *Kubernetes) CloseAsync() {
	k.closeOnce.Do(func() {
		close(k.closeChan)
	})
}

// WaitForClose blocks until the input has closed down.
func (k *Kubernetes) WaitForClose(timeout time.Duration) error {
	select {
	case <-k.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------

func (k *Kubernetes) loop() {
	defer func() {
		close(k.transactionsChan)
		close(k.closedChan)
	}()

	if err := k.mgr.Start(k.closeChan); err != nil {
		k.log.Errorf("error running manager: %v", err)
	}
}

//------------------------------------------------------------------------------

// Reconcile implements the required controller interface
func (k *Kubernetes) Reconcile(req reconcile.Request) (reconcile.Result, error) {
	resp := reconcile.Result{}
	log := k.log.WithFields(map[string]string{
		"namespace": req.Namespace,
		"name":      req.Name,
	})

	u := unstructured.Unstructured{}
	u.SetGroupVersionKind(k.gvk)
	if err := k.mgr.GetCache().Get(context.Background(), req.NamespacedName, &u); err != nil {
		log.Infoln("error fetching object ")
		return resp, client.IgnoreNotFound(err)
	}

	b, err := u.MarshalJSON()
	if err != nil {
		log.Errorf("error marshalling object: %v", err)
		return resp, err
	}

	part := message.NewPart(b)
	part.SetMetadata(bmeta.New(map[string]string{}))
	msg := message.New(nil)
	msg.Append(part)

	// send batch to downstream processors
	select {
	case k.transactionsChan <- types.NewTransaction(msg, k.resChan):
	case <-k.closeChan:
		k.log.Infoln("input closing...")
		return resp, nil
	}

	// check transaction success
	select {
	case result := <-k.resChan:
		// check for requeue after metadata attribute
		requeueAfter := msg.Get(0).Metadata().Get("requeue_after")
		if requeueAfter != "" {
			requeueAfterDur, err := time.ParseDuration(requeueAfter)
			if err != nil {
				log.Warnf("invalid requeue_after duration: %s", requeueAfter)
			} else {
				log.Debugf("requeueing object after %s", requeueAfter)
				resp.RequeueAfter = requeueAfterDur
			}
		}

		// handle error
		if err := result.Error(); err != nil {
			log.Errorln(err.Error())
			return resp, err
		}
	case <-k.closeChan:
	}

	return resp, nil
}
