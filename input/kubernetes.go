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
	klog "github.com/cludden/benthos-kubernetes/log"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
	"sigs.k8s.io/controller-runtime/pkg/event"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
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

// KubernetesConfig defines runtime configuration for a kubernetes input
type KubernetesConfig struct {
	Watches []Watch `json:"watches,omitempty" yaml:"watches,omitempty"`
}

// Watch defines a controller configuration
type Watch struct {
	Group      string    `json:"group" yaml:"group"`
	Version    string    `json:"version" yaml:"version"`
	Kind       string    `json:"kind" yaml:"kind"`
	Namespaces []string  `json:"namespaces,omitempty" yaml:"namespaces,omitempty"`
	Selector   *selector `json:"selector,omitempty" yaml:"selector,omitempty"`
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
func (w *Watch) GVK() schema.GroupVersionKind {
	gvk := schema.GroupVersionKind{
		Group:   w.Group,
		Version: w.Version,
		Kind:    w.Kind,
	}
	return gvk
}

// Options returns a list of watch predicates using runtime config
func (w *Watch) Options() ([]builder.ForOption, error) {
	var opts []builder.ForOption

	if len(w.Namespaces) > 0 {
		namespaces := map[string]struct{}{}
		for _, ns := range w.Namespaces {
			namespaces[ns] = struct{}{}
		}

		matchesNamespace := func(ns string) bool {
			_, ok := namespaces[ns]
			return ok
		}

		opts = append(opts, builder.WithPredicates(predicate.Funcs{
			CreateFunc: func(e event.CreateEvent) bool {
				return matchesNamespace(e.Meta.GetNamespace())
			},
			DeleteFunc: func(e event.DeleteEvent) bool {
				return matchesNamespace(e.Meta.GetNamespace())
			},
			GenericFunc: func(e event.GenericEvent) bool {
				return matchesNamespace(e.Meta.GetNamespace())
			},
			UpdateFunc: func(e event.UpdateEvent) bool {
				return matchesNamespace(e.MetaNew.GetNamespace())
			},
		}))
	}

	if w.Selector != nil {
		selector := metav1.LabelSelector{
			MatchLabels: w.Selector.MatchLabels,
		}

		for i := 0; i < len(w.Selector.MatchExpressions); i++ {
			expr := w.Selector.MatchExpressions[i]
			selector.MatchExpressions = append(selector.MatchExpressions, metav1.LabelSelectorRequirement{
				Key:      expr.Key,
				Operator: expr.Operator,
				Values:   expr.Values,
			})
		}

		if selector.Size() > 0 {
			selector, err := metav1.LabelSelectorAsSelector(&selector)
			if err != nil {
				return nil, fmt.Errorf("error parsing selector: %v", err)
			}

			opts = append(opts, builder.WithPredicates(predicate.Funcs{
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
			}))
		}
	}

	return opts, nil
}

//------------------------------------------------------------------------------

// Kubernetes input watches one or more k8s resources
type Kubernetes struct {
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
	logf.SetLogger(klog.New(log))
	// define input
	c := &Kubernetes{
		log:   log,
		stats: stats,

		resChan:          make(chan types.Response),
		transactionsChan: make(chan types.Transaction),
		closeChan:        make(chan struct{}),
		closedChan:       make(chan struct{}),
	}

	// initalize controller manager
	cmgr, err := manager.New(config.GetConfigOrDie(), manager.Options{})
	if err != nil {
		log.Errorf("error initializing controller manager: %v", err)
		return nil, err
	}

	// register controllers for each configured watch
	for _, w := range conf.Watches {
		gvk := w.GVK()
		u := unstructured.Unstructured{}
		u.SetGroupVersionKind(gvk)

		opts, err := w.Options()
		if err != nil {
			return nil, fmt.Errorf("error creating predicates for gvk %s: %v", gvk.String(), err)
		}

		err = builder.ControllerManagedBy(cmgr).
			For(&u, opts...).
			Complete(c.Reconciler(gvk))
		if err != nil {
			return nil, fmt.Errorf("could not create controller for gvk %s: %v", gvk.String(), err)
		}

		log.Infof("registered controller for %s", gvk.String())
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

// Reconciler returns a reconciler function scoped to the specified GVK
func (k *Kubernetes) Reconciler(gvk schema.GroupVersionKind) reconcile.Reconciler {
	return reconcile.Func(func(req reconcile.Request) (reconcile.Result, error) {
		resp := reconcile.Result{}
		fields := map[string]string{
			"group":     gvk.Group,
			"kind":      gvk.Kind,
			"namespace": req.Namespace,
			"name":      req.Name,
			"version":   gvk.Version,
		}
		log := k.log.WithFields(fields)

		u := unstructured.Unstructured{}
		u.SetGroupVersionKind(gvk)

		if err := k.mgr.GetCache().Get(context.Background(), req.NamespacedName, &u); err != nil {
			if err := client.IgnoreNotFound(err); err != nil {
				log.Debugf("error fetching object: %v", err)
				return resp, err
			}
			fields["deleted"] = "1"
		}

		b, err := u.MarshalJSON()
		if err != nil {
			log.Errorf("error marshalling object: %v", err)
			return resp, err
		}

		part := message.NewPart(b)
		part.SetMetadata(bmeta.New(fields))
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
	})
}
