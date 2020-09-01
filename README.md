# benthos-kubernetes

a kubernetes plugin [benthos](https://github.com/Jeffail/benthos) which includes the following components:

**Inputs:**

- `kubernetes` streams kubernetes objects for one or more configured watches

**Outputs:**

- `kubernetes` creates, updates, and deleted kubernetes objects
- `kubernetes_status` writes object status to kubernetes

## Installing

- with Docker
  ```shell
  $ docker run cludden/benthos-kubernetes -h
  ```
- download a [release](https://github.com/cludden/benthos-kubernetes/releases)
- as a benthos [plugin](./cmd/benthos/main.go)

## Getting Started

Sample benthos stream config:

```yaml
input:
  type: kubernetes
  plugin:
    watches:
      # watch pods in all namespaces
      - group: ""
        version: v1
        kind: Pod
      # watch custom resources in specified namespaces
      - group: example.com
        version: v1alpha1
        kind: Foo
        namespaces: [default, kube-system]
      # watch custom resources that match a label selector
      - group: example.com
        version: v1apha1
        kind: Bar
        selector:
          matchLabels:
            color: blue
          matchExpressions:
            - key: color
              operator: NotIn
              values: [green, yellow]
      # watch replica sets and reconcile when their pods are modified
      - version: v1
        kind: ReplicaSet
        owns:
          - version: v1
            kind: Pod

pipeline:
  processors:
    - bloblang: |
        root = match {
          meta().exists("deleted") => deleted()
        }

output:
  switch:
    outputs:
      - condition:
          bloblang: metadata.finalizers.or([]).contains("finalizer.foos.example.com") != true
        output:
          type: kubernetes
          plugin: {}
          processors:
            - bloblang: |
                meta requeue_after = "1ms"
                map finalizer {
                  root = this
                  metadata.finalizers = metadata.finalizers.append("finalizer.foos.example.com")
                }
                root = match {
                  metadata.finalizers.or([]).contains("finalizer.foos.example.com") => deleted()
                  _ => this.apply("finalizer")
                }
            - sync_response: {}
            - log:
                message: adding finalizer...

      - output:
          type: kubernetes_status
          plugin: {}
          processors:
            - bloblang: |
                root = this
                status.observedGeneration = metadata.generation
                status.lastReconciledAt = timestamp_utc("2006-01-02T15:04:05.000000000Z")
                status.status = if metadata.exists("deletionTimestamp") {
                  "Destroying"
                } else {
                  "Reconciling"
                }
            - log:
                message: updating status...
```

Or see [examples](./example)

### Metadata

This input adds the following metadata fields to each message:

```
- deleted (present only if object has been deleted)
- group
- kind
- name
- namespace
- version
```

Additionally, this input will check for a `requeue_after` metadata entry on the [synchronous response](https://www.benthos.dev/docs/guides/sync_responses), and if found, will requeue the object for reconciliation.

## License

Licensed under the [MIT License](LICENSE.md)  
Copyright (c) 2020 Chris Ludden
