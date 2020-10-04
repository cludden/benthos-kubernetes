# benthos-kubernetes

a collection of [benthos](https://github.com/Jeffail/benthos) plugins for integrating with Kubernetes.

#### Inputs

- [kubernetes](./doc/kubernetes_input.md) streams kubernetes objects for one or more configured watches

#### Outputs

- [kubernetes](./doc/kubernetes_output.md) creates, updates, and deleted kubernetes objects
- [kubernetes_status](./doc/kubernetes_status_output.md) writes object status to kubernetes

#### Processors

- [kubernetes](./doc/kubernetes_processor.md) performs operations against a kubernetes cluster

## Installing

- with Docker
  ```shell
  $ docker run cludden/benthos-kubernetes \
    --list-input-plugins \
    --list-output-plugins \
    --list-processor-plugins
  ```
- download a [release](https://github.com/cludden/benthos-kubernetes/releases)
- as a benthos [plugin](./cmd/benthos/main.go)

  ```go
  package main

  import (
    "github.com/Jeffail/benthos/v3/lib/service"
    _ "github.com/cludden/benthos-kubernetes/input"
    _ "github.com/cludden/benthos-kubernetes/output"
    _ "github.com/cludden/benthos-kubernetes/processor"
  )

  func main() {
    service.Run()
  }
  ```

## Getting Started

```yaml
input:
  type: kubernetes
  plugin:
    watches:
      - group: example.com
        version: v1
        kind: Foo
        owns:
          - group: example.com
            version: v1
            kind: Bar
    result:
      requeue: meta().exists("requeue")
      requeue_after: ${!meta("requeue_after").catch("")}

pipeline:
  processors:
    - switch:
        # ignore deleted items
        - check: meta().exists("deleted")
          processors:
            - bloblang: root = deleted()

        # reconcile dependent resources
        - processors:
            - branch:
                processors:
                  - bloblang: |
                      apiVersion = "example.com/v1"
                      kind = "Bar"
                      metadata.labels = metadata.labels
                      metadata.name = metadata.name
                      metadata.namespace = metadata.namespace
                      metadata.ownerReferences = [{
                        "apiVersion": apiVersion,
                        "kind": kind,
                        "controller": true,
                        "blockOwnerDeletion": true,
                        "name": metadata.name,
                        "uid": metadata.uid
                      }]
                      spec = spec
                  - type: kubernetes
                    plugin:
                      operator: get
                  - type: kubernetes
                    plugin:
                      operator: ${! if errored() { "create" } else { "update" } }
                result_map: |
                  root.status.bar = metadata.uid
                  root.status.status = "Ready"

output:
  type: kubernetes_status
  plugin: {}
```

Additional examples provided [here](./example)

## License

Licensed under the [MIT License](LICENSE.md)  
Copyright (c) 2020 Chris Ludden
