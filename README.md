# benthos-kubernetes

a collections of [benthos](https://github.com/Jeffail/benthos) plugins for integrating with Kubernetes.

**Inputs:**

- [kubernetes](./doc/kubernetes_input.md) streams kubernetes objects for one or more configured watches

**Outputs:**

- [kubernetes](./doc/kubernetes_output.md) creates, updates, and deleted kubernetes objects
- [kubernetes_status](./doc/kubernetes_status_output.md) writes object status to kubernetes

**Processors:**

- [kubernetes](./doc/kubernetes_processor.md) performs operations against a kubernetes cluster

## Installing

- with Docker
  ```shell
  $ docker run cludden/benthos-kubernetes -h
  ```
- download a [release](https://github.com/cludden/benthos-kubernetes/releases)
- as a benthos [plugin](./cmd/benthos/main.go)

## Getting Started

See [examples](./example/status.yml)

## License

Licensed under the [MIT License](LICENSE.md)  
Copyright (c) 2020 Chris Ludden
