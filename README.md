# benthos-kubernetes-input
a [benthos](https://github.com/Jeffail/benthos) kubernetes input plugin

## Installing
- with Docker
  ```shell
  $ docker run cludden/benthos-kubernetes-input -h
  ```
- download a [release](https://github.com/cludden/benthos-kubernetes-input/releases)
- as benthos [plugin](./cmd/benthos/main.go)

## Getting Started
Sample benthos stream config:
```yaml
input:
  type: kubernetes
  plugin:
    group: ""
    version: v1
    kind: Pod

output:
  stdout: {}
```
Or see [examples](./example)

## License
Licensed under the [MIT License](LICENSE.md)  
Copyright (c) 2020 Chris Ludden
