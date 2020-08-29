# benthos-kubernetes-input
a [benthos](https://github.com/Jeffail/benthos) kubernetes input plugin that streams resource messages based on one or more configured watches.

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

output:
  stdout: {}
```
Or see [examples](./example)

## Metadata
This input adds the following metadata fields to each message:
```
- deleted (present only if object has been deleted)
- group
- kind
- name
- namespace
- version
```

## License
Licensed under the [MIT License](LICENSE.md)  
Copyright (c) 2020 Chris Ludden
