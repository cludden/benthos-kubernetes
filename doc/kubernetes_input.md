# kubernetes

streams kubernetes objects for one or more configured watches

**Examples**
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
```

## Fields

`group`

resource group

Type: `string`
Default: `""`

`kind`

resource kind

Type: `string`
Default: `""`

`version`

resource version

Type: `string`
Default: `""`

`namespaces`

optional namespace filter

Type: `[]string`

`owns`

optional list of dependencies to watch

Type: `[]object({group: string, version: string, kind: string })`

`selector`

optional label selector

Type: `object`

`selector.matchLabels`

optional label selector match requirements

Type: `object`

`selector.matchExpressions`

optional label selector match expressions

Type: `object({key: string, operator: string, values: []string})`


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

## Synchronous Responses

Additionally, this input will check for a `requeue_after` metadata entry on [synchronous response](https://www.benthos.dev/docs/guides/sync_responses) messages, and if found, will requeue the object for reconciliation.