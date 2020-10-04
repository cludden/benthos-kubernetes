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
    result:
      requeue: meta().exists("requeue")
      requeue_after: ${!meta("requeue_after).or("")}
```

## Fields

### `result`

Customize the result of a reconciliation request via [synchronous responses](https://www.benthos.dev/docs/guides/sync_responses).

Type: `object`

### `result.requeue`

A [Bloblang query](https://www.benthos.dev/docs/guides/bloblang/about/) that should return a boolean value indicating whether the input resource should be requeued. An empty string disables this functionality.

Type: `string`
Default: `""`

### `result.requeue_after`

Specify a duration after which the input resource should be requeued. This is a string value, which allows you to customize it based on resulting payloads and their metadata using [interpolation functions](https://www.benthos.dev/docs/configuration/interpolation#bloblang-queries). An empty string disables this functionality.

Type: `string`
Default: `""`

### `watches[]`

A list of watch configurations that specify the set of kubernetes objects to target.

Type: `list(object)`
Default: `[]`
Required: `true`

### `watches[].group`

Resource group selector

Type: `string`
Default: `""`

### `watches[].kind`

Resource kind selector

Type: `string`
Default: `""`
Required: `true`

### `watches[].namespaces`

Resource namespace selector. An empty array here indicates cluster scope.

Type: `list(string)`
Default: `[]`

### `watches[].owns[]`

Specifies an optional list of dependencies to watch. This requires the correct owner references to be present on the dependent objects.

Type: `list(object)`
Default: `[]`

### `watches[].owns[].group`

Dependency group selector

Type: `string`
Default: `""`

### `watches[].owns[].kind`

Dependency kind selector

Type: `string`
Default: `""`
Required: `true`

### `watches[].owns[].version`

Dependency version selector

Type: `string`
Default: `""`
Required: `true`

### `watches[].selector`

Optional label selector to apply as target filter.

Type: `object`
Default: `{}`

### `watches[].selector.matchExpressions[]`

List of label match expressions to apply as target filter.

Type: `list(object)`
Default: `{}`

### `watches[].selector.matchExpressions[].key`

Subject of the given expression.

Type: `string`
Default: `""`
Required: `true`

### `watches[].selector.matchExpressions[].operator`

Operator of the given expression (e.g. `Exists`, `In`, `NotIn`)

Type: `string`
Default: `""`
Required: `true`

### `watches[].selector.matchExpressions[].values[]`

List of values applied to operator in order to evaluate the expression.

Type: `string`
Default: `[]`

### `watches[].selector.matchLabels`

Map of key value label pairs to use as target filter.

Type: `map(string)`
Default: `{}`

### `watches[].version`

Resource version selector

Type: `string`
Default: `""`
Required: `true`

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
