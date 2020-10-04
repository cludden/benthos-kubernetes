# kubernetes

performs operations against a kubernetes cluster

## Fields

### `deletion_propagation`

Specifies the [deletion propagation policy](https://kubernetes.io/docs/concepts/workloads/controllers/garbage-collection/#controlling-how-the-garbage-collector-deletes-dependents) used with the `delete` operator.

Type: `string`
Default: `Background`
Options: `Background`, `Foreground`, `Orphan`

### `operator`

Specifies the kubernetes client operation to perform.

Type: `string`
Options: `create`, `delete`, `get`, `status`, `update`

### `operator_mapping`

A [Bloblang mapping](https://www.benthos.dev/docs/guides/bloblang/about/) that resolves to valid operator.

Type: `string`

### `parts[]`

An optional array of message indexes of a batch that the processor should apply to. If left empty all messages are processed. This field is only applicable when batching messages at the input level.

Indexes can be negative, and if so the part will be selected from the end counting backwards starting from -1.

Type: `list(number)`
Default: `[]`
