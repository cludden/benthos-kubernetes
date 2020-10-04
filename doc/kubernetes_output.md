# kubernetes

creates, updates, and deleted kubernetes objects based

This output will perform the following actions for all message parts:

- fail if the payload is not a valid kubernetes object
- delete the object if a `deleted` metadata key is present
- update the object if a `uid` is present
- create the object if no `uid` is present

**Examples**

```yaml
input:
  type: kubernetes
  plugin:
    watches:
      - group: example.com
        version: v1
        kind: Foo

pipeline:
  processors:
    - bloblang: |
        map finalizer {
            root = this
            metadata.finalizers = metadata.finalizers.append("finalizer.foos.example.com")
        }
        root = match {
            meta().exists("deleted") => deleted()
            metadata.finalizers.or([]).contains("finalizer.foos.example.com") => deleted()
            _ => this.apply("finalizer")
        }

output:
  type: kubernetes
  plugin: {}
```

## Fields

### `deletion_propagation`

Specifies the [deletion propagation policy](https://kubernetes.io/docs/concepts/workloads/controllers/garbage-collection/#controlling-how-the-garbage-collector-deletes-dependents) when performing `delete` operations.

Type: `string`
Default: `Background`
Options: `Background`, `Foreground`, `Orphan`

### `max_in_flight`

The maximum number of messages to have in flight at a given time. Increase this to improve throughput.

Type: `number`
Default: `1`
