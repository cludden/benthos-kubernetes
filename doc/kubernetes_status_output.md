# kubernetes_status

updates a kubernetes object's status subresource

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
        map status {
          root = this
          status.observedGeneration = metadata.generation
          status.lastReconciledAt = timestamp_utc("2006-01-02T15:04:05.000000000Z")
          status.status = match {
            metadata.exists("deletionTimestamp") => "Destroying"
            _ => "Reconciling"
          }
        }
        root = match {
          meta().exists("deleted") => deleted()
          _ => this.apply("status")
        }

output:
  type: kubernetes_status
  plugin: {}
```

## Fields

### `max_in_flight`

The maximum number of messages to have in flight at a given time. Increase this to improve throughput.

Type: `number`
Default: `1`
