# kubernetes_status

updates a kubernetes object's status subresource

**Examples**
```yaml
input:
  type: kubernetes
  plugin:
    watches:
      - group: example.com
        version: v1alpha1
        kind: Foo

pipeline:
  processors:
    - bloblang: |
        root = match {
          meta().exists("deleted") => deleted()
        }

output:
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
```