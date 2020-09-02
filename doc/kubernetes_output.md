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
        version: v1alpha1
        kind: Foo

pipeline:
  processors:
    - bloblang: |
        root = match {
          meta().exists("deleted") => deleted()
        }

output:
  type: kubernetes
  plugin: {}
  processors:
    - bloblang: |
        map finalizer {
            root = this
            metadata.finalizers = metadata.finalizers.append("finalizer.foos.example.com")
        }
        root = match {
            metadata.finalizers.or([]).contains("finalizer.foos.example.com") => deleted()
            _ => this.apply("finalizer")
        }
```