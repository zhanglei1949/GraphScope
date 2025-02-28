# Stream Replication

Interactive support stream replication, with one primary node and multiple secondary node. The primary node handles all write queries, and read queries are dispatched to all nodes.

In `StreamReplication`, it is important to let all nodes use a consistent metadata storage. In our design, we let primary node handles all writes to metadata store, while standby node could only read the metadata from metadata service.

```yaml
replication_config:
  type: stream_replication # or stream_replication, by default standalone
  primary_node: localhost:7777 # The primary node address for stream_replication
  secondary_nodes: # The secondary node addresses for stream_replication
    - localhost:7778
    - localhost:7779
```

In the secondary nodes, it will entry read-only mode, it means that it will reject any write request. 