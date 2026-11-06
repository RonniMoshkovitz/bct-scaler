# Autoscaler Service Design

## Scale-up approach

Currently, the design optimizes for correctness and simplicity over throughput, as the data is
limited to a handful of clusters with a few files each. This fits the assignment's time frame
and scope.

At this point there's no throughput problem to solve. But if this had to handle real production
volume, these are the things I'd change first:

1. **Concurrent S3 downloads:**
   One actual bottleneck at scale is the network-bound S3 `GetObject`, so a small thread pool fetching several
   files at once (while still appending to each cluster's progress under a lock, and checking "all files arrived"
   atomically) is the first lever.

2. **Bound memory per file:**
   Today a file is decompressed whole, decoded and split into entries (roughly 3× its decompressed size at
   once), along with every record keeping its full JSON dict until its cluster is finalized. At GB scale I'd
   stream S3's `Body` through `gzip.GzipFile`, assemble entries line by line, and keep only the metrics the
   rules reference, as floats.

3. **Cheaper rule evaluation:**
   Each evaluation point slices its window and re-reads the metric from each record: O(records × window × rules).
   That's nothing at our scale, but it isn't at production scale. The fix is one precomputed value array per rule,
   prefix sums for `avg` and the `min_batches` count, and monotonic deques for `min`/`max`.

4. **More than one consumer:**
   At large scale we'd eventually need more than one replica of the service. To make sure the processing
   works properly, I'd make sure all files that belong to the same cluster end up delivered to the same instance.
   For this we can use delivery partitioning by cluster. Using an SQS FIFO queue with `MessageGroupId = cluster_id`
   would be the solution.
   Another solution would be the durable per-file record store described under "Failure & recovery", with a per-cluster
   finalize lock.

## Failure & recovery

**Handled:**

- **A malformed or truncated progress record** never crashes the run.
- **A transient S3/SQS/DynamoDB error** is absorbed first by boto3's own retry config
  (`max_attempts=5`, standard mode). If a single message's processing still fails beyond that,
  the message is simply not deleted, so it's redelivered after the queue's visibility
  timeout and retried.
- **A corrupt file or one that can never be read** is skipped on its first delivery. An S3
  object's bytes don't change between deliveries, so retrying a parse failure could only fail
  the same way again. The same applies to `NoSuchKey`, `NoSuchBucket` or `AccessDenied`.
- **A file that can't be read on any redelivery** is given up on after
  `MAX_RECEIVE_ATTEMPTS` deliveries (default 5). The count is SQS's own
  `ApproximateReceiveCount` — the same counter a redrive policy uses — rather than an in-memory
  counter, so it survives a service restart. On the last attempt the message is deleted and the
  file is counted toward its cluster's completion with no records, so the cluster is evaluated
  on the files that could be read instead of waiting forever.
  **In production this is the dead-letter queue's job:** a redrive policy with
  `maxReceiveCount` would move the message to a DLQ for inspection and replay, instead of
  dropping it. The service can't set that up here within the scope of the assignment, so it
  does this itself.
- **Duplicate delivery** is handled because `ClusterProgress` knows whether a file was already seen, and skips it if so.
- **A failed ack** has the same effect as a duplicate delivery, since the file has already been
  processed by the time it gets redelivered.

**Not handled:**

 - All the clusters' records are kept **in memory**, so the service has **state**. If the service crashes mid-processing,
   all the `ClusterProgress` data is lost. Messages still in the queue at that point can be re-received by a restarted process,
   but messages already **deleted** for a cluster that hadn't finished finalizing cannot be recovered by restarting, since SQS
   has no record of them anymore. Eventually that cluster would be stuck below 100% files-received forever.
   For a production-grade version, I'd fix this by keeping the clusters' data in a durable scratch store as it's parsed, so a
   restart can rebuild cluster state from there instead of from memory.

## Judgment calls the spec doesn't settle

**Log parsing:**

- **A `timestamp` without a UTC offset** is read as UTC.
- **A file that isn't valid gzip** is skipped.

**Rule evaluation:**

- **The window is filtered over the cluster's full record set**, not just the records walked
  so far. This only matters on an exact timestamp tie.
- **Two decisions at the same timestamp.** With `cooldown_minutes: 0`, two records sharing a
  timestamp could both fire, but the table's key is `(cluster_id, decision_ts)`, so the second
  write would overwrite the first.

**Rules file and manifest:**

- **A malformed rules file or manifest fails the run**.
- **A cluster with `num_log_files: 0`** is finalized at once with no decisions.

**Queue and run:**

- **When a cluster is complete:** once it has seen `num_log_files` distinct file keys. This includes skipped files.
- **A message for a cluster not in the manifest, with a malformed body, or whose cluster was already finalized** is deleted.
