# Autoscaler Service Design

## Scale-up approach

The current design optimizes for correctness and simplicity over throughput. This is because the data is limited to a few clusters, each holding just a handful of files, and one of the main goals in the assignment evaluation is correctness. This matches the assignment's time frame and scope.

At this point there is no throughput problem to solve. However, if this design had to handle production volume, the following changes would be the first ones to make:

1. **Concurrent S3 downloads:**
   One real bottleneck when working at scale is the network-heavy S3 `GetObject` function.
   For scale I'd use a small thread pool to fetch several files at once, adding to each cluster's progress under a lock and checking if all files have been received.

2. **Bound memory per file:**
   Today a log file is decompressed whole. Then gets decoded and split into entries (roughly three times its decompressed size at once). Each record keeps its original JSON until the cluster is finalized. At the GB scale I’d stream S3’s `Body` through `gzip.GzipFile` so I can build entries line by line, and only keep the metrics that the rules reference, as floats.

3. **Cheaper rule evaluation:**
   In the current implementation each evaluation point slices its window, goes through every record in that window and reads the metric values of each record. That is O(records × window × rules). At our scale that’s no big deal, but at production scale it becomes a problem. For scale, I'd precompute one value array for each rule: prefix sums for `avg` and for counting `min_batches`, and monotonic deques for `min` and `max` calculations.

4. **More than one replica:**
   At scale we want to be able to scale horizontally, meaning we will run multiple replicas at the same time. By configuring an SQS queue with `cluster_id` as `MessageGroupId` we can prevent two instances from handling the same cluster's files at the same time. However, this does not pin a cluster to one instance for its whole lifetime, so a different replica can take a message for the same cluster once the first replica has sent an ack. Because of this, keeping cluster state in memory is fragile. I would solve the problem by using a per file record store and adding a per cluster finalize lock.

## Failure & recovery

**Handled:**

- **A malformed or truncated progress record** never crashes the run.
- **A transient S3/SQS/DynamoDB error** is first handled by boto3's own retry configuration.
  If a single message still fails after those attempts, the message stays in the queue.
  It is then redelivered after the queue's visibility timeout and retried.
- **A corrupt file or one that can never be read** is skipped on its first delivery. An S3
  object's bytes don't change between deliveries, so retrying a parse failure could only fail
  the same way again. The same applies to `NoSuchKey`, `NoSuchBucket` or `AccessDenied`.
- **A file that can't be read on any redelivery** is given up on after `MAX_RECEIVE_ATTEMPTS`
  deliveries. This is the same counter that a redrive policy uses (instead of an in-memory
  counter), so it remains after a service restart. On the last attempt the message is removed
  and the file is marked as seen with no records, so the cluster is evaluated on the files that
  could be read instead of waiting forever.
  > [!NOTE]
  > **In production this is the dead letter queue's job:** a redrive policy with
  > `maxReceiveCount` would move the message to a DLQ for inspection and replay, instead of
  > discarding it. The service can't set that up here within the scope of the assignment, so it
  > handles it itself.
- **Duplicate delivery** is handled because the service knows what files it has already seen, and skips reappearing ones.
- **A failed ack** has the same effect as a duplicate delivery, since the file has already been
  processed by the time it gets redelivered.

**Not handled:**

- **Clusters' records are kept in memory**, so the service has state. If the service fails
  mid-processing, all the records processed so far are lost. Messages still in the queue at that
  point can be received again by a restarted process, but messages already deleted for a cluster
  that hadn't finished finalizing cannot be recovered, meaning that cluster's data is lost for
  good. Eventually a cluster missing data like that would be stuck below 100% files received,
  forever ,keeping the service running. I'd fix this by keeping each cluster's processed data 
  in durable storage as it's parsed, so a restart can rebuild cluster state from there.

## Judgment calls

**Log parsing:**

- **A `timestamp` without a UTC offset** is read as UTC.
- **A file that isn't valid gzip** is skipped.

**Rule evaluation:**

- **The window is filtered over the cluster's full record set**, not just the records walked
  so far. This only matters on an exact timestamp tie.
- **Two decisions at the same timestamp.** With `cooldown_minutes: 0`, two records sharing a
  timestamp could both fire, so the second write would overwrite the first.

**Rules file and manifest:**

- **A malformed rules file or manifest fails the run**.
- **A cluster with `num_log_files: 0`** is finalized at once with no decisions.

**Queue and run:**

- **When a cluster is complete:** once it has seen `num_log_files` distinct file keys. This includes skipped files.
- **A message for a cluster not in the manifest, with a malformed body, or whose cluster was already finalized** is deleted.
