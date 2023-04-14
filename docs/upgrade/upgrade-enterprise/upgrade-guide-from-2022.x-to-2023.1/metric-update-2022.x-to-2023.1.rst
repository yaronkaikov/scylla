ScyllaDB Enterprise Metric Update - ScyllaDB Enterprise 2022.x to 2023.1
============================================================================

.. toctree::
   :maxdepth: 2
   :hidden:

ScyllaDB Enterprise 2023.1 Dashboards are available as part of the latest |mon_root|.

Renamed Metrics
~~~~~~~~~~~~~~~~

The following metrics are renamed in ScyllaDB Enterprise 2023.1:

.. list-table::
   :widths: 25 150
   :header-rows: 1

   * - 2022.x
     - 2023.1
   * - scylla_memory_regular_virtual_dirty_bytes
     - scylla_memory_regular_unspooled_dirty_bytes
   * - scylla_memory_system_virtual_dirty_bytes	
     - scylla_memory_system_unspooled_dirty_bytes
   * - scylla_memory_virtual_dirty_bytes
     - scylla_memory_unspooled_dirty_bytes



New Metrics
~~~~~~~~~~~~

The following metrics are new in ScyllaDB Enterprise 2023.1 compared to **2022.2**:

.. list-table::
   :widths: 25 150
   :header-rows: 1

   * - Metric
     - Description
   * - scylla_database_disk_reads
     - Holds the number of currently active disk read operations.
   * - scylla_database_reads_memory_consumption
     - Holds the amount of memory consumed by current read operations issued on behalf of streaming
   * - scylla_database_sstables_read
     - Holds the number of currently read sstables.
   * - scylla_memory_malloc_failed
     - Total count of failed memory allocations
   * - scylla_raft_add_entries
     - How many entries were added on this node
   * - scylla_raft_applied_entries
     - How many log entries were applied
   * - scylla_raft_group0_status
     - Status of the raft group: 0 - disabled, 1 - normal, 2 - aborted
   * - scylla_raft_in_memory_log_size
     - Size of in-memory part of the log
   * - scylla_raft_log_memory_usage	
     - Memory usage of in-memory part of the log in bytes
   * - scylla_raft_messages_received
     - How many messages were received
   * - scylla_raft_messages_sent
     - How many messages were send
   * - scylla_raft_persisted_log_entries
     - How many log entries were persisted
   * - scylla_raft_polls
     - How many time raft state machine was polled
   * - scylla_raft_queue_entries_for_apply
     - How many log entries were queued to be applied
   * - scylla_raft_sm_load_snapshot
     - How many times user state machine was reloaded with a snapshot
   * - scylla_raft_snapshots_taken
     - How many time the user's state machine was snapshotted
   * - scylla_raft_store_snapshot
     - How many snapshot were persisted
   * - scylla_raft_store_term_and_vote
     - How many times term and vote were persisted
   * - scylla_raft_truncate_persisted_log
     - How many times log was truncated on storage
   * - scylla_raft_waiter_awoken
     - How many waiters got result back
   * - scylla_raft_waiter_dropped	
     - How many waiters did not get result back
   * - scylla_storage_proxy_coordinator_read_latency_summary	
     - Read latency summary
   * - scylla_storage_proxy_coordinator_write_latency_summary	
     - Write latency summary
   * - scylla_streaming_finished_percentage	
     - Finished percentage of node operation on this shard
   * - scylla_view_update_generator_sstables_pending_work
     - Number of bytes remaining to be processed from SSTables for view updates


The following metrics are new in ScyllaDB Enterprise 2023.1 compared to **2022.1**:

.. list-table::
   :widths: 25 150
   :header-rows: 1

   * - Metric
     - Description
   * - scylla_alien_receive_batch_queue_length
     - Current receive batch queue length
   * - scylla_cache_rows_compacted_with_tombstones	
     - Number of rows scanned during write of a tombstone for the purpose of compaction in cache
   * - scylla_cache_rows_dropped_by_tombstones
     - Number of rows dropped in cache by a tombstone write
   * - scylla_commitlog_active_allocations
     - Current number of active allocations.
   * - scylla_commitlog_blocked_on_new_segment
     - Number of allocations blocked on acquiring new segment.
   * - scylla_commitlog_bytes_flush_requested
     - Counts number of bytes requested to be flushed (persisted).
   * - scylla_commitlog_bytes_released
     - Counts number of bytes released from disk. (Deleted/recycled)
   * - scylla_compaction_manager_completed_compactions
     - Holds the number of completed compaction tasks.
   * - scylla_compaction_manager_failed_compactions
     - Holds the number of failed compaction tasks.
   * - scylla_compaction_manager_normalized_backlog
     - Holds the sum of normalized compaction backlog for all tables in the system. Backlog is normalized by dividing backlog by shard's available memory.
   * - scylla_compaction_manager_postponed_compactions	
     - Holds the number of tables with postponed compaction.
   * - scylla_compaction_manager_validation_errors	
     - Holds the number of encountered validation errors.
   * - scylla_cql_select_parallelized
     - Counts the number of parallelized aggregation SELECT query executions.
   * - scylla_database_disk_reads
     - Holds the number of currently active disk read operations.
   * - scylla_database_reads_memory_consumption
     - Holds the amount of memory consumed by current read operations issued on behalf of streaming
   * - scylla_database_sstables_read
     - Holds the number of currently read sstables.
   * - scylla_database_total_reads_rate_limited	
     - Counts read operations which were rejected on the replica side because the per-partition limit was reached.
   * - scylla_database_total_writes_rate_limited	
     - Counts write operations which were rejected on the replica side because the per-partition limit was reached.
   * - scylla_forward_service_requests_dispatched_to_other_nodes	
     - How many forward requests were dispatched to other nodes
   * - scylla_forward_service_requests_dispatched_to_own_shards	
     - How many forward requests were dispatched to local shards
   * - scylla_forward_service_requests_executed
     - How many forward requests were executed
   * - scylla_gossip_live
     - How many live nodes the current node sees
   * - scylla_gossip_unreachable
     - How many unreachable nodes the current node sees
   * - scylla_io_queue_adjusted_consumption
     - Consumed disk capacity units adjusted for class shares and idling preemption
   * - scylla_io_queue_consumption
     - Accumulated disk capacity units consumed by this class; an increment per-second rate indicates full utilization
   * - scylla_io_queue_total_split_bytes	
     - Total number of bytes split
   * - scylla_io_queue_total_split_ops	
     - Total number of requests split
   * - scylla_memory_malloc_failed
     - Total count of failed memory allocations
   * - scylla_per_partition_rate_limiter_allocations
     - Number of times a entry was allocated over an empty/expired entry.
   * - scylla_per_partition_rate_limiter_failed_allocations	
     - Number of times the rate limiter gave up trying to allocate.
   * - scylla_per_partition_rate_limiter_load_factor	
     - Current load factor of the hash table (upper bound, may be overestimated).
   * - scylla_per_partition_rate_limiter_probe_count	
     - Number of probes made during lookups.
   * - scylla_per_partition_rate_limiter_successful_lookups
     - Number of times a lookup returned an already allocated entry.
   * - scylla_raft_add_entries
     - How many entries were added on this node
   * - scylla_raft_applied_entries
     - How many log entries were applied
   * - scylla_raft_group0_status
     - Status of the raft group: 0 - disabled, 1 - normal, 2 - aborted
   * - scylla_raft_in_memory_log_size
     - Size of in-memory part of the log
   * - scylla_raft_log_memory_usage	
     - Memory usage of in-memory part of the log in bytes
   * - scylla_raft_messages_received
     - How many messages were received
   * - scylla_raft_messages_sent
     - How many messages were send
   * - scylla_raft_persisted_log_entries
     - How many log entries were persisted
   * - scylla_raft_polls
     - How many time raft state machine was polled
   * - scylla_raft_queue_entries_for_apply
     - How many log entries were queued to be applied
   * - scylla_raft_sm_load_snapshot
     - How many times user state machine was reloaded with a snapshot
   * - scylla_raft_snapshots_taken
     - How many time the user's state machine was snapshotted
   * - scylla_raft_store_snapshot
     - How many snapshot were persisted
   * - scylla_raft_store_term_and_vote
     - How many times term and vote were persisted
   * - scylla_raft_truncate_persisted_log
     - How many times log was truncated on storage
   * - scylla_raft_waiter_awoken
     - How many waiters got result back
   * - scylla_raft_waiter_dropped	
     - How many waiters did not get result back
   * - scylla_reactor_aio_outsizes	
     - Total number of aio operations that exceed IO limit
   * - scylla_schema_commitlog_active_allocations	
     - Current number of active allocations.
   * - scylla_schema_commitlog_allocating_segments
     - Holds the number of not closed segments that still have some free space. This value should not get too high.
   * - scylla_schema_commitlog_alloc	
     - Counts number of times a new mutation has been added to a segment. Divide bytes_written by this value to get the average number of bytes per mutation written to the disk.
   * - scylla_schema_commitlog_blocked_on_new_segment	
     - Number of allocations blocked on acquiring new segment.
   * - scylla_schema_commitlog_bytes_flush_requested
     - Counts number of bytes requested to be flushed (persisted).
   * - scylla_schema_commitlog_bytes_released	
     - Counts number of bytes released from disk. (Deleted/recycled)
   * - scylla_schema_commitlog_bytes_written	
     - Counts number of bytes written to the disk. Divide this value by "alloc" to get the average number of bytes per mutation written to the disk.
   * - scylla_schema_commitlog_cycle	
     - Counts number of commitlog write cycles - when the data is written from the internal memory buffer to the disk.
   * - scylla_schema_commitlog_disk_active_bytes	
     - Holds size of disk space in bytes used for data so far. A too high value indicates that we have some bottleneck in the writing to sstables path.
   * - scylla_schema_commitlog_disk_slack_end_bytes
     - Holds size of disk space in bytes unused because of segment switching (end slack). A too high value indicates that we do not write enough data to each segment.
   * - scylla_schema_commitlog_disk_total_bytes	
     - Holds size of disk space in bytes reserved for data so far. A too high value indicates that we have some bottleneck in the writing to sstables path.
   * - scylla_schema_commitlog_flush	
     - Counts number of times the flush() method was called for a file.
   * - scylla_schema_commitlog_flush_limit_exceeded	
     - Counts number of times a flush limit was exceeded. A non-zero value indicates that there are too many pending flush operations (see pending_flushes) and some of them will be blocked till the total amount of pending flush operations drops below 5.
   * - scylla_schema_commitlog_memory_buffer_bytes
     - Holds the total number of bytes in internal memory buffers.
   * - scylla_schema_commitlog_pending_allocations	
     - Holds number of currently pending allocations. A non-zero value indicates that we have a bottleneck in the disk write flow.
   * - scylla_schema_commitlog_pending_flushes
     - Holds number of currently pending flushes. See the related flush_limit_exceeded metric.
   * - scylla_schema_commitlog_requests_blocked_memory
     - Counts number of requests blocked due to memory pressure. A non-zero value indicates that the commitlog memory quota is not enough to serve the required amount of requests.
   * - scylla_schema_commitlog_segments
     - Holds the current number of segments.
   * - scylla_schema_commitlog_slack
     - Counts number of unused bytes written to the disk due to disk segment alignment.
   * - scylla_schema_commitlog_unused_segments
     - Holds the current number of unused segments. A non-zero value indicates that the disk write path became temporary slow.
   * - scylla_sstables_pi_auto_scale_events
     - Number of promoted index auto-scaling events
   * - scylla_storage_proxy_coordinator_read_latency_summary	
     - Read latency summary
   * - scylla_storage_proxy_coordinator_write_latency_summary	
     - Write latency summary
   * - scylla_streaming_finished_percentage	
     - Finished percentage of node operation on this shard
   * - scylla_view_update_generator_sstables_pending_work
     - Number of bytes remaining to be processed from SSTables for view updates


Reporting Latencies
~~~~~~~~~~~~~~~~~~~~

ScyllaDB Enterprise 2023.1 comes with a new approach to reporting latencies, which are reported using histograms and 
summaries:

* Histograms are reported per node.
* Summaries are reported per shard and contain P50, P95, and P99 latency.

For more information on Prometheus histograms and summaries, see the `Prometheus documentation <https://prometheus.io/docs/practices/histograms/>`_.