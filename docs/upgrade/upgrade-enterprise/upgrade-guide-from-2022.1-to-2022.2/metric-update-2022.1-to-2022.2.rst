ScyllaDB Enterprise Metric Update - ScyllaDB Enterprise 2022.1 to 2022.2
============================================================================

.. toctree::
   :maxdepth: 2
   :hidden:

ScyllaDB Enterprise 2022.2 Dashboards are available as part of the latest |mon_root|.

New Metrics
---------------

The following metrics are new in ScyllaDB Enterprise 2022.2 compared to 2022.1:

.. list-table::
   :widths: 25 150
   :header-rows: 1

   * - Metric
     - Description
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
   * - scylla_commitlog_disk_slack_end_bytes
     - Holds size of disk space in bytes unused because of segment switching (end slack). A too high value indicates that we do not write enough data to each segment.
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
   * - scylla_cql_authorized_prepared_statements_unprivileged_entries_evictions_on_size
     - Counts a number of evictions of prepared statements from the authorized prepared statements cache after they have been used only once. An increasing counter suggests the user may be preparing a different statement for each request instead of reusing the same prepared statement with parameters.
   * - scylla_cql_select_parallelized
     - Counts the number of parallelized aggregation SELECT query executions.
   * - scylla_cql_unprivileged_entries_evictions_on_size
     - Counts a number of evictions of prepared statements from the prepared statements cache after they have been used only once. An increasing counter suggests the user may be preparing a different statement for each request instead of reusing the same prepared statement with parameters.
   * - scylla_database_reads_shed_due_to_overload
     - The number of reads shed because the admission queue reached its max capacity. When the queue is full, excessive reads are shed to avoid overload.
   * - scylla_database_sstable_read_queue_overloads
     - Counts the number of times the sstable read queue was overloaded. A non-zero value indicates that we have to drop read requests because they arrive faster than we can serve them.
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
   * - scylla_io_queue_disk_queue_length
     - Number of requests in the disk
   * - scylla_io_queue_starvation_time_sec
     - Total time spent starving for disk
   * - scylla_io_queue_total_delay_sec
     - Total time spent in the queue
   * - scylla_io_queue_total_exec_sec
     - Total time spent in disk
   * - scylla_io_queue_total_read_bytes
     - Total read bytes passed in the queue
   * - scylla_io_queue_total_read_ops
     - Total read operations passed in the queue
   * - scylla_io_queue_total_split_bytes	
     - Total number of bytes split
   * - scylla_io_queue_total_split_ops	
     - Total number of requests split
   * - scylla_io_queue_total_write_bytes
     - Total write bytes passed in the queue
   * - scylla_io_queue_total_write_ops
     - Total write operations passed in the queue
   * - scylla_node_ops_finished_percentage
     - Finished percentage of node operation on this shard
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
   * - scylla_reactor_aio_outsizes	
     - Total number of aio operations that exceed IO limit
   * - scylla_schema_commitlog_active_allocations	
     - Current number of active allocations.
   * - scylla_scheduler_starvetime_ms
     - Accumulated starvation time of this task queue; an increment rate of 1000ms per second indicates the scheduler feels really bad
   * - scylla_scheduler_waittime_ms
     - Accumulated waittime of this task queue; an increment rate of 1000ms per second indicates queue is waiting for something (e.g. IO)
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
   * - scylla_sstables_index_page_cache_bytes_in_std
     - Total number of bytes in temporary buffers which live in the std allocator
   * - scylla_sstables_index_page_evictions
     - Index pages which got evicted from memory
   * - scylla_sstables_index_page_populations
     - Index pages which got populated into memory
   * - scylla_sstables_index_page_used_bytes
     - Amount of bytes used by index pages in memory
   * - scylla_sstables_pi_auto_scale_events
     - Number of promoted index auto-scaling events
   * - scylla_storage_proxy_coordinator_read_rate_limited
     - Number of read requests which were rejected by replicas because rate limit for the partition was reached.
   * - scylla_storage_proxy_coordinator_write_rate_limited
     - Number of write requests which were rejected by replicas because rate limit for the partition was reached.
   * - scylla_storage_proxy_coordinator_writes_failed_due_to_too_many_in_flight_hints
     - Number of CQL write requests which failed because the hinted handoff mechanism is overloaded and cannot store any more in-flight hints
   * - scylla_transport_auth_responses
     - Counts the total number of received CQL AUTH messages.
   * - scylla_transport_batch_requests
     - Counts the total number of received CQL BATCH messages.
   * - scylla_transport_cql_errors_total
     - Counts the total number of returned CQL errors.
   * - scylla_transport_execute_requests
     - Counts the total number of received CQL EXECUTE messages.
   * - scylla_transport_options_requests
     - Counts the total number of received CQL OPTIONS messages.
   * - scylla_transport_prepare_requests
     - Counts the total number of received CQL PREPARE messages.
   * - scylla_transport_query_requests
     - Counts the total number of received CQL QUERY messages.
   * - scylla_transport_register_requests
     - Counts the total number of received CQL REGISTER messages.
   * - scylla_transport_startups
     - Counts the total number of received CQL STARTUP messages.

Removed Metrics
-----------------

The following metrics were removed in ScyllaDB Enterprise 2022.2:

* scylla_database_querier_cache_memory_based_evictions
* scylla_memory_streaming_dirty_bytes
* scylla_memory_streaming_virtual_dirty_bytes
* scylla_repair_row_from_disk_bytes
* scylla_repair_row_from_disk_nr
* scylla_repair_rx_hashes_nr
* scylla_repair_rx_row_bytes
* scylla_repair_rx_row_nr
* scylla_repair_tx_hashes_nr
* scylla_repair_tx_row_bytes
* scylla_repair_tx_row_nr
* scylla_thrift_current_connections
* scylla_thrift_served
* scylla_thrift_thrift_connections