ScyllaDB Enterprise Metric Update - ScyllaDB Enterprise 2023.1 to 2024.1
============================================================================

.. toctree::
   :maxdepth: 2
   :hidden:

ScyllaDB Enterprise 2024.1 Dashboards are available as part of the latest |mon_root|.

New Metrics
~~~~~~~~~~~~

The following metrics are new in ScyllaDB Enterprise 2024.1 compared to 2023.1:

.. list-table::
   :widths: 25 150
   :header-rows: 1

   * - Metric
     - Description
   * - scylla_cache_rows_compacted
     - Total amount of attempts to compact expired rows during read.
   * - scylla_cql_authorized_prepared_statements_privileged_entries_evictions_on_size
     - Counts the number of evictions of prepared statements from the authorized prepared statements cache after they have been used more than once.
   * - scylla_cql_maximum_replication_factor_fail_violations
     - Counts the number of maximum_replication_factor_fail_threshold guardrail violations, i.e. attempts to create a keyspace with RF on one of the DCs above the set guardrail.
   * - scylla_cql_maximum_replication_factor_warn_violations
     - Counts the number of maximum_replication_factor_warn_threshold guardrail violations, i.e. attempts to create a keyspace with RF on one of the DCs above the set guardrail
   * - scylla_cql_minimum_replication_factor_fail_violations
     - Counts the number of minimum_replication_factor_fail_threshold guardrail violations, i.e. attempts to create a keyspace with RF on one of the DCs below the set guardrail
   * - scylla_cql_minimum_replication_factor_warn_violations
     - Counts the number of minimum_replication_factor_warn_threshold guardrail violations, i.e. attempts to create a keyspace with RF on one of the DCs below the set guardrail.
   * - scylla_database_querier_cache_scheduling_group_mismatches
     - Counts querier cache lookups that found a cached querier but had to drop it due to scheduling group mismatch.
   * - scylla_hints_for_views_manager_send_errors
     - Number of unexpected errors during sending; sending will be retried later.
   * - scylla_hints_manager_send_errors
     - Number of unexpected errors during sending; sending will be retried later.
   * - scylla_reactor_stalls
     - A histogram of reactor stall durations.
   * - scylla_storage_proxy_coordinator_background_read_repairs
     - Number of background read repairs.
   * - scylla_storage_proxy_coordinator_background_writes_failed
     - Number of write requests that failed after CL was reached.
   * - scylla_storage_proxy_coordinator_canceled_read_repairs
     - Number of global read repairs canceled due to a concurrent write.
   * - scylla_storage_proxy_coordinator_cas_dropped_prune
     - How many times a coordinator did not perform prune after CAS.
   * - scylla_storage_proxy_coordinator_cas_failed_read_round_optimization
     - CAS read rounds issued only if the previous value is missing on some replica.
   * - scylla_storage_proxy_coordinator_cas_read_contention
     - How many contended reads were encountered.
   * - scylla_storage_proxy_coordinator_cas_read_latency_summary
     - CAS read latency summary.
   * - scylla_storage_proxy_coordinator_cas_read_latency
     - Transactional read latency histogram.
   * - scylla_storage_proxy_coordinator_cas_read_timeouts
     - Number of transactional read requests failed due to a timeout.
   * - scylla_storage_proxy_coordinator_cas_read_unavailable
     - Number of transactional read requests failed due to an "unavailable" error.
   * - scylla_storage_proxy_coordinator_cas_read_unfinished_commit
     - Number of transaction commit attempts that occurred on read.
   * - scylla_storage_proxy_coordinator_cas_total_operations
     - Number of total Paxos operations executed (reads and writes).
   * - scylla_storage_proxy_coordinator_cas_write_condition_not_met
     - Number of transaction preconditions that did not match current values.
   * - scylla_storage_proxy_coordinator_cas_write_contention
     - How many contended writes were encountered.
   * - scylla_storage_proxy_coordinator_cas_write_latency_summary
     - CAS write latency summary.
   * - scylla_storage_proxy_coordinator_cas_write_latency
     - Transactional write latency histogram.
   * - scylla_storage_proxy_coordinator_cas_write_timeout_due_to_uncertainty
     - How many times write timeout was reported because of uncertainty in the result.
   * - scylla_storage_proxy_coordinator_cas_write_timeouts
     - Number of transactional write requests failed due to a timeout.
   * - scylla_storage_proxy_coordinator_cas_write_unavailable
     - Number of transactional write requests failed due to an "unavailable" error.
   * - scylla_storage_proxy_coordinator_cas_write_unfinished_commit
     - Number of transaction commit attempts that occurred on write.
   * - scylla_storage_proxy_coordinator_foreground_read_repairs
     - Number of foreground read repairs.
   * - scylla_storage_proxy_coordinator_range_timeouts
     - Number of range read operations failed due to a timeout.
   * - scylla_storage_proxy_coordinator_range_unavailable
     - Number of range read operations failed due to an "unavailable" error.
   * - scylla_storage_proxy_coordinator_read_rate_limited
     - Number of read requests that were rejected by replicas because the rate limit for the partition was reached.
   * - scylla_storage_proxy_coordinator_read_retries
     - Number of read retry attempts.
   * - scylla_storage_proxy_coordinator_reads_coordinator_outside_replica_set
     - Number of CQL read requests that arrived to a non-replica and had to be forwarded to a replica.
   * - scylla_storage_proxy_coordinator_read_timeouts
     - Number of read requests failed due to a timeout.
   * - scylla_storage_proxy_coordinator_read_unavailable
     - Number read requests failed due to an "unavailable" error.
   * - scylla_storage_proxy_coordinator_speculative_data_reads
     - Number of speculative data read requests that were sent
   * - scylla_storage_proxy_coordinator_speculative_digest_reads
     - Number of speculative digest read requests that were sent.
   * - scylla_storage_proxy_coordinator_throttled_writes
     - Number of throttled write requests.
   * - scylla_storage_proxy_coordinator_write_rate_limited
     - Number of write requests that were rejected by replicas because the rate limit for the partition was reached.
   * - scylla_storage_proxy_coordinator_writes_coordinator_outside_replica_set
     - Number of CQL write requests that arrived to a non-replica and had to be forwarded to a replica.
   * - scylla_storage_proxy_coordinator_writes_failed_due_to_too_many_in_flight_hints
     - Number of CQL write requests that failed because the hinted handoff mechanism is overloaded and cannot store any more in-flight hints.
   * - scylla_storage_proxy_coordinator_write_timeouts
     - Number of write requests failed due to a timeout.
   * - scylla_storage_proxy_coordinator_write_unavailable
     - Number of write requests failed due to an "unavailable" error.
   * - scylla_storage_proxy_replica_cas_dropped_prune
     - How many times a coordinator did not perfom prune after CAS.
   * - scylla_storage_proxy_replica_cross_shard_ops
     - Number of operations that crossed a shard boundary.
   * - scylla_storage_proxy_replica_forwarded_mutations
     - Number of mutations forwarded to other replica nodes.
   * - scylla_storage_proxy_replica_forwarding_errors
     - Number of errors during forwarding mutations to other replica nodes.
   * - scylla_storage_proxy_replica_reads
     - Number of remote data read requests this node received.
   * - scylla_storage_proxy_replica_received_counter_updates
     - Number of counter updates received by this node acting as an update leader.
   * - scylla_storage_proxy_replica_received_mutations
     - Number of mutations received by a replica node.
   * - scylla_transport_cql_request_bytes
     - Counts the total number of received bytes in CQL messages of a specific kind.
   * - scylla_transport_cql_requests_count
     - Counts the total number of CQL messages of a specific kind.
   * - scylla_transport_cql_response_bytes
     - Counts the total number of sent response bytes for CQL requests of a specific kind.
   * - scylla_view_update_generator_sstables_to_move_count
     - Number of sets of sstables that are already processed and waiting to be moved from their staging directory.

