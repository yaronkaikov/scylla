# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Tests for database metrics exposed via Prometheus.
#############################################################################

import logging
import math

import cassandra.concurrent

from .util import new_test_table, ScyllaMetrics

logger = logging.getLogger(__name__)


def get_total_reads(cql, classes):
    total = 0
    for cls in classes:
        value = ScyllaMetrics.query(cql).get("scylla_database_total_reads", labels={"class": cls})
        if value is not None:
            total += value
    return total


def do_run(cql, read_func):
    # A user connection starts in the sl:driver scheduling group and only migrates to
    # sl:default once the server detects it as a user connection, so reads issued by the
    # session may be accounted under either class. Sum both to count all user reads.
    classes = ["sl:default", "sl:driver"]

    initial_reads = get_total_reads(cql, classes)

    if not initial_reads:
        # Versions before 2025.1 merge some metrics into the "user" class
        classes = ["user"]
        initial_reads = get_total_reads(cql, classes)

    total_count = read_func()

    final_reads = get_total_reads(cql, classes)

    added = final_reads - initial_reads
    min_count = total_count
    max_count = total_count * 1.1
    assert min_count <= added <= max_count, \
        f"Expected additional reads to be in the [{min_count}, {max_count}] range, but metrics show {added} reads"


# Following scylladb/scylla:0c6bbc8 queries are now classified by its initiator, so here is a
# small test that aims to ensure that when a user runs queries, they will be marked as user
# initiated (here we are checking the `scylla_database_total_reads` metric under class="user"
# or the sl:default/sl:driver scheduling group classes).
def test_total_reads_user(cql, test_keyspace, scylla_only):
    schema = "c1 int, c2 int, primary key (c1)"
    with new_test_table(cql, test_keyspace, schema) as table:
        count = 100
        keys = range(count)
        cassandra.concurrent.execute_concurrent_with_args(
            cql, cql.prepare(f"INSERT INTO {table} (c1, c2) VALUES (?, ?)"),
            [(k, k) for k in keys], concurrency=32)

        select_stmt = cql.prepare(f"SELECT * FROM {table} WHERE c1=?")

        reads_per_key = 16
        params = []
        for k in keys:
            params += [(k,) for _ in range(reads_per_key)]

        def read_func():
            cassandra.concurrent.execute_concurrent_with_args(cql, select_stmt, params, concurrency=32)
            return count * reads_per_key

        do_run(cql, read_func)


# Same principle as test_total_reads_user, but read a system table instead,
# and check that the reads are still classified as "user" reads.
def test_total_reads_system(cql, scylla_only):
    all_rows = list(cql.execute("SELECT keyspace_name, table_name, column_name FROM system_schema.columns"))

    params = []
    for _ in range(math.ceil(1000 / len(all_rows))):
        for r in all_rows:
            params.append((r.keyspace_name, r.table_name, r.column_name))

    select_stmt = cql.prepare(
        "SELECT * FROM system_schema.columns WHERE keyspace_name=? AND table_name=? AND column_name=?")

    def read_func():
        cassandra.concurrent.execute_concurrent_with_args(cql, select_stmt, params, concurrency=32)
        return len(params)

    do_run(cql, read_func)
