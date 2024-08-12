#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: ScyllaDB-Proprietary
#
"""
Test RPC compression
"""
from test.pylib.internal_types import ServerInfo
from test.pylib.rest_client import ScyllaMetrics
from test.pylib.util import wait_for_cql_and_get_hosts, unique_name
from test.pylib.manager_client import ManagerClient
import pytest
import asyncio
import time
import logging
import random
from cassandra.cluster import ConsistencyLevel
from cassandra.query import SimpleStatement
import contextlib
import typing
import functools

logger = logging.getLogger(__name__)

async def live_update_config(manager: ManagerClient, servers: list[ServerInfo], key: str, value: str):
    cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, servers, deadline = time.time() + 60)
    await asyncio.gather(*[cql.run_async("UPDATE system.config SET value=%s WHERE name=%s", [value, key], host=host) for host in hosts])

def uncompressed_sent(metrics: list[ScyllaMetrics], algo: str) -> float:
    return sum([m.get("scylla_rpc_compression_bytes_sent", {"algorithm": algo}) for m in metrics])
def compressed_sent(metrics: list[ScyllaMetrics], algo: str) -> float:
    return sum([m.get("scylla_rpc_compression_compressed_bytes_sent", {"algorithm": algo}) for m in metrics])
def approximately_equal(a: float, b: float, factor: float) -> bool:
    assert factor < 1.0
    return factor < a / b < (1/factor)
async def get_metrics(manager: ManagerClient, servers: list[ServerInfo]) -> list[ScyllaMetrics]:
    return await asyncio.gather(*[manager.metrics.query(s.ip_addr) for s in servers])

async def with_retries(test_once: typing.Callable[[], typing.Awaitable], timeout: float):
    async with asyncio.timeout(timeout):
        while True:
            try:
                await test_once()
            except Exception as e:
                logger.info(f"test attempt failed with {e}, retrying")
                await asyncio.sleep(1)
            else:
                break

@pytest.mark.asyncio
async def test_basic(manager: ManagerClient) -> None:
    """Tests basic functionality of internode compression.
    Also, tests that changing internode_compression_zstd_max_cpu_fraction from 0.0 to 1.0 enables zstd as expected.
    """
    cfg = {
        'internode_compression': "all",
        'internode_compression_zstd_max_cpu_fraction': 0.0}
    logger.info(f"Booting initial cluster")
    servers = [await manager.server_add(config=cfg) for i in range(2)]

    cql = manager.get_cql()

    replication_factor = 2
    ks = unique_name()
    await cql.run_async(f"create keyspace {ks} with replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {replication_factor}}}")
    await cql.run_async(f"create table {ks}.cf (pk int, v blob, primary key (pk))")
    write_stmt = cql.prepare(f"update {ks}.cf set v = ? where pk = ?")
    write_stmt.consistency_level = ConsistencyLevel.ALL

    # 128 kiB message, should give compression ratio of ~0.5 for lz4 and ~0.25 for zstd.
    message = b''.join(bytes(random.choices(range(16), k=1024)) * 2 for _ in range(64))

    async def test_algo(algo: str, expected_ratio: float):
        n_messages = 100
        metrics_before = await get_metrics(manager, servers)
        await asyncio.gather(*[cql.run_async(write_stmt, parameters=[message, pk]) for pk in range(n_messages)])
        metrics_after = await get_metrics(manager, servers)

        volume = n_messages * len(message) * (replication_factor - 1)
        uncompressed = uncompressed_sent(metrics_after, algo) - uncompressed_sent(metrics_before, algo)
        compressed = compressed_sent(metrics_after, algo) - compressed_sent(metrics_before, algo)
        assert approximately_equal(uncompressed, volume, 0.9)
        assert approximately_equal(compressed, expected_ratio * volume, 0.9)

    await with_retries(functools.partial(test_algo, "lz4", 0.5), timeout=600)

    await live_update_config(manager, servers, "internode_compression_zstd_max_cpu_fraction", "1.0")

    await with_retries(functools.partial(test_algo, "zstd", 0.25), timeout=600)
