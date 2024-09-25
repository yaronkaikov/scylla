#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: ScyllaDB-Proprietary
#

import logging
import pytest
import time

from test.topology.conftest import skip_mode


logger = logging.getLogger(__name__)


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_repair_succeeds_with_unitialized_bm(manager):
    await manager.server_add()
    await manager.server_add()
    servers = await manager.running_servers()

    cql = manager.get_cql()

    cql.execute("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}")
    cql.execute("CREATE TABLE ks.tbl (pk int, ck int, PRIMARY KEY (pk, ck)) WITH tombstone_gc = {'mode': 'repair'}")

    await manager.api.enable_injection(servers[1].ip_addr, "repair_flush_hints_batchlog_handler_bm_uninitialized", True, {})
    logs = [await manager.server_open_log(srv.server_id) for srv in servers]
    marks = [await log.mark() for log in logs]

    await manager.api.client.post("/storage_service/repair_async/ks", servers[0].ip_addr)

    time.sleep(2)
    matches = await logs[1].grep("Failed to process repair_flush_hints_batchlog_request", from_mark=marks[1])
    assert len(matches) == 1
    matches = await logs[0].grep("failed, continue to run repair", from_mark=marks[0])
    assert len(matches) == 1
