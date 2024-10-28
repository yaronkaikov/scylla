#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: ScyllaDB-Proprietary
#
import pytest
import time
import asyncio
import logging
from test.pylib.util import wait_for_cql_and_get_hosts
from test.pylib.manager_client import ManagerClient
from test.topology.conftest import skip_mode
from cassandra.protocol import InvalidRequest

logger = logging.getLogger(__name__)

@pytest.mark.asyncio
@skip_mode('release', 'error injection is not supported in release mode')
async def test_workload_prioritization_upgrade(manager: ManagerClient):
    # This test simulates OSS->enterprise upgrade in v1 service levels.
    # Using error injection, the test disables WORKLOAD_PRIORITIZATION feature
    # and removes `shares` column from system_distributed.service_levels table.
    config = {
        'experimental_features': [], # disable raft topology
        'enable_user_defined_functions': False,
        'authenticator': 'AllowAllAuthenticator',
        'authorizer': 'AllowAllAuthorizer',
        'error_injections_at_startup': ['suppress_workload_prioritization', 'service_levels_v1_table_without_shares'],
    }
    servers = [await manager.server_add(config=config) for _ in range(3)]
    cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    # Validate that service levels' table has no `shares` column
    sl_schema = await cql.run_async("DESC TABLE system_distributed.service_levels")
    assert "shares int" not in sl_schema[0].create_statement
    with pytest.raises(InvalidRequest):
        await cql.run_async("CREATE SERVICE LEVEL sl1 WITH shares = 100")
    
    # Do rolling restart of the cluster and remove error injections
    for server in servers:
        await manager.server_update_config(server.server_id, 'error_injections_at_startup', [])
        await manager.server_stop_gracefully(server.server_id)
        await manager.server_start(server.server_id)
        await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    # Validate that `shares` column was added
    logs = [await manager.server_open_log(server.server_id) for server in servers]
    await logs[0].wait_for("Workload prioritization v1 started|Workload prioritization v1 is already started", timeout=30)
    sl_schema_upgraded = await cql.run_async("DESC TABLE system_distributed.service_levels")
    assert "shares int" in sl_schema_upgraded[0].create_statement
    await cql.run_async("CREATE SERVICE LEVEL sl2 WITH shares = 100")
