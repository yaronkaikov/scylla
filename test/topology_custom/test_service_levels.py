#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: ScyllaDB-Proprietary
#
import pytest
import time
import asyncio
import logging
from test.pylib.util import wait_for_cql_and_get_hosts, unique_name
from test.pylib.manager_client import ManagerClient
from test.topology.conftest import skip_mode
from test.topology.util import wait_for_token_ring_and_group0_consistency
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

# Reproduces issue scylla-enterprise#4912
@pytest.mark.asyncio
async def test_service_level_metric_name_change(manager: ManagerClient) -> None:
    s = await manager.server_add()
    await wait_for_token_ring_and_group0_consistency(manager, time.time() + 30)
    cql = manager.get_cql()

    sl1 = unique_name()
    sl2 = unique_name()

    # creates scheduling group `sl:sl1`
    await cql.run_async(f"CREATE SERVICE LEVEL {sl1}")
    # renames scheduling group `sl:sl1` to `sl_deleted:sl1`
    await cql.run_async(f"DROP SERVICE LEVEL {sl1}")
    # renames scheduling group `sl_deleted:sl1` to `sl:sl2`
    await cql.run_async(f"CREATE SERVICE LEVEL {sl2}")
    # creates scheduling group `sl:sl1`
    await cql.run_async(f"CREATE SERVICE LEVEL {sl1}")
    # In issue #4912, service_level_controller thought there was no room
    # for `sl:sl1` scheduling group because create_scheduling_group() failed due to
    # `seastar::metrics::double_registration (registering metrics twice for metrics: transport_cql_requests_count)`
    # but the scheduling group was actually created.
    # When sl2 is dropped, service_level_controller tries to rename its
    # scheduling group to `sl:sl1`, triggering 
    # `seastar::metrics::double_registration (registering metrics twice for metrics: scheduler_runtime_ms)`
    await cql.run_async(f"DROP SERVICE LEVEL {sl2}")

    # Check if group0 is healthy
    s2 = await manager.server_add()
    await wait_for_token_ring_and_group0_consistency(manager, time.time() + 30)