# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: ScyllaDB-Proprietary

from util import random_string
from conftest import new_dynamodb_session
from test_metrics import metrics, get_metrics, check_increases_metric
import time

def test_service_level_metrics(test_table, request, dynamodb, metrics):
    print("Please make sure authorization is enforced in your Scylla installation: alternator_enforce_authorization: true")
    p = random_string()
    c = random_string()
    _ = get_metrics(metrics)
    # Use additional user created by test/alternator/run to execute write under sl_alternator service level.
    ses = new_dynamodb_session(request, dynamodb, user='alternator_custom_sl')
    # service_level_controler acts asynchronously in a loop so we can fail metric check
    # if it hasn't processed service level update yet. It can take as long as 10 seconds.
    started = time.time()
    timeout = 30
    while True:
        try:
            with check_increases_metric(metrics,
                                        ['scylla_storage_proxy_coordinator_write_latency_count'],
                                        {'scheduling_group_name': 'sl:sl_alternator'}):
                ses.meta.client.put_item(TableName=test_table.name, Item={'p': p, 'c': c})
            break # no exception, test passed
        except:
            if time.time() - started > timeout:
                raise
            else:
                time.sleep(0.5) # retry
