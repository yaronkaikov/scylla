# Copyright 2021-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import pytest
import sys
import requests
import threading
import time

# Use the util.py library from ../cql-pytest:
sys.path.insert(1, sys.path[0] + '/../cql-pytest')
from util import unique_name, new_test_table, new_test_keyspace, new_materialized_view, new_secondary_index
from rest_util import new_test_snapshot, scylla_inject_error

# "keyspace" function: Creates and returns a temporary keyspace to be
# used in tests that need a keyspace. The keyspace is created with RF=1,
def new_keyspace(cql, this_dc):
    name = unique_name()
    cql.execute(f"CREATE KEYSPACE {name} WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', '{this_dc}' : 1 }}")
    return name

def test_storage_service_keyspaces(cql, this_dc, rest_api):
    with new_test_keyspace(cql, f"WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', '{this_dc}' : 1 }}") as keyspace:
        resp_user = rest_api.send("GET", "storage_service/keyspaces", { "type": "user" })
        resp_user.raise_for_status()
        keyspaces_user = resp_user.json()
        assert keyspace in keyspaces_user
        # don't assume all that starts with system is non-user, but "system" certainly is not.
        assert not "system" in keyspaces_user

        resp_nls = rest_api.send("GET", "storage_service/keyspaces", { "type": "non_local_strategy" })
        resp_nls.raise_for_status()
        assert keyspace in resp_nls.json()

        resp_all = rest_api.send("GET", "storage_service/keyspaces", { "type": "all" })
        resp_all.raise_for_status()
        assert keyspace in resp_all.json()

        resp = rest_api.send("GET", "storage_service/keyspaces")
        resp.raise_for_status()
        assert keyspace in resp.json()


def do_test_storage_service_attribute_api_keyspace(cql, this_dc, rest_api, api_name):
    keyspace = new_keyspace(cql, this_dc)
    # test empty keyspace
    resp = rest_api.send("DELETE", f"storage_service/{api_name}/{keyspace}")
    resp.raise_for_status()

    resp = rest_api.send("POST", f"storage_service/{api_name}/{keyspace}")
    resp.raise_for_status()

    # test non-empty keyspace
    with new_test_table(cql, keyspace, "a int, PRIMARY KEY (a)") as t:
        resp = rest_api.send("DELETE", f"storage_service/{api_name}/{keyspace}")
        resp.raise_for_status()

        resp = rest_api.send("POST", f"storage_service/{api_name}/{keyspace}")
        resp.raise_for_status()

        # non-existing keyspace
        resp = rest_api.send("POST", f"storage_service/{api_name}/XXX")
        assert resp.status_code == requests.codes.bad_request

    cql.execute(f"DROP KEYSPACE {keyspace}")

def test_storage_service_auto_compaction_keyspace(cql, this_dc, rest_api):
    do_test_storage_service_attribute_api_keyspace(cql, this_dc, rest_api, "auto_compaction")

def test_storage_service_tombstone_gc_keyspace(cql, this_dc, rest_api):
    do_test_storage_service_attribute_api_keyspace(cql, this_dc, rest_api, "tombstone_gc")

def do_test_storage_service_attribute_api_table(cql, this_dc, rest_api, api_name):
    keyspace = new_keyspace(cql, this_dc)
    with new_test_table(cql, keyspace, "a int, PRIMARY KEY (a)") as t:
        test_table = t.split('.')[1]
        resp = rest_api.send("DELETE", f"storage_service/{api_name}/{keyspace}", { "cf": test_table })
        resp.raise_for_status()

        resp = rest_api.send("POST", f"storage_service/{api_name}/{keyspace}", { "cf": test_table })
        resp.raise_for_status()

        # non-existing table
        resp = rest_api.send("POST", f"storage_service/{api_name}/{keyspace}", { "cf": "XXX" })
        assert resp.status_code == requests.codes.bad_request

    cql.execute(f"DROP KEYSPACE {keyspace}")

def test_storage_service_auto_compaction_table(cql, this_dc, rest_api):
    do_test_storage_service_attribute_api_table(cql, this_dc, rest_api, "auto_compaction")

def test_storage_service_tombstone_gc_table(cql, this_dc, rest_api):
    do_test_storage_service_attribute_api_table(cql, this_dc, rest_api, "tombstone_gc")

def do_test_storage_service_attribute_api_tables(cql, this_dc, rest_api, api_name):
    keyspace = new_keyspace(cql, this_dc)
    with new_test_table(cql, keyspace, "a int, PRIMARY KEY (a)") as t0:
        with new_test_table(cql, keyspace, "a int, PRIMARY KEY (a)") as t1:
            test_tables = [t0.split('.')[1], t1.split('.')[1]]
            resp = rest_api.send("DELETE", f"storage_service/{api_name}/{keyspace}", { "cf": f"{test_tables[0]},{test_tables[1]}" })
            resp.raise_for_status()

            resp = rest_api.send("POST", f"storage_service/{api_name}/{keyspace}", { "cf": f"{test_tables[0]},{test_tables[1]}" })
            resp.raise_for_status()

            # non-existing table
            resp = rest_api.send("POST", f"storage_service/{api_name}/{keyspace}", { "cf": f"{test_tables[0]},XXX" })
            assert resp.status_code == requests.codes.bad_request

    cql.execute(f"DROP KEYSPACE {keyspace}")

def test_storage_service_auto_compaction_tables(cql, this_dc, rest_api):
    do_test_storage_service_attribute_api_tables(cql, this_dc, rest_api, "auto_compaction")

def test_storage_service_tombstone_gc_tables(cql, this_dc, rest_api):
    do_test_storage_service_attribute_api_tables(cql, this_dc, rest_api, "tombstone_gc")

def test_storage_service_keyspace_offstrategy_compaction(cql, this_dc, rest_api):
    keyspace = new_keyspace(cql, this_dc)
    with new_test_table(cql, keyspace, "a int, PRIMARY KEY (a)") as t0:
        resp = rest_api.send("POST", f"storage_service/keyspace_offstrategy_compaction/{keyspace}")
        resp.raise_for_status()

    cql.execute(f"DROP KEYSPACE {keyspace}")

def test_storage_service_keyspace_offstrategy_compaction_tables(cql, this_dc, rest_api):
    keyspace = new_keyspace(cql, this_dc)
    with new_test_table(cql, keyspace, "a int, PRIMARY KEY (a)") as t0:
        with new_test_table(cql, keyspace, "a int, PRIMARY KEY (a)") as t1:
            test_tables = [t0.split('.')[1], t1.split('.')[1]]

            resp = rest_api.send("POST", f"storage_service/keyspace_offstrategy_compaction/{keyspace}", { "cf": f"{test_tables[0]},{test_tables[1]}" })
            resp.raise_for_status()

            # non-existing table
            resp = rest_api.send("POST", f"storage_service/keyspace_offstrategy_compaction/{keyspace}", { "cf": f"{test_tables[0]},XXX" })
            assert resp.status_code == requests.codes.bad_request

    cql.execute(f"DROP KEYSPACE {keyspace}")

def test_storage_service_keyspace_scrub(cql, this_dc, rest_api):
    with new_test_keyspace(cql, f"WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', '{this_dc}' : 1 }}") as keyspace:
        with new_test_table(cql, keyspace, "a int, PRIMARY KEY (a)") as t0:
            with new_test_table(cql, keyspace, "a int, PRIMARY KEY (a)") as t1:
                test_tables = [t0.split('.')[1], t1.split('.')[1]]

                resp = rest_api.send("GET", f"storage_service/keyspace_scrub/{keyspace}")
                resp.raise_for_status()

                resp = rest_api.send("GET", f"storage_service/keyspace_scrub/{keyspace}", { "cf": f"{test_tables[1]}" })
                resp.raise_for_status()

                resp = rest_api.send("GET", f"storage_service/keyspace_scrub/{keyspace}", { "cf": f"{test_tables[0]},{test_tables[1]}" })
                resp.raise_for_status()

                # non-existing table
                resp = rest_api.send("POST", f"storage_service/keyspace_scrub/{keyspace}", { "cf": f"{test_tables[0]},XXX" })
                assert resp.status_code == requests.codes.not_found

def test_storage_service_keyspace_scrub_abort(cql, this_dc, rest_api):
    with new_test_keyspace(cql, f"WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', '{this_dc}' : 1 }}") as keyspace:
        with new_test_table(cql, keyspace, "a int, PRIMARY KEY (a)") as t0:
            with scylla_inject_error(rest_api, "rest_api_keyspace_scrub_abort"):
                cql.execute(f"INSERT INTO {t0} (a) VALUES (42)")
                resp = rest_api.send("POST", f"storage_service/keyspace_flush/{keyspace}")
                resp.raise_for_status()
                resp = rest_api.send("GET", f"storage_service/keyspace_scrub/{keyspace}",  { "scrub_mode": "ABORT" })
                assert resp.content == b'1'

def test_storage_service_keyspace_scrub_mode(cql, this_dc, rest_api):
    with new_test_keyspace(cql, f"WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', '{this_dc}' : 1 }}") as keyspace:
        with new_test_table(cql, keyspace, "a int, PRIMARY KEY (a)") as t0:
            with new_test_table(cql, keyspace, "a int, PRIMARY KEY (a)") as t1:
                test_tables = [t0.split('.')[1], t1.split('.')[1]]

                resp = rest_api.send("GET", f"storage_service/keyspace_scrub/{keyspace}", { "cf": f"{test_tables[0]}", "scrub_mode": "VALIDATE" })
                resp.raise_for_status()

                resp = rest_api.send("GET", f"storage_service/keyspace_scrub/{keyspace}", { "cf": f"{test_tables[0]}", "scrub_mode": "XXX" })
                assert resp.status_code == requests.codes.bad_request

                resp = rest_api.send("GET", f"storage_service/keyspace_scrub/{keyspace}", { "cf": f"{test_tables[0]}", "quarantine_mode": "ONLY" })
                resp.raise_for_status()

                resp = rest_api.send("GET", f"storage_service/keyspace_scrub/{keyspace}", { "cf": f"{test_tables[0]}", "quarantine_mode": "YYY" })
                assert resp.status_code == requests.codes.bad_request

def test_storage_service_keyspace_bad_param(cql, this_dc, rest_api):
    with new_test_keyspace(cql, f"WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', '{this_dc}' : 1 }}") as keyspace:
        # Url must include the keyspace param.
        resp = rest_api.send("GET", f"storage_service/keyspace_scrub")
        assert resp.status_code == requests.codes.not_found

        # Url must include the keyspace param.
        # It cannot be given as an optional param
        resp = rest_api.send("GET", f"storage_service/keyspace_scrub", { "keyspace": "{keyspace}" })
        assert resp.status_code == requests.codes.not_found

        # Optional param cannot use the same name as a mandatory (positional, in url) param.
        resp = rest_api.send("GET", f"storage_service/keyspace_scrub/{keyspace}", { "keyspace": "{keyspace}" })
        assert resp.status_code == requests.codes.bad_request

        # Unknown parameter (See https://github.com/scylladb/scylla/pull/10090)
        resp = rest_api.send("GET", f"storage_service/keyspace_scrub/{keyspace}", { "foo": "bar" })
        assert resp.status_code == requests.codes.bad_request

# Unfortunately by default Python threads print their exceptions
# (e.g., assertion failures) but don't propagate them to the join(),
# so the overall test doesn't fail. The following Thread wrapper
# causes join() to rethrow the exception, so the test will fail.
class ThreadWrapper(threading.Thread):
    def run(self):
        try:
            self.ret = self._target(*self._args, **self._kwargs)
        except BaseException as e:
            self.exception = e
    def join(self, timeout=None):
        super().join(timeout)
        if hasattr(self, 'exception'):
            raise self.exception
        return self.ret

# Reproduce issue #9061, where if we have a partition key with characters
# that need escaping in JSON, the toppartitions response failed to escape
# them. The underlying bug was a Seastar bug in JSON in the HTTP server:
# https://github.com/scylladb/seastar/issues/460
def test_toppartitions_pk_needs_escaping(cql, this_dc, rest_api):
    with new_test_keyspace(cql, f"WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', '{this_dc}' : 1 }}") as keyspace:
        with new_test_table(cql, keyspace, "p text PRIMARY KEY") as table:
            # Use a newline character as part of the partition key pk. When
            # toppartitions later returns it, it must escape it (as pk_json)
            # or yield an invalid JSON with a literal newline in a string.
            pk = 'hi\nhello'
            pk_json = r'hi\nhello'
            # Unfortunately, the toppartitions API doesn't let us mark the
            # beginning and end of the sampling period. Instead we need to
            # start the toppartitions for a predefined period, and in
            # parallel, make the request. Usually, a very short sampling
            # period suffices, but very rarely on slow builds and overloaded
            # machines, even one second is not enough (see #13223), so we'll
            # try the same test several times with an increasing period.
            # This means that this test will usually pass quickly - but in some
            # cases it can take some time (currently, 10.24 seconds) to fail.
            period_sec = 0.01
            while period_sec < 6:  # last period will be 5.12 seconds
                def toppartitions():
                    ks, cf = table.split('.')
                    resp = rest_api.send('GET', 'storage_service/toppartitions', {'table_filters': f'{ks}:{cf}', 'duration': str(int(period_sec*1000))})
                    assert resp.ok
                    # resp.json() will raise an error if not valid JSON
                    resp.json()
                    assert pk_json in resp.text
                def insert():
                    # We need to wait enough time for the toppartitions request
                    # to have been sent, but unfortunately we don't know when
                    # this happens because the request doesn't return until the
                    # "duration" ends. So we hope period_sec/2 was enough.
                    # If it wasn't, we'll try again with increased period_sec.
                    time.sleep(period_sec/2)
                    stmt = cql.prepare(f"INSERT INTO {table} (p) VALUES (?)")
                    cql.execute(stmt, [pk])
                t1 = ThreadWrapper(target=toppartitions)
                t2 = ThreadWrapper(target=insert)
                t1.start()
                t2.start()
                try:
                    t1.join()
                    t2.join()
                    # The test passed
                    return
                # Failing the "assert pk_json in resp.text" above is when
                # we want to retry (with a higher period_sec). Any other
                # error, like unparsable JSON, tells us immediately that
                # we failed the test so we re-raise the exception.
                except AssertionError:
                    period_sec *= 2
                except:
                    raise
        # If we're here, we didn't "return" above so the test failed
        pytest.fail(f'Test failed, even {period_sec/2}s was not enough.')


# TODO: check that keyspace_flush actually does anything, like create new sstables.
def test_storage_service_flush(cql, this_dc, rest_api):
    with new_test_keyspace(cql, f"WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', '{this_dc}' : 1 }}") as keyspace:
        with new_test_table(cql, keyspace, "p text PRIMARY KEY") as table0:
            ks, t0 = table0.split('.')
            stmt = cql.prepare(f"INSERT INTO {table0} (p) VALUES (?)")
            cql.execute(stmt, ["pk0"])
            with new_test_table(cql, keyspace, "p text PRIMARY KEY") as table1:
                _, t1 = table1.split('.')
                stmt = cql.prepare(f"INSERT INTO {table1} (p) VALUES (?)")
                cql.execute(stmt, ["pk1"])

                # test the keyspace_flush doesn't produce any errors when called on existing keyspace and table(s)
                resp = rest_api.send("POST", f"storage_service/keyspace_flush/{ks}")
                resp.raise_for_status()

                resp = rest_api.send("POST", f"storage_service/keyspace_flush/{ks}", { "cf": f"{t0}"})
                resp.raise_for_status()

                resp = rest_api.send("POST", f"storage_service/keyspace_flush/{ks}", { "cf": f"{t0},{t1}"})
                resp.raise_for_status()

                # test error when keyspace_flush is called on non-existing keyspace or table(s)
                resp = rest_api.send("POST", f"storage_service/keyspace_flush/no_such_keyspace")
                assert resp.status_code == requests.codes.bad_request

                resp = rest_api.send("POST", f"storage_service/keyspace_flush/{ks}", { "cf": f"no_such_table"} )
                assert resp.status_code == requests.codes.bad_request

                resp = rest_api.send("POST", f"storage_service/keyspace_flush/{ks}", { "cf": f"{t0},no_such_table,{t1}"} )
                assert resp.status_code == requests.codes.bad_request

def test_storage_service_snapshot(cql, this_dc, rest_api):
    resp = rest_api.send("GET", "storage_service/snapshots")
    resp.raise_for_status()

    def verify_snapshot_details(expected):
        resp = rest_api.send("GET", "storage_service/snapshots")
        found = False
        for data in resp.json():
            if data['key'] == expected['key']:
                assert not found
                found = True
                sort_key = lambda v: f"{v['ks']}-{v['cf']}"
                value = sorted([v for v in data['value'] if not v['ks'].startswith('system')], key=sort_key)
                expected_value = sorted(expected['value'], key=sort_key)
                assert len(value) == len(expected_value), f"length mismatch: expected {expected_value} but got {value}"
                for i in range(len(value)):
                    v = value[i]
                    # normalize `total` and `live`
                    # since we care only if they are zero or not
                    v['total'] = 1 if v['total'] else 0
                    v['live'] = 1 if v['live'] else 0
                    ev = expected_value[i]
                    assert v == ev
        assert found, f"key='{expected['key']}' not found in {resp.json()}"

    with new_test_keyspace(cql, f"WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', '{this_dc}' : 1 }}") as keyspace0:
        with new_test_table(cql, keyspace0, "p text PRIMARY KEY") as table00:
            ks0, cf00 = table00.split('.')
            stmt = cql.prepare(f"INSERT INTO {table00} (p) VALUES (?)")
            cql.execute(stmt, ["pk0"])

            # single keyspace / table
            with new_test_snapshot(rest_api, ks0, cf00) as snapshot0:
                verify_snapshot_details({
                    'key': snapshot0,
                    'value': [{'ks': ks0, 'cf': cf00, 'total': 1, 'live': 0}]
                })

                cql.execute(f"TRUNCATE {table00}")
                verify_snapshot_details({
                    'key': snapshot0,
                    'value': [{'ks': ks0, 'cf': cf00, 'total': 1, 'live': 1}]
                })

            with new_test_table(cql, keyspace0, "p text PRIMARY KEY") as table01:
                _, cf01 = table01.split('.')
                stmt = cql.prepare(f"INSERT INTO {table01} (p) VALUES (?)")
                cql.execute(stmt, ["pk1"])

                # single keyspace / multiple tables
                with new_test_snapshot(rest_api, ks0, [cf00, cf01]) as snapshot1:
                    verify_snapshot_details({
                        'key': snapshot1,
                        'value': [
                            {'ks': ks0, 'cf': cf00, 'total': 0, 'live': 0},
                            {'ks': ks0, 'cf': cf01, 'total': 1, 'live': 0}
                        ]
                    })

                with new_test_keyspace(cql, f"WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', '{this_dc}' : 1 }}") as keyspace1:
                    with new_test_table(cql, keyspace1, "p text PRIMARY KEY") as table10:
                        ks1, cf10 = table10.split('.')

                        # multiple keyspaces
                        with new_test_snapshot(rest_api, [ks0, ks1]) as snapshot2:
                            verify_snapshot_details({
                                'key': snapshot2,
                                'value': [
                                    {'ks': ks0, 'cf': cf00, 'total': 0, 'live': 0},
                                    {'ks': ks0, 'cf': cf01, 'total': 1, 'live': 0},
                                    {'ks': ks1, 'cf': cf10, 'total': 0, 'live': 0}
                                ]
                            })

                        # all keyspaces
                        with new_test_snapshot(rest_api, ) as snapshot3:
                            verify_snapshot_details({
                                'key': snapshot3,
                                'value': [
                                    {'ks': ks0, 'cf': cf00, 'total': 0, 'live': 0},
                                    {'ks': ks0, 'cf': cf01, 'total': 1, 'live': 0},
                                    {'ks': ks1, 'cf': cf10, 'total': 0, 'live': 0}
                                ]
                            })

# Verify that snapshots of materialized views and secondary indexes are disallowed.
def test_storage_service_snapshot_mv_si(cql, this_dc, rest_api):
    resp = rest_api.send("GET", "storage_service/snapshots")
    resp.raise_for_status()

    with new_test_keyspace(cql, f"WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', '{this_dc}' : 1 }}") as keyspace:
        schema = 'p int, v text, primary key (p)'
        with new_test_table(cql, keyspace, schema) as table:
            with new_materialized_view(cql, table, '*', 'v, p', 'v is not null and p is not null') as mv:
                try:
                    with new_test_snapshot(rest_api, keyspace, mv.split('.')[1]) as snap:
                        pytest.fail(f"Snapshot of materialized view {mv} should have failed")
                except requests.HTTPError:
                    pass

            with new_secondary_index(cql, table, 'v') as si:
                try:
                    with new_test_snapshot(rest_api, keyspace, si.split('.')[1]) as snap:
                        pytest.fail(f"Snapshot of secondary index {si} should have failed")
                except requests.HTTPError:
                    pass

# ...but not when the snapshot is automatic (pre-scrub).
def test_materialized_view_pre_scrub_snapshot(cql, this_dc, rest_api):
    with new_test_keyspace(cql, f"WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', '{this_dc}' : 1 }}") as keyspace:
        schema = 'p int, v text, primary key (p)'
        with new_test_table(cql, keyspace, schema) as table:
            stmt = cql.prepare(f"INSERT INTO {table} (p, v) VALUES (?, ?)")
            cql.execute(stmt, [0, 'hello'])

            with new_materialized_view(cql, table, '*', 'v, p', 'v is not null and p is not null') as mv:
                resp = rest_api.send("GET", f"storage_service/keyspace_scrub/{keyspace}")
                resp.raise_for_status()

            with new_secondary_index(cql, table, 'v') as si:
                resp = rest_api.send("GET", f"storage_service/keyspace_scrub/{keyspace}")
                resp.raise_for_status()

def test_range_to_endpoint_map(cql, this_dc, rest_api):
    with new_test_keyspace(cql, f"WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', '{this_dc}' : 1 }}") as keyspace:
        resp = rest_api.send("GET", f"storage_service/range_to_endpoint_map/{keyspace}")
        resp.raise_for_status()

def test_describe_ring(cql, this_dc, rest_api):
    resp = rest_api.send("GET", "storage_service/describe_ring")
    resp.raise_for_status()

    with new_test_keyspace(cql, f"WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', '{this_dc}' : 1 }}") as keyspace:
        resp = rest_api.send("GET", f"storage_service/describe_ring/{keyspace}")
        resp.raise_for_status()

def test_storage_service_keyspace_cleanup(cql, this_dc, rest_api):
    with new_test_keyspace(cql, f"WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', '{this_dc}' : 1 }}") as keyspace:
        schema = 'p int, v text, primary key (p)'
        with new_test_table(cql, keyspace, schema) as t0:
            stmt = cql.prepare(f"INSERT INTO {t0} (p, v) VALUES (?, ?)")
            cql.execute(stmt, [0, 'hello'])

            with new_test_table(cql, keyspace, schema) as t1:
                stmt = cql.prepare(f"INSERT INTO {t1} (p, v) VALUES (?, ?)")
                cql.execute(stmt, [1, 'world'])

                test_tables = [t0.split('.')[1], t1.split('.')[1]]

                resp = rest_api.send("POST", f"storage_service/keyspace_flush/{keyspace}")
                resp.raise_for_status()

                resp = rest_api.send("POST", f"storage_service/keyspace_cleanup/{keyspace}")
                resp.raise_for_status()

                resp = rest_api.send("POST", f"storage_service/keyspace_cleanup/{keyspace}", { "cf": f"{test_tables[1]}" })
                resp.raise_for_status()

                resp = rest_api.send("POST", f"storage_service/keyspace_cleanup/{keyspace}", { "cf": f"{test_tables[0]},{test_tables[1]}" })
                resp.raise_for_status()

                # non-existing table
                resp = rest_api.send("POST", f"storage_service/keyspace_cleanup/{keyspace}", { "cf": f"{test_tables[0]},XXX" })
                assert resp.status_code == requests.codes.bad_request

                resp = rest_api.send("POST", f"storage_service/keyspace_cleanup/{keyspace}")
                resp.raise_for_status()

def test_storage_service_keyspace_cleanup_with_no_owned_ranges(cql, this_dc, rest_api):
    with new_test_keyspace(cql, f"WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', '{this_dc}' : 1 }}") as keyspace:
        schema = 'p int, v text, primary key (p)'
        with new_test_table(cql, keyspace, schema) as t0:
            stmt = cql.prepare(f"INSERT INTO {t0} (p, v) VALUES (?, ?)")
            cql.execute(stmt, [0, 'hello'])

            # the node fixture may hold snapshots from other tests
            # so keep the tags of the snapshots explicitly taken by this instance
            my_snapshot_tags = dict()

            def make_snapshot_tag(name):
                tag = f"{int(time.time())}-{name}"
                my_snapshot_tags[name] = tag
                return tag

            def snapshot_tag(name):
                return my_snapshot_tags[name]

            resp = rest_api.send("POST", f"storage_service/keyspace_flush/{keyspace}")
            resp.raise_for_status()
            resp = rest_api.send("POST", "storage_service/snapshots", {'kn': keyspace, 'tag': make_snapshot_tag('after-flush')})
            resp.raise_for_status()

            cql.execute(f"ALTER KEYSPACE {keyspace} WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', '{this_dc}' : 0 }}")
            resp = rest_api.send("POST", "storage_service/snapshots", {'kn': keyspace, 'tag': make_snapshot_tag('after-alter-keyspace')})
            resp.raise_for_status()

            resp = rest_api.send("POST", f"storage_service/keyspace_cleanup/{keyspace}")
            resp.raise_for_status()
            resp = rest_api.send("POST", "storage_service/snapshots", {'kn': keyspace, 'tag': make_snapshot_tag('after-cleanup')})
            resp.raise_for_status()

            resp = rest_api.send("GET", "storage_service/snapshots")
            resp.raise_for_status()
            snapshots = dict()
            for p in resp.json():
                key = p['key']
                assert isinstance(key, str), f"key is expected to be a string: {p}"
                if key in my_snapshot_tags.values():
                    value = p['value']
                    if isinstance(value, list):
                        assert len(value) == 1, f"Expecting a single value in {p}"
                        value = value[0]
                    assert isinstance(value, dict), f"value is expected to be a dict: {p}"
                    snapshots[key] = value

            print(f"snapshot metadata: {snapshots}")

            assert snapshots[snapshot_tag('after-flush')]['total'] > 0, f"snapshots after flush should have non-zero data: {snapshots}"
            assert snapshots[snapshot_tag('after-alter-keyspace')]['total'] == snapshots[snapshot_tag('after-flush')]['total'], f"snapshots after alter-keyspace should have the same data as after flush: {snapshots}"
            assert snapshots[snapshot_tag('after-cleanup')]['total'] == 0, f"snapshots after clean should have no data: {snapshots}"

def test_storage_service_keyspace_upgrade_sstables(cql, this_dc, rest_api):
    with new_test_keyspace(cql, f"WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', '{this_dc}' : 1 }}") as keyspace:
        schema = 'p int, v text, primary key (p)'
        with new_test_table(cql, keyspace, schema) as t0:
            stmt = cql.prepare(f"INSERT INTO {t0} (p, v) VALUES (?, ?)")
            cql.execute(stmt, [0, 'hello'])

            with new_test_table(cql, keyspace, schema) as t1:
                stmt = cql.prepare(f"INSERT INTO {t1} (p, v) VALUES (?, ?)")
                cql.execute(stmt, [1, 'world'])

                test_tables = [t0.split('.')[1], t1.split('.')[1]]

                resp = rest_api.send("POST", f"storage_service/keyspace_flush/{keyspace}")
                resp.raise_for_status()

                resp = rest_api.send("GET", f"storage_service/keyspace_upgrade_sstables/{keyspace}")
                resp.raise_for_status()

                resp = rest_api.send("GET", f"storage_service/keyspace_upgrade_sstables/{keyspace}", { "exclude_current_version": "true" })
                resp.raise_for_status()

                resp = rest_api.send("GET", f"storage_service/keyspace_upgrade_sstables/{keyspace}", { "cf": f"{test_tables[1]}" })
                resp.raise_for_status()

                resp = rest_api.send("GET", f"storage_service/keyspace_upgrade_sstables/{keyspace}", { "cf": f"{test_tables[0]},{test_tables[1]}" })
                resp.raise_for_status()

                # non-existing table
                resp = rest_api.send("GET", f"storage_service/keyspace_upgrade_sstables/{keyspace}", { "cf": f"{test_tables[0]},XXX" })
                assert resp.status_code == requests.codes.bad_request

def test_storage_service_system_keyspace_repair(rest_api):
    resp = rest_api.send("POST", "storage_service/repair_async/system")
    resp.raise_for_status()
    sequence_number = resp.json()
    assert sequence_number > 0, "Repair got invalid sequence number"

    resp = rest_api.send("GET", "task_manager/list_module_tasks/repair")
    resp.raise_for_status()
    assert not [stats for stats in resp.json() if stats["sequence_number"] == sequence_number], "Repair task for keyspace with local replication strategy was created"
