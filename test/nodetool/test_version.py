#
# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: ScyllaDB-Proprietary
#

from rest_api_mock import expected_request


def test_version(nodetool):
    out = nodetool("version", expected_requests=[
        expected_request("GET", "/storage_service/release_version", response="1.2.3")])

    assert out == "ReleaseVersion: 1.2.3\n"
