#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
from pathlib import PosixPath

from pytest import Collector

from test.pylib.cpp.boost.boost_facade import BoostTestFacade
from test.pylib.cpp.common_cpp_conftest import collect_items


def pytest_collect_file(file_path: PosixPath, parent: Collector):
    """
    Method triggered automatically by pytest to collect files from a directory.
    These tests can use BoostFacade since they're Boost tests located in different directory.
    """
    if file_path.suffix == '.cc':
        return collect_items(file_path, parent, facade=BoostTestFacade(parent.config))
