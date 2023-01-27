/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#include "debug.hh"

namespace debug {

seastar::sharded<replica::database>* the_database = nullptr;

}
