/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#pragma once

#include "api.hh"

namespace locator {
class snitch_ptr;
}

namespace api {

void set_endpoint_snitch(http_context& ctx, httpd::routes& r, sharded<locator::snitch_ptr>&);
void unset_endpoint_snitch(http_context& ctx, httpd::routes& r);

}
