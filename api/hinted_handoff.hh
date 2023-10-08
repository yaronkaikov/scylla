/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#pragma once

#include <seastar/core/sharded.hh>
#include "api.hh"

namespace service { class storage_proxy; }

namespace api {

void set_hinted_handoff(http_context& ctx, httpd::routes& r, sharded<service::storage_proxy>& p);
void unset_hinted_handoff(http_context& ctx, httpd::routes& r);

}
