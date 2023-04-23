/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#pragma once

#include <seastar/core/sharded.hh>
#include "api.hh"

namespace service { class storage_service; }

namespace api {

void set_storage_proxy(http_context& ctx, httpd::routes& r, sharded<service::storage_service>& ss);
void unset_storage_proxy(http_context& ctx, httpd::routes& r);

}
