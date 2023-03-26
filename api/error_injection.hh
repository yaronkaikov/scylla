/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#pragma once

#include "api.hh"

namespace api {

void set_error_injection(http_context& ctx, httpd::routes& r);

}
