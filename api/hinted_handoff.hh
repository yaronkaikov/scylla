/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#pragma once

#include "api.hh"

namespace gms {

class gossiper;

}

namespace api {

void set_hinted_handoff(http_context& ctx, httpd::routes& r, gms::gossiper& g);
void unset_hinted_handoff(http_context& ctx, httpd::routes& r);

}
