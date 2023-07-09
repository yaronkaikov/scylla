/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (ScyllaDB-Proprietary and Apache-2.0)
 */

#pragma once

#include <vector>
#include "query-request.hh"
#include "types/types.hh"
#include "schema/schema_fwd.hh"
#include "counters.hh"
#include "cql3/expr/expression.hh"

namespace cql3 {

namespace selection {


// An entry in the SELECT clause.
struct prepared_selector {
    expr::expression expr;
    ::shared_ptr<column_identifier> alias;
};

bool processes_selection(const prepared_selector&);


}

}
