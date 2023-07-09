/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */


#pragma once

#include "cql3/expr/expression.hh"
#include "data_dictionary/data_dictionary.hh"

namespace cql3::selection {

expr::expression make_count_rows_function_expression();
bool selectable_processes_selection(const expr::expression& raw_selectable);

}
