/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#pragma once

#include "mutation_consumer_concepts.hh"

enum class consume_in_reverse {
    no = 0,
    yes,
};
