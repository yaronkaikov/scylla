/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#pragma once

namespace sstables {

enum class compaction_strategy_type {
    null,
    size_tiered,
    leveled,
    time_window,
    in_memory,
    incremental,
};

enum class reshape_mode { strict, relaxed };
}
