/*
 * Copyright (C) 2019-present ScyllaDB
 *
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#pragma once

#include <cmath>

#include "compaction_backlog_manager.hh"
#include "incremental_compaction_strategy.hh"

using namespace sstables;

// The only difference to size tiered backlog tracker is that it will calculate
// backlog contribution using total bytes of each sstable run instead of total
// bytes of an individual sstable object.
class incremental_backlog_tracker final : public compaction_backlog_tracker::impl {
    incremental_compaction_strategy_options _options;
    int64_t _total_bytes = 0;
    int64_t _total_backlog_bytes = 0;
    unsigned _threshold = 0;
    double _sstables_backlog_contribution = 0.0f;
    std::unordered_set<sstables::run_id> _sstable_runs_contributing_backlog;
    std::unordered_map<sstables::run_id, sstable_run> _all;

    struct inflight_component {
        int64_t total_bytes = 0;
        double contribution = 0;
    };

    inflight_component compacted_backlog(const compaction_backlog_tracker::ongoing_compactions& ongoing_compactions) const;

public:
    double log4(double x) const {
        static const double inv_log_4 = 1.0f / std::log(4);
        return log(x) * inv_log_4;
    }

    void refresh_sstables_backlog_contribution();

    incremental_backlog_tracker(incremental_compaction_strategy_options options);

    virtual double backlog(const compaction_backlog_tracker::ongoing_writes& ow, const compaction_backlog_tracker::ongoing_compactions& oc) const override;

    // Removing could be the result of a failure of an in progress write, successful finish of a
    // compaction, or some one-off operation, like drop
    virtual void replace_sstables(std::vector<sstables::shared_sstable> old_ssts, std::vector<sstables::shared_sstable> new_ssts) override;

    int64_t total_bytes() const {
        return _total_bytes;
    }
};
