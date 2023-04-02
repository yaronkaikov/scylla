/*
 * Copyright (C) 2019 ScyllaDB
 *
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#include "sstables/sstables.hh"
#include "sstables/sstable_set.hh"
#include "compaction.hh"
#include "compaction_manager.hh"
#include "incremental_compaction_strategy.hh"
#include "incremental_backlog_tracker.hh"
#include <boost/range/numeric.hpp>
#include <boost/range/algorithm.hpp>
#include <boost/range/adaptors.hpp>

namespace sstables {

extern logging::logger clogger;

uint64_t incremental_compaction_strategy::avg_size(std::vector<sstables::sstable_run>& runs) const {
    uint64_t n = 0;

    if (runs.empty()) {
        return 0;
    }
    for (auto& r : runs) {
        n += r.data_size();
    }
    return n / runs.size();
}

bool incremental_compaction_strategy::is_bucket_interesting(const std::vector<sstables::sstable_run>& bucket, size_t min_threshold) {
    return bucket.size() >= min_threshold;
}

bool incremental_compaction_strategy::is_any_bucket_interesting(const std::vector<std::vector<sstables::sstable_run>>& buckets, size_t min_threshold) const {
    return boost::algorithm::any_of(buckets, [&] (const std::vector<sstables::sstable_run>& bucket) {
        return this->is_bucket_interesting(bucket, min_threshold);
    });
}

std::vector<sstable_run_and_length>
incremental_compaction_strategy::create_run_and_length_pairs(const std::vector<sstables::sstable_run>& runs) {

    std::vector<sstable_run_and_length> run_length_pairs;
    run_length_pairs.reserve(runs.size());

    for(auto& r : runs) {
        assert(r.data_size() != 0);
        run_length_pairs.emplace_back(r, r.data_size());
    }

    return run_length_pairs;
}

std::vector<std::vector<sstables::sstable_run>>
incremental_compaction_strategy::get_buckets(const std::vector<sstables::sstable_run>& runs, const incremental_compaction_strategy_options& options) {
    auto sorted_runs = create_run_and_length_pairs(runs);

    std::sort(sorted_runs.begin(), sorted_runs.end(), [] (sstable_run_and_length& i, sstable_run_and_length& j) {
        return i.second < j.second;
    });

    using bucket_type = std::vector<sstables::sstable_run>;
    std::vector<bucket_type> bucket_list;
    std::vector<double> bucket_average_size_list;

    for (auto& pair : sorted_runs) {
        size_t size = pair.second;

        // look for a bucket containing similar-sized runs:
        // group in the same bucket if it's w/in (bucket_low, bucket_high) of the average for this bucket,
        // or this file and the bucket are all considered "small" (less than `minSSTableSize`)
        if (!bucket_list.empty()) {
            auto& bucket_average_size = bucket_average_size_list.back();

            if ((size > (bucket_average_size * options.bucket_low) && size < (bucket_average_size * options.bucket_high)) ||
                    (size < options.min_sstable_size && bucket_average_size < options.min_sstable_size)) {
                auto& bucket = bucket_list.back();
                auto total_size = bucket.size() * bucket_average_size;
                auto new_average_size = (total_size + size) / (bucket.size() + 1);
                auto smallest_run_in_bucket = bucket[0].data_size();

                // SSTables are added in increasing size order so the bucket's
                // average might drift upwards.
                // Don't let it drift too high, to a point where the smallest
                // SSTable might fall out of range.
                if (size < options.min_sstable_size || smallest_run_in_bucket > new_average_size * options.bucket_low) {
                    bucket.push_back(pair.first);
                    bucket_average_size = new_average_size;
                    continue;
                }
            }
        }

        // no similar bucket found; put it in a new one
        bucket_type new_bucket = {pair.first};
        bucket_list.push_back(std::move(new_bucket));
        bucket_average_size_list.push_back(size);
    }

    return bucket_list;
}

std::vector<sstables::sstable_run>
incremental_compaction_strategy::most_interesting_bucket(std::vector<std::vector<sstables::sstable_run>> buckets,
        size_t min_threshold, size_t max_threshold)
{
    std::vector<sstable_run_bucket_and_length> interesting_buckets;
    interesting_buckets.reserve(buckets.size());

    for (auto& bucket : buckets) {
        bucket.resize(std::min(bucket.size(), max_threshold));
        if (is_bucket_interesting(bucket, min_threshold)) {
            auto avg = avg_size(bucket);
            interesting_buckets.push_back({ std::move(bucket), avg });
        }
    }

    if (interesting_buckets.empty()) {
        return std::vector<sstables::sstable_run>();
    }
    // Pick the bucket with more elements, as efficiency of same-tier compactions increases with number of files.
    auto& max = *std::max_element(interesting_buckets.begin(), interesting_buckets.end(),
                    [] (sstable_run_bucket_and_length& i, sstable_run_bucket_and_length& j) {
        return i.first.size() < j.first.size();
    });
    return std::move(max.first);
}

compaction_descriptor
incremental_compaction_strategy::find_garbage_collection_job(const compaction::table_state& t, std::vector<size_bucket_t>& buckets) {
    auto worth_dropping_tombstones = [this, now = db_clock::now()] (const sstable_run& run, gc_clock::time_point gc_before) {
        // for the purpose of checking if a run is stale, picking any fragment *composing the same run*
        // will be enough as the difference in write time is acceptable.
        if (run.all().empty() || (now-_tombstone_compaction_interval < (*run.all().begin())->data_file_write_time())) {
            return false;
        }
        return run.estimate_droppable_tombstone_ratio(gc_before) >= _tombstone_threshold;
    };
    auto gc_before = gc_clock::now() - t.schema()->gc_grace_seconds();
    auto can_garbage_collect = [&] (const size_bucket_t& bucket) {
        return boost::algorithm::any_of(bucket, [&] (const sstable_run& r) {
            return worth_dropping_tombstones(r, gc_before);
        });
    };

    // To make sure that expired tombstones are persisted in a timely manner, ICS will cross-tier compact
    // two closest-in-size buckets such that tombstones will eventually reach the top of the LSM tree,
    // making it possible to purge them.

    // Start from the largest tier as it's more likely to satisfy conditions for tombstones to be purged.
    auto it = buckets.rbegin();
    for (; it != buckets.rend(); it++) {
        if (can_garbage_collect(*it)) {
            break;
        }
    }
    if (it == buckets.rend()) {
        clogger.debug("ICS: nothing to garbage collect in {} buckets for {}.{}", buckets.size(), t.schema()->ks_name(), t.schema()->cf_name());
        return compaction_descriptor();
    }

    size_bucket_t& first_bucket = *it;
    std::vector<sstables::sstable_run> input = std::move(first_bucket);

    if (buckets.size() >= 2) {
        // If the largest tier needs GC, then compact it with the second largest.
        // Any smaller tier needing GC will be compacted with the larger and closest-in-size one.
        // It's done this way to reduce write amplification and satisfy conditions for purging tombstones.
        it = it == buckets.rbegin() ? std::next(it) : std::prev(it);

        size_bucket_t& second_bucket = *it;

        input.reserve(input.size() + second_bucket.size());
        std::move(second_bucket.begin(), second_bucket.end(), std::back_inserter(input));
    }
    clogger.debug("ICS: starting garbage collection on {} runs for {}.{}", input.size(), t.schema()->ks_name(), t.schema()->cf_name());

    return compaction_descriptor(runs_to_sstables(std::move(input)), service::get_local_compaction_priority(), 0, _fragment_size);
}

compaction_descriptor
incremental_compaction_strategy::get_sstables_for_compaction(table_state& t, strategy_control& control, std::vector<sstables::shared_sstable> candidates) {
    // make local copies so they can't be changed out from under us mid-method
    size_t min_threshold = t.min_compaction_threshold();
    size_t max_threshold = t.schema()->max_compaction_threshold();

    auto buckets = get_buckets(sstables_to_runs(std::move(candidates)));

    if (is_any_bucket_interesting(buckets, min_threshold)) {
        std::vector<sstables::sstable_run> most_interesting = most_interesting_bucket(std::move(buckets), min_threshold, max_threshold);
        return sstables::compaction_descriptor(runs_to_sstables(std::move(most_interesting)), service::get_local_compaction_priority(), 0, _fragment_size);
    }
    // If we are not enforcing min_threshold explicitly, try any pair of sstable runs in the same tier.
    if (!t.compaction_enforce_min_threshold() && is_any_bucket_interesting(buckets, 2)) {
        std::vector<sstables::sstable_run> most_interesting = most_interesting_bucket(std::move(buckets), 2, max_threshold);
        return sstables::compaction_descriptor(runs_to_sstables(std::move(most_interesting)), service::get_local_compaction_priority(), 0, _fragment_size);
    }

    // The cross-tier behavior is only triggered once we're done with all the pending same-tier compaction to
    // increase overall efficiency.
    if (control.has_ongoing_compaction(t)) {
        return sstables::compaction_descriptor();
    }

    auto desc = find_garbage_collection_job(t, buckets);
    if (!desc.sstables.empty()) {
        return desc;
    }

    if (_space_amplification_goal) {
        if (buckets.size() < 2) {
            return sstables::compaction_descriptor();
        }
        // Let S0 be the size of largest tier
        // Let S1 be the size of second-largest tier,
        // SA will be (S0 + S1) / S0

        // Don't try SAG if there's an ongoing compaction, because if largest tier is being compacted,
        // SA would be calculated incorrectly, which may result in an unneeded cross-tier compaction.

        auto find_two_largest_tiers = [this] (std::vector<size_bucket_t>&& buckets) -> std::tuple<size_bucket_t, size_bucket_t> {
            std::partial_sort(buckets.begin(), buckets.begin()+2, buckets.end(), [this] (size_bucket_t& i, size_bucket_t& j) {
                return avg_size(i) > avg_size(j); // descending order
            });
            return { std::move(buckets[0]), std::move(buckets[1]) };
        };

        auto total_size = [] (const size_bucket_t& bucket) -> uint64_t {
            return boost::accumulate(bucket | boost::adaptors::transformed(std::mem_fn(&sstable_run::data_size)), uint64_t(0));
        };

        auto [s0, s1] = find_two_largest_tiers(std::move(buckets));
        uint64_t s0_size = total_size(s0), s1_size = total_size(s1);
        double space_amplification = double(s0_size + s1_size) / s0_size;

        if (space_amplification > _space_amplification_goal) {
            clogger.debug("ICS: doing cross-tier compaction of two largest tiers, to reduce SA {} to below SAG {}",
                          space_amplification, *_space_amplification_goal);
            // Aims at reducing space amplification, to below SAG, by compacting together the two largest tiers
            std::vector<sstables::sstable_run> cross_tier_input = std::move(s0);
            cross_tier_input.reserve(cross_tier_input.size() + s1.size());
            std::move(s1.begin(), s1.end(), std::back_inserter(cross_tier_input));

            return sstables::compaction_descriptor(runs_to_sstables(std::move(cross_tier_input)),
                                                   service::get_local_compaction_priority(), 0, _fragment_size);
        }
    }

    return sstables::compaction_descriptor();
}

compaction_descriptor
incremental_compaction_strategy::get_major_compaction_job(table_state& t, std::vector<sstables::shared_sstable> candidates) {
    if (candidates.empty()) {
        return compaction_descriptor();
    }
    return compaction_descriptor(std::move(candidates), service::get_local_compaction_priority(), 0, _fragment_size);
}

int64_t incremental_compaction_strategy::estimated_pending_compactions(table_state& t) const {
    size_t min_threshold = t.schema()->min_compaction_threshold();
    size_t max_threshold = t.schema()->max_compaction_threshold();
    std::vector<sstables::shared_sstable> sstables;
    int64_t n = 0;

    sstables.reserve(t.main_sstable_set().all()->size());
    for (auto all_sstables = t.main_sstable_set(); auto entry : *all_sstables.all()) {
        sstables.push_back(entry);
    }

    for (auto& bucket : get_buckets(t.main_sstable_set().select_sstable_runs(sstables))) {
        if (bucket.size() >= min_threshold) {
            n += (bucket.size() + max_threshold - 1) / max_threshold;
        }
    }
    return n;
}

std::vector<shared_sstable>
incremental_compaction_strategy::runs_to_sstables(std::vector<sstable_run> runs) {
    return boost::accumulate(runs, std::vector<shared_sstable>(), [&] (std::vector<shared_sstable>&& v, const sstable_run& run) {
        v.insert(v.end(), run.all().begin(), run.all().end());
        return std::move(v);
    });
}

std::vector<sstable_run>
incremental_compaction_strategy::sstables_to_runs(std::vector<shared_sstable> sstables) {
    std::unordered_map<sstables::run_id, sstable_run> runs;
    for (auto&& sst : sstables) {
        runs[sst->run_identifier()].insert(std::move(sst));
    }
    return boost::copy_range<std::vector<sstable_run>>(runs | boost::adaptors::map_values);
}

void incremental_compaction_strategy::sort_run_bucket_by_first_key(size_bucket_t& bucket, size_t max_elements, const schema_ptr& schema) {
    std::partial_sort(bucket.begin(), bucket.begin() + max_elements, bucket.end(), [&schema](const sstable_run& a, const sstable_run& b) {
        auto sst_first_key_less = [&schema] (const shared_sstable& sst_a, const shared_sstable& sst_b) {
            return sst_a->get_first_decorated_key().tri_compare(*schema, sst_b->get_first_decorated_key()) <= 0;
        };
        auto& a_first = *boost::min_element(a.all(), sst_first_key_less);
        auto& b_first = *boost::min_element(b.all(), sst_first_key_less);
        return a_first->get_first_decorated_key().tri_compare(*schema, b_first->get_first_decorated_key()) <= 0;
    });
}

compaction_descriptor
incremental_compaction_strategy::get_reshaping_job(std::vector<shared_sstable> input, schema_ptr schema, const ::io_priority_class& iop, reshape_mode mode) const {
    size_t offstrategy_threshold = std::max(schema->min_compaction_threshold(), 4);
    size_t max_sstables = std::max(schema->max_compaction_threshold(), int(offstrategy_threshold));

    if (mode == reshape_mode::relaxed) {
        offstrategy_threshold = max_sstables;
    }

    auto run_count = boost::copy_range<std::unordered_set<run_id>>(input | boost::adaptors::transformed(std::mem_fn(&sstable::run_identifier))).size();
    if (run_count >= offstrategy_threshold && mode == reshape_mode::strict) {
        std::sort(input.begin(), input.end(), [&schema] (const shared_sstable& a, const shared_sstable& b) {
            return dht::ring_position(a->get_first_decorated_key()).less_compare(*schema, dht::ring_position(b->get_first_decorated_key()));
        });
        // All sstables can be reshaped at once if the amount of overlapping will not cause memory usage to be high,
        // which is possible because partitioned set is able to incrementally open sstables during compaction
        if (sstable_set_overlapping_count(schema, input) <= max_sstables) {
            compaction_descriptor desc(std::move(input), iop, 0/* level */, _fragment_size);
            desc.options = compaction_type_options::make_reshape();
            return desc;
        }
    }

    for (auto& bucket : get_buckets(sstables_to_runs(std::move(input)))) {
        if (bucket.size() >= offstrategy_threshold) {
            // preserve token contiguity by prioritizing runs with the lowest first keys.
            if (bucket.size() > max_sstables) {
                sort_run_bucket_by_first_key(bucket, max_sstables, schema);
                bucket.resize(max_sstables);
            }
            compaction_descriptor desc(runs_to_sstables(std::move(bucket)), iop, 0/* level */, _fragment_size);
            desc.options = compaction_type_options::make_reshape();
            return desc;
        }
    }

    return compaction_descriptor();
}

std::vector<compaction_descriptor>
incremental_compaction_strategy::get_cleanup_compaction_jobs(table_state& t, std::vector<shared_sstable> candidates) const {
    std::vector<compaction_descriptor> ret;
    const auto& schema = t.schema();
    unsigned max_threshold = schema->max_compaction_threshold();

    for (auto& bucket : get_buckets(sstables_to_runs(std::move(candidates)))) {
        if (bucket.size() > max_threshold) {
            // preserve token contiguity
            sort_run_bucket_by_first_key(bucket, bucket.size(), schema);
        }
        auto it = bucket.begin();
        while (it != bucket.end()) {
            unsigned remaining = std::distance(it, bucket.end());
            unsigned needed = std::min(remaining, max_threshold);
            std::vector<sstable_run> runs;
            std::move(it, it + needed, std::back_inserter(runs));
            ret.push_back(compaction_descriptor(runs_to_sstables(std::move(runs)), service::get_local_compaction_priority(), 0/* level */, _fragment_size));
            std::advance(it, needed);
        }
    }
    return ret;
}

std::unique_ptr<compaction_backlog_tracker::impl>
incremental_compaction_strategy::make_backlog_tracker() const {
    return std::make_unique<incremental_backlog_tracker>(_options);
}

incremental_compaction_strategy::incremental_compaction_strategy(const std::map<sstring, sstring>& options)
    : compaction_strategy_impl(options)
    , _options(options)
{
    using namespace cql3::statements;
    auto option_value = compaction_strategy_impl::get_value(options, FRAGMENT_SIZE_OPTION);
    auto fragment_size_in_mb = property_definitions::to_int(FRAGMENT_SIZE_OPTION, option_value, DEFAULT_MAX_FRAGMENT_SIZE_IN_MB);

    if (fragment_size_in_mb < 100) {
        clogger.warn("SStable size of {}MB is configured. The value may lead to sstable run having an substatial amount of fragments, leading to undesired overhead.",
                     fragment_size_in_mb);
    }
    _fragment_size = fragment_size_in_mb*1024*1024;

    if ((option_value = compaction_strategy_impl::get_value(options, SPACE_AMPLIFICATION_GOAL_OPTION))) {
        _space_amplification_goal = property_definitions::to_double(SPACE_AMPLIFICATION_GOAL_OPTION, option_value, 0.0);
        if (_space_amplification_goal <= 1.0) {
            throw exceptions::configuration_exception("Incremental Compaction Strategy - " \
                "value of space_amplification_goal must be greater than 1.");
        }
    }
}

}
