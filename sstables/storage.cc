/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "storage.hh"

#include <cerrno>
#include <boost/algorithm/string.hpp>

#include <exception>
#include <seastar/coroutine/exception.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/util/file.hh>

#include "sstables/exceptions.hh"
#include "sstables/sstables_manager.hh"
#include "sstables/sstable_version.hh"
#include "sstables/integrity_checked_file_impl.hh"
#include "sstables/writer.hh"
#include "db/system_keyspace.hh"
#include "utils/lister.hh"
#include "utils/overloaded_functor.hh"
#include "utils/memory_data_sink.hh"
#include "utils/s3/client.hh"

#include "checked-file-impl.hh"

namespace sstables {

// cannot define these classes in an anonymous namespace, as we need to
// declare these storage classes as "friend" of class sstable
class filesystem_storage final : public sstables::storage {
    sstring dir;
    std::optional<sstring> temp_dir; // Valid while the sstable is being created, until sealed

private:
    using mark_for_removal = bool_class<class mark_for_removal_tag>;


    future<> check_create_links_replay(const sstable& sst, const sstring& dst_dir, generation_type dst_gen, const std::vector<std::pair<sstables::component_type, sstring>>& comps) const;
    future<> remove_temp_dir();
    virtual future<> create_links(const sstable& sst, const sstring& dir) const override;
    future<> create_links_common(const sstable& sst, sstring dst_dir, generation_type dst_gen, mark_for_removal mark_for_removal) const;
    future<> touch_temp_dir(const sstable& sst);
    future<> move(const sstable& sst, sstring new_dir, generation_type generation, delayed_commit_changes* delay) override;

    virtual void change_dir_for_test(sstring nd) override {
        dir = std::move(nd);
    }

public:
    explicit filesystem_storage(sstring dir_) : dir(std::move(dir_)) {}

    virtual future<> seal(const sstable& sst) override;
    virtual future<> snapshot(const sstable& sst, sstring dir, absolute_path abs) const override;
    virtual future<> change_state(const sstable& sst, sstring to, generation_type generation, delayed_commit_changes* delay) override;
    // runs in async context
    virtual void open(sstable& sst, const io_priority_class& pc) override;
    virtual future<> wipe(const sstable& sst) noexcept override;
    virtual future<file> open_component(const sstable& sst, component_type type, open_flags flags, file_open_options options, bool check_integrity) override;
    virtual future<data_sink> make_data_or_index_sink(sstable& sst, component_type type, io_priority_class pc) override;
    virtual future<data_sink> make_component_sink(sstable& sst, component_type type, open_flags oflags, file_output_stream_options options) override;
    virtual future<> destroy(const sstable& sst) override { return make_ready_future<>(); }

    virtual sstring prefix() const override { return dir; }
};

future<data_sink> filesystem_storage::make_data_or_index_sink(sstable& sst, component_type type, io_priority_class pc) {
    file_output_stream_options options;
    options.io_priority_class = pc;
    options.buffer_size = sst.sstable_buffer_size;
    options.write_behind = 10;

    assert(type == component_type::Data || type == component_type::Index);
    return make_file_data_sink(type == component_type::Data ? std::move(sst._data_file) : std::move(sst._index_file), options);
}

future<data_sink> filesystem_storage::make_component_sink(sstable& sst, component_type type, open_flags oflags, file_output_stream_options options) {
    return sst.new_sstable_component_file(sst._write_error_handler, type, oflags).then([options = std::move(options)] (file f) mutable {
        return make_file_data_sink(std::move(f), std::move(options));
    });
}

static future<file> open_sstable_component_file_non_checked(std::string_view name, open_flags flags, file_open_options options,
        bool check_integrity) noexcept {
    if (flags != open_flags::ro && check_integrity) {
        return open_integrity_checked_file_dma(name, flags, options);
    }
    return open_file_dma(name, flags, options);
}

future<file> filesystem_storage::open_component(const sstable& sst, component_type type, open_flags flags, file_open_options options, bool check_integrity) {
    auto create_flags = open_flags::create | open_flags::exclusive;
    auto readonly = (flags & create_flags) != create_flags;
    auto tgt_dir = !readonly && temp_dir ? dir + "/" + sstable::sst_dir_basename(sst._generation) : dir;
    auto name = sst.filename(tgt_dir, type);

    auto f = open_sstable_component_file_non_checked(name, flags, options, check_integrity);

    if (!readonly) {
        f = with_file_close_on_failure(std::move(f), [&sst, type, name = std::move(name)] (file fd) mutable {
            return sst.rename_new_sstable_component_file(name, sst.filename(type)).then([fd = std::move(fd)] () mutable {
                return make_ready_future<file>(std::move(fd));
            });
        });
    }

    return f;
}

void filesystem_storage::open(sstable& sst, const io_priority_class& pc) {
    touch_temp_dir(sst).get0();
    auto file_path = sst.filename(component_type::TemporaryTOC);

    // Writing TOC content to temporary file.
    // If creation of temporary TOC failed, it implies that that boot failed to
    // delete a sstable with temporary for this column family, or there is a
    // sstable being created in parallel with the same generation.
    file_output_stream_options options;
    options.buffer_size = 4096;
    options.io_priority_class = pc;
    auto w = sst.make_component_file_writer(component_type::TemporaryTOC, std::move(options)).get0();

    bool toc_exists = file_exists(sst.filename(component_type::TOC)).get0();
    if (toc_exists) {
        // TOC will exist at this point if write_components() was called with
        // the generation of a sstable that exists.
        w.close();
        remove_file(file_path).get();
        throw std::runtime_error(format("SSTable write failed due to existence of TOC file for generation {:d} of {}.{}", sst._generation, sst._schema->ks_name(), sst._schema->cf_name()));
    }

    sst.write_toc(std::move(w));

    // Flushing parent directory to guarantee that temporary TOC file reached
    // the disk.
    sst.sstable_write_io_check(sync_directory, dir).get();
}

future<> filesystem_storage::seal(const sstable& sst) {
    // SSTable sealing is about renaming temporary TOC file after guaranteeing
    // that each component reached the disk safely.
    co_await remove_temp_dir();
    auto dir_f = co_await open_checked_directory(sst._write_error_handler, dir);
    // Guarantee that every component of this sstable reached the disk.
    co_await sst.sstable_write_io_check([&] { return dir_f.flush(); });
    // Rename TOC because it's no longer temporary.
    co_await sst.sstable_write_io_check(rename_file, sst.filename(component_type::TemporaryTOC), sst.filename(component_type::TOC));
    co_await sst.sstable_write_io_check([&] { return dir_f.flush(); });
    co_await sst.sstable_write_io_check([&] { return dir_f.close(); });
    // If this point was reached, sstable should be safe in disk.
    sstlog.debug("SSTable with generation {} of {}.{} was sealed successfully.", sst._generation, sst._schema->ks_name(), sst._schema->cf_name());
}

future<> filesystem_storage::touch_temp_dir(const sstable& sst) {
    if (temp_dir) {
        return make_ready_future<>();
    }
    auto tmp = dir + "/" + sstable::sst_dir_basename(sst._generation);
    sstlog.debug("Touching temp_dir={}", tmp);
    auto fut = sst.sstable_touch_directory_io_check(tmp);
    return fut.then([this, tmp = std::move(tmp)] () mutable {
        temp_dir = std::move(tmp);
    });
}

future<> filesystem_storage::remove_temp_dir() {
    if (!temp_dir) {
        return make_ready_future<>();
    }
    sstlog.debug("Removing temp_dir={}", temp_dir);
    return remove_file(*temp_dir).then_wrapped([this] (future<> f) {
        if (!f.failed()) {
            temp_dir.reset();
            return make_ready_future<>();
        }
        auto ep = f.get_exception();
        sstlog.error("Could not remove temporary directory: {}", ep);
        return make_exception_future<>(ep);
    });
}

static bool is_same_file(const seastar::stat_data& sd1, const seastar::stat_data& sd2) noexcept {
    return sd1.device_id == sd2.device_id && sd1.inode_number == sd2.inode_number;
}

static future<bool> same_file(sstring path1, sstring path2) noexcept {
    return when_all_succeed(file_stat(std::move(path1)), file_stat(std::move(path2))).then_unpack([] (seastar::stat_data sd1, seastar::stat_data sd2) {
        return is_same_file(sd1, sd2);
    });
}

// support replay of link by considering link_file EEXIST error as successful when the newpath is hard linked to oldpath.
future<> idempotent_link_file(sstring oldpath, sstring newpath) noexcept {
    bool exists = false;
    std::exception_ptr ex;
    try {
        co_await link_file(oldpath, newpath);
    } catch (const std::system_error& e) {
        ex = std::current_exception();
        exists = (e.code().value() == EEXIST);
    } catch (...) {
        ex = std::current_exception();
    }
    if (!ex) {
        co_return;
    }
    if (exists && (co_await same_file(oldpath, newpath))) {
        co_return;
    }
    co_await coroutine::return_exception_ptr(std::move(ex));
}

// Check is the operation is replayed, possibly when moving sstables
// from staging to the base dir, for example, right after create_links completes,
// and right before deleting the source links.
// We end up in two valid sstables in this case, so make create_links idempotent.
future<> filesystem_storage::check_create_links_replay(const sstable& sst, const sstring& dst_dir, generation_type dst_gen,
        const std::vector<std::pair<sstables::component_type, sstring>>& comps) const {
    return parallel_for_each(comps, [this, &sst, &dst_dir, dst_gen] (const auto& p) mutable {
        auto comp = p.second;
        auto src = sstable::filename(dir, sst._schema->ks_name(), sst._schema->cf_name(), sst._version, sst._generation, sst._format, comp);
        auto dst = sstable::filename(dst_dir, sst._schema->ks_name(), sst._schema->cf_name(), sst._version, dst_gen, sst._format, comp);
        return do_with(std::move(src), std::move(dst), [this] (const sstring& src, const sstring& dst) mutable {
            return file_exists(dst).then([&, this] (bool exists) mutable {
                if (!exists) {
                    return make_ready_future<>();
                }
                return same_file(src, dst).then_wrapped([&, this] (future<bool> fut) {
                    if (fut.failed()) {
                        auto eptr = fut.get_exception();
                        sstlog.error("Error while linking SSTable: {} to {}: {}", src, dst, eptr);
                        return make_exception_future<>(eptr);
                    }
                    auto same = fut.get0();
                    if (!same) {
                        auto msg = format("Error while linking SSTable: {} to {}: File exists", src, dst);
                        sstlog.error("{}", msg);
                        return make_exception_future<>(malformed_sstable_exception(msg, dir));
                    }
                    return make_ready_future<>();
                });
            });
        });
    });
}

/// create_links_common links all component files from the sstable directory to
/// the given destination directory, using the provided generation.
///
/// It first checks if this is a replay of a previous
/// create_links call, by testing if the destination names already
/// exist, and if so, if they point to the same inodes as the
/// source names.  Otherwise, we return an error.
/// This is an indication that something went wrong.
///
/// Creating the links is done by:
/// First, linking the source TOC component to the destination TemporaryTOC,
/// to mark the destination for rollback, in case we crash mid-way.
/// Then, all components are linked.
///
/// Note that if scylla crashes at this point, the destination SSTable
/// will have both a TemporaryTOC file and a regular TOC file.
/// It should be deleted on restart, thus rolling the operation backwards.
///
/// Eventually, if \c mark_for_removal is unset, the detination
/// TemporaryTOC is removed, to "commit" the destination sstable;
///
/// Otherwise, if \c mark_for_removal is set, the TemporaryTOC at the destination
/// is moved to the source directory to mark the source sstable for removal,
/// thus atomically toggling crash recovery from roll-back to roll-forward.
///
/// Similar to the scenario described above, crashing at this point
/// would leave the source sstable marked for removal, possibly
/// having both a TemporaryTOC file and a regular TOC file, and
/// then the source sstable should be deleted on restart, rolling the
/// operation forward.
///
/// Note that idempotent versions of link_file and rename_file
/// are used.  These versions handle EEXIST errors that may happen
/// when the respective operations are replayed.
///
/// \param sst - the sstable to work on
/// \param dst_dir - the destination directory.
/// \param generation - the generation of the destination sstable
/// \param mark_for_removal - mark the sstable for removal after linking it to the destination dst_dir
future<> filesystem_storage::create_links_common(const sstable& sst, sstring dst_dir, generation_type generation, mark_for_removal mark_for_removal) const {
    sstlog.trace("create_links: {} -> {} generation={} mark_for_removal={}", sst.get_filename(), dst_dir, generation, mark_for_removal);
    auto comps = sst.all_components();
    co_await check_create_links_replay(sst, dst_dir, generation, comps);
    // TemporaryTOC is always first, TOC is always last
    auto dst = sstable::filename(dst_dir, sst._schema->ks_name(), sst._schema->cf_name(), sst._version, generation, sst._format, component_type::TemporaryTOC);
    co_await sst.sstable_write_io_check(idempotent_link_file, sst.filename(component_type::TOC), std::move(dst));
    co_await sst.sstable_write_io_check(sync_directory, dst_dir);
    co_await parallel_for_each(comps, [this, &sst, &dst_dir, generation] (auto p) {
        auto src = sstable::filename(dir, sst._schema->ks_name(), sst._schema->cf_name(), sst._version, sst._generation, sst._format, p.second);
        auto dst = sstable::filename(dst_dir, sst._schema->ks_name(), sst._schema->cf_name(), sst._version, generation, sst._format, p.second);
        return sst.sstable_write_io_check(idempotent_link_file, std::move(src), std::move(dst));
    });
    co_await sst.sstable_write_io_check(sync_directory, dst_dir);
    auto dst_temp_toc = sstable::filename(dst_dir, sst._schema->ks_name(), sst._schema->cf_name(), sst._version, generation, sst._format, component_type::TemporaryTOC);
    if (mark_for_removal) {
        // Now that the source sstable is linked to new_dir, mark the source links for
        // deletion by leaving a TemporaryTOC file in the source directory.
        auto src_temp_toc = sstable::filename(dir, sst._schema->ks_name(), sst._schema->cf_name(), sst._version, sst._generation, sst._format, component_type::TemporaryTOC);
        co_await sst.sstable_write_io_check(rename_file, std::move(dst_temp_toc), std::move(src_temp_toc));
        co_await sst.sstable_write_io_check(sync_directory, dir);
    } else {
        // Now that the source sstable is linked to dir, remove
        // the TemporaryTOC file at the destination.
        co_await sst.sstable_write_io_check(remove_file, std::move(dst_temp_toc));
    }
    co_await sst.sstable_write_io_check(sync_directory, dst_dir);
    sstlog.trace("create_links: {} -> {} generation={}: done", sst.get_filename(), dst_dir, generation);
}

future<> filesystem_storage::create_links(const sstable& sst, const sstring& dir) const {
    return create_links_common(sst, dir, sst._generation, mark_for_removal::no);
}

future<> filesystem_storage::snapshot(const sstable& sst, sstring dir, absolute_path abs) const {
    if (!abs) {
        dir = this->dir + "/" + dir + "/";
    }
    co_await sst.sstable_touch_directory_io_check(dir);
    co_await create_links(sst, dir);
}

future<> filesystem_storage::move(const sstable& sst, sstring new_dir, generation_type new_generation, delayed_commit_changes* delay_commit) {
    co_await touch_directory(new_dir);
    sstring old_dir = dir;
    sstlog.debug("Moving {} old_generation={} to {} new_generation={} do_sync_dirs={}",
            sst.get_filename(), sst._generation, new_dir, new_generation, delay_commit == nullptr);
    co_await create_links_common(sst, new_dir, new_generation, mark_for_removal::yes);
    dir = new_dir;
    generation_type old_generation = sst._generation;
    co_await coroutine::parallel_for_each(sst.all_components(), [&sst, old_generation, old_dir] (auto p) {
        return sst.sstable_write_io_check(remove_file, sstable::filename(old_dir, sst._schema->ks_name(), sst._schema->cf_name(), sst._version, old_generation, sst._format, p.second));
    });
    auto temp_toc = sstable_version_constants::get_component_map(sst._version).at(component_type::TemporaryTOC);
    co_await sst.sstable_write_io_check(remove_file, sstable::filename(old_dir, sst._schema->ks_name(), sst._schema->cf_name(), sst._version, old_generation, sst._format, temp_toc));
    if (delay_commit == nullptr) {
        co_await when_all(sst.sstable_write_io_check(sync_directory, old_dir), sst.sstable_write_io_check(sync_directory, new_dir)).discard_result();
    } else {
        delay_commit->_dirs.insert(old_dir);
        delay_commit->_dirs.insert(new_dir);
    }
}

future<> filesystem_storage::change_state(const sstable& sst, sstring to, generation_type new_generation, delayed_commit_changes* delay_commit) {
    auto path = fs::path(dir);
    auto current = path.filename().native();

    // Moving between states means moving between basedir/state subdirectories.
    // However, normal state maps to the basedir itself and thus there's no way
    // to check if current is normal_dir. The best that can be done here is to
    // check that it's not anything else
    if (current == staging_dir || current == upload_dir || current == quarantine_dir) {
        if (to == quarantine_dir && current != staging_dir) {
            // Legacy exception -- quarantine from anything but staging
            // moves to the current directory quarantine subdir
            path = path / to;
        } else {
            path = path.parent_path() / to;
        }
    } else {
        current = normal_dir;
        path = path / to;
    }

    if (current == to) {
        co_return; // Already there
    }

    sstlog.info("Moving sstable {} to {}", sst.get_filename(), path);
    co_await move(sst, path.native(), std::move(new_generation), delay_commit);
}

future<> filesystem_storage::wipe(const sstable& sst) noexcept {
    // We must be able to generate toc_filename()
    // in order to delete the sstable.
    // Running out of memory here will terminate.
    auto name = [&sst] () noexcept {
        memory::scoped_critical_alloc_section _;
        return sst.toc_filename();
    }();

    try {
        co_await remove_by_toc_name(name);
    } catch (...) {
        // Log and ignore the failure since there is nothing much we can do about it at this point.
        // a. Compaction will retry deleting the sstable in the next pass, and
        // b. in the future sstables_manager is planned to handle sstables deletion.
        // c. Eventually we may want to record these failures in a system table
        //    and notify the administrator about that for manual handling (rather than aborting).
        sstlog.warn("Failed to delete {}: {}. Ignoring.", name, std::current_exception());
    }

    if (temp_dir) {
        try {
            co_await recursive_remove_directory(fs::path(*temp_dir));
            temp_dir.reset();
        } catch (...) {
            sstlog.warn("Exception when deleting temporary sstable directory {}: {}", *temp_dir, std::current_exception());
        }
    }
}

class s3_storage : public sstables::storage {
    shared_ptr<s3::client> _client;
    sstring _bucket;
    sstring _location;
    std::optional<sstring> _remote_prefix;

    static constexpr auto status_creating = "creating";
    static constexpr auto status_sealed = "sealed";
    static constexpr auto status_removing = "removing";

    sstring make_s3_object_name(const sstable& sst, component_type type);

    future<> ensure_remote_prefix(const sstable& sst);

public:
    s3_storage(shared_ptr<s3::client> client, sstring bucket, sstring dir)
        : _client(std::move(client))
        , _bucket(std::move(bucket))
        , _location(std::move(dir))
    {
    }

    virtual future<> seal(const sstable& sst) override;
    virtual future<> snapshot(const sstable& sst, sstring dir, absolute_path abs) const override;
    virtual future<> change_state(const sstable& sst, sstring to, generation_type generation, delayed_commit_changes* delay) override;
    // runs in async context
    virtual void open(sstable& sst, const io_priority_class& pc) override;
    virtual future<> wipe(const sstable& sst) noexcept override;
    virtual future<file> open_component(const sstable& sst, component_type type, open_flags flags, file_open_options options, bool check_integrity) override;
    virtual future<data_sink> make_data_or_index_sink(sstable& sst, component_type type, io_priority_class pc) override;
    virtual future<data_sink> make_component_sink(sstable& sst, component_type type, open_flags oflags, file_output_stream_options options) override;
    virtual future<> destroy(const sstable& sst) override {
        return make_ready_future<>();
    }

    virtual sstring prefix() const override { return _location; }
};

sstring s3_storage::make_s3_object_name(const sstable& sst, component_type type) {
    return format("/{}/{}/{}", _bucket, *_remote_prefix, sstable_version_constants::get_component_map(sst.get_version()).at(type));
}

future<> s3_storage::ensure_remote_prefix(const sstable& sst) {
    if (!_remote_prefix) {
        auto uuid = co_await sst.manager().system_keyspace().sstables_registry_lookup_entry(_location, sst.generation());
        _remote_prefix = uuid.to_sstring();
    }
}

void s3_storage::open(sstable& sst, const io_priority_class& pc) {
    auto uuid = utils::UUID_gen::get_time_UUID();
    entry_descriptor desc("", "", "", sst._generation, sst._version, sst._format, component_type::TOC);
    sst.manager().system_keyspace().sstables_registry_create_entry(_location, uuid, status_creating, std::move(desc)).get();
    _remote_prefix = uuid.to_sstring();

    memory_data_sink_buffers bufs;
    sst.write_toc(
        file_writer(
            output_stream<char>(
                data_sink(
                    std::make_unique<memory_data_sink>(bufs)
                )
            )
        )
    );
    _client->put_object(make_s3_object_name(sst, component_type::TOC), std::move(bufs)).get();
}

future<file> s3_storage::open_component(const sstable& sst, component_type type, open_flags flags, file_open_options options, bool check_integrity) {
    co_await ensure_remote_prefix(sst);
    co_return _client->make_readable_file(make_s3_object_name(sst, type));
}

future<data_sink> s3_storage::make_data_or_index_sink(sstable& sst, component_type type, io_priority_class pc) {
    assert(type == component_type::Data || type == component_type::Index);
    co_await ensure_remote_prefix(sst);
    co_return _client->make_upload_sink(make_s3_object_name(sst, type));
}

future<data_sink> s3_storage::make_component_sink(sstable& sst, component_type type, open_flags oflags, file_output_stream_options options) {
    co_await ensure_remote_prefix(sst);
    co_return _client->make_upload_sink(make_s3_object_name(sst, type));
}

future<> s3_storage::seal(const sstable& sst) {
    co_await sst.manager().system_keyspace().sstables_registry_update_entry_status(_location, sst.generation(), status_sealed);
}

future<> s3_storage::change_state(const sstable& sst, sstring to, generation_type generation, delayed_commit_changes* delay) {
    // FIXME -- this "move" means changing sstable state, e.g. move from staging
    // or upload to base. To make this work the "status" part of the entry location
    // must be detached from the entry location itself, see PR#12707
    co_await coroutine::return_exception(std::runtime_error("Moving S3 objects not implemented"));
}

future<> s3_storage::wipe(const sstable& sst) noexcept {
    auto& sys_ks = sst.manager().system_keyspace();

    co_await sys_ks.sstables_registry_update_entry_status(_location, sst.generation(), status_removing);

    co_await coroutine::parallel_for_each(sst._recognized_components, [this, &sst] (auto type) -> future<> {
        co_await _client->delete_object(make_s3_object_name(sst, type));
    });

    co_await sys_ks.sstables_registry_delete_entry(_location, sst.generation());
}

future<> s3_storage::snapshot(const sstable& sst, sstring dir, absolute_path abs) const {
    co_await coroutine::return_exception(std::runtime_error("Snapshotting S3 objects not implemented"));
}

std::unique_ptr<sstables::storage> make_storage(sstables_manager& manager, const data_dictionary::storage_options& s_opts, sstring dir) {
    return std::visit(overloaded_functor {
        [dir] (const data_dictionary::storage_options::local& loc) mutable -> std::unique_ptr<sstables::storage> {
            return std::make_unique<sstables::filesystem_storage>(std::move(dir));
        },
        [dir, &manager] (const data_dictionary::storage_options::s3& os) mutable -> std::unique_ptr<sstables::storage> {
            return std::make_unique<sstables::s3_storage>(manager.get_endpoint_client(os.endpoint), os.bucket, std::move(dir));
        }
    }, s_opts.value);
}

} // namespace sstables
