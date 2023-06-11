/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#pragma once

#include "seastarx.hh"
#include "log.hh"
#include "auth/role_manager.hh"
#include "auth/authenticated_user.hh"
#include <seastar/core/sstring.hh>
#include <seastar/core/distributed.hh>
#include <seastar/core/abort_source.hh>
#include <seastar/core/file.hh>
#include "auth/service.hh"
#include <map>
#include <stack>
#include <unordered_set>
#include "qos_common.hh"
#include "service/endpoint_lifecycle_subscriber.hh"
#include "qos_configuration_change_subscriber.hh"

namespace db {
    class system_distributed_keyspace;
}
namespace qos {
/**
 *  a structure to hold a service level
 *  data and configuration.
 */
struct service_level {
     service_level_options slo;
     scheduling_group sg;
     bool marked_for_deletion;
     bool is_static;
};

/**
 *  The service_level_controller class is an implementation of the service level
 *  controller design.
 *  It is logically divided into 2 parts:
 *      1. Global controller which is responsible for all of the data and plumbing
 *      manipulation.
 *      2. Local controllers that act upon the data and facilitates execution in
 *      the service level context: i.e functions in their service level's
 *      scheduling group and io operations with their correct io priority.
 */
class service_level_controller : public peering_sharded_service<service_level_controller>, public service::endpoint_lifecycle_subscriber {
public:
    class service_level_distributed_data_accessor {
    public:
        virtual future<qos::service_levels_info> get_service_levels() const = 0;
        virtual future<qos::service_levels_info> get_service_level(sstring service_level_name) const = 0;
        virtual future<> set_service_level(sstring service_level_name, qos::service_level_options slo) const = 0;
        virtual future<> drop_service_level(sstring service_level_name) const = 0;
    };
    using service_level_distributed_data_accessor_ptr = ::shared_ptr<service_level_distributed_data_accessor>;

private:
    struct global_controller_data {
        service_levels_info  static_configurations{};
        std::deque<scheduling_group> deleted_scheduling_groups{};
        service_level_options default_service_level_config;
        // The below future is used to serialize work so no reordering can occur.
        // This is needed so for example: delete(x), add(x) will not reverse yielding
        // a completely different result than the one intended.
        future<> work_future = make_ready_future();
        semaphore notifications_serializer = semaphore(1);
        future<> distributed_data_update = make_ready_future();
        abort_source dist_data_update_aborter;
        scheduling_group default_sg;
        bool destroy_default_sg;
        // a counter for making unique temp scheduling groups names
        int unique_group_counter;
        // A flag that indicates that we exhausted all of our scheduling groups
        // and we can't create new ones.
        bool scheduling_groups_exhausted = false;
    };

    std::unique_ptr<global_controller_data> _global_controller_db;

    static constexpr shard_id global_controller = 0;

    std::map<sstring, service_level> _service_levels_db;
    std::unordered_map<sstring, sstring> _role_to_service_level;
    std::pair<const sstring*, service_level*> _sl_lookup[max_scheduling_groups()];
    service_level _default_service_level;
    service_level_distributed_data_accessor_ptr _sl_data_accessor;
    sharded<auth::service>& _auth_service;
    std::chrono::time_point<seastar::lowres_clock> _last_successful_config_update;
    unsigned _logged_intervals;
    atomic_vector<qos_configuration_change_subscriber*> _subscribers;
public:
    service_level_controller(sharded<auth::service>& auth_service, service_level_options default_service_level_config,
            scheduling_group default_scheduling_group, bool destroy_default_sg_on_drain = false);

    /**
     * this function must be called *once* from any shard before any other functions are called.
     * No other function should be called before the future returned by the function is resolved.
     * @return a future that resolves when the initialization is over.
     */
    future<> start();

    void set_distributed_data_accessor(service_level_distributed_data_accessor_ptr sl_data_accessor);

    /**
     *  Adds a service level configuration if it doesn't exists, and updates
     *  an the existing one if it does exist.
     *  Handles both, static and non static service level configurations.
     * @param name - the service level name.
     * @param slo - the service level configuration
     * @param is_static - is this configuration static or not
     */
    future<> add_service_level(sstring name, service_level_options slo, bool is_static = false);

    /**
     *  Removes a service level configuration if it exists.
     *  Handles both, static and non static service level configurations.
     * @param name - the service level name.
     * @param remove_static - indicates if it is a removal of a static configuration
     * or not.
     */
    future<> remove_service_level(sstring name, bool remove_static);

    /**
     * stops the distributed updater
     * @return a future that is resolved when the updates stopped
     */
    future<> drain();

    /**
     * stops all ongoing operations if exists
     * @return a future that is resolved when all operations has stopped
     */
    future<> stop();

    /**
     * this is an executor of a function with arguments under a service level
     * that corresponds to a given user.
     * @param usr - the user for determining the service level
     * @param func - the function to be executed
     * @return a future that is resolved when the function's operation is resolved
     * (if it returns a future). or a ready future containing the returned value
     * from the function/
     */
    template <typename Ret>
    futurize_t<Ret> with_user_service_level(const std::optional<auth::authenticated_user>& usr, noncopyable_function<Ret()> func) {
        if (usr) {
            auth::service& ser = _auth_service.local();
            return auth::get_roles(ser, *usr).then([this] (auth::role_set roles) {
                return find_service_level(roles);
            }).then([this, func = std::move(func)] (std::optional<service_level_options> opts) mutable {
                auto& service_level_name = (opts && opts->shares_name) ? *opts->shares_name : default_service_level_name;
                return with_service_level(service_level_name, std::move(func));
            });
        } else {
            return with_service_level(default_service_level_name, std::move(func));
        }
    }

    /**
     * this is an executor of a function with arguments under a specific
     * service level.
     * @param service_level_name
     * @param func - the function to be executed
     * @param args - the arguments to  pass to the function.
     * @return a future that is resolved when the function's operation is resolved
     * (if it returns a future). or a ready future containing the returned value
     * from the function/
     */
    template <typename Ret>
    futurize_t<Ret> with_service_level(sstring service_level_name, noncopyable_function<Ret()> func) {
        service_level& sl = get_service_level(service_level_name);
        return with_scheduling_group(sl.sg, std::move(func));
    }

    /**
     * @return the default service level scheduling group (see service_level_controller::initialize).
     */
    scheduling_group get_default_scheduling_group();
    /**
     * Get the scheduling group for a specific service level.
     * @param service_level_name - the service level which it's scheduling group
     * should be returned.
     * @return if the service level exists the service level's scheduling group. else
     * get_scheduling_group("default")
     */
    scheduling_group get_scheduling_group(sstring service_level_name);
    /**
     * @return the name of the currently active service level if such exists or an empty
     * optional if no active service level.
     */
    std::optional<sstring> get_active_service_level();
    /**
     * @return true if the currently active service level is the default service level.
     */
    bool is_default_service_level();

    /**
     * Chack the distributed data for changes in a constant interval and updates
     * the service_levels configuration in accordance (adds, removes, or updates
     * service levels as necessairy).
     * @param interval - the interval is seconds to check the distributed data.
     * @return a future that is resolved when the update loop stops.
     */
    void update_from_distributed_data(std::chrono::duration<float> interval);

    /**
     * Updates the service level data from the distributed data store.
     * @return a future that is resolved when the update is done
     */
    future<> update_service_levels_from_distributed_data();


    future<> add_distributed_service_level(sstring name, service_level_options slo, bool if_not_exsists);
    future<> alter_distributed_service_level(sstring name, service_level_options slo);
    future<> drop_distributed_service_level(sstring name, bool if_exists);
    future<service_levels_info> get_distributed_service_levels();
    future<service_levels_info> get_distributed_service_level(sstring service_level_name);

    /**
     * Returns the service level options **in effect** for a user having the given
     * collection of roles.
     * @param roles - the collection of roles to consider
     * @return the effective service level options - they may in particular be a combination
     *         of options from multiple service levels
     */
    future<std::optional<service_level_options>> find_service_level(auth::role_set roles);

    /**
     * Gets the service level data by name.
     * @param service_level_name - the name of the requested service level
     * @return the service level data if it exists (in the local controller) or
     * get_service_level("default") otherwise.
     */
    service_level& get_service_level(sstring service_level_name) {
        auto sl_it = _service_levels_db.find(service_level_name);
        if (sl_it == _service_levels_db.end() || sl_it->second.marked_for_deletion) {
            sl_it = _service_levels_db.find(default_service_level_name);
        }
        return sl_it->second;
    }

    /**
     * Returns true if `service_level_name` is recognised as a
     * service level name and false otherwise.
     *
     * @param service_level_name - the service level name to test.
     **/
     bool has_service_level(sstring service_level_name) {
         return _service_levels_db.contains(service_level_name);
     }

private:
    /**
     *  Adds a service level configuration if it doesn't exists, and updates
     *  an the existing one if it does exist.
     *  Handles both, static and non static service level configurations.
     * @param name - the service level name.
     * @param slo - the service level configuration
     * @param is_static - is this configuration static or not
     */
    future<> do_add_service_level(sstring name, service_level_options slo, bool is_static = false);

    /**
     *  Removes a service level configuration if it exists.
     *  Handles both, static and non static service level configurations.
     * @param name - the service level name.
     * @param remove_static - indicates if it is a removal of a static configuration
     * or not.
     */
    future<> do_remove_service_level(sstring name, bool remove_static);

    /**
     * The notify functions are used by the global service level controller
     * to propagate configuration changes to the local controllers.
     * the returned future is resolved when the local controller is done acting
     * on the notification. updates are done in sequence so their meaning will not
     * change due to execution reordering.
     */
    future<> notify_service_level_added(sstring name, service_level sl_data);
    future<> notify_service_level_updated(sstring name, service_level_options slo);
    future<> notify_service_level_removed(sstring name);

    enum class  set_service_level_op_type {
        add_if_not_exists,
        add,
        alter
    };

    /** Validate that we can handle an addition of another service level
     *  Must be called from on the global controller
     */
    future<bool> validate_before_service_level_add();
    future<> set_distributed_service_level(sstring name, service_level_options slo, set_service_level_op_type op_type);
public:

    /**
     *  Register a subscriber for service level configuration changes
     *  notifications
     * @param subscriber - a pointer to the subscriber.
     *
     * Note: the caller is responsible to keep the pointed to object alive until performing
     * a call to service_level_controller::unregister_subscriber()).
     *
     * Note 2: the subscription is per shard.
     */
    void register_subscriber(qos_configuration_change_subscriber* subscriber);
    future<> unregister_subscriber(qos_configuration_change_subscriber* subscriber);

    static sstring default_service_level_name;

    virtual void on_join_cluster(const gms::inet_address& endpoint) override;
    virtual void on_leave_cluster(const gms::inet_address& endpoint) override;
    virtual void on_up(const gms::inet_address& endpoint) override;
    virtual void on_down(const gms::inet_address& endpoint) override;
};
}
