/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#pragma once

#include "raft/raft.hh"
#include "service/raft/group0_fwd.hh"
#include "utils/UUID.hh"
#include <seastar/core/future.hh>
#include <seastar/core/timer.hh>

namespace gms {
class gossiper;
}

namespace replica {
class database;
}

namespace raft {
class server;
}


namespace service {

class raft_group_registry;


class group0_server_accessor {
    raft_group_registry& _raft_gr;
    raft::group_id _group0_id;

public:
    group0_server_accessor(raft_group_registry& raft_gr, raft::group_id group0_id)
        : _raft_gr(raft_gr)
        , _group0_id(group0_id) {
    }

    [[nodiscard]] raft::server* get_server() const;
};

class group0_state_id_handler {

    replica::database& _local_db;
    gms::gossiper& _gossiper;
    const raft_address_map& _address_map;
    group0_server_accessor _server_accessor;
    lowres_clock::duration _refresh_interval;

    timer<> _timer;
    bool _stopped = false;

    utils::UUID _state_id_last_advertised;
    utils::UUID _state_id_last_reconcile;

    static lowres_clock::duration get_refresh_interval(const replica::database& db);

    void refresh();

public:
    group0_state_id_handler(replica::database& local_db, gms::gossiper& gossiper, const raft_address_map& address_map, group0_server_accessor server_accessor);

    ~group0_state_id_handler();

    void start();
    void stop();

    future<> advertise_state_id(utils::UUID state_id);
};

} // namespace service