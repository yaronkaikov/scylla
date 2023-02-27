/*
 * Copyright (C) 2015 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#pragma once

#include "encryption.hh"

namespace replica {
class database;
}

namespace service {
class migration_manager;
}

namespace encryption {

class replicated_key_provider_factory : public key_provider_factory {
public:
    replicated_key_provider_factory();
    ~replicated_key_provider_factory();

    shared_ptr<key_provider> get_provider(encryption_context&, const options&) override;

    static void init();
    static future<> on_started(const ::replica::database&, service::migration_manager&);
};

}
