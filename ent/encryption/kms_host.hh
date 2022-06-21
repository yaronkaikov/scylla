/*
 * Copyright (C) 2022 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#pragma once 

#include <vector>
#include <optional>
#include <chrono>
#include <iosfwd>
#include <string>

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>

#include "symmetric_key.hh"

namespace encryption {

class encryption_context;
struct key_info;

class kms_host {
public:
    struct host_options {
        std::string endpoint;
        // or...
        std::string host;
        uint16_t    port;
        bool        https = true;
        // auth
        std::string aws_access_key_id;
        std::string aws_secret_access_key;
        std::string aws_region;
        // key to use for keys
        std::string master_key;
        // tls. if unspeced, use system for https
        // AWS does not (afaik?) allow certificate auth
        // but we keep the option available just in case.
        std::string certfile;
        std::string keyfile;
        std::string truststore;
        std::string priority_string;

        std::optional<std::chrono::milliseconds> key_cache_expiry;
    };
    using id_type = bytes;

    kms_host(encryption_context&, const std::string& name, const host_options&);
    kms_host(encryption_context&, const std::string& name, const std::unordered_map<sstring, sstring>&);
    ~kms_host();

    future<> init();

    future<std::tuple<shared_ptr<symmetric_key>, id_type>> get_or_create_key(const key_info&);
    future<shared_ptr<symmetric_key>> get_key_by_id(const id_type&, const key_info&);
private:
    class impl;
    std::unique_ptr<impl> _impl;
};

}
