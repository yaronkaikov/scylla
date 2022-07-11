/*
 * Copyright (C) 2022 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#include <boost/lexical_cast.hpp>
#include <regex>

#include "kms_key_provider.hh"
#include "kms_host.hh"

namespace encryption {

class kms_key_provider : public key_provider {
public:
    kms_key_provider(::shared_ptr<kms_host> kms_host, std::string name, std::optional<std::string> master_key)
        : _kms_host(std::move(kms_host))
        , _name(std::move(name))
        , _master_key(std::move(master_key))
    {}
    future<std::tuple<key_ptr, opt_bytes>> key(const key_info& info, opt_bytes id) override {
        if (id) {
            return _kms_host->get_key_by_id(*id, info).then([id](key_ptr k) {
                return make_ready_future<std::tuple<key_ptr, opt_bytes>>(std::tuple(k, id));
            });
        }
        return _kms_host->get_or_create_key(info, _master_key).then([](std::tuple<key_ptr, opt_bytes> k_id) {
            return make_ready_future<std::tuple<key_ptr, opt_bytes>>(k_id);
        });
    }
    void print(std::ostream& os) const override {
        os << _name;
    }
private:
    ::shared_ptr<kms_host> _kms_host;
    std::string _name;
    std::optional<std::string> _master_key;
};


shared_ptr<key_provider> kms_key_provider_factory::get_provider(encryption_context& ctxt, const options& map) {
    opt_wrapper opts(map);
    auto kms_host = opts("kms_host");
    auto master_key = opts("master_key");

    if (!kms_host) {
        throw std::invalid_argument("kms_host must be provided");
    }

    auto host = ctxt.get_kms_host(*kms_host);
    auto id = kms_host.value() + ":" + master_key.value_or(host->options().master_key);
    auto provider = ctxt.get_cached_provider(id);

    if (!provider) {
        provider = ::make_shared<kms_key_provider>(host, *kms_host, master_key);
        ctxt.cache_provider(id, provider);
    }

    return provider;
}

}
