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
    kms_key_provider(::shared_ptr<kms_host> kms_host, sstring name)
        : _kms_host(std::move(kms_host))
        , _name(std::move(name))
    {}
    future<std::tuple<key_ptr, opt_bytes>> key(const key_info& info, opt_bytes id) override {
        if (id) {
            return _kms_host->get_key_by_id(*id, info).then([id](key_ptr k) {
                return make_ready_future<std::tuple<key_ptr, opt_bytes>>(std::tuple(k, id));
            });
        }
        return _kms_host->get_or_create_key(info).then([](std::tuple<key_ptr, opt_bytes> k_id) {
            return make_ready_future<std::tuple<key_ptr, opt_bytes>>(k_id);
        });
    }
    void print(std::ostream& os) const override {
        os << _name;
    }
private:
    ::shared_ptr<kms_host> _kms_host;
    sstring _name;
};


shared_ptr<key_provider> kms_key_provider_factory::get_provider(encryption_context& ctxt, const options& map) {
    opt_wrapper opts(map);
    auto host = opts("kms_host");
    if (!host) {
        throw std::invalid_argument("kms_host must be provided");
    }

    auto provider = ctxt.get_cached_provider(*host);

    if (!provider) {
        provider = ::make_shared<kms_key_provider>(ctxt.get_kms_host(*host), *host);
        ctxt.cache_provider(*host, provider);
    }

    return provider;
}

}
