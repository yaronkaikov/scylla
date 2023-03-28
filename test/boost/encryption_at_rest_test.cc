/*
 * Copyright (C) 2016 ScyllaDB
 */



#include <boost/range/irange.hpp>
#include <boost/range/adaptors.hpp>
#include <boost/range/algorithm.hpp>
#include <boost/test/unit_test.hpp>
#include <stdint.h>
#include <random>

#include <seastar/core/future-util.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/defer.hh>

#include <seastar/testing/test_case.hh>

#include "ent/encryption/encryption.hh"
#include "ent/encryption/symmetric_key.hh"
#include "ent/encryption/local_file_provider.hh"
#include "test/lib/tmpdir.hh"
#include "test/lib/random_utils.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/cql_assertions.hh"
#include "db/config.hh"
#include "db/extensions.hh"
#include "init.hh"

using namespace encryption;
namespace fs = std::filesystem;

static future<> test_provider(const std::string& options, const tmpdir& tmp, const std::string& extra_yaml = {}) {
    auto make_config = [&] {
        auto ext = std::make_shared<db::extensions>();
        auto cfg = seastar::make_shared<db::config>(ext);
        cfg->data_file_directories({tmp.path().string()});

        if (!extra_yaml.empty()) {
            boost::program_options::options_description desc;
            boost::program_options::options_description_easy_init init(&desc);
            configurable::append_all(*cfg, init);
            cfg->read_from_yaml(extra_yaml);
        }

        return std::make_tuple(cfg, ext);
    };

    std::string pk = "apa";
    std::string v = "ko";

    {
        auto [cfg, ext] = make_config();

        co_await do_with_cql_env_thread([&] (cql_test_env& env) {
            env.execute_cql(fmt::format("create table t (pk text primary key, v text) WITH scylla_encryption_options={{{}}}", options)).get();
            env.execute_cql(fmt::format("insert into ks.t (pk, v) values ('{}', '{}')", pk, v)).get();
        }, cfg, {}, cql_test_init_configurables{ *ext });
    }

    {
        auto [cfg, ext] = make_config();

        co_await do_with_cql_env_thread([&] (cql_test_env& env) {
            require_rows(env, "select * from ks.t", {{utf8_type->decompose(pk), utf8_type->decompose(v)}});
        }, cfg, {}, cql_test_init_configurables{ *ext });
    }
}

SEASTAR_TEST_CASE(test_local_file_provider) {
    tmpdir tmp;
    auto keyfile = tmp.path() / "secret_key";
    co_await test_provider(fmt::format("'key_provider': 'LocalFileSystemKeyProviderFactory', 'secret_key_file': '{}', 'cipher_algorithm':'AES/CBC/PKCS5Padding', 'secret_key_strength': 128", keyfile.string()), tmp);
}

static future<> create_key_file(const fs::path& path, const std::vector<key_info>& key_types) {
    std::ostringstream ss;

    for (auto& info : key_types) {
        symmetric_key k(info);
        ss << info.alg << ":" << info.len << ":" << base64_encode(k.key()) << std::endl;
    }

    auto s = ss.str();
    co_await seastar::recursive_touch_directory(fs::path(path).remove_filename().string());
    co_await write_text_file_fully(path.string(), s);
}

SEASTAR_TEST_CASE(test_replicated_provider) {
    tmpdir tmp;
    auto keyfile = tmp.path() / "secret_key";
    auto sysdir = tmp.path() / "system_keys";
    auto syskey = sysdir / "system_key";
    auto yaml = fmt::format("system_key_directory: {}", sysdir.string());

    co_await create_key_file(syskey, { { "AES/CBC/PKCSPadding", 256 }});
    co_await test_provider("'key_provider': 'ReplicatedKeyProviderFactory', 'system_key_file': 'system_key', 'cipher_algorithm':'AES/CBC/PKCS5Padding', 'secret_key_strength': 128", tmp, yaml);
}

SEASTAR_TEST_CASE(test_kmip_provider) {
    // KMIP relies on a reachable server. We have two servers available for QA.
    // Make this test optional, since reachability of KMIP server below is not guaranteed.
    // Run test with ENABLE_KMIP_TEST=1 to actually test anything
    auto do_kmip_test = std::getenv("ENABLE_KMIP_TEST");

    if (do_kmip_test == nullptr || !strcasecmp(do_kmip_test, "0") || !strcasecmp(do_kmip_test, "false")) {
        BOOST_TEST_MESSAGE("Skipping test. Set ENABLE_KMIP_TEST=1 to run");
        co_return;
    }

    tmpdir tmp;

    // TODO: can we have a better reference to resource dir?
    auto resourcedir = "./test/resource/certs";
    // QA test server. Same as used in dtests.
    auto host = "52.21.171.245";
    auto yaml = fmt::format(R"foo(
        kmip_hosts:
            kmip_test:
                hosts: {0}
                certificate: {1}/scylla.pem
                keyfile: {1}/scylla.pem
                truststore: {1}/cacert.pem
                priority_string: SECURE128:+RSA:-VERS-TLS1.0:-ECDHE-ECDSA
                )foo"
        , host, resourcedir
    );
    co_await test_provider("'key_provider': 'KmipKeyProviderFactory', 'kmip_host': 'kmip_test', 'cipher_algorithm':'AES/CBC/PKCS5Padding', 'secret_key_strength': 128", tmp, yaml);
}
