/*
 * Copyright (C) 2016 ScyllaDB
 */



#include <boost/range/irange.hpp>
#include <boost/range/adaptors.hpp>
#include <boost/range/algorithm.hpp>
#include <boost/test/unit_test.hpp>
#include <stdint.h>
#include <random>
#include <regex>

#include <seastar/core/future-util.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/defer.hh>
#include <seastar/net/dns.hh>

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
#include "sstables/sstables.hh"

using namespace encryption;
namespace fs = std::filesystem;

static future<> test_provider(const std::string& options, const tmpdir& tmp, const std::string& extra_yaml = {}, unsigned n_tables = 1, unsigned n_restarts = 1, const std::string& explicit_provider = {}) {
    auto host_id = locator::host_id::create_random_id();
    auto make_config = [&] {
        auto ext = std::make_shared<db::extensions>();
        auto cfg = seastar::make_shared<db::config>(ext);
        cfg->data_file_directories({tmp.path().string()});
        cfg->host_id = host_id;

        // Currently the test fails with consistent_cluster_management = true. See #2995.
        cfg->consistent_cluster_management(false);

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
            for (auto i = 0u; i < n_tables; ++i) {
                if (options.empty()) {
                    env.execute_cql(fmt::format("create table t{} (pk text primary key, v text)", i)).get();
                } else {
                    env.execute_cql(fmt::format("create table t{} (pk text primary key, v text) WITH scylla_encryption_options={{{}}}", i, options)).get();
                }
                env.execute_cql(fmt::format("insert into ks.t{} (pk, v) values ('{}', '{}')", i, pk, v)).get();
            }
        }, cfg, {}, cql_test_init_configurables{ *ext });
    }

    for (auto rs = 0u; rs < n_restarts; ++rs) {
        auto [cfg, ext] = make_config();

        co_await do_with_cql_env_thread([&] (cql_test_env& env) {
            for (auto i = 0u; i < n_tables; ++i) {
                require_rows(env, fmt::format("select * from ks.t{}", i), {{utf8_type->decompose(pk), utf8_type->decompose(v)}});

                auto provider = explicit_provider;

                // check that all sstables have the defined provider class (i.e. are encrypted using correct optons)
                if (provider.empty() && options.find("'key_provider'") != std::string::npos) {
                    static std::regex ex(R"foo('key_provider'\s*:\s*'(\w+)')foo");

                    std::smatch m;
                    BOOST_REQUIRE(std::regex_search(options.begin(), options.end(), m, ex));
                    provider = m[1].str();
                    BOOST_REQUIRE(!provider.empty());
                }
                if (!provider.empty()) {
                    env.db().invoke_on_all([&](replica::database& db) {
                        auto& cf = db.find_column_family("ks", "t" + std::to_string(i));
                        auto sstables = cf.get_sstables_including_compacted_undeleted();

                        if (sstables) {
                            for (auto& t : *sstables) {
                                auto sst_provider = encryption::encryption_provider(*t);
                                BOOST_REQUIRE_EQUAL(provider, sst_provider);
                            }
                        }
                    }).get0();
                }
            }
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

static future<> do_test_replicated_provider(unsigned n_tables, unsigned n_restarts) {
    tmpdir tmp;
    auto keyfile = tmp.path() / "secret_key";
    auto sysdir = tmp.path() / "system_keys";
    auto syskey = sysdir / "system_key";
    auto yaml = fmt::format("system_key_directory: {}", sysdir.string());

    co_await create_key_file(syskey, { { "AES/CBC/PKCSPadding", 256 }});

    BOOST_REQUIRE(fs::exists(syskey));;

    co_await test_provider("'key_provider': 'ReplicatedKeyProviderFactory', 'system_key_file': 'system_key', 'cipher_algorithm':'AES/CBC/PKCS5Padding', 'secret_key_strength': 128", tmp, yaml, n_tables, n_restarts);

    BOOST_REQUIRE(fs::exists(tmp.path()));
}

SEASTAR_TEST_CASE(test_replicated_provider) {
    co_await do_test_replicated_provider(1, 1);
}

SEASTAR_TEST_CASE(test_replicated_provider_many_tables) {
    co_await do_test_replicated_provider(100, 5);
}

static future<> kmip_test_helper(const std::function<future<>(std::string_view, const tmpdir&)>& f) {
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

    co_await f(resourcedir, tmp);
}

SEASTAR_TEST_CASE(test_kmip_provider) {
    co_await kmip_test_helper([](std::string_view resourcedir, const tmpdir& tmp) -> future<> {
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
    });
}


SEASTAR_TEST_CASE(test_kmip_provider_multiple_hosts) {
    /**
     * Tests for #3251. KMIP connector ends up in endless loop if using more than one
     * fallover host. This is only in initial connection (in real life only in initial connection verification).
     * 
     * We don't have access to more than one KMIP server for testing (at a time).
     * Pretend to have failover by using a local proxy.
    */
    co_await kmip_test_helper([](std::string_view resourcedir, const tmpdir& tmp) -> future<> {
        // QA test server. Same as used in dtests.
        constexpr auto host = "52.21.171.245";

        // Bind to any local port.
        auto server_sock = seastar::listen(socket_address(0x7f000001, 0));
        bool go_on = true;

        // fake more than one host by creating a local proxy.
        auto f = [](bool& go_on, server_socket& server_sock) -> future<> {
            std::vector<future<>> work;

            auto addr = co_await seastar::net::dns::resolve_name(host);
            size_t connections_done = 0;

            while (go_on) {
                try {
                    auto client = co_await server_sock.accept();

                    if (++connections_done > 30) {
                        server_sock.abort_accept();
                        break;
                    }

                    constexpr uint16_t port = 5696u;
                    auto dst = co_await seastar::connect(socket_address(addr, port));

                    auto f = [&]() -> future<> {      
                        auto& s = client.connection;
                        auto& ldst = dst;

                        auto do_io = [](connected_socket& src, connected_socket& dst, bool& go_on) -> future<> {
                            auto sin = src.input();
                            auto dout = dst.output();

                            while (go_on && !sin.eof()) {
                                auto buf = co_await sin.read();
                                co_await dout.write(std::move(buf));
                                co_await dout.flush();
                            }
                            co_await dout.close();
                        };

                        co_await when_all(do_io(s, ldst, go_on), do_io(ldst, s, go_on));
                    }();

                    work.emplace_back(std::move(f));
                } catch (...) {
                }
            }

            for (auto&& f : work) {
                co_await std::move(f);
            }
        }(go_on, server_sock);

        auto host2 = boost::lexical_cast<std::string>(server_sock.local_address());

        auto yaml = fmt::format(R"foo(
            kmip_hosts:
                kmip_test:
                    hosts: {0}, {2} 
                    certificate: {1}/scylla.pem
                    keyfile: {1}/scylla.pem
                    truststore: {1}/cacert.pem
                    priority_string: SECURE128:+RSA:-VERS-TLS1.0:-ECDHE-ECDSA
                    )foo"
            , host, resourcedir, host2
        );

        std::exception_ptr ex;

        try {
            co_await test_provider("'key_provider': 'KmipKeyProviderFactory', 'kmip_host': 'kmip_test', 'cipher_algorithm':'AES/CBC/PKCS5Padding', 'secret_key_strength': 128", tmp, yaml);
        } catch (...) {
            ex = std::current_exception();    
        }

        go_on = false;
        server_sock.abort_accept();
        co_await std::move(f);

        if (ex) {
            std::rethrow_exception(ex);
        }
    });
}

/*
Simple test of KMS provider. Still has some caveats:

    1.) Uses aws CLI credentials for auth. I.e. you need to have a valid
        ~/.aws/credentials for the user running the test.
    2.) I can't figure out a good way to set up a key "everyone" can access. So user needs
        to have read/encrypt access to the key alias (default "alias/kms_encryption_test")
        in the scylla AWS account.

    A "better" solution might be to create dummmy user only for KMS testing with only access
    to a single key, and no other priviledges. But that seems dangerous as well.

    For this reason, this test is parameterized with env vars:
    * ENABLE_KMS_TEST - set to non-zero (0)/false to run
    * KMS_KEY_ALIAS - default "alias/kms_encryption_test" - set to key alias you have access to.
    * KMS_AWS_REGION - default us-east-1 - set to whatever region your key is in.

*/
static future<> kms_test_helper(std::function<future<>(const tmpdir&, std::string_view, std::string_view, std::string_view)> f) {
    auto do_kms_test = std::getenv("ENABLE_KMS_TEST");

    if (do_kms_test == nullptr || !strcasecmp(do_kms_test, "0") || !strcasecmp(do_kms_test, "false")) {
        BOOST_TEST_MESSAGE("Skipping test. Set ENABLE_KMS_TEST=1 to run");
        co_return;
    }

    static const std::string default_kms_alias = "alias/kms_encryption_test";

    const char* kms_key_alias = std::getenv("KMS_KEY_ALIAS");
    if (kms_key_alias == nullptr) {
        kms_key_alias = default_kms_alias.data();
    }

    static const std::string default_aws_region = "us-east-1";

    const char* kms_aws_region = std::getenv("KMS_AWS_REGION");
    if (kms_aws_region == nullptr) {
        kms_aws_region = default_aws_region.data();
    }

    static const std::string default_profile = "default";

    const char* kms_aws_profile = std::getenv("KMS_AWS_PROFILE");
    if (kms_aws_profile == nullptr) {
        kms_aws_profile = std::getenv("AWS_PROFILE");
    }
    if (kms_aws_profile == nullptr) {
        kms_aws_profile = default_profile.data();
    }

    tmpdir tmp;

    co_await f(tmp, kms_key_alias, kms_aws_region, kms_aws_profile);
}

SEASTAR_TEST_CASE(test_kms_provider) {
    co_await kms_test_helper([](const tmpdir& tmp, std::string_view kms_key_alias, std::string_view kms_aws_region, std::string_view kms_aws_profile) -> future<> {
        /**
         * Note: NOT including any auth stuff here. The provider will pick up AWS credentials
         * from ~/.aws/credentials
         */
        auto yaml = fmt::format(R"foo(
            kms_hosts:
                kms_test:
                    master_key: {0}
                    aws_region: {1}
                    aws_profile: {2}
                    )foo"
            , kms_key_alias, kms_aws_region, kms_aws_profile
        );

        co_await test_provider("'key_provider': 'KmsKeyProviderFactory', 'kms_host': 'kms_test', 'cipher_algorithm':'AES/CBC/PKCS5Padding', 'secret_key_strength': 128", tmp, yaml);
    });
}

SEASTAR_TEST_CASE(test_kms_provider_with_master_key_in_cf) {
    co_await kms_test_helper([](const tmpdir& tmp, std::string_view kms_key_alias, std::string_view kms_aws_region, std::string_view kms_aws_profile) -> future<> {
        /**
         * Note: NOT including any auth stuff here. The provider will pick up AWS credentials
         * from ~/.aws/credentials
         */
        auto yaml = fmt::format(R"foo(
            kms_hosts:
                kms_test:
                    aws_region: {1}
                    aws_profile: {2}
                    )foo"
            , kms_key_alias, kms_aws_region, kms_aws_profile
        );

        // should fail
        BOOST_REQUIRE_THROW(
            co_await test_provider("'key_provider': 'KmsKeyProviderFactory', 'kms_host': 'kms_test', 'cipher_algorithm':'AES/CBC/PKCS5Padding', 'secret_key_strength': 128", tmp, yaml)
            , std::exception
        );

        // should be ok
        co_await test_provider(fmt::format("'key_provider': 'KmsKeyProviderFactory', 'kms_host': 'kms_test', 'master_key': '{}', 'cipher_algorithm':'AES/CBC/PKCS5Padding', 'secret_key_strength': 128", kms_key_alias)
            , tmp, yaml
            );
    });
}


SEASTAR_TEST_CASE(test_user_info_encryption) {
    tmpdir tmp;
    auto keyfile = tmp.path() / "secret_key";

    auto yaml = fmt::format(R"foo(
        user_info_encryption:
            enabled: True
            key_provider: LocalFileSystemKeyProviderFactory
            secret_key_file: {}
            cipher_algorithm: AES/CBC/PKCS5Padding
            secret_key_strength: 128
        )foo"
    , keyfile.string());

    co_await test_provider({}, tmp, yaml, 4, 1, "LocalFileSystemKeyProviderFactory" /* verify encrypted even though no kp in options*/);
}
