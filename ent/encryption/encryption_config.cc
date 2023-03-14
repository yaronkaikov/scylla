/*
 * Copyright (C) 2015 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#include <boost/filesystem.hpp>

#include "db/config.hh"
#include "utils/config_file_impl.hh"

#include "init.hh"
#include "encryption_config.hh"
#include "encryption.hh"

encryption::encryption_config::encryption_config()
                : config_file()
// BEGIN entry definitions

        , system_key_directory(this, "system_key_directory", value_status::Used, "resources/system_keys",
                                R"foo(The directory where system keys are kept

This directory should have 700 permissions and belong to the scylla user)foo")

		, config_encryption_active(this, "config_encryption_active", value_status::Used, false, "")

        , config_encryption_key_name(this, "config_encryption_key_name", value_status::Used, "system_key",
                                "Set to the local encryption key filename or KMIP key URL to use for configuration file property value decryption")

        , system_info_encryption(this, "system_info_encryption", value_status::Used,
                                { { "enabled", "false" }, { "cipher_algorithm",
                                                "AES/CBC/PKCS5Padding" }, {
                                                "secret_key_strength", "128" },
                                                },
                                R"foo(System information encryption settings

If enabled, system tables that may contain sensitive information (system.batchlog,
system.paxos), hints files and commit logs are encrypted with the
encryption settings below.

When enabling system table encryption on a node with existing data, run
`nodetool upgradesstables -a` on the listed tables to encrypt existing data.

When tracing is enabled, sensitive info will be written into the tables in the
system_traces keyspace. Those tables should be configured to encrypt their data
on disk.

It is recommended to use remote encryption keys from a KMIP server when using 
Transparent Data Encryption (TDE) features.
Local key support is provided when a KMIP server is not available.

See the scylla documentation for available key providers and their properties.
)foo")
		, kmip_hosts(this, "kmip_hosts", value_status::Used, { },
                                R"foo(KMIP host(s). 

The unique name of kmip host/cluster that can be referenced in table schema.

host.yourdomain.com={ hosts=<host1[:port]>[, <host2[:port]>...], keyfile=/path/to/keyfile, truststore=/path/to/truststore.pem, key_cache_millis=<cache ms>, timeout=<timeout ms> }:...

The KMIP connection management only supports failover, so all requests will go through a 
single KMIP server. There is no load balancing, as no KMIP servers (at the time of this writing)
support read replication, or other strategies for availability.

Hosts are tried in the order they appear here. Add them in the same sequence they'll fail over in.

KMIP requests will fail over/retry 'max_command_retries' times (default 3)

)foo")
		  , kms_hosts(this, "kms_hosts", value_status::Used, { },
                                R"foo(KMS host(s). 

The unique name of kms host that can be referenced in table schema.

host.yourdomain.com={ endpoint=<http(s)://host[:port]>, aws_access_key_id=<AWS access id>, aws_secret_access_key=<AWS secret key>, aws_region=<AWS region>, master_key=<alias or id>, keyfile=/path/to/keyfile, truststore=/path/to/truststore.pem, key_cache_millis=<cache ms>, timeout=<timeout ms> }:...

Actual connection can be either an explicit endpoint (<host>:<port>), or selected automatic via aws_region.

Authentication can be explicit with aws_access_key_id and aws_secret_access_key. Either secret or both can be ommitted
in which case the provider will try to read them from AWS credentials in ~/.aws/credentials

master_key is an AWS KMS key id or alias from which all keys used for actual encryption of scylla data will be derived.
This key must be pre-created with access policy allowing the above AWS id Encrypt, Decrypt and GenerateDataKey operations.

)foo")
// END entry definitions
{}

static class : public encryption::encryption_config, public configurable {
public:
    void append_options(db::config& cfg, boost::program_options::options_description_easy_init& init) override {
        // hook into main scylla.yaml.
        cfg.add(values());
    }
    future<> initialize(const boost::program_options::variables_map& opts, const db::config& cfg, db::extensions& exts) override {
        return encryption::register_extensions(cfg, *this, exts);
    }
} cfg;
