/*
 * Copyright (C) 2022 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */
#include <deque>
#include <unordered_map>
#include <regex>
#include <algorithm>

#include <seastar/net/dns.hh>
#include <seastar/net/api.hh>
#include <seastar/net/tls.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/reactor.hh>
#include <seastar/json/formatter.hh>

#include <boost/beast/http.hpp>

#include <fmt/chrono.h>

#include "kms_host.hh"
#include "encryption.hh"
#include "symmetric_key.hh"
#include "utils/hash.hh"
#include "utils/loading_cache.hh"
#include "utils/UUID.hh"
#include "utils/UUID_gen.hh"
#include "utils/rjson.hh"
#include "marshal_exception.hh"
#include "db/config.hh"

using namespace std::chrono_literals;
using namespace std::string_literals;

logger kms_log("kms");

class kms_error : public std::exception {
    std::string _type, _msg;
public:
    kms_error(std::string_view type, std::string_view msg)
        : _type(type)
        , _msg(format("{}: {}", type, msg))
    {}
    const std::string& type() const {
        return _type;
    }
    const char* what() const noexcept override {
        return _msg.c_str();
    }
};

namespace kms_errors {
    [[maybe_unused]] static const char* AccessDeniedException = "AccessDeniedException";
    [[maybe_unused]] static const char* IncompleteSignature = "IncompleteSignature";
    [[maybe_unused]] static const char* InternalFailure = "InternalFailure";
    [[maybe_unused]] static const char* InvalidAction = "InvalidAction";
    [[maybe_unused]] static const char* InvalidClientTokenId = "InvalidClientTokenId";
    [[maybe_unused]] static const char* InvalidParameterCombination = "InvalidParameterCombination";
    [[maybe_unused]] static const char* InvalidParameterValue = "InvalidParameterValue";
    [[maybe_unused]] static const char* InvalidQueryParameter = "InvalidQueryParameter";
    [[maybe_unused]] static const char* MalformedQueryString = "MalformedQueryString";
    [[maybe_unused]] static const char* MissingAction = "MissingAction";
    [[maybe_unused]] static const char* MissingAuthenticationToken = "MissingAuthenticationToken";
    [[maybe_unused]] static const char* MissingParameter = "MissingParameter";
    [[maybe_unused]] static const char* NotAuthorized = "NotAuthorized";
    [[maybe_unused]] static const char* OptInRequired = "OptInRequired";
    [[maybe_unused]] static const char* RequestExpired = "RequestExpired";
    [[maybe_unused]] static const char* ServiceUnavailable = "ServiceUnavailable";
    [[maybe_unused]] static const char* ThrottlingException = "ThrottlingException";
    [[maybe_unused]] static const char* ValidationError = "ValidationError";
    [[maybe_unused]] static const char* DependencyTimeoutException = "DependencyTimeoutException";
    [[maybe_unused]] static const char* InvalidArnExceptio = "InvalidArnException";
    [[maybe_unused]] static const char* KMSInternalException = "KMSInternalException";
    [[maybe_unused]] static const char* NotFoundException = "NotFoundException";
    [[maybe_unused]] static const char* AlreadyExistsException = "AlreadyExistsException";
}

class encryption::kms_host::impl {
public:
    // set a rather long expiry. normal KMS policies are 365-day rotation of keys.
    // we can do with 10 minutes. CMH. maybe even longer.
    // (see comments below on what keys are here)
    static inline constexpr std::chrono::milliseconds default_expiry = 600s;
    static inline constexpr std::chrono::milliseconds default_refresh = 1200s;

    impl(encryption_context& ctxt, const std::string& name, const host_options& options)
        : _ctxt(ctxt)
        , _name(name)
        , _options(options)
        , _attr_cache(utils::loading_cache_config{
            .max_size = std::numeric_limits<size_t>::max(),
            .expiry = options.key_cache_expiry.value_or(default_expiry),
            .refresh = default_refresh}, kms_log, std::bind(&impl::create_key, this, std::placeholders::_1))
        , _id_cache(utils::loading_cache_config{
            .max_size = std::numeric_limits<size_t>::max(),
            .expiry = options.key_cache_expiry.value_or(default_expiry),
            .refresh = default_refresh}, kms_log, std::bind(&impl::find_key, this, std::placeholders::_1))
    {
        // check if we have an explicit endpoint set.
        if (!_options.endpoint.empty()) {
            static std::regex simple_url(R"foo((https?):\/\/(?:([\w\.]+)|\[([\w:]+)\]):?(\d+)?\/?)foo");
            std::transform(_options.endpoint.begin(), _options.endpoint.end(), _options.endpoint.begin(), ::tolower);
            std::smatch m;
            if (!std::regex_match(_options.endpoint, m, simple_url)) {
                throw std::invalid_argument(format("Could not parse URL: {}", _options.endpoint));
            }
            _options.https = m[1].str() == "https";
            _options.host = m[2].length() > 0 ? m[2].str() : m[3].str();
            _options.port = m[4].length() > 0 ? std::stoi(m[4].str()) : 0;
        }
        if (_options.endpoint.empty() && _options.host.empty() && _options.aws_region.empty()) {
            throw std::invalid_argument("No AWS region or endpoint specified");
        }
        if (_options.port == 0) {
            _options.port = _options.https ? 443 : 80; 
        }
        kms_log.trace("Added KMS node {}={}", name, _options.endpoint.empty() 
            ? (_options.host.empty() ? _options.aws_region : _options.host)
            : _options.endpoint 
        );
    }
    ~impl() = default;

    future<> init();
    future<std::tuple<shared_ptr<symmetric_key>, id_type>> get_or_create_key(const key_info&);
    future<shared_ptr<symmetric_key>> get_key_by_id(const id_type&, const key_info&);

    static const inline key_info master_key_info = { "<none>", 0 };
private:
    class httpclient;
    using key_and_id_type = std::tuple<shared_ptr<symmetric_key>, id_type>;

    struct key_info_hash {
        size_t operator()(const key_info& i) const {
            return utils::tuple_hash()(std::tie(i.alg, i.len));
        }
    };

    future<rjson::value> post(std::string_view, const rjson::value&);

    future<key_and_id_type> create_key(const key_info&);
    future<bytes> find_key(const id_type&);

    encryption_context& _ctxt;
    std::string _name;
    host_options _options;
    utils::loading_cache<key_info, key_and_id_type, 2, utils::loading_cache_reload_enabled::yes,
        utils::simple_entry_size<key_and_id_type>, key_info_hash> _attr_cache;
    utils::loading_cache<id_type, bytes, 2, utils::loading_cache_reload_enabled::yes, 
        utils::simple_entry_size<bytes>> _id_cache;
    shared_ptr<seastar::tls::certificate_credentials> _creds;
    std::unordered_map<bytes, shared_ptr<symmetric_key>> _cache;
    bool _initialized = false;
};


namespace beast = boost::beast;     // from <boost/beast.hpp>
namespace http = beast::http;       // from <boost/beast/http.hpp>

/**
 * Not in seastar. Because nowhere near complete, thought through or
 * capable of dealing with anything but tiny aws messages.
 * 
 * TODO: formalize and move to seastar
 */
class encryption::kms_host::impl::httpclient {
public:
    httpclient(std::string host, uint16_t port, shared_ptr<seastar::tls::certificate_credentials> = {});

    httpclient& add_header(std::string_view key, std::string_view value);

    using result_type = http::response<http::string_body>;
    using request_type = http::request<http::string_body>;

    future<result_type> send();
    future<result_type> post();
    future<result_type> get();

    using method_type = http::verb;

    void method(method_type);
    void content(std::string_view);
    void target(std::string_view);

    request_type& request() {
        return _req;
    }
    const request_type& request() const {
        return _req;
    }
private:

    std::string _host;
    uint16_t _port;
    shared_ptr<seastar::tls::certificate_credentials> _creds;
    request_type _req;
};

encryption::kms_host::impl::httpclient::httpclient(std::string host, uint16_t port, shared_ptr<seastar::tls::certificate_credentials> creds)
    : _host(std::move(host))
    , _port(port)
    , _creds(std::move(creds))
{}

encryption::kms_host::impl::httpclient& encryption::kms_host::impl::httpclient::add_header(std::string_view key, std::string_view value) {
    _req.set(beast::string_view(key.data(), key.size()), beast::string_view(value.data(), value.size()));
    return *this;
}

future<encryption::kms_host::impl::httpclient::result_type> encryption::kms_host::impl::httpclient::send() {
    auto addr = co_await net::dns::resolve_name(_host);
    socket_address sa(addr, _port);
    connected_socket s = co_await (_creds 
        ? tls::connect(_creds, sa)
        : seastar::connect(sa)
    );

    s.set_keepalive(true);
    s.set_nodelay(true);

    auto out = s.output();
    auto in = s.input();

    http::serializer<true, http::string_body, typename decltype(_req)::fields_type> ser(_req);

    beast::error_code ec;
    std::exception_ptr ex;

    http::parser<false, http::string_body> p(result_type{});

    try {
        while (!ser.is_done()) {
            future<> f = make_ready_future<>();
            ser.next(ec, [&](beast::error_code& ec, auto&& buffers) {
                for (auto const buffer : beast::buffers_range (buffers)) {
                    f = f.then([&out, data = buffer.data(), size = buffer.size()] {
                        return out.write(static_cast<const char*>(data), size);
                    });
                }
                ser.consume(beast::buffer_bytes(buffers));
            });

            co_await std::move(f);

            if (ec.failed()) {
                break;
            }
        }

        co_await out.flush();

        p.eager(true);
        p.skip(false);

        if (!ec.failed()) {
            while (!p.is_done()) {
                auto buf = co_await in.read();
                if (buf.empty()) {
                    break;
                }
                // parse
                boost::asio::const_buffer wrap(buf.get(), buf.size());
                p.put(wrap, ec);
                if (ec.failed() && ec != http::error::need_more) {
                    break;
                }
                ec.clear();
            }
        }
    } catch (...) {
        ex = std::current_exception();
    }

    try {
        co_await out.close();
    } catch (...) {
        if (!ex) {
            ex = std::current_exception();
        }
    }
    try {
        co_await in.close();
    } catch (...) {
        if (!ex) {
            ex = std::current_exception();
        }
    }

    if (ec.failed()) {
        throw std::system_error(ec);
    }
    if (ex) {
        std::rethrow_exception(ex);
    }

    co_return p.release();
}

void encryption::kms_host::impl::httpclient::method(method_type m) {
    _req.method(m);
}

void encryption::kms_host::impl::httpclient::content(std::string_view body) {
    _req.body().assign(body.begin(), body.end());
    _req.set(http::field::content_length, std::to_string(_req.body().size()));
}

void encryption::kms_host::impl::httpclient::target(std::string_view target) {
    _req.target(std::string(target));
}

future<std::tuple<shared_ptr<encryption::symmetric_key>, encryption::kms_host::id_type>> encryption::kms_host::impl::get_or_create_key(const key_info& info) {
    return _attr_cache.get(info);
}

future<shared_ptr<encryption::symmetric_key>> encryption::kms_host::impl::get_key_by_id(const id_type& id, const key_info& info) {
    // note: since KMS does not really have any actual "key" associtation of id -> key,
    // we only cache/query raw bytes of some length. (See below).
    // Thus keys returned are always new objects. But they are not huge...
    auto data = co_await _id_cache.get(id);
    co_return make_shared<symmetric_key>(info, data);
}

// helper to build AWS request and parse result.
future<rjson::value> encryption::kms_host::impl::post(std::string_view target, const rjson::value& query) {
    auto creds = _creds;
    // if we are https, we need at least a credentials object that says "use system trust"
    if (!creds && _options.https) {
        creds = ::make_shared<seastar::tls::certificate_credentials>();

        if (!_options.priority_string.empty()) {
            creds->set_priority_string(_options.priority_string);
        } else {
            creds->set_priority_string(db::config::default_tls_priority);
        }

        if (!_options.certfile.empty()) {
            co_await creds->set_x509_key_file(_options.certfile, _options.keyfile, seastar::tls::x509_crt_format::PEM);
        }
        if (!_options.truststore.empty()) {
            co_await creds->set_x509_trust_file(_options.truststore, seastar::tls::x509_crt_format::PEM);
        } else {
            co_await creds->set_system_trust();
        }
        _creds = creds;
    }

    // some of this could be shared with alternator
    static constexpr const char* CONTENT_TYPE_HEADER = "content-type";
    static constexpr const char* HOST_HEADER = "host";
    static constexpr const char* AWS_DATE_HEADER = "X-Amz-Date";
    static constexpr const char* AWS_AUTHORIZATION_HEADER = "authorization";

    static constexpr const char* AMZ_TARGET_HEADER = "x-amz-target";
    static constexpr const char* AWS_HMAC_SHA256 = "AWS4-HMAC-SHA256";
    static constexpr const char* AWS4_REQUEST = "aws4_request";
    static constexpr const char* SIGNING_KEY = "AWS4";
    static constexpr const char* CREDENTIAL = "Credential";
    static constexpr const char* SIGNATURE = "Signature";
    static constexpr const char* SIGNED_HEADERS = "SignedHeaders";
    [[maybe_unused]] static constexpr const char* ACTION_HEADER = "Action";

    static constexpr const char* ISO_8601_BASIC = "{:%Y%m%dT%H%M%SZ}";
    static constexpr const char* SIMPLE_DATE_FORMAT_STR = "{:%Y%m%d}";
    static constexpr auto NEWLINE = '\n';

    if (_options.host.empty()) {
        // resolve region -> endpoint
        assert(!_options.aws_region.empty());
        static const char AWS_GLOBAL[] = "aws-global";
        static const char US_EAST_1[] = "us-east-1"; // US East (N. Virginia)
        static const char CN_NORTH_1[] = "cn-north-1"; // China (Beijing)
        static const char CN_NORTHWEST_1[] = "cn-northwest-1"; // China (Ningxia)
        static const char US_ISO_EAST_1[] = "us-iso-east-1";  // US ISO East
        static const char US_ISOB_EAST_1[] = "us-isob-east-1"; // US ISOB East (Ohio)

        // Fallback to us-east-1 if global endpoint does not exists.
        auto region = _options.aws_region == AWS_GLOBAL ? US_EAST_1 : _options.aws_region;

        std::stringstream ss;
        ss << "kms" << "." << region;

        if (region == CN_NORTH_1 || region == CN_NORTHWEST_1) {
            ss << ".amazonaws.com.cn";
        } else if (region == US_ISO_EAST_1) {
            ss << ".c2s.ic.gov";
        } else if (region == US_ISOB_EAST_1) {
            ss << ".sc2s.sgov.gov";
        } else {
            ss << ".amazonaws.com";
        }

        _options.host = ss.str();
    }

    // if we did not get full auth info in config, we can try to 
    // retrieve it from environment
    if (_options.aws_access_key_id.empty() || _options.aws_secret_access_key.empty()) {
        auto key_id = std::getenv("AWS_ACCESS_KEY_ID");
        auto key = std::getenv("AWS_SECRET_ACCESS_KEY");
        if (_options.aws_access_key_id.empty() && key_id) {
            kms_log.debug("No aws id specified. Using environment AWS_ACCESS_KEY_ID");
            _options.aws_access_key_id = key_id;
        }
        if (_options.aws_secret_access_key.empty() && key) {
            kms_log.debug("No aws secret specified. Using environment AWS_SECRET_ACCESS_KEY");
            _options.aws_secret_access_key = key;
        }
    }

    // if we did not get full auth info in config or env, we can try to 
    // retrieve it from ~/.aws/credentials
    if (_options.aws_access_key_id.empty() || _options.aws_secret_access_key.empty()) {
        auto home = std::getenv("HOME");
        if (home) {
            auto credentials = std::string(home) + "/.aws/credentials";
            auto credentials_exists = co_await seastar::file_exists(credentials);
            if (credentials_exists) {
                kms_log.debug("No aws id/secret specified. Trying to read credentials from {}", credentials);
                try {
                    auto buf = co_await read_text_file_fully(credentials);

                    static std::regex cred_line(R"foo(\s*([^\s]+)\s*=\s*([^\s]+)\s*\n)foo");
                    std::cregex_iterator i(buf.get(), buf.get() + buf.size(), cred_line), e;

                    std::string id, secret;
                    while (i != e) {
                        std::string key((*i)[1].str());
                        std::string val((*i)[2].str());
                        if (key == "aws_access_key_id") {
                            id = val;
                        } else if (key == "aws_secret_access_key") {
                            secret = val;
                        }
                        ++i;
                    }

                    if (!id.empty() && !_options.aws_access_key_id.empty() && id != _options.aws_access_key_id) {
                        throw std::invalid_argument(format("Mismatched aws id: {} != {}", id, _options.aws_access_key_id));
                    }
                    if (!id.empty() && _options.aws_access_key_id.empty()) {
                        _options.aws_access_key_id = id;
                    }
                    if (!secret.empty() && _options.aws_secret_access_key.empty()) {
                        _options.aws_secret_access_key = secret;
                    }
                    kms_log.debug("Read credentials from {} ({}:{}{})", credentials, _options.aws_access_key_id                    
                        , _options.aws_secret_access_key.substr(0, 2)
                        , std::string(_options.aws_secret_access_key.size()-2, '-')
                    );
                } catch (...) {
                    kms_log.debug("Could not read credentials: {}", std::current_exception());
                }
            }
        }
    }

    kms_log.trace("Building request: {} ({}:{})", target, _options.host, _options.port);

    httpclient client(_options.host, _options.port, std::move(creds));
    auto action = "TrentService."s + std::string(target);

    auto now = db_clock::now();
    auto t_now = fmt::gmtime(db_clock::to_time_t(now));
    auto timestamp = fmt::format(ISO_8601_BASIC, t_now);

    // see https://docs.aws.amazon.com/general/latest/gr/sigv4-create-canonical-request.html
    // see AWS SDK.
    // see https://docs.aws.amazon.com/general/latest/gr/sigv4-signed-request-examples.html
    std::stringstream signedHeadersStream;
    std::stringstream canonicalRequestStream;

    canonicalRequestStream 
        << "POST" << NEWLINE 
        << "/" << NEWLINE << NEWLINE
        ;

    auto to_lower = [](std::string_view s) {
        std::string tmp(s.size(), 0);
        std::transform(s.begin(), s.end(), tmp.begin(), ::tolower);
        return tmp;
    };

    auto add_signed_header = [&](std::string_view name, std::string_view value) {
        client.add_header(name, value);
        auto lname = to_lower(name);
        canonicalRequestStream << lname << ":" << value << NEWLINE;
        if (signedHeadersStream.tellp() != 0) {
            signedHeadersStream << ';';
        }
        signedHeadersStream << lname;
    };

    // headers must be sorted!
    auto host = _options.endpoint.empty() 
        ? _options.host 
        : _options.endpoint.substr(_options.endpoint.find_last_of('/')+1)
        ;

    add_signed_header(CONTENT_TYPE_HEADER, "application/x-amz-json-1.1");
    add_signed_header(HOST_HEADER, host);
    add_signed_header(AWS_DATE_HEADER, timestamp);
    add_signed_header(AMZ_TARGET_HEADER, action);

    client.add_header("Accept-Encoding", "identity");
    client.add_header("Accept", "*/*");

    auto make_hash = [&](std::string_view s) {
        auto sha256 = calculate_sha256(bytes_view(reinterpret_cast<const int8_t*>(s.data()), s.size()));
        auto hash = to_hex(sha256);
        return hash;
    };

    auto content = rjson::print(query);
    auto hash = make_hash(content);

    auto signedHeadersValue = signedHeadersStream.str();
    canonicalRequestStream << NEWLINE << signedHeadersValue << NEWLINE << hash;
    auto canonicalRequestString = canonicalRequestStream.str();
    auto canonicalRequestHash = make_hash(canonicalRequestString);

    kms_log.trace("Canonical request: {}", canonicalRequestString);

    auto simpleDate = fmt::format(SIMPLE_DATE_FORMAT_STR, t_now);
    auto serviceName = "kms";

    std::stringstream stringToSignStream;
    stringToSignStream << AWS_HMAC_SHA256 << NEWLINE 
        << timestamp << NEWLINE 
        << simpleDate << "/" << _options.aws_region << "/"
        << serviceName << "/" << AWS4_REQUEST << NEWLINE 
        << canonicalRequestHash
        ;
    auto stringToSign = stringToSignStream.str();

    // these log messages intentionally made to mimic aws sdk/boto3
    kms_log.trace("StringToSign: {}", stringToSign);

    std::string finalSignature;

    {
        auto tobv = [](std::string_view s) {
            return bytes_view(reinterpret_cast<const int8_t*>(s.data()), s.size());
        };

        auto signingKey = SIGNING_KEY + _options.aws_secret_access_key;
        auto kDate = hmac_sha256(tobv(simpleDate), tobv(signingKey));
        auto kRegion = hmac_sha256(tobv(_options.aws_region), kDate);
        auto kService = hmac_sha256(tobv(serviceName), kRegion);
        auto hashResult = hmac_sha256(tobv(AWS4_REQUEST), kService);
        auto finalHash = hmac_sha256(tobv(stringToSign), hashResult);
        finalSignature = to_hex(finalHash);
    }

    std::stringstream authStream;
    authStream << AWS_HMAC_SHA256 << " " 
        << CREDENTIAL << "=" << _options.aws_access_key_id << "/" << simpleDate << "/" << _options.aws_region 
        << "/" << serviceName << "/" << AWS4_REQUEST << ", " << SIGNED_HEADERS 
        << "=" << signedHeadersValue << ", " << SIGNATURE << "=" << finalSignature
        ;

    auto awsAuthString = authStream.str();

    client.add_header(AWS_AUTHORIZATION_HEADER, awsAuthString);
    client.target("/");
    client.content(content);
    client.method(httpclient::method_type::post);

    kms_log.trace("Request: {}", client.request());

    auto res = co_await client.send();

    kms_log.trace("Result: status={}, response={}", res.result_int(), res);

    auto body = rjson::empty_object();

    if (!res.body().empty()) {
        try {
            body = rjson::parse(std::string_view(res.body().data(), res.body().size()));
        } catch (...) {
            if (res.result() == http::status::ok) {
                throw;
            }
            // assume non-json formatted error. fall back to parsing below
        }
    } 

    if (res.result() != http::status::ok) {
        // try to format as good an error as we can.
        static const char* message_lc_header = "message";
        static const char* message_cc_header = "Message";
        static const char* error_type_header = "x-amzn-ErrorType";
        static const char* type_header = "__type";

        auto o = rjson::get_opt<std::string>(body, message_lc_header);
        if (!o) {
            o = rjson::get_opt<std::string>(body, message_cc_header);
        }
        auto msg = o.value_or("Unknown error");

        o = rjson::get_opt<std::string>(body, error_type_header);
        if (!o) {
            o = rjson::get_opt<std::string>(body, type_header);
        }
        // this should never happen with aws, but...
        auto type = o ? *o : [&]() -> std::string {
            switch (res.result()) {
            case http::status::unauthorized: case http::status::forbidden: return "AccessDenied";
            case http::status::not_found: return "ResourceNotFound";
            case http::status::too_many_requests: return "SlowDown";
            case http::status::internal_server_error: return "InternalError";
            case http::status::service_unavailable: return "ServiceUnavailable";
            case http::status::request_timeout: case http::status::gateway_timeout:
            case http::status::network_connect_timeout_error: 
                return "RequestTimeout";
            default:
                return format("{}", res.result());
            }
        }();

        throw kms_error(type, msg);
    }

    co_return body;
}

future<encryption::kms_host::impl::key_and_id_type> encryption::kms_host::impl::create_key(const key_info& info) {
    /**
     * AWS KMS does _not_ allow us to actually have "named keys" that can be used externally,
     * i.e. exported to us, here, for bulk encryption.
     * All named keys are 100% internal, the only options we have is using the
     * "GenerateDataKey" API. This creates a new (epiphermal) key, encrypts it 
     * using a named (internal) key, and gives us both raw and encrypted blobs
     * for usage as a local key.
     * To be able to actually re-use this key again, on decryption of data,
     * we employ the strategy recommended (https://docs.aws.amazon.com/kms/latest/APIReference/API_GenerateDataKey.html)
     * namely actually embedding the encrypted key in the key ID associated with 
     * the locally encrypted data. So ID:s become pretty big.
     * 
     * For ID -> key, we simply split the ID into the encrypted key part, and
     * the master key name part, decrypt the first using the second (AWS KMS Decrypt),
     * and create a local key using the result.
     * 
     * Data recovery:
     * Assuming you have data encrypted using a KMS generated key, you will have
     * metadata detailing algorithm, key length etc (see sstable metadata, and key info).
     * Metadata will also include a byte blob representing the ID of the encryption key.
     * For KMS, the ID will actually be a text string:
     *  <AWS key id>:<base64 encoded blob>
     *
     * I.e. something like:
     *   761f258a-e2e9-40b3-8891-602b1b8b947e:e56sadfafa3324ff=/wfsdfwssdf
     * or 
     *   arn:aws:kms:us-east-1:797456418907:key/761f258a-e2e9-40b3-8891-602b1b8b947e:e56sadfafa3324ff=/wfsdfwssdf
     *
     * (last colon is separator) 
     *
     * The actual data key can be retreived by doing a KMS "Decrypt" of the data blob part
     * using the KMS key referenced by the key ID. This gives back actual key data that can
     * be used to create a symmetric_key with algo, length etc as specified by metadata.
     *
     */

    // avoid creating too many keys and too many calls. If we are not shard 0, delegate there.
    if (this_shard_id() != 0) {
        auto [data, id] = co_await smp::submit_to(0, [this, info]() -> future<std::tuple<bytes, id_type>> {
            auto host = _ctxt.get_kms_host(_name);
            auto [k, id] = co_await host->get_or_create_key(info);
            co_return std::make_tuple(k != nullptr ? k->key() : bytes{}, id);
        });
        co_return key_and_id_type{ 
            data.empty() ? nullptr : make_shared<symmetric_key>(info, data), 
            id 
        };
    }

    // we use a special (illegal) info object to signal we want to find master 
    // key
    if (&info == &master_key_info) {
        kms_log.debug("Looking up master key");

        auto query = rjson::empty_object();
        rjson::add(query, "KeyId", _options.master_key);
        auto response = co_await post("DescribeKey", query);
        kms_log.debug("Master key exists");

        _initialized = true;

        co_return key_and_id_type{
            nullptr, // no key
            bytes(_options.master_key.begin(), _options.master_key.end())
        };
    }

    // before getting any "real" key, we must always ensure we 
    // have an existing master
    if (!_initialized) {
        co_await get_or_create_key(master_key_info);
    }

    // normal. note: since external keys are _not_ stored,
    // there is nothing we can "look up" or anything. Always 
    // new key here.

    kms_log.debug("Creating new key: {}", info);

    auto query = rjson::empty_object();

    rjson::add(query, "KeyId", std::string(_options.master_key.begin(), _options.master_key.end()));
    rjson::add(query, "NumberOfBytes", info.len/8);

    auto response = co_await post("GenerateDataKey", query);
    auto data = base64_decode(rjson::get<std::string>(response, "Plaintext"));
    auto enc = rjson::get<std::string>(response, "CiphertextBlob");
    auto kid = rjson::get<std::string>(response, "KeyId");

    auto key = make_shared<symmetric_key>(info, data);
    bytes id(kid.size() + 1 + enc.size(), 0);
    auto i = std::copy(kid.begin(), kid.end(), id.begin());
    *i++ = ':';
    std::copy(enc.begin(), enc.end(), i);

    co_return key_and_id_type{ key, id };
}

future<bytes> encryption::kms_host::impl::find_key(const id_type& id) {
    // avoid creating too many keys and too many calls. If we are not shard 0, delegate there.
    if (this_shard_id() != 0) {
        co_return co_await smp::submit_to(0, [this, id]() -> future<bytes> {
            auto host = _ctxt.get_kms_host(_name);
            auto bytes = co_await host->_impl->_id_cache.get(id);
            co_return bytes;
        });
    }

    // See create_key. ID consists of <master id>:<encrypted key blob>.
    // master id can (and will) contain ':', but blob will not.
    // (we are being wasteful, and keeping the base64 encoding - easier to read)
    auto pos = id.find_last_of(':');
    if (pos == id_type::npos) {
        throw std::invalid_argument(format("Not a valid key id: {}", id));
    }

    kms_log.debug("Finding key: {}", id);

    std::string kid(id.begin(), id.begin() + pos);
    std::string enc(id.begin() + pos + 1, id.end());

    auto query = rjson::empty_object();
    rjson::add(query, "CiphertextBlob", enc);
    rjson::add(query, "KeyId", kid);

    auto response = co_await post("Decrypt", query);
    auto data = base64_decode(rjson::get<std::string>(response, "Plaintext"));

    // we know nothing about key type etc, so just return data.
    co_return data;
}

encryption::kms_host::kms_host(encryption_context& ctxt, const std::string& name, const host_options& options)
    : _impl(std::make_unique<impl>(ctxt, name, options))
{}

encryption::kms_host::kms_host(encryption_context& ctxt, const std::string& name, const std::unordered_map<sstring, sstring>& map)
    : kms_host(ctxt, name, [&map] {
        host_options opts;
        map_wrapper<std::unordered_map<sstring, sstring>> m(map);

        auto get_or_error = [&](auto& what, auto& error) {
            try {
                return m(what).value();
            } catch (std::bad_optional_access&) {
                throw std::invalid_argument(format("No {} specified", error));
            }
        };

        opts.aws_access_key_id = m("aws_access_key_id").value_or("");
        opts.aws_secret_access_key = m("aws_secret_access_key").value_or("");
        opts.aws_region = m("aws_region").value_or("");

        // use "endpoint" semantics to match AWS configs.
        opts.endpoint = m("endpoint").value_or("");
        opts.host = m("host").value_or("");
        opts.port = std::stoi(m("port").value_or("0"));

        opts.master_key = get_or_error("master_key", "Master Key");
        opts.keyfile = m("keyfile").value_or("");
        opts.truststore = m("truststore").value_or("");
        opts.priority_string = m("priority_string").value_or("");

        return opts;
    }())
{}

encryption::kms_host::~kms_host() = default;

future<> encryption::kms_host::init() {
    // precreate master key
    co_await get_or_create_key(impl::master_key_info);
}

future<std::tuple<shared_ptr<encryption::symmetric_key>, encryption::kms_host::id_type>> encryption::kms_host::get_or_create_key(const key_info& info) {
    return _impl->get_or_create_key(info);
}

future<shared_ptr<encryption::symmetric_key>> encryption::kms_host::get_key_by_id(const id_type& id, const key_info& info) {
    return _impl->get_key_by_id(id, info);
}

