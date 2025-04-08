/**
 * Credentials.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2025 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifdef FLOW_GRPC_ENABLED
#ifndef FDBRPC_FLOW_GRPC_TLS_CREDENTIAL_RELOADER_H
#define FDBRPC_FLOW_GRPC_TLS_CREDENTIAL_RELOADER_H

#include <memory>

#include <grpcpp/security/credentials.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/security/tls_certificate_provider.h>
#include <grpcpp/security/tls_credentials_options.h>

#include "flow/Knobs.h"
#include "flow/TLSConfig.actor.h"

namespace ge = grpc::experimental;

// Abstract base class for providing gRPC authentication credentials.
//
// This class defines a common interface for obtaining gRPC server and client credentials. gRPC
// Channels used by server and clients will use this to obtain credentials. Implementations may
// provide secure or insecure credentials based on the underlying configuration.
class GrpcCredentialProvider {
public:
	virtual ~GrpcCredentialProvider() {};

	virtual std::shared_ptr<grpc::ServerCredentials> serverCredentials() const = 0;
	virtual std::shared_ptr<grpc::ChannelCredentials> clientCredentials() const = 0;
	virtual bool validate() const = 0;
};

// Provides insecure (unauthenticated) gRPC credentials.
//
// This implementation of `GrpcCredentialProvider` returns insecure credentials, allowing gRPC
// connections without authentication or encryption.
class GrpcInsecureCredentialProvider : public GrpcCredentialProvider {
public:
	std::shared_ptr<grpc::ServerCredentials> serverCredentials() const override {
		return grpc::InsecureServerCredentials();
	}

	std::shared_ptr<grpc::ChannelCredentials> clientCredentials() const override {
		return grpc::InsecureChannelCredentials();
	}

	bool validate() const override { return true; }
};

// Provides TLS-based gRPC credentials with dynamic certificate reloading.
//
// This implementation uses a `FileWatcherCertificateProvider` to dynamically reload TLS
// certificates from the filesystem at runtime. It supports mutual TLS (mTLS) by enforcing client
// certificate validation.
//
// This provider is used in both client and server implementations, ensuring behavior consistent
// with FlowTransport. To enable mutual TLS (mTLS), the server options are configured with
// `GRPC_SSL_REQUEST_AND_REQUIRE_CLIENT_CERTIFICATE_AND_VERIFY`, which mandates that clients present
// a valid certificate for authentication.
class GrpcTlsCredentialProvider : public GrpcCredentialProvider {
public:
	GrpcTlsCredentialProvider(TLSConfig* config)
	  : provider_(std::make_shared<ge::FileWatcherCertificateProvider>(config->getKeyPathSync(),
	                                                                   config->getCertificatePathSync(),
	                                                                   config->getCAPathSync(),
	                                                                   FLOW_KNOBS->TLS_CERT_REFRESH_DELAY_SECONDS)),
	    server_options_(ge::TlsServerCredentialsOptions(provider_)) {

		// Required for mTLS.
		server_options_.set_cert_request_type(GRPC_SSL_REQUEST_AND_REQUIRE_CLIENT_CERTIFICATE_AND_VERIFY);

		// Since the files can change, we'll need refetch from provider.
		server_options_.watch_root_certs();
		server_options_.watch_identity_key_cert_pairs();
		client_options_.watch_root_certs();
		client_options_.watch_identity_key_cert_pairs();

		// Set the provider for client. Server and client channels within same process
		// share same provider.
		client_options_.set_certificate_provider(provider_);
	}

	std::shared_ptr<grpc::ServerCredentials> serverCredentials() const override {
		return ge::TlsServerCredentials(server_options_);
	}

	std::shared_ptr<grpc::ChannelCredentials> clientCredentials() const override {
		return ge::TlsCredentials(client_options_);
	}

	bool validate() const override { return provider_->ValidateCredentials().ok(); }

private:
	std::shared_ptr<ge::FileWatcherCertificateProvider> provider_;
	ge::TlsServerCredentialsOptions server_options_;
	ge::TlsChannelCredentialsOptions client_options_;
};

// Provides TLS-based gRPC credentials with static, in-memory certificate storage.
//
// Unlike `GrpcTlsCredentialProvider`, this implementation does not watch the filesystem for
// updates. Instead, it loads TLS certificates from static data at initialization. This is useful
// for embedding credentials in a secure environment where dynamic updates are not required. For
// now, its just used in tests.
class GrpcTlsCredentialStaticProvider : public GrpcCredentialProvider {
public:
	GrpcTlsCredentialStaticProvider(const std::string& key, const std::string& cert, const std::string& ca)
	  : provider_(
	        std::make_shared<ge::StaticDataCertificateProvider>(ca,
	                                                            std::vector{ ge::IdentityKeyCertPair{ key, cert } })),
	    server_options_(ge::TlsServerCredentialsOptions(provider_)) {

		// Required for mTLS
		server_options_.set_cert_request_type(GRPC_SSL_REQUEST_AND_REQUIRE_CLIENT_CERTIFICATE_AND_VERIFY); // mTLS

		// Since the files can change, we'll need refetch from provider. Not sure why static
		// config needs this, but it won't work without it. (FIXME)
		server_options_.watch_root_certs();
		server_options_.watch_identity_key_cert_pairs();
		client_options_.watch_root_certs();
		client_options_.watch_identity_key_cert_pairs();

		// Set the provider for client. Server and client channels within same process
		// share same provider.
		client_options_.set_certificate_provider(provider_);
	}

	bool validate() const override { return provider_->ValidateCredentials().ok(); }

	std::shared_ptr<grpc::ServerCredentials> serverCredentials() const override {
		return ge::TlsServerCredentials(server_options_);
	}

	std::shared_ptr<grpc::ChannelCredentials> clientCredentials() const override {
		return ge::TlsCredentials(client_options_);
	}

private:
	std::shared_ptr<ge::StaticDataCertificateProvider> provider_;
	ge::TlsServerCredentialsOptions server_options_;
	ge::TlsChannelCredentialsOptions client_options_;
};

#endif // FDBRPC_FLOW_GRPC_TLS_CREDENTIAL_RELOADER_H
#endif // FLOW_GRPC_ENABLED
