/*
 * FDBLibTLSPolicy.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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

#include "FDBLibTLS/FDBLibTLSPolicy.h"
#include "FDBLibTLS/FDBLibTLSSession.h"
#include "flow/Trace.h"

#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/x509.h>

#include <algorithm>
#include <exception>
#include <map>
#include <string>
#include <vector>

#include <string.h>
#include <limits.h>

FDBLibTLSPolicy::FDBLibTLSPolicy(Reference<FDBLibTLSPlugin> plugin)
  : plugin(plugin), tls_cfg(nullptr), roots(nullptr), session_created(false), ca_data_set(false), cert_data_set(false),
    key_data_set(false), verify_peers_set(false) {

	if ((tls_cfg = tls_config_new()) == nullptr) {
		TraceEvent(SevError, "FDBLibTLSConfigError").log();
		throw std::runtime_error("FDBLibTLSConfigError");
	}

	// Require client certificates for authentication.
	tls_config_verify_client(tls_cfg);
}

FDBLibTLSPolicy::~FDBLibTLSPolicy() {
	sk_X509_pop_free(roots, X509_free);
	tls_config_free(tls_cfg);
}

ITLSSession* FDBLibTLSPolicy::create_session(bool is_client,
                                             const char* servername,
                                             TLSSendCallbackFunc send_func,
                                             void* send_ctx,
                                             TLSRecvCallbackFunc recv_func,
                                             void* recv_ctx,
                                             void* uid) {
	if (is_client) {
		// If verify peers has been set then there is no point specifying a
		// servername, since this will be ignored - the servername should be
		// matched by the verify criteria instead.
		if (verify_peers_set && servername != nullptr) {
			TraceEvent(SevError, "FDBLibTLSVerifyPeersWithServerName").log();
			return nullptr;
		}

		// If verify peers has not been set, then require a server name to
		// avoid an accidental lack of name validation.
		if (!verify_peers_set && servername == nullptr) {
			TraceEvent(SevError, "FDBLibTLSNoServerName").log();
			return nullptr;
		}
	}

	session_created = true;
	try {
		return new FDBLibTLSSession(Reference<FDBLibTLSPolicy>::addRef(this),
		                            is_client,
		                            servername,
		                            send_func,
		                            send_ctx,
		                            recv_func,
		                            recv_ctx,
		                            uid);
	} catch (...) {
		return nullptr;
	}
}

static int password_cb(char* buf, int size, int rwflag, void* u) {
	const char* password = (const char*)u;
	int plen;

	if (size < 0)
		return 0;
	if (u == nullptr)
		return 0;

	plen = strlen(password);
	if (plen > size)
		return 0;

	// Note: buf does not need to be NUL-terminated since
	// we return an explicit length.
	strncpy(buf, password, size);

	return plen;
}

struct stack_st_X509* FDBLibTLSPolicy::parse_cert_pem(const uint8_t* cert_pem, size_t cert_pem_len) {
	struct stack_st_X509* certs = nullptr;
	X509* cert = nullptr;
	BIO* bio = nullptr;
	int errnum;

	if (cert_pem_len > INT_MAX)
		goto err;
	if ((bio = BIO_new_mem_buf((void*)cert_pem, cert_pem_len)) == nullptr) {
		TraceEvent(SevError, "FDBLibTLSOutOfMemory").log();
		goto err;
	}
	if ((certs = sk_X509_new_null()) == nullptr) {
		TraceEvent(SevError, "FDBLibTLSOutOfMemory").log();
		goto err;
	}

	ERR_clear_error();
	while ((cert = PEM_read_bio_X509(bio, nullptr, password_cb, nullptr)) != nullptr) {
		if (!sk_X509_push(certs, cert)) {
			TraceEvent(SevError, "FDBLibTLSOutOfMemory").log();
			goto err;
		}
	}

	// Ensure that the NULL cert was caused by EOF and not some other failure.
	errnum = ERR_peek_last_error();
	if (ERR_GET_LIB(errnum) != ERR_LIB_PEM || ERR_GET_REASON(errnum) != PEM_R_NO_START_LINE) {
		char errbuf[256];

		ERR_error_string_n(errnum, errbuf, sizeof(errbuf));
		TraceEvent(SevError, "FDBLibTLSCertDataError").detail("LibcryptoErrorMessage", errbuf);
		goto err;
	}

	if (sk_X509_num(certs) < 1) {
		TraceEvent(SevError, "FDBLibTLSNoCerts").log();
		goto err;
	}

	BIO_free(bio);

	return certs;

err:
	sk_X509_pop_free(certs, X509_free);
	X509_free(cert);
	BIO_free(bio);

	return nullptr;
}

bool FDBLibTLSPolicy::set_ca_data(const uint8_t* ca_data, int ca_len) {
	if (ca_data_set) {
		TraceEvent(SevError, "FDBLibTLSCAAlreadySet").log();
		return false;
	}
	if (session_created) {
		TraceEvent(SevError, "FDBLibTLSPolicyAlreadyActive").log();
		return false;
	}

	if (ca_len < 0)
		return false;
	sk_X509_pop_free(roots, X509_free);
	if ((roots = parse_cert_pem(ca_data, ca_len)) == nullptr)
		return false;

	if (tls_config_set_ca_mem(tls_cfg, ca_data, ca_len) == -1) {
		TraceEvent(SevError, "FDBLibTLSCAError").detail("LibTLSErrorMessage", tls_config_error(tls_cfg));
		return false;
	}

	ca_data_set = true;

	return true;
}

bool FDBLibTLSPolicy::set_cert_data(const uint8_t* cert_data, int cert_len) {
	if (cert_data_set) {
		TraceEvent(SevError, "FDBLibTLSCertAlreadySet").log();
		return false;
	}
	if (session_created) {
		TraceEvent(SevError, "FDBLibTLSPolicyAlreadyActive").log();
		return false;
	}

	if (tls_config_set_cert_mem(tls_cfg, cert_data, cert_len) == -1) {
		TraceEvent(SevError, "FDBLibTLSCertError").detail("LibTLSErrorMessage", tls_config_error(tls_cfg));
		return false;
	}

	cert_data_set = true;

	return true;
}

bool FDBLibTLSPolicy::set_key_data(const uint8_t* key_data, int key_len, const char* password) {
	EVP_PKEY* key = nullptr;
	BIO* bio = nullptr;
	bool rc = false;

	if (key_data_set) {
		TraceEvent(SevError, "FDBLibTLSKeyAlreadySet").log();
		goto err;
	}
	if (session_created) {
		TraceEvent(SevError, "FDBLibTLSPolicyAlreadyActive").log();
		goto err;
	}

	if (password != nullptr) {
		char* data;
		long len;

		if ((bio = BIO_new_mem_buf((void*)key_data, key_len)) == nullptr) {
			TraceEvent(SevError, "FDBLibTLSOutOfMemory").log();
			goto err;
		}
		ERR_clear_error();
		if ((key = PEM_read_bio_PrivateKey(bio, nullptr, password_cb, (void*)password)) == nullptr) {
			int errnum = ERR_peek_error();
			char errbuf[256];

			if ((ERR_GET_LIB(errnum) == ERR_LIB_PEM && ERR_GET_REASON(errnum) == PEM_R_BAD_DECRYPT) ||
			    (ERR_GET_LIB(errnum) == ERR_LIB_EVP && ERR_GET_REASON(errnum) == EVP_R_BAD_DECRYPT)) {
				TraceEvent(SevError, "FDBLibTLSIncorrectPassword").log();
			} else {
				ERR_error_string_n(errnum, errbuf, sizeof(errbuf));
				TraceEvent(SevError, "FDBLibTLSPrivateKeyError").detail("LibcryptoErrorMessage", errbuf);
			}
			goto err;
		}
		BIO_free(bio);
		if ((bio = BIO_new(BIO_s_mem())) == nullptr) {
			TraceEvent(SevError, "FDBLibTLSOutOfMemory").log();
			goto err;
		}
		if (!PEM_write_bio_PrivateKey(bio, key, nullptr, nullptr, 0, nullptr, nullptr)) {
			TraceEvent(SevError, "FDBLibTLSOutOfMemory").log();
			goto err;
		}
		if ((len = BIO_get_mem_data(bio, &data)) <= 0) {
			TraceEvent(SevError, "FDBLibTLSOutOfMemory").log();
			goto err;
		}
		if (tls_config_set_key_mem(tls_cfg, (const uint8_t*)data, len) == -1) {
			TraceEvent(SevError, "FDBLibTLSKeyError").detail("LibTLSErrorMessage", tls_config_error(tls_cfg));
			goto err;
		}
	} else {
		if (tls_config_set_key_mem(tls_cfg, key_data, key_len) == -1) {
			TraceEvent(SevError, "FDBLibTLSKeyError").detail("LibTLSErrorMessage", tls_config_error(tls_cfg));
			goto err;
		}
	}

	key_data_set = true;
	rc = true;

err:
	BIO_free(bio);
	EVP_PKEY_free(key);
	return rc;
}

bool FDBLibTLSPolicy::set_verify_peers(int count, const uint8_t* verify_peers[], int verify_peers_len[]) {
	if (verify_peers_set) {
		TraceEvent(SevError, "FDBLibTLSVerifyPeersAlreadySet").log();
		return false;
	}
	if (session_created) {
		TraceEvent(SevError, "FDBLibTLSPolicyAlreadyActive").log();
		return false;
	}

	if (count < 1) {
		TraceEvent(SevError, "FDBLibTLSNoVerifyPeers").log();
		return false;
	}

	for (int i = 0; i < count; i++) {
		try {
			std::string verifyString((const char*)verify_peers[i], verify_peers_len[i]);
			int start = 0;
			while (start < verifyString.size()) {
				int split = verifyString.find('|', start);
				if (split == std::string::npos) {
					break;
				}
				if (split == start || verifyString[split - 1] != '\\') {
					auto verify = makeReference<FDBLibTLSVerify>(verifyString.substr(start, split - start));
					verify_rules.push_back(verify);
					start = split + 1;
				}
			}
			auto verify = makeReference<FDBLibTLSVerify>(verifyString.substr(start));
			verify_rules.push_back(verify);
		} catch (const std::runtime_error&) {
			verify_rules.clear();
			std::string verifyString((const char*)verify_peers[i], verify_peers_len[i]);
			TraceEvent(SevError, "FDBLibTLSVerifyPeersParseError").detail("Config", verifyString);
			return false;
		}
	}

	// All verification is manually handled (as requested via configuration).
	tls_config_insecure_noverifycert(tls_cfg);
	tls_config_insecure_noverifyname(tls_cfg);
	tls_config_insecure_noverifytime(tls_cfg);

	verify_peers_set = true;

	return true;
}
