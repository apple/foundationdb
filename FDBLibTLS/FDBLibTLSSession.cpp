/*
 * FDBLibTLSSession.cpp
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

#include "FDBLibTLS/FDBLibTLSSession.h"

#include "flow/flow.h"
#include "flow/Trace.h"

#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/pem.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>
#include <openssl/x509_vfy.h>

#include <exception>

#include <set>
#include <string.h>
#include <limits.h>

static ssize_t tls_read_func(struct tls* ctx, void* buf, size_t buflen, void* cb_arg) {
	FDBLibTLSSession* session = (FDBLibTLSSession*)cb_arg;

	int rv = session->recv_func(session->recv_ctx, (uint8_t*)buf, buflen);
	if (rv < 0)
		return 0;
	if (rv == 0)
		return TLS_WANT_POLLIN;
	return (ssize_t)rv;
}

static ssize_t tls_write_func(struct tls* ctx, const void* buf, size_t buflen, void* cb_arg) {
	FDBLibTLSSession* session = (FDBLibTLSSession*)cb_arg;

	int rv = session->send_func(session->send_ctx, (const uint8_t*)buf, buflen);
	if (rv < 0)
		return 0;
	if (rv == 0)
		return TLS_WANT_POLLOUT;
	return (ssize_t)rv;
}

FDBLibTLSSession::FDBLibTLSSession(Reference<FDBLibTLSPolicy> policy,
                                   bool is_client,
                                   const char* servername,
                                   TLSSendCallbackFunc send_func,
                                   void* send_ctx,
                                   TLSRecvCallbackFunc recv_func,
                                   void* recv_ctx,
                                   void* uidptr)
  : tls_ctx(nullptr), tls_sctx(nullptr), is_client(is_client), policy(policy), send_func(send_func), send_ctx(send_ctx),
    recv_func(recv_func), recv_ctx(recv_ctx), handshake_completed(false), lastVerifyFailureLogged(0.0) {
	if (uidptr)
		uid = *(UID*)uidptr;

	if (is_client) {
		if ((tls_ctx = tls_client()) == nullptr) {
			TraceEvent(SevError, "FDBLibTLSClientError", uid).log();
			throw std::runtime_error("FDBLibTLSClientError");
		}
		if (tls_configure(tls_ctx, policy->tls_cfg) == -1) {
			TraceEvent(SevError, "FDBLibTLSConfigureError", uid).detail("LibTLSErrorMessage", tls_error(tls_ctx));
			tls_free(tls_ctx);
			throw std::runtime_error("FDBLibTLSConfigureError");
		}
		if (tls_connect_cbs(tls_ctx, tls_read_func, tls_write_func, this, servername) == -1) {
			TraceEvent(SevError, "FDBLibTLSConnectError", uid).detail("LibTLSErrorMessage", tls_error(tls_ctx));
			tls_free(tls_ctx);
			throw std::runtime_error("FDBLibTLSConnectError");
		}
	} else {
		if ((tls_sctx = tls_server()) == nullptr) {
			TraceEvent(SevError, "FDBLibTLSServerError", uid).log();
			throw std::runtime_error("FDBLibTLSServerError");
		}
		if (tls_configure(tls_sctx, policy->tls_cfg) == -1) {
			TraceEvent(SevError, "FDBLibTLSConfigureError", uid).detail("LibTLSErrorMessage", tls_error(tls_sctx));
			tls_free(tls_sctx);
			throw std::runtime_error("FDBLibTLSConfigureError");
		}
		if (tls_accept_cbs(tls_sctx, &tls_ctx, tls_read_func, tls_write_func, this) == -1) {
			TraceEvent(SevError, "FDBLibTLSAcceptError", uid).detail("LibTLSErrorMessage", tls_error(tls_sctx));
			tls_free(tls_sctx);
			throw std::runtime_error("FDBLibTLSAcceptError");
		}
	}
}

FDBLibTLSSession::~FDBLibTLSSession() {
	// This would ideally call tls_close(), however that means either looping
	// in a destructor or doing it opportunistically...
	tls_free(tls_ctx);
	tls_free(tls_sctx);
}

bool match_criteria_entry(const std::string& criteria, ASN1_STRING* entry, MatchType mt) {
	bool rc = false;
	ASN1_STRING* asn_criteria = nullptr;
	unsigned char* criteria_utf8 = nullptr;
	int criteria_utf8_len = 0;
	unsigned char* entry_utf8 = nullptr;
	int entry_utf8_len = 0;

	if ((asn_criteria = ASN1_IA5STRING_new()) == nullptr)
		goto err;
	if (ASN1_STRING_set(asn_criteria, criteria.c_str(), criteria.size()) != 1)
		goto err;
	if ((criteria_utf8_len = ASN1_STRING_to_UTF8(&criteria_utf8, asn_criteria)) < 1)
		goto err;
	if ((entry_utf8_len = ASN1_STRING_to_UTF8(&entry_utf8, entry)) < 1)
		goto err;
	if (mt == MatchType::EXACT) {
		if (criteria_utf8_len == entry_utf8_len && memcmp(criteria_utf8, entry_utf8, criteria_utf8_len) == 0)
			rc = true;
	} else if (mt == MatchType::PREFIX) {
		if (criteria_utf8_len <= entry_utf8_len && memcmp(criteria_utf8, entry_utf8, criteria_utf8_len) == 0)
			rc = true;
	} else if (mt == MatchType::SUFFIX) {
		if (criteria_utf8_len <= entry_utf8_len &&
		    memcmp(criteria_utf8, entry_utf8 + (entry_utf8_len - criteria_utf8_len), criteria_utf8_len) == 0)
			rc = true;
	}

err:
	ASN1_STRING_free(asn_criteria);
	free(criteria_utf8);
	free(entry_utf8);
	return rc;
}

bool match_name_criteria(X509_NAME* name, NID nid, const std::string& criteria, MatchType mt) {
	X509_NAME_ENTRY* name_entry;
	int idx;

	// If name does not exist, or has multiple of this RDN, refuse to proceed.
	if ((idx = X509_NAME_get_index_by_NID(name, nid, -1)) < 0)
		return false;
	if (X509_NAME_get_index_by_NID(name, nid, idx) != -1)
		return false;
	if ((name_entry = X509_NAME_get_entry(name, idx)) == nullptr)
		return false;

	return match_criteria_entry(criteria, name_entry->value, mt);
}

bool match_extension_criteria(X509* cert, NID nid, const std::string& value, MatchType mt) {
	if (nid != NID_subject_alt_name && nid != NID_issuer_alt_name) {
		// I have no idea how other extensions work.
		return false;
	}
	auto pos = value.find(':');
	if (pos == value.npos) {
		return false;
	}
	std::string value_gen = value.substr(0, pos);
	std::string value_val = value.substr(pos + 1, value.npos);
	STACK_OF(GENERAL_NAME)* sans =
	    reinterpret_cast<STACK_OF(GENERAL_NAME)*>(X509_get_ext_d2i(cert, nid, nullptr, nullptr));
	if (sans == nullptr) {
		return false;
	}
	int num_sans = sk_GENERAL_NAME_num(sans);
	bool rc = false;
	for (int i = 0; i < num_sans && !rc; ++i) {
		GENERAL_NAME* altname = sk_GENERAL_NAME_value(sans, i);
		std::string matchable;
		switch (altname->type) {
		case GEN_OTHERNAME:
			break;
		case GEN_EMAIL:
			if (value_gen == "EMAIL" && match_criteria_entry(value_val, altname->d.rfc822Name, mt)) {
				rc = true;
				break;
			}
		case GEN_DNS:
			if (value_gen == "DNS" && match_criteria_entry(value_val, altname->d.dNSName, mt)) {
				rc = true;
				break;
			}
		case GEN_X400:
		case GEN_DIRNAME:
		case GEN_EDIPARTY:
			break;
		case GEN_URI:
			if (value_gen == "URI" && match_criteria_entry(value_val, altname->d.uniformResourceIdentifier, mt)) {
				rc = true;
				break;
			}
		case GEN_IPADD:
			if (value_gen == "IP" && match_criteria_entry(value_val, altname->d.iPAddress, mt)) {
				rc = true;
				break;
			}
		case GEN_RID:
			break;
		}
	}
	sk_GENERAL_NAME_pop_free(sans, GENERAL_NAME_free);
	return rc;
}

bool match_criteria(X509* cert,
                    X509_NAME* subject,
                    NID nid,
                    const std::string& criteria,
                    MatchType mt,
                    X509Location loc) {
	switch (loc) {
	case X509Location::NAME: {
		return match_name_criteria(subject, nid, criteria, mt);
	}
	case X509Location::EXTENSION: {
		return match_extension_criteria(cert, nid, criteria, mt);
	}
	}
	// Should never be reachable.
	return false;
}

std::tuple<bool, std::string> FDBLibTLSSession::check_verify(Reference<FDBLibTLSVerify> verify,
                                                             struct stack_st_X509* certs) {
	X509_STORE_CTX* store_ctx = nullptr;
	X509_NAME *subject, *issuer;
	bool rc = false;
	X509* cert = nullptr;
	// if returning false, give a reason string
	std::string reason = "";

	// If certificate verification is disabled, there's nothing more to do.
	if (!verify->verify_cert)
		return std::make_tuple(true, reason);

	// Verify the certificate.
	if ((store_ctx = X509_STORE_CTX_new()) == nullptr) {
		TraceEvent(SevError, "FDBLibTLSOutOfMemory", uid).log();
		reason = "Out of memory";
		goto err;
	}
	if (!X509_STORE_CTX_init(store_ctx, nullptr, sk_X509_value(certs, 0), certs)) {
		reason = "Store ctx init";
		goto err;
	}
	X509_STORE_CTX_trusted_stack(store_ctx, policy->roots);
	X509_STORE_CTX_set_default(store_ctx, is_client ? "ssl_server" : "ssl_client");
	if (!verify->verify_time)
		X509_VERIFY_PARAM_set_flags(X509_STORE_CTX_get0_param(store_ctx), X509_V_FLAG_NO_CHECK_TIME);
	if (X509_verify_cert(store_ctx) <= 0) {
		const char* errstr = X509_verify_cert_error_string(X509_STORE_CTX_get_error(store_ctx));
		reason = "Verify cert error: " + std::string(errstr);
		goto err;
	}

	// Check subject criteria.
	cert = sk_X509_value(store_ctx->chain, 0);
	if ((subject = X509_get_subject_name(cert)) == nullptr) {
		reason = "Cert subject error";
		goto err;
	}
	for (auto& pair : verify->subject_criteria) {
		if (!match_criteria(
		        cert, subject, pair.first, pair.second.criteria, pair.second.match_type, pair.second.location)) {
			reason = "Cert subject match failure";
			goto err;
		}
	}

	// Check issuer criteria.
	if ((issuer = X509_get_issuer_name(cert)) == nullptr) {
		reason = "Cert issuer error";
		goto err;
	}
	for (auto& pair : verify->issuer_criteria) {
		if (!match_criteria(
		        cert, issuer, pair.first, pair.second.criteria, pair.second.match_type, pair.second.location)) {
			reason = "Cert issuer match failure";
			goto err;
		}
	}

	// Check root criteria - this is the subject of the final certificate in the stack.
	cert = sk_X509_value(store_ctx->chain, sk_X509_num(store_ctx->chain) - 1);
	if ((subject = X509_get_subject_name(cert)) == nullptr) {
		reason = "Root subject error";
		goto err;
	}
	for (auto& pair : verify->root_criteria) {
		if (!match_criteria(
		        cert, subject, pair.first, pair.second.criteria, pair.second.match_type, pair.second.location)) {
			reason = "Root subject match failure";
			goto err;
		}
	}

	// If we got this far, everything checked out...
	rc = true;

err:
	X509_STORE_CTX_free(store_ctx);

	return std::make_tuple(rc, reason);
}

bool FDBLibTLSSession::verify_peer() {
	struct stack_st_X509* certs = nullptr;
	const uint8_t* cert_pem;
	size_t cert_pem_len;
	bool rc = false;
	std::set<std::string> verify_failure_reasons;
	bool verify_success;
	std::string verify_failure_reason;

	// If no verify peer rules have been set, we are relying on standard
	// libtls verification.
	if (policy->verify_rules.empty())
		return true;

	if ((cert_pem = tls_peer_cert_chain_pem(tls_ctx, &cert_pem_len)) == nullptr) {
		TraceEvent(SevError, "FDBLibTLSNoCertError", uid).log();
		goto err;
	}
	if ((certs = policy->parse_cert_pem(cert_pem, cert_pem_len)) == nullptr)
		goto err;

	// Any matching rule is sufficient.
	for (auto& verify_rule : policy->verify_rules) {
		std::tie(verify_success, verify_failure_reason) = check_verify(verify_rule, certs);
		if (verify_success) {
			rc = true;
			break;
		} else {
			if (verify_failure_reason.length() > 0)
				verify_failure_reasons.insert(verify_failure_reason);
		}
	}

	if (!rc) {
		// log the various failure reasons
		if (now() - lastVerifyFailureLogged > 1.0) {
			for (std::string reason : verify_failure_reasons) {
				lastVerifyFailureLogged = now();
				TraceEvent("FDBLibTLSVerifyFailure", uid).suppressFor(1.0).detail("Reason", reason);
			}
		}
	}

err:
	sk_X509_pop_free(certs, X509_free);

	return rc;
}

int FDBLibTLSSession::handshake() {
	int rv = tls_handshake(tls_ctx);

	switch (rv) {
	case 0:
		if (!verify_peer())
			return FAILED;
		handshake_completed = true;
		return SUCCESS;
	case TLS_WANT_POLLIN:
		return WANT_READ;
	case TLS_WANT_POLLOUT:
		return WANT_WRITE;
	default:
		TraceEvent("FDBLibTLSHandshakeError", uid).suppressFor(1.0).detail("LibTLSErrorMessage", tls_error(tls_ctx));
		return FAILED;
	}
}

int FDBLibTLSSession::read(uint8_t* data, int length) {
	if (!handshake_completed) {
		TraceEvent(SevError, "FDBLibTLSReadHandshakeError").log();
		return FAILED;
	}

	ssize_t n = tls_read(tls_ctx, data, length);
	if (n > 0) {
		if (n > INT_MAX) {
			TraceEvent(SevError, "FDBLibTLSReadOverflow").log();
			return FAILED;
		}
		return (int)n;
	}
	if (n == 0) {
		TraceEvent("FDBLibTLSReadEOF").suppressFor(1.0);
		return FAILED;
	}
	if (n == TLS_WANT_POLLIN)
		return WANT_READ;
	if (n == TLS_WANT_POLLOUT)
		return WANT_WRITE;

	TraceEvent("FDBLibTLSReadError", uid).suppressFor(1.0).detail("LibTLSErrorMessage", tls_error(tls_ctx));
	return FAILED;
}

int FDBLibTLSSession::write(const uint8_t* data, int length) {
	if (!handshake_completed) {
		TraceEvent(SevError, "FDBLibTLSWriteHandshakeError", uid).log();
		return FAILED;
	}

	ssize_t n = tls_write(tls_ctx, data, length);
	if (n > 0) {
		if (n > INT_MAX) {
			TraceEvent(SevError, "FDBLibTLSWriteOverflow", uid).log();
			return FAILED;
		}
		return (int)n;
	}
	if (n == 0) {
		TraceEvent("FDBLibTLSWriteEOF", uid).suppressFor(1.0);
		return FAILED;
	}
	if (n == TLS_WANT_POLLIN)
		return WANT_READ;
	if (n == TLS_WANT_POLLOUT)
		return WANT_WRITE;

	TraceEvent("FDBLibTLSWriteError", uid).suppressFor(1.0).detail("LibTLSErrorMessage", tls_error(tls_ctx));
	return FAILED;
}
