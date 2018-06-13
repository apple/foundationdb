/*
 * FDBLibTLSSession.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

#include "FDBLibTLSSession.h"

#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/pem.h>
#include <openssl/x509.h>
#include <openssl/x509_vfy.h>

#include <exception>

#include <set>
#include <string.h>
#include <limits.h>

static ssize_t tls_read_func(struct tls *ctx, void *buf, size_t buflen, void *cb_arg)
{
	FDBLibTLSSession *session = (FDBLibTLSSession *)cb_arg;

	int rv = session->recv_func(session->recv_ctx, (uint8_t *)buf, buflen);
	if (rv < 0)
		return 0;
	if (rv == 0)
		return TLS_WANT_POLLIN;
	return (ssize_t)rv;
}

static ssize_t tls_write_func(struct tls *ctx, const void *buf, size_t buflen, void *cb_arg)
{
	FDBLibTLSSession *session = (FDBLibTLSSession *)cb_arg;

	int rv = session->send_func(session->send_ctx, (const uint8_t *)buf, buflen);
	if (rv < 0)
		return 0;
	if (rv == 0)
		return TLS_WANT_POLLOUT;
	return (ssize_t)rv;
}

FDBLibTLSSession::FDBLibTLSSession(Reference<FDBLibTLSPolicy> policy, bool is_client, const char* servername, TLSSendCallbackFunc send_func, void* send_ctx, TLSRecvCallbackFunc recv_func, void* recv_ctx, void* uid) :
	tls_ctx(NULL), tls_sctx(NULL), is_client(is_client), policy(policy), send_func(send_func), send_ctx(send_ctx),
	recv_func(recv_func), recv_ctx(recv_ctx), handshake_completed(false), uid(uid) {

	if (is_client) {
		if ((tls_ctx = tls_client()) == NULL) {
			policy->logf("FDBLibTLSClientError", uid, true, NULL);
			throw std::runtime_error("FDBLibTLSClientError");
		}
		if (tls_configure(tls_ctx, policy->tls_cfg) == -1) {
			policy->logf("FDBLibTLSConfigureError", uid, true, "LibTLSErrorMessage", tls_error(tls_ctx), NULL);
			tls_free(tls_ctx);
			throw std::runtime_error("FDBLibTLSConfigureError");
		}
		if (tls_connect_cbs(tls_ctx, tls_read_func, tls_write_func, this, servername) == -1) {
			policy->logf("FDBLibTLSConnectError", uid, true, "LibTLSErrorMessage", tls_error(tls_ctx), NULL);
			tls_free(tls_ctx);
			throw std::runtime_error("FDBLibTLSConnectError");
		}
	} else {
		if ((tls_sctx = tls_server()) == NULL) {
			policy->logf("FDBLibTLSServerError", uid, true, NULL);
			throw std::runtime_error("FDBLibTLSServerError");
		}
		if (tls_configure(tls_sctx, policy->tls_cfg) == -1) {
			policy->logf("FDBLibTLSConfigureError", uid, true, "LibTLSErrorMessage", tls_error(tls_sctx), NULL);
			tls_free(tls_sctx);
			throw std::runtime_error("FDBLibTLSConfigureError");
		}
		if (tls_accept_cbs(tls_sctx, &tls_ctx, tls_read_func, tls_write_func, this) == -1) {
			policy->logf("FDBLibTLSAcceptError", uid, true, "LibTLSErrorMessage", tls_error(tls_sctx), NULL);
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

bool match_criteria(X509_NAME *name, int nid, const char *value, size_t len) {
	unsigned char *name_entry_utf8 = NULL, *criteria_utf8 = NULL;
	int name_entry_utf8_len, criteria_utf8_len;
	ASN1_STRING *criteria = NULL;
	X509_NAME_ENTRY *name_entry;
	BIO *bio;
	bool rc = false;
	int idx;

	if ((criteria = ASN1_IA5STRING_new()) == NULL)
		goto err;
	if (ASN1_STRING_set(criteria, value, len) != 1)
		goto err;

	// If name does not exist, or has multiple of this RDN, refuse to proceed.
	if ((idx = X509_NAME_get_index_by_NID(name, nid, -1)) < 0)
		goto err;
	if (X509_NAME_get_index_by_NID(name, nid, idx) != -1)
		goto err;
	if ((name_entry = X509_NAME_get_entry(name, idx)) == NULL)
		goto err;

	// Convert both to UTF8 and compare.
	if ((criteria_utf8_len = ASN1_STRING_to_UTF8(&criteria_utf8, criteria)) < 1)
		goto err;
	if ((name_entry_utf8_len = ASN1_STRING_to_UTF8(&name_entry_utf8, name_entry->value)) < 1)
		goto err;
	if (criteria_utf8_len == name_entry_utf8_len &&
	    memcmp(criteria_utf8, name_entry_utf8, criteria_utf8_len) == 0)
		rc = true;

 err:
	ASN1_STRING_free(criteria);
	free(criteria_utf8);
	free(name_entry_utf8);

	return rc;
}

std::tuple<bool,std::string> FDBLibTLSSession::check_verify(Reference<FDBLibTLSVerify> verify, struct stack_st_X509 *certs) {
	X509_STORE_CTX *store_ctx = NULL;
	X509_NAME *subject, *issuer;
	BIO *bio = NULL;
	bool rc = false;
	// if returning false, give a reason string
	std::string reason = "";

	// If certificate verification is disabled, there's nothing more to do.
	if (!verify->verify_cert)
		return std::make_tuple(true, reason);

	// Verify the certificate.
	if ((store_ctx = X509_STORE_CTX_new()) == NULL) {
		policy->logf("FDBLibTLSOutOfMemory", uid, true, NULL);
		reason = "FDBLibTLSOutOfMemory";
		goto err;
	}
	if (!X509_STORE_CTX_init(store_ctx, NULL, sk_X509_value(certs, 0), certs)) {
		reason = "FDBLibTLSStoreCtxInit";
		goto err;
	}
	X509_STORE_CTX_trusted_stack(store_ctx, policy->roots);
	X509_STORE_CTX_set_default(store_ctx, is_client ? "ssl_server" : "ssl_client");
	if (!verify->verify_time)
		X509_VERIFY_PARAM_set_flags(X509_STORE_CTX_get0_param(store_ctx), X509_V_FLAG_NO_CHECK_TIME);
	if (X509_verify_cert(store_ctx) <= 0) {
		const char *errstr = X509_verify_cert_error_string(X509_STORE_CTX_get_error(store_ctx));
		reason = "FDBLibTLSVerifyCert VerifyError " + std::string(errstr);
		goto err;
	}

	// Check subject criteria.
	if ((subject = X509_get_subject_name(sk_X509_value(store_ctx->chain, 0))) == NULL) {
		reason = "FDBLibTLSCertSubjectError";
		goto err;
	}
	for (auto &pair: verify->subject_criteria) {
		if (!match_criteria(subject, pair.first, pair.second.c_str(), pair.second.size())) {
			reason = "FDBLibTLSCertSubjectMatchFailure";
			goto err;
		}
	}

	// Check issuer criteria.
	if ((issuer = X509_get_issuer_name(sk_X509_value(store_ctx->chain, 0))) == NULL) {
		reason = "FDBLibTLSCertIssuerError";
		goto err;
	}
	for (auto &pair: verify->issuer_criteria) {
		if (!match_criteria(issuer, pair.first, pair.second.c_str(), pair.second.size())) {
			reason = "FDBLibTLSCertIssuerMatchFailure";
			goto err;
		}
	}

	// Check root criteria - this is the subject of the final certificate in the stack.
	if ((subject = X509_get_subject_name(sk_X509_value(store_ctx->chain, sk_X509_num(store_ctx->chain) - 1))) == NULL) {
		reason = "FDBLibTLSRootSubjectError";
		goto err;
	}
	for (auto &pair: verify->root_criteria) {
		if (!match_criteria(subject, pair.first, pair.second.c_str(), pair.second.size())) {
			reason = "FDBLibTLSRootSubjectMatchFailure";
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
	struct stack_st_X509 *certs = NULL;
	const uint8_t *cert_pem;
	size_t cert_pem_len;
	bool rc = false;
	std::set<std::string> verify_failure_reasons;
	bool verify_success;
	std::string verify_failure_reason;

	// If no verify peer rules have been set, we are relying on standard
	// libtls verification.
	if (policy->verify_rules.empty())
		return true;

	if ((cert_pem = tls_peer_cert_chain_pem(tls_ctx, &cert_pem_len)) == NULL) {
		policy->logf("FDBLibTLSNoCertError", uid, true, NULL);
		goto err;
	}
	if ((certs = policy->parse_cert_pem(cert_pem, cert_pem_len)) == NULL)
		goto err;

	// Any matching rule is sufficient.
	for (auto &verify_rule: policy->verify_rules) {
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
		for (std::string reason : verify_failure_reasons) {
			policy->logf(reason.c_str(), uid, false, NULL);
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
		policy->logf("FDBLibTLSHandshakeError", uid, false, "LibTLSErrorMessage", tls_error(tls_ctx), NULL);
		return FAILED;
	}
}

int FDBLibTLSSession::read(uint8_t* data, int length) {
	if (!handshake_completed) {
		policy->logf("FDBLibTLSReadHandshakeError", uid, true, NULL);
		return FAILED;
	}

	ssize_t n = tls_read(tls_ctx, data, length);
	if (n > 0) {
		if (n > INT_MAX) {
			policy->logf("FDBLibTLSReadOverflow", uid, true, NULL);
			return FAILED;
		}
		return (int)n;
	}
	if (n == 0) {
		policy->logf("FDBLibTLSReadEOF", uid, false, NULL);
		return FAILED;
	}
	if (n == TLS_WANT_POLLIN)
		return WANT_READ;
	if (n == TLS_WANT_POLLOUT)
		return WANT_WRITE;

	policy->logf("FDBLibTLSReadError", uid, false, "LibTLSErrorMessage", tls_error(tls_ctx), NULL);
	return FAILED;
}

int FDBLibTLSSession::write(const uint8_t* data, int length) {
	if (!handshake_completed) {
		policy->logf("FDBLibTLSWriteHandshakeError", uid, true, NULL);
		return FAILED;
	}

	ssize_t n = tls_write(tls_ctx, data, length);
	if (n > 0) {
		if (n > INT_MAX) {
			policy->logf("FDBLibTLSWriteOverflow", uid, true, NULL);
			return FAILED;
		}
		return (int)n;
	}
	if (n == 0) {
		policy->logf("FDBLibTLSWriteEOF", uid, false, NULL);
		return FAILED;
	}
	if (n == TLS_WANT_POLLIN)
		return WANT_READ;
	if (n == TLS_WANT_POLLOUT)
		return WANT_WRITE;

	policy->logf("FDBLibTLSWriteError", uid, false, "LibTLSErrorMessage", tls_error(tls_ctx), NULL);
	return FAILED;
}
