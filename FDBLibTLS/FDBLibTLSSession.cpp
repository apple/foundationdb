// Apple Proprietary and Confidential Information

#include "FDBLibTLSSession.h"

#include <openssl/bio.h>
#include <openssl/pem.h>
#include <openssl/x509.h>

#include <exception>
#include <iostream>

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

FDBLibTLSSession::FDBLibTLSSession(Reference<FDBLibTLSPolicy> policy, bool is_client, TLSSendCallbackFunc send_func, void* send_ctx, TLSRecvCallbackFunc recv_func, void* recv_ctx, void* uid) :
	tls_ctx(NULL), tls_sctx(NULL), policy(policy), send_func(send_func), send_ctx(send_ctx), recv_func(recv_func), recv_ctx(recv_ctx), handshake_completed(false), uid(uid) {

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
		if (tls_connect_cbs(tls_ctx, tls_read_func, tls_write_func, this, NULL) == -1) {
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

int password_cb(char *buf, int size, int rwflag, void *u);

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

bool FDBLibTLSSession::check_criteria() {
	X509_NAME *subject, *issuer;
	const uint8_t *cert_pem;
	size_t cert_pem_len;
	X509 *cert = NULL;
	BIO *bio = NULL;
	bool rc = false;

	// If certificate verification is disabled, there's nothing more to do.
	if (!policy->verify_cert)
		return true;

	// If no criteria have been specified, then we're done.
	if (policy->subject_criteria.size() == 0 && policy->issuer_criteria.size() == 0)
		return true;

	if ((cert_pem = tls_peer_cert_chain_pem(tls_ctx, &cert_pem_len)) == NULL) {
		policy->logf("FDBLibTLSNoCertError", uid, true, NULL);
		goto err;
	}
	if ((bio = BIO_new_mem_buf((void *)cert_pem, cert_pem_len)) == NULL) {
		policy->logf("FDBLibTLSOutOfMemory", NULL, true, NULL);
		goto err;
	}
	if ((cert = PEM_read_bio_X509(bio, NULL, password_cb, NULL)) == NULL) {
		policy->logf("FDBLibTLSCertPEMError", uid, true, NULL);
		goto err;
	}

	// Check subject criteria.
	if ((subject = X509_get_subject_name(cert)) == NULL) {
		policy->logf("FDBLibTLSCertSubjectError", uid, true, NULL);
		goto err;
	}
	for (auto &pair: policy->subject_criteria) {
		if (!match_criteria(subject, pair.first, pair.second.c_str(), pair.second.size())) {
			policy->logf("FDBLibTLSCertSubjectMatchFailure", uid, true, NULL);
			goto err;
		}
        }

	// Check issuer criteria.
	if ((issuer = X509_get_issuer_name(cert)) == NULL) {
		policy->logf("FDBLibTLSCertIssuerError", uid, true, NULL);
		goto err;
	}
	for (auto &pair: policy->issuer_criteria) {
		if (!match_criteria(issuer, pair.first, pair.second.c_str(), pair.second.size())) {
			policy->logf("FDBLibTLSCertIssuerMatchFailure", uid, true, NULL);
			goto err;
		}
        }

	// If we got this far, everything checked out...
	rc = true;

 err:
	BIO_free_all(bio);
	X509_free(cert);

	return rc;
}

int FDBLibTLSSession::handshake() {
	int rv = tls_handshake(tls_ctx);

	switch (rv) {
	case 0:
		if (!check_criteria())
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
