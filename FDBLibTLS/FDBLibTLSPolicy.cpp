// Apple Proprietary and Confidential Information

#include "FDBLibTLSPolicy.h"
#include "FDBLibTLSSession.h"

#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/objects.h>
#include <openssl/obj_mac.h>
#include <openssl/pem.h>
#include <openssl/x509.h>

#include <algorithm>
#include <exception>
#include <map>
#include <string>
#include <vector>


FDBLibTLSPolicy::FDBLibTLSPolicy(Reference<FDBLibTLSPlugin> plugin, ITLSLogFunc logf):
	plugin(plugin), logf(logf), tls_cfg(NULL), session_created(false), cert_data_set(false),
	key_data_set(false), verify_peers_set(false), verify_cert(true), verify_time(true) {

	if ((tls_cfg = tls_config_new()) == NULL) {
		logf("FDBLibTLSConfigError", NULL, true, NULL);
		throw std::runtime_error("FDBLibTLSConfigError");
	}

	// Require client certificates for authentication.
	tls_config_verify_client(tls_cfg);

	// Name verification is always manually handled (if requested via configuration).
	tls_config_insecure_noverifyname(tls_cfg);
}

FDBLibTLSPolicy::~FDBLibTLSPolicy() {
	tls_config_free(tls_cfg);
}

ITLSSession* FDBLibTLSPolicy::create_session(bool is_client, TLSSendCallbackFunc send_func, void* send_ctx, TLSRecvCallbackFunc recv_func, void* recv_ctx, void* uid) {
	session_created = true;
	try {
		return new FDBLibTLSSession(Reference<FDBLibTLSPolicy>::addRef(this), is_client, send_func, send_ctx, recv_func, recv_ctx, uid);
	} catch ( ... ) {
		return NULL;
	}
}

static int hexValue(char c) {
	static char const digits[] = "0123456789ABCDEF";

	if (c >= 'a' && c <= 'f')
		c -= ('a' - 'A');

	int value = std::find(digits, digits + 16, c) - digits;
	if (value >= 16) {
		throw std::runtime_error("hexValue");
	}
	return value;
}

// Does not handle "raw" form (e.g. #28C4D1), only escaped text
static std::string de4514(std::string const& input, int start, int& out_end) {
	std::string output;

	if(input[start] == '#' || input[start] == ' ') {
		out_end = start;
		return output;
	}

	int space_count = 0;

	for(int p = start; p < input.size();) {
		switch(input[p]) {
		case '\\': // Handle escaped sequence

			// Backslash escaping nothing!
			if(p == input.size() - 1) {
				out_end = p;
				goto FIN;
			}

			switch(input[p+1]) {
			case ' ':
			case '"':
			case '#':
			case '+':
			case ',':
			case ';':
			case '<':
			case '=':
			case '>':
			case '\\':
				output += input[p+1];
				p += 2;
				space_count = 0;
				continue;

			default:
				// Backslash escaping pair of hex digits requires two characters
				if(p == input.size() - 2) {
					out_end = p;
					goto FIN;
				}

				try {
					output += hexValue(input[p+1]) * 16 + hexValue(input[p+2]);
					p += 3;
					space_count = 0;
					continue;
				} catch( ... ) {
					out_end = p;
					goto FIN;
				}
			}

		case '"':
		case '+':
		case ',':
		case ';':
		case '<':
		case '>':
		case 0:
			// All of these must have been escaped
			out_end = p;
			goto FIN;

		default:
			// Character is what it is
			output += input[p];
			if(input[p] == ' ')
				space_count++;
			else
				space_count = 0;
			p++;
		}
	}

	out_end = input.size();

 FIN:
	out_end -= space_count;
	output.resize(output.size() - space_count);

	return output;
}

static std::pair<std::string, std::string> splitPair(std::string const& input, char c) {
	int p = input.find_first_of(c);
	if(p == input.npos) {
		throw std::runtime_error("splitPair");
	}
	return std::make_pair(input.substr(0, p), input.substr(p+1, input.size()));
}

static int abbrevToNID(std::string const& sn) {
	int nid = NID_undef;

	if (sn == "C" || sn == "CN" || sn == "L" || sn == "ST" || sn == "O" || sn == "OU")
		nid = OBJ_sn2nid(sn.c_str());
	if (nid == NID_undef)
		throw std::runtime_error("abbrevToNID");

	return nid;
}

void FDBLibTLSPolicy::parse_verify(std::string input) {
	int s = 0;

	while (s < input.size()) {
		int eq = input.find('=', s);

		if (eq == input.npos)
			throw std::runtime_error("parse_verify");

		std::string term = input.substr(s, eq - s);

		if (term.find("Check.") == 0) {
			if (eq + 2 > input.size())
				throw std::runtime_error("parse_verify");
			if (eq + 2 != input.size() && input[eq + 2] != ',')
				throw std::runtime_error("parse_verify");

			bool* flag;

			if (term == "Check.Valid")
				flag = &verify_cert;
			else if (term == "Check.Unexpired")
				flag = &verify_time;
			else
				throw std::runtime_error("parse_verify");

			if (input[eq + 1] == '0')
				*flag = false;
			else if (input[eq + 1] == '1')
				*flag = true;
			else
				throw std::runtime_error("parse_verify");

			s = eq + 3;
		} else {
			std::map<int, std::string>* criteria = &subject_criteria;

			if (term.find('.') != term.npos) {
				auto scoped = splitPair(term, '.');

				if (scoped.first == "S" || scoped.first == "Subject")
					criteria = &subject_criteria;
				else if (scoped.first == "I" || scoped.first == "Issuer")
					criteria = &issuer_criteria;
				else
					throw std::runtime_error("parse_verify");

				term = scoped.second;
			}

			int remain;
			auto unesc = de4514(input, eq + 1, remain);

			if (remain == eq + 1)
				throw std::runtime_error("parse_verify");

			criteria->insert(std::make_pair(abbrevToNID(term), unesc));

			if (remain != input.size() && input[remain] != ',')
				throw std::runtime_error("parse_verify");

			s = remain + 1;
		}
	}
}

void FDBLibTLSPolicy::reset_verify() {
        verify_cert = true;
        verify_time = true;
	subject_criteria = {};
	issuer_criteria = {};
}

int password_cb(char *buf, int size, int rwflag, void *u) {
	// A no-op password callback is provided simply to stop libcrypto
	// from trying to use its own password reading functionality.
	return 0;
}

bool FDBLibTLSPolicy::set_cert_data(const uint8_t* cert_data, int cert_len) {
	struct stack_st_X509 *certs = NULL;
	unsigned long errnum;
	X509 *cert = NULL;
	BIO *bio = NULL;
	long data_len;
	char *data;
	bool rc = false;

	// The cert data contains one or more PEM encoded certificates - the
	// first certificate is for this host, with any additional certificates
	// being the full certificate chain. As such, the last certificate
	// is the trusted root certificate. If only one certificate is provided
	// then it is required to be a self-signed certificate, which is also
	// treated as the trusted root.

	if (cert_data_set) {
		logf("FDBLibTLSCertAlreadySet", NULL, true, NULL);
		goto err;
	}
	if (session_created) {
		logf("FDBLibTLSPolicyAlreadyActive", NULL, true, NULL);
		goto err;
	}

	if ((certs = sk_X509_new_null()) == NULL) {
		logf("FDBLibTLSOutOfMemory", NULL, true, NULL);
		goto err;
	}
	if ((bio = BIO_new_mem_buf((void *)cert_data, cert_len)) == NULL) {
		logf("FDBLibTLSOutOfMemory", NULL, true, NULL);
		goto err;
	}

	ERR_clear_error();
	while ((cert = PEM_read_bio_X509(bio, NULL, password_cb, NULL)) != NULL) {
		if (!sk_X509_push(certs, cert)) {
			logf("FDBLibTLSOutOfMemory", NULL, true, NULL);
			goto err;
		}
	}

	// Ensure that the NULL cert was caused by EOF and not some other failure.
	errnum = ERR_peek_last_error();
	if (ERR_GET_LIB(errnum) != ERR_LIB_PEM || ERR_GET_REASON(errnum) != PEM_R_NO_START_LINE) {
		char errbuf[256];

		ERR_error_string_n(errnum, errbuf, sizeof(errbuf));
		logf("FDBLibTLSCertDataError", NULL, true, "LibcryptoErrorMessage", errbuf, NULL);
		goto err;
	}

	if (sk_X509_num(certs) < 1) {
		logf("FDBLibTLSNoCerts", NULL, true, NULL);
		goto err;
	}

	BIO_free_all(bio);
	if ((bio = BIO_new(BIO_s_mem())) == NULL) {
		logf("FDBLibTLSOutOfMemory", NULL, true, NULL);
		goto err;
	}
	if (!PEM_write_bio_X509(bio, sk_X509_value(certs, sk_X509_num(certs) - 1))) {
		logf("FDBLibTLSCertWriteError", NULL, true, NULL);
		goto err;
	}
	if ((data_len = BIO_get_mem_data(bio, &data)) <= 0) {
		logf("FDBLibTLSCertError", NULL, true, NULL);
		goto err;
	}

	if (tls_config_set_ca_mem(tls_cfg, (const uint8_t *)data, data_len) == -1) {
		logf("FDBLibTLSSetCAError", NULL, true, "LibTLSErrorMessage", tls_config_error(tls_cfg), NULL);
		goto err;
	}

	if (sk_X509_num(certs) > 1) {
		BIO_free_all(bio);
		if ((bio = BIO_new(BIO_s_mem())) == NULL) {
			logf("FDBLibTLSOutOfMemory", NULL, true, NULL);
			goto err;
		}
		for (int i = 0; i < sk_X509_num(certs) - 1; i++) {
			if (!PEM_write_bio_X509(bio, sk_X509_value(certs, i))) {
				logf("FDBLibTLSCertWriteError", NULL, true, NULL);
				goto err;
			}
		}
		if ((data_len = BIO_get_mem_data(bio, &data)) <= 0) {
			logf("FDBLibTLSCertError", NULL, true, NULL);
			goto err;
		}
	}

	if (tls_config_set_cert_mem(tls_cfg, (const uint8_t *)data, data_len) == -1) {
		logf("FDBLibTLSSetCertError", NULL, true, "LibTLSErrorMessage", tls_config_error(tls_cfg), NULL);
		goto err;
	}

	rc = true;

 err:
	sk_X509_pop_free(certs, X509_free);
	X509_free(cert);
	BIO_free_all(bio);

	return rc;
}

bool FDBLibTLSPolicy::set_key_data(const uint8_t* key_data, int key_len) {
	if (key_data_set) {
		logf("FDBLibTLSKeyAlreadySet", NULL, true, NULL);
		return false;
	}
	if (session_created) {
		logf("FDBLibTLSPolicyAlreadyActive", NULL, true, NULL);
		return false;
	}

	if (tls_config_set_key_mem(tls_cfg, key_data, key_len) == -1) {
		logf("FDBLibTLSKeyError", NULL, true, "LibTLSErrorMessage", tls_config_error(tls_cfg), NULL);
		return false;
	}

	key_data_set = true;

	return true;
}

bool FDBLibTLSPolicy::set_verify_peers(const uint8_t* verify_peers, int verify_peers_len) {
	if (verify_peers_set) {
		logf("FDBLibTLSVerifyPeersAlreadySet", NULL, true, NULL);
		return false;
	}
	if (session_created) {
		logf("FDBLibTLSPolicyAlreadyActive", NULL, true, NULL);
		return false;
	}

	try {
		parse_verify(std::string((const char*)verify_peers, verify_peers_len));
	} catch ( const std::runtime_error& e ) {
		reset_verify();
		logf("FDBLibTLSVerifyPeersParseError", NULL, true, "Config", verify_peers, NULL);
		return false;
	}

	if (!verify_cert)
		tls_config_insecure_noverifycert(tls_cfg);

	if (!verify_time)
		tls_config_insecure_noverifytime(tls_cfg);

	verify_peers_set = true;

	return true;
}
