// Apple Proprietary and Confidential Information

#ifndef FDB_LIBTLS_POLICY_H
#define FDB_LIBTLS_POLICY_H

#pragma once

#include "FDBLibTLSPlugin.h"
#include "ITLSPlugin.h"
#include "ReferenceCounted.h"

#include <map>
#include <string>

struct FDBLibTLSPolicy: ITLSPolicy, ReferenceCounted<FDBLibTLSPolicy> {
	FDBLibTLSPolicy(Reference<FDBLibTLSPlugin> plugin, ITLSLogFunc logf);
	virtual ~FDBLibTLSPolicy();

	virtual void addref() { ReferenceCounted<FDBLibTLSPolicy>::addref(); }
	virtual void delref() { ReferenceCounted<FDBLibTLSPolicy>::delref(); }

	Reference<FDBLibTLSPlugin> plugin;
	ITLSLogFunc logf;

	virtual ITLSSession* create_session(bool is_client, TLSSendCallbackFunc send_func, void* send_ctx, TLSRecvCallbackFunc recv_func, void* recv_ctx, void* uid);

	void parse_verify(std::string input);
	void reset_verify(void);

	virtual bool set_cert_data(const uint8_t* cert_data, int cert_len);
	virtual bool set_key_data(const uint8_t* key_data, int key_len);
	virtual bool set_verify_peers(const uint8_t* verify_peers, int verify_peers_len);

	struct tls_config *tls_cfg;

	bool session_created;

	bool cert_data_set;
	bool key_data_set;
	bool verify_peers_set;

        bool verify_cert;
        bool verify_time;

	std::map<int, std::string> subject_criteria;
	std::map<int, std::string> issuer_criteria;
};

#endif /* FDB_LIBTLS_POLICY_H */
