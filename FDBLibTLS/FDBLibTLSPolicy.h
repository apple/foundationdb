/*
 * FDBLibTLSPolicy.h
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

#ifndef FDB_LIBTLS_POLICY_H
#define FDB_LIBTLS_POLICY_H

#pragma once

#include "fdbrpc/ITLSPlugin.h"
#include "flow/FastRef.h"

#include "FDBLibTLS/FDBLibTLSPlugin.h"
#include "FDBLibTLS/FDBLibTLSVerify.h"

#include <string>
#include <vector>

struct FDBLibTLSPolicy : ITLSPolicy, ReferenceCounted<FDBLibTLSPolicy> {
	FDBLibTLSPolicy(Reference<FDBLibTLSPlugin> plugin);
	virtual ~FDBLibTLSPolicy();

	virtual void addref() { ReferenceCounted<FDBLibTLSPolicy>::addref(); }
	virtual void delref() { ReferenceCounted<FDBLibTLSPolicy>::delref(); }

	Reference<FDBLibTLSPlugin> plugin;

	virtual ITLSSession* create_session(bool is_client,
	                                    const char* servername,
	                                    TLSSendCallbackFunc send_func,
	                                    void* send_ctx,
	                                    TLSRecvCallbackFunc recv_func,
	                                    void* recv_ctx,
	                                    void* uid);

	struct stack_st_X509* parse_cert_pem(const uint8_t* cert_pem, size_t cert_pem_len);
	void parse_verify(std::string input);
	void reset_verify(void);

	virtual bool set_ca_data(const uint8_t* ca_data, int ca_len);
	virtual bool set_cert_data(const uint8_t* cert_data, int cert_len);
	virtual bool set_key_data(const uint8_t* key_data, int key_len, const char* password);
	virtual bool set_verify_peers(int count, const uint8_t* verify_peers[], int verify_peers_len[]);

	struct tls_config* tls_cfg;

	bool session_created;

	bool ca_data_set;
	bool cert_data_set;
	bool key_data_set;
	bool verify_peers_set;

	struct stack_st_X509* roots;

	std::vector<Reference<FDBLibTLSVerify>> verify_rules;
};

#endif /* FDB_LIBTLS_POLICY_H */
