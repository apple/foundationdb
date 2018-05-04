/*
 * FDBLibTLSPolicy.h
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

#ifndef FDB_LIBTLS_POLICY_H
#define FDB_LIBTLS_POLICY_H

#pragma once

#include "fdblibtlsPlugin.h"
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
