/*
 * EncryptKeyProxy.actor.h
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

#pragma once

// When actually compiled (NO_INTELLISENSE), include the generated version of this file.  In intellisense use the source
// version.
#if defined(NO_INTELLISENSE) && !defined(ENCRYPT_KEY_PROXY_ACTOR_G_H)
#define ENCRYPT_KEY_PROXY_ACTOR_G_H
#include "fdbserver/EncryptKeyProxy.actor.g.h"
#elif !defined(ENCRYPT_KEY_PROXY_ACTOR_H)
#define ENCRYPT_KEY_PROXY_ACTOR_H
#include "flow/flow.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace EncryptKeyProxy {

inline bool canRetryWith(Error e) {
	// The below are the only errors that should be retried, all others should throw immediately
	switch (e.code()) {
	case error_code_encrypt_keys_fetch_failed:
	case error_code_timed_out:
	case error_code_connection_failed:
		return true;
	default:
		return false;
	}
}

} // namespace EncryptKeyProxy

ACTOR template <class T>
Future<T> kmsReqWithExponentialBackoff(std::function<Future<T>()> func, StringRef funcName) {
	state int numRetries = 0;
	state double kmsBackoff = FLOW_KNOBS->EKP_KMS_CONNECTION_BACKOFF;
	TraceEvent("KMSRequestStart").detail("Function", funcName);

	loop {
		try {
			T val = wait(func());
			return val;
		} catch (Error& e) {
			TraceEvent(SevWarn, "KMSRequestReceivedError").detail("Function", funcName).detail("ErrorCode", e.code());
			if (!EncryptKeyProxy::canRetryWith(e)) {
				throw e;
			}
			if (numRetries >= FLOW_KNOBS->EKP_KMS_CONNECTION_RETRIES) {
				TraceEvent(SevWarnAlways, "KMSRequestRetryLimitExceeded")
				    .detail("Function", funcName)
				    .detail("ErrorCode", e.code());
				// TODO: Should we throw a time out error here?
				throw e;
			}
			numRetries++;
			wait(delay(kmsBackoff));
			kmsBackoff = kmsBackoff * 2; // exponential backoff
		}
	}
}

#include "flow/unactorcompiler.h"
#endif