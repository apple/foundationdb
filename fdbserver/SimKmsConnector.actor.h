/*
 * SimKmsConnector.actor.h
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

#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_SIMKMSCONNECTOR_ACTOR_G_H)
#define FDBSERVER_SIMKMSCONNECTOR_ACTOR_G_H
#include "fdbserver/SimKmsConnector.actor.g.h"
#elif !defined(FDBSERVER_SIMKMSCONNECTOR_ACTOR_H)
#define FDBSERVER_SIMKMSCONNECTOR_ACTOR_H

#include "fdbserver/KmsConnector.h"
#include "flow/BlobCipher.h"

#include "flow/actorcompiler.h" // This must be the last #include.

using SimEncryptKey = std::string;
struct SimEncryptKeyCtx {
	EncryptCipherBaseKeyId id;
	SimEncryptKey key;

	SimEncryptKeyCtx() : id(0) {}
	explicit SimEncryptKeyCtx(EncryptCipherBaseKeyId kId, const char* data) : id(kId), key(data) {}
};

struct SimKmsConnectorContext {
	uint32_t maxEncryptionKeys;
	std::unordered_map<EncryptCipherBaseKeyId, std::unique_ptr<SimEncryptKeyCtx>> simEncryptKeyStore;

	SimKmsConnectorContext() : maxEncryptionKeys(0) {}
	explicit SimKmsConnectorContext(uint32_t keyCount) : maxEncryptionKeys(keyCount) {
		uint8_t buffer[AES_256_KEY_LENGTH];

		// Construct encryption keyStore.
		for (int i = 0; i < maxEncryptionKeys; i++) {
			generateRandomData(&buffer[0], AES_256_KEY_LENGTH);
			SimEncryptKeyCtx ctx(i, reinterpret_cast<const char*>(buffer));
			simEncryptKeyStore[i] = std::make_unique<SimEncryptKeyCtx>(i, reinterpret_cast<const char*>(buffer));
		}
	}
};

class SimKmsConnector : public KmsConnector {
public:
	SimKmsConnector();
	Future<Void> connectorCore(KmsConnectorInterface interf);

private:
	ACTOR Future<Void> simConnectorCore(SimKmsConnectorContext* ctx, KmsConnectorInterface interf);

	SimKmsConnectorContext simKmsConnCtx;
};

#endif