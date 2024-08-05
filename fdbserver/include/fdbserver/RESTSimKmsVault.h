/*
 * RESTSimKmsVault.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#ifndef FDBSERVER_REST_SIM_KMS_VAULT_H
#define FDBSERVER_REST_SIM_KMS_VAULT_H
#pragma once

#include "fdbrpc/HTTP.h"
#include "fdbrpc/simulator.h"

const std::string REST_SIM_KMS_VAULT_GET_ENCRYPTION_KEYS_BY_KEY_IDS_RESOURCE = "/get-encryption-keys-by-key-ids";
const std::string REST_SIM_KMS_VAULT_GET_ENCRYPTION_KEYS_BY_DOMAIN_IDS_RESOURCE = "/get-encryption-keys-by-domain-ids";
const std::string REST_SIM_KMS_VAULT_GET_BLOB_METADATA_RESOURCE = "/get-blob-metadata";

struct RESTSimKmsVaultRequestHandler : HTTP::IRequestHandler, ReferenceCounted<RESTSimKmsVaultRequestHandler> {
	Future<Void> handleRequest(Reference<HTTP::IncomingRequest> req,
	                           Reference<HTTP::OutgoingResponse> response) override;
	Reference<HTTP::IRequestHandler> clone() override { return makeReference<RESTSimKmsVaultRequestHandler>(); }

	void addref() override { ReferenceCounted<RESTSimKmsVaultRequestHandler>::addref(); }
	void delref() override { ReferenceCounted<RESTSimKmsVaultRequestHandler>::delref(); }
};

#endif