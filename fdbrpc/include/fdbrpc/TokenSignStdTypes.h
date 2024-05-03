/*
 * TokenSignStlTypes.h
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
#ifndef FDBRPC_TOKEN_SIGN_STD_TYPES_H
#define FDBRPC_TOKEN_SIGN_STD_TYPES_H
#include "fdbrpc/TokenSpec.h"
#include <string>
#include <vector>

// Below functions build as a library separate from fdbrpc
// The intent is to re-use the key/token generation part in a way that the input, the output,
// and possible exceptions are all standard types, such that it can be used outside the FDB/Flow world,
// especially for testing and benchmarking purposes

namespace authz::jwt::stdtypes {

namespace detail {

// work around the fact that std::vector takes more than one template parameter
template <class T>
using VectorAlias = std::vector<T>;

} // namespace detail

using TokenSpec = BasicTokenSpec<std::string, detail::VectorAlias>;

// Generate an elliptic curve private key on a P-256 curve, and serialize it as PEM.
std::string makeEcP256PrivateKeyPem();

// If this function was to take PrivateKey class as parameter,
// users of this library would need to link to flow in order to use it.
// To avoid that, keep the key input as PEM and suffer the parsing overhead.
std::string signToken(const TokenSpec& tokenSpec, const std::string& privateKeyPem);

} // namespace authz::jwt::stdtypes

#endif // FDBRPC_TOKEN_SIGN_STD_TYPES_H
