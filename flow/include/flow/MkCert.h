/*
 * MkCert.h
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

#ifndef MKCERT_H
#define MKCERT_H

#include "flow/Arena.h"
#include "flow/Error.h"
#include "flow/PKey.h"
#include <fmt/format.h>
#include <string>
#include <type_traits>
#include <variant>

namespace mkcert {

// Print X509 cert PEM in human-readable form
void printCert(FILE* out, StringRef certPem);

// Print PKCS#8 private key PEM in human-readable form
void printPrivateKey(FILE* out, StringRef privateKeyPem);

PrivateKey makeEcP256();

PrivateKey makeRsa4096Bit();

struct Asn1EntryRef {
	// field must match one of ASN.1 object short/long names: e.g. "C", "countryName", "CN", "commonName",
	// "subjectAltName", ...
	StringRef field;
	StringRef bytes;
};

struct ServerRootCA {};
struct ServerIntermediateCA {
	unsigned level;
};
struct Server {};
struct ClientRootCA {};
struct ClientIntermediateCA {
	unsigned level;
};
struct Client {};

struct CertKind {

	CertKind() noexcept = default;

	template <class Kind>
	CertKind(Kind kind) noexcept : value(std::in_place_type<Kind>, kind) {}

	template <class Kind>
	bool is() const noexcept {
		return std::holds_alternative<Kind>(value);
	}

	template <class Kind>
	Kind const& get() const {
		return std::get<Kind>(value);
	}

	bool isServerSide() const noexcept { return is<ServerRootCA>() || is<ServerIntermediateCA>() || is<Server>(); }

	bool isClientSide() const noexcept { return !isServerSide(); }

	bool isRootCA() const noexcept { return is<ServerRootCA>() || is<ClientRootCA>(); }

	bool isIntermediateCA() const noexcept { return is<ServerIntermediateCA>() || is<ClientIntermediateCA>(); }

	bool isLeaf() const noexcept { return is<Server>() || is<Client>(); }

	bool isCA() const noexcept { return !isLeaf(); }

	StringRef getCommonName(StringRef prefix, Arena& arena) const;

	std::variant<ServerRootCA, ServerIntermediateCA, Server, ClientRootCA, ClientIntermediateCA, Client> value;
};

struct CertSpecRef {
	using SelfType = CertSpecRef;
	long serialNumber;
	// offset in number of seconds relative to now, i.e. cert creation
	long offsetNotBefore;
	long offsetNotAfter;
	VectorRef<Asn1EntryRef> subjectName;
	// time offset relative to time of cert creation (now)
	VectorRef<Asn1EntryRef> extensions;
	// make test-only sample certificate whose fields are inferred from CertKind
	static SelfType make(Arena& arena, CertKind kind);
};

struct CertAndKeyRef {
	using SelfType = CertAndKeyRef;
	StringRef certPem;
	StringRef privateKeyPem;

	void printCert(FILE* out) {
		if (!certPem.empty()) {
			::mkcert::printCert(out, certPem);
		}
	}

	void printPrivateKey(FILE* out) {
		if (!privateKeyPem.empty()) {
			::mkcert::printPrivateKey(out, privateKeyPem);
		}
	}

	bool empty() const noexcept { return certPem.empty() && privateKeyPem.empty(); }

	SelfType deepCopy(Arena& arena) {
		auto ret = SelfType{};
		if (!certPem.empty())
			ret.certPem = StringRef(arena, certPem);
		if (!privateKeyPem.empty())
			ret.privateKeyPem = StringRef(arena, privateKeyPem);
		return ret;
	}

	// Empty (default) issuer produces a self-signed certificate
	static SelfType make(Arena& arena, CertSpecRef spec, CertAndKeyRef issuer);
};

using CertChainRef = VectorRef<CertAndKeyRef>;

// Concatenate chain of PEMs to one StringRef
StringRef concatCertChain(Arena& arena, CertChainRef chain);

enum class ESide : int { Server, Client };

// Generate a chain of valid cert specs that have consistent subject/issuer names and
// is valid for typical server/client TLS scenario
// The 'side' parameter makes a difference in the commonName ("CN") field of the produced specs
VectorRef<CertSpecRef> makeCertChainSpec(Arena& arena, unsigned length, ESide side);

// For empty (default) rootAuthority, the last item in specs is used to generate rootAuthority
// Otherwise, rootAuthority is deep-copied to first element of returned chain
CertChainRef makeCertChain(Arena& arena, VectorRef<CertSpecRef> specs, Optional<CertAndKeyRef> rootAuthority);

// Make stub cert chain of given length inc. root authority
// Note: side does not imply anything different other than the choice of common names
CertChainRef makeCertChain(Arena& arena, unsigned depth, ESide side);

} // namespace mkcert

#endif /*MKCERT_H*/
