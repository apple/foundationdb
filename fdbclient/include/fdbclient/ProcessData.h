/*
 * ProcessData.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

#ifndef FDBCLIENT_PROCESSDATA_H
#define FDBCLIENT_PROCESSDATA_H
#pragma once

#include "fdbclient/ProcessClass.h"
#include "fdbrpc/Locality.h"

struct ProcessData {
	LocalityData locality;
	ProcessClass processClass;
	NetworkAddress address;
	Optional<NetworkAddress> grpcAddress;

	ProcessData() {}
	ProcessData(LocalityData locality,
	            ProcessClass processClass,
	            NetworkAddress address,
	            Optional<NetworkAddress> grpcAddress)
	  : locality(locality), processClass(processClass), address(address), grpcAddress(grpcAddress) {}

	// To change this serialization, ProtocolVersion::WorkerListValue must be updated, and downgrades need to be
	// considered
	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, locality, processClass, address);

		if constexpr (!is_fb_function<Ar>) {
			if (ar.protocolVersion().hasGrpcEndpoint()) {
				serializer(ar, grpcAddress);
			}
		} else {
			serializer(ar, grpcAddress);
		}
	}

	struct sort_by_address {
		bool operator()(ProcessData const& a, ProcessData const& b) const { return a.address < b.address; }
	};
};

#endif // FDBCLIENT_PROCESSDATA_H
