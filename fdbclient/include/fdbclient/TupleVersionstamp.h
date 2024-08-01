/*
 * TupleVersionstamp.h
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

#ifndef FDBCLIENT_VERSIONSTAMP_H
#define FDBCLIENT_VERSIONSTAMP_H

#pragma once

#include "flow/Arena.h"

const size_t VERSIONSTAMP_TUPLE_SIZE = 12;

struct TupleVersionstamp {
	// Version = invalid version, batch/user version = 0
	static inline const Standalone<StringRef> DEFAULT_VERSIONSTAMP =
	    "\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x00"_sr;

	TupleVersionstamp() : data(DEFAULT_VERSIONSTAMP) {}
	TupleVersionstamp(StringRef);
	TupleVersionstamp(int64_t version, uint16_t batchNumber, uint16_t userVersion = 0);

	int64_t getVersion() const;
	int16_t getBatchNumber() const;
	int16_t getUserVersion() const;
	size_t size() const;
	const uint8_t* begin() const;
	bool operator==(const TupleVersionstamp&) const;

private:
	Standalone<StringRef> data;
};

#endif