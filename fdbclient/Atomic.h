/*
 * Atomic.h
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

#ifndef FLOW_FDBCLIENT_ATOMIC_H
#define FLOW_FDBCLIENT_ATOMIC_H
#pragma once

#include "CommitTransaction.h"

static ValueRef doLittleEndianAdd(const ValueRef& existingValue, const ValueRef& otherOperand, Arena& ar) {
	if(!existingValue.size()) return otherOperand;
	if(!otherOperand.size()) return otherOperand;
	
	uint8_t* buf = new (ar) uint8_t [otherOperand.size()];
	int i = 0;
	int carry = 0;
		
	for(i = 0; i<std::min(existingValue.size(), otherOperand.size()); i++) {
		int sum = existingValue[i] + otherOperand[i] + carry;
		buf[i] = sum;
		carry = sum >> 8;
	}
	for (; i<otherOperand.size(); i++) {
		int sum = otherOperand[i] + carry;
		buf[i] = sum;
		carry = sum >> 8;
	}

	return StringRef(buf, i);	
}

static ValueRef doAnd(const ValueRef& existingValue, const ValueRef& otherOperand, Arena& ar) {
	if(!otherOperand.size()) return otherOperand;
	
	uint8_t* buf = new (ar) uint8_t [otherOperand.size()];
	int i = 0;
	
	for(i = 0; i<std::min(existingValue.size(), otherOperand.size()); i++)
		buf[i] = existingValue[i] & otherOperand[i];
	for(; i<otherOperand.size(); i++)
		buf[i] = 0x0;

	return StringRef(buf, i);
}

static ValueRef doNewAnd(const ValueRef& existingValue, const ValueRef& otherOperand, Arena& ar) {
	if (!existingValue.size())
		return otherOperand;

	return doAnd(existingValue, otherOperand, ar);
}

static ValueRef doOr(const ValueRef& existingValue, const ValueRef& otherOperand, Arena& ar) {
	if(!existingValue.size()) return otherOperand;
	if(!otherOperand.size()) return otherOperand;

	uint8_t* buf = new (ar) uint8_t [otherOperand.size()];
	int i = 0;
	
	for(i = 0; i<std::min(existingValue.size(), otherOperand.size()); i++)
		buf[i] = existingValue[i] | otherOperand[i];
	for(; i<otherOperand.size(); i++)
		buf[i] = otherOperand[i];

	return StringRef(buf, i);
}

static ValueRef doXor(const ValueRef& existingValue, const ValueRef& otherOperand, Arena& ar) {
	if(!existingValue.size()) return otherOperand;
	if(!otherOperand.size()) return otherOperand;
	
	uint8_t* buf = new (ar) uint8_t [otherOperand.size()];
	int i = 0;
	
	for(i = 0; i<std::min(existingValue.size(), otherOperand.size()); i++)
		buf[i] = existingValue[i] ^ otherOperand[i];

	for(; i<otherOperand.size(); i++)
		buf[i] = otherOperand[i];

	return StringRef(buf, i);
}

static ValueRef doAppendIfFits(const ValueRef& existingValue, const ValueRef& otherOperand, Arena& ar) {
	if(!existingValue.size()) return otherOperand;
	if(!otherOperand.size()) return existingValue;
	if(existingValue.size() + otherOperand.size() > CLIENT_KNOBS->VALUE_SIZE_LIMIT) {
		TEST( true ) //AppendIfFIts resulted in truncation
		return existingValue;
	}

	uint8_t* buf = new (ar) uint8_t [existingValue.size() + otherOperand.size()];
	int i,j;

	for(i = 0; i<existingValue.size(); i++)
		buf[i] = existingValue[i];

	for(j = 0; j<otherOperand.size(); j++)
		buf[i+j] = otherOperand[j];

	return StringRef(buf, i+j);
}

static ValueRef doMax(const ValueRef& existingValue, const ValueRef& otherOperand, Arena& ar) {
	if (!existingValue.size()) return otherOperand;
	if (!otherOperand.size()) return otherOperand;

	int i,j;

	for (i = otherOperand.size() - 1; i >= existingValue.size(); i--) {
		if (otherOperand[i] != 0) {
			return otherOperand;
		}
	}

	for (; i >= 0; i--) {
		if (otherOperand[i] > existingValue[i]) {
			return otherOperand;
		}
		else if (otherOperand[i] < existingValue[i]) {
			uint8_t* buf = new (ar) uint8_t [otherOperand.size()];
			for (j = 0; j < std::min(existingValue.size(), otherOperand.size()); j++) {
				buf[j] = existingValue[j];
			}
			for (; j < otherOperand.size(); j++) {
				buf[j] = 0x0;
			}
			return StringRef(buf, j);
		}
	}

	return otherOperand;
}

// Big Endian version of doMin
static ValueRef doByteMax(const ValueRef& existingValue, const ValueRef& otherOperand, Arena& ar) {
	if (!otherOperand.size() || !existingValue.size()) return otherOperand;

	if (existingValue > otherOperand)
		return existingValue;

	return otherOperand;
}

static ValueRef doMin(const ValueRef& existingValue, const ValueRef& otherOperand, Arena& ar) {
	if (!otherOperand.size()) return otherOperand;

	int i,j;

	for (i = otherOperand.size() - 1; i >= existingValue.size(); i--) {
		if (otherOperand[i] != 0) {
			uint8_t* buf = new (ar)uint8_t[otherOperand.size()];
			for (j = 0; j < std::min(existingValue.size(), otherOperand.size()); j++) {
				buf[j] = existingValue[j];
			}
			for (; j < otherOperand.size(); j++) {
				buf[j] = 0x0;
			}
			return StringRef(buf, j);
		}
	}

	for (; i >= 0; i--) {
		if (otherOperand[i] > existingValue[i]) {
			uint8_t* buf = new (ar)uint8_t[otherOperand.size()];
			for (j = 0; j < std::min(existingValue.size(), otherOperand.size()); j++) {
				buf[j] = existingValue[j];
			}
			for (; j < otherOperand.size(); j++) {
				buf[j] = 0x0;
			}
			return StringRef(buf, j);
		}
		else if (otherOperand[i] < existingValue[i]) {
			return otherOperand;
		}
	}

	return otherOperand;
}

static ValueRef doNewMin(const ValueRef& existingValue, const ValueRef& otherOperand, Arena& ar) {
	if (!existingValue.size())
		return otherOperand;

	return doMin(existingValue, otherOperand, ar);
}

// Big Endian version of doMin
static ValueRef doByteMin(const ValueRef& existingValue, const ValueRef& otherOperand, Arena& ar) {
	if (!otherOperand.size() || !existingValue.size()) return otherOperand;
	
	if (existingValue < otherOperand)
		return existingValue;

	return otherOperand;
}

/*
* Returns the range corresponding to the specified versionstamp key.
*/
static KeyRangeRef getVersionstampKeyRange(Arena& arena, const KeyRef &key, const KeyRef &maxKey) {
	KeyRef begin(arena, key);
	KeyRef end(arena, key);

	if (begin.size() < 2)
		throw client_invalid_operation();

	int16_t pos;
	memcpy(&pos, begin.end() - sizeof(int16_t), sizeof(int16_t));
	pos = littleEndian16(pos);
	begin = begin.substr(0, begin.size() - 2);
	end = end.substr(0, end.size() - 1);
	mutateString(end)[end.size()-1] = 0;

	if (pos < 0 || pos + 10 > begin.size())
		throw client_invalid_operation();

	memset(mutateString(begin) + pos, 0, 10);
	memset(mutateString(end) + pos, '\xff', 10);

	return KeyRangeRef(begin, std::min(end, maxKey));
}

static void placeVersionstamp( uint8_t* destination, Version version, uint16_t transactionNumber ) {
	version = bigEndian64(version);
	transactionNumber = bigEndian16(transactionNumber);
	static_assert( sizeof(version) == 8, "version size mismatch" );
	memcpy( destination, &version, sizeof(version) );
	static_assert( sizeof(transactionNumber) == 2, "txn num size mismatch");
	memcpy( destination + sizeof(version), &transactionNumber, sizeof(transactionNumber) );
}

static void transformSetVersionstampedKey( MutationRef& mutation, Version version, uint16_t transactionNumber ) {
	// This transforms a SetVersionstampedKey mutation into a SetValue mutation.  
	// It is the responsibility of the caller to also add a write conflict range for the new mutation's key.
	if (mutation.param1.size() >= 2) {
		int16_t pos;
		memcpy(&pos, mutation.param1.end() - sizeof(int16_t), sizeof(int16_t));
		pos = littleEndian16(pos);
		mutation.param1 = mutation.param1.substr(0, mutation.param1.size() - 2);

		if (pos >= 0 && pos + 10 <= mutation.param1.size()) {
			placeVersionstamp( mutateString(mutation.param1) + pos, version, transactionNumber );
		}
	}

	mutation.type = MutationRef::SetValue;
}

static void transformSetVersionstampedValue( MutationRef& mutation, Version version, uint16_t transactionNumber ) {
	if (mutation.param2.size() >= 10)
		placeVersionstamp( mutateString(mutation.param2), version, transactionNumber );

	mutation.type = MutationRef::SetValue;
}

#endif