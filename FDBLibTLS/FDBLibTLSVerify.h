/*
 * FDBLibTLSVerify.h
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

#ifndef FDB_LIBTLS_VERIFY_H
#define FDB_LIBTLS_VERIFY_H

#pragma once

#include <stdint.h>

#include "ReferenceCounted.h"

#include <map>
#include <string>

struct FDBLibTLSVerify: ReferenceCounted<FDBLibTLSVerify> {
	FDBLibTLSVerify(std::string verify);
	virtual ~FDBLibTLSVerify();

	virtual void addref() { ReferenceCounted<FDBLibTLSVerify>::addref(); }
	virtual void delref() { ReferenceCounted<FDBLibTLSVerify>::delref(); }

	void parse_verify(std::string input);

	bool verify_cert;
	bool verify_time;

	std::map<int, std::string> subject_criteria;
	std::map<int, std::string> issuer_criteria;
	std::map<int, std::string> root_criteria;
};

#endif /* FDB_LIBTLS_VERIFY_H */
