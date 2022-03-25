/*
 * FDBLibTLSPlugin.h
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

#ifndef FDB_LIBTLS_PLUGIN_H
#define FDB_LIBTLS_PLUGIN_H

#pragma once

#include "fdbrpc/ITLSPlugin.h"
#include "flow/FastRef.h"

#include <tls.h>

struct FDBLibTLSPlugin : ITLSPlugin, ReferenceCounted<FDBLibTLSPlugin> {
	FDBLibTLSPlugin();
	virtual ~FDBLibTLSPlugin();

	virtual void addref() { ReferenceCounted<FDBLibTLSPlugin>::addref(); }
	virtual void delref() { ReferenceCounted<FDBLibTLSPlugin>::delref(); }

	virtual ITLSPolicy* create_policy();

	int rc;
};

#endif /* FDB_LIBTLS_PLUGIN_H */
