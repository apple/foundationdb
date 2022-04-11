/*
 * FDBLibTLSPlugin.cpp
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

#include "boost/config.hpp"

#include "FDBLibTLS/FDBLibTLSPlugin.h"
#include "FDBLibTLS/FDBLibTLSPolicy.h"
#include "flow/Trace.h"

#include <string.h>

FDBLibTLSPlugin::FDBLibTLSPlugin() {
	// tls_init is not currently thread safe - caller's responsibility.
	rc = tls_init();
}

FDBLibTLSPlugin::~FDBLibTLSPlugin() {}

ITLSPolicy* FDBLibTLSPlugin::create_policy() {
	if (rc < 0) {
		// Log the failure from tls_init during our constructor.
		TraceEvent(SevError, "FDBLibTLSInitError").detail("LibTLSErrorMessage", "failed to initialize libtls");
		return nullptr;
	}
	return new FDBLibTLSPolicy(Reference<FDBLibTLSPlugin>::addRef(this));
}

extern "C" BOOST_SYMBOL_EXPORT void* get_tls_plugin(const char* plugin_type_name_and_version) {
	if (strcmp(plugin_type_name_and_version, FDBLibTLSPlugin::get_plugin_type_name_and_version()) == 0) {
		return new FDBLibTLSPlugin;
	}
	return nullptr;
}
