/*
 * JsonTraceLogFormatter.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2018 Apple Inc. and the FoundationDB project authors
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

#include "flow/FastRef.h"
#include "flow/Trace.h"

struct JsonTraceLogFormatter final : public ITraceLogFormatter, ReferenceCounted<JsonTraceLogFormatter> {
	char const* getExtension() const override;
	char const* getHeader() const override; // Called when starting a new file
	char const* getFooter() const override; // Called when ending a file
	std::string formatEvent(TraceEventFields const&) const override; // Called for each event

	void addref() override;
	void delref() override;
};
