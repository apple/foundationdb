/*
 * FDBExecHelper.h
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

#ifndef FDBSERVER_KVSTORE_FDBEXECHELPER_H
#define FDBSERVER_KVSTORE_FDBEXECHELPER_H
#pragma once

#include <map>
#include <string>
#include <vector>

#include "fdbclient/FDBTypes.h"
#include "flow/Arena.h"
#include "flow/flow.h"

class ExecCmdValueString {
public:
	ExecCmdValueString() {}
	explicit ExecCmdValueString(StringRef cmdValueString);

	StringRef getBinaryPath() const;
	VectorRef<StringRef> getBinaryArgs() const;
	void setCmdValueString(StringRef cmdValueString);
	StringRef getCmdValueString() const;
	void dbgPrint() const;

private:
	void parseCmdValue();

	Standalone<StringRef> cmdValueString;
	Standalone<VectorRef<StringRef>> binaryArgs;
	StringRef binaryPath;
};

Future<int> execHelper(ExecCmdValueString* execArg, UID snapUID, std::string folder, std::string role);
void setDataVersion(UID uid, Version version);
void setDataDurableVersion(UID uid, Version version);
void printStorageVersionInfo();

#endif
