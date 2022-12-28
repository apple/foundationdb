/*
 * AsyncFileChaos.h
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

#ifndef FDBRPC_ASYNCFILECHAOS_H
#define FDBRPC_ASYNCFILECHAOS_H

#pragma once

#include "flow/flow.h"
#include "flow/IAsyncFile.h"
#include "flow/network.h"
#include "flow/ActorCollection.h"
#include "flow/ChaosMetrics.h"
#include "fdbrpc/simulator.h"

class AsyncFileChaos final : public IAsyncFile, public ReferenceCounted<AsyncFileChaos> {
private:
	Reference<IAsyncFile> file;
	bool enabled;

public:
	explicit AsyncFileChaos(Reference<IAsyncFile> file) : file(file) {
		// We only allow chaos events on storage files
		enabled = file->getFilename().find("storage-") != std ::string::npos &&
		          file->getFilename().find("sqlite-wal") == std ::string::npos;
	}

	void addref() override { ReferenceCounted<AsyncFileChaos>::addref(); }
	void delref() override { ReferenceCounted<AsyncFileChaos>::delref(); }

	double getDelay() const;

	Future<int> read(void* data, int length, int64_t offset) override;

	Future<Void> write(void const* data, int length, int64_t offset) override;

	Future<Void> truncate(int64_t size) override;

	Future<Void> sync() override;

	Future<int64_t> size() const override;

	int64_t debugFD() const override { return file->debugFD(); }

	std ::string getFilename() const override { return file->getFilename(); }
};

#endif // FDBRPC_ASYNCFILECHAOS_H