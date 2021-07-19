/*
 * VersionedBTree.actor.cpp
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

#include "flow/flow.h"
#include "flow/serialize.h"
#include "flow/genericactors.actor.h"
#include "fdbrpc/IAsyncFile.h"
#include "flow/network.h"
#include "flow/ActorCollection.h"
#include "flow/actorcompiler.h"


//template <class AsyncFileType>
class AsyncFileDelayed final : public IAsyncFile, public ReferenceCounted<AsyncFileDelayed> {
private:
	Reference<IAsyncFile> file;
public:
	explicit AsyncFileDelayed(Reference<IAsyncFile> file) : file(file) {}

	void addref() override { ReferenceCounted<AsyncFileDelayed>::addref(); }
	void delref() override { ReferenceCounted<AsyncFileDelayed>::delref(); }

	uint8_t toggleNthBit(uint8_t b, uint8_t n) {
		auto singleBitMask = uint8_t(1) << (n);
		return b ^ singleBitMask;
	}

	void flipBits(void* data, int length, double percentBitFlips) {
		auto toFlip = int(float(length*8) * percentBitFlips / 100);
		TraceEvent("AsyncFileFlipBits").detail("ToFlip", toFlip);
		for (auto i = 0; i < toFlip; i++) {
			auto byteOffset = deterministicRandom()->randomInt64(0, length);
			auto bitOffset = uint8_t(deterministicRandom()->randomInt(0, 8));
			((uint8_t *)data)[byteOffset] = toggleNthBit(((uint8_t *)data)[byteOffset], bitOffset);
		}
	}

	Future<int> read(void* data, int length, int64_t offset) override {
		double delay = 0.0;
		auto res = g_network->global(INetwork::enFailureInjector);
		if (res)
			delay = static_cast<DiskFailureInjector*>(res)->getDiskDelay();
		TraceEvent("AsyncFileDelayedRead").detail("ThrottleDelay", delay);
		return delayed(file->read(data, length, offset), delay);
	}

	Future<Void> write(void const* data, int length, int64_t offset) override {
		double delay = 0.0;
		char* pdata = nullptr;
		auto res = g_network->global(INetwork::enBitFlipper);
		if (res) {
			auto percentBitFlips = static_cast<BitFlipper*>(res)->getPercentBitFlips();
			if (percentBitFlips > 0.0) {
				TraceEvent("AsyncFileCorruptWrite").detail("PercentBitFlips", percentBitFlips);
				pdata = new char[length];
				memcpy(pdata, data, length);
				flipBits(pdata, length, percentBitFlips);
				auto diff = memcmp(pdata, data, length);
				if (diff)
					TraceEvent("AsyncFileCorruptWriteDiff").detail("Diff", diff);
			}
		}

		res = g_network->global(INetwork::enFailureInjector);
		if (res)
			delay = static_cast<DiskFailureInjector*>(res)->getDiskDelay();
		TraceEvent("AsyncFileDelayedWrite").detail("ThrottleDelay", delay);
		return delayed(file->write((pdata != nullptr) ? pdata : data, length, offset), delay);
	}

	Future<Void> truncate(int64_t size) override {
		double delay = 0.0;
		auto res = g_network->global(INetwork::enFailureInjector);
		if (res)
			delay = static_cast<DiskFailureInjector*>(res)->getDiskDelay();
		return delayed(file->truncate(size), delay);
	}

	Future<Void> sync() override {
		double delay = 0.0;
		auto res = g_network->global(INetwork::enFailureInjector);
		if (res)
			delay = static_cast<DiskFailureInjector*>(res)->getDiskDelay();
		return delayed(file->sync(), delay);
	}

	Future<int64_t> size() const override {
		double delay = 0.0;
		auto res = g_network->global(INetwork::enFailureInjector);
		if (res)
			delay = static_cast<DiskFailureInjector*>(res)->getDiskDelay();
		return delayed(file->size(), delay);
	}

	int64_t debugFD() const override {
		return file->debugFD();
	}

	std::string getFilename() const override {
		return file->getFilename();
	}
};
