/*
 * AsyncFileChaos.actor.h
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

#include "flow/flow.h"
#include "flow/serialize.h"
#include "flow/genericactors.actor.h"
#include "fdbrpc/IAsyncFile.h"
#include "flow/network.h"
#include "flow/ActorCollection.h"
#include "flow/actorcompiler.h"

// template <class AsyncFileType>
class AsyncFileChaos final : public IAsyncFile, public ReferenceCounted<AsyncFileChaos> {
private:
	Reference<IAsyncFile> file;
	bool enabled;

public:
	explicit AsyncFileChaos(Reference<IAsyncFile> file) : file(file) {
		// We only allow chaos events on storage files
		enabled = (file->getFilename().find("storage-") != std::string::npos);
	}

	void addref() override { ReferenceCounted<AsyncFileChaos>::addref(); }
	void delref() override { ReferenceCounted<AsyncFileChaos>::delref(); }

	double getDelay() const {
		double delayFor = 0.0;
		if (!enabled)
			return delayFor;

		auto res = g_network->global(INetwork::enDiskFailureInjector);
		if (res) {
			DiskFailureInjector* delayInjector = static_cast<DiskFailureInjector*>(res);
			delayFor = delayInjector->getDiskDelay();

			// increment the metric for disk delays
			if (delayFor > 0.0) {
				auto res = g_network->global(INetwork::enChaosMetrics);
				if (res) {
					ChaosMetrics* chaosMetrics = static_cast<ChaosMetrics*>(res);
					chaosMetrics->diskDelays++;
				}
			}
		}
		return delayFor;
	}

	Future<int> read(void* data, int length, int64_t offset) override {
		double diskDelay = getDelay();

		if (diskDelay == 0.0)
			return file->read(data, length, offset);

		// Wait for diskDelay before submitting the I/O
		// Template types are being provided explicitly because they can't be automatically deduced for some reason.
		return mapAsync<Void, std::function<Future<int>(Void)>, int>(
		    delay(diskDelay), [=](Void _) -> Future<int> { return file->read(data, length, offset); });
	}

	Future<Void> write(void const* data, int length, int64_t offset) override {
		Arena arena;
		char* pdata = nullptr;

		// Check if a bit flip event was injected, if so, copy the buffer contents
		// with a random bit flipped in a new buffer and use that for the write
		auto res = g_network->global(INetwork::enBitFlipper);
		if (enabled && res) {
			auto bitFlipPercentage = static_cast<BitFlipper*>(res)->getBitFlipPercentage();
			if (bitFlipPercentage > 0.0) {
				auto bitFlipProb = bitFlipPercentage / 100;
				if (deterministicRandom()->random01() < bitFlipProb) {
					pdata = (char*)arena.allocate4kAlignedBuffer(length);
					memcpy(pdata, data, length);
					// flip a random bit in the copied buffer
					pdata[deterministicRandom()->randomInt(0, length)] ^= (1 << deterministicRandom()->randomInt(0, 8));

					// increment the metric for bit flips
					auto res = g_network->global(INetwork::enChaosMetrics);
					if (res) {
						ChaosMetrics* chaosMetrics = static_cast<ChaosMetrics*>(res);
						chaosMetrics->bitFlips++;
					}
				}
			}
		}

		double diskDelay = getDelay();
		if (diskDelay == 0.0) {
			if (pdata)
				return holdWhile(arena, file->write(pdata, length, offset));

			return file->write(data, length, offset);
		}

		// Wait for diskDelay before submitting the I/O
		return mapAsync<Void, std::function<Future<Void>(Void)>, Void>(delay(diskDelay), [=](Void _) -> Future<Void> {
			if (pdata)
				return holdWhile(arena, file->write(pdata, length, offset));

			return file->write(data, length, offset);
		});
	}

	Future<Void> truncate(int64_t size) override {
		double diskDelay = getDelay();
		if (diskDelay == 0.0)
			return file->truncate(size);

		// Wait for diskDelay before submitting the I/O
		return mapAsync<Void, std::function<Future<Void>(Void)>, Void>(
		    delay(diskDelay), [=](Void _) -> Future<Void> { return file->truncate(size); });
	}

	Future<Void> sync() override {
		double diskDelay = getDelay();
		if (diskDelay == 0.0)
			return file->sync();

		// Wait for diskDelay before submitting the I/O
		return mapAsync<Void, std::function<Future<Void>(Void)>, Void>(
		    delay(diskDelay), [=](Void _) -> Future<Void> { return file->sync(); });
	}

	Future<int64_t> size() const override {
		double diskDelay = getDelay();
		if (diskDelay == 0.0)
			return file->size();

		// Wait for diskDelay before submitting the I/O
		return mapAsync<Void, std::function<Future<int64_t>(Void)>, int64_t>(
		    delay(diskDelay), [=](Void _) -> Future<int64_t> { return file->size(); });
	}

	int64_t debugFD() const override { return file->debugFD(); }

	std::string getFilename() const override { return file->getFilename(); }
};
