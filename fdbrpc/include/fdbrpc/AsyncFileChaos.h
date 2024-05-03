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

#pragma once

#include "flow/flow.h"
#include "flow/serialize.h"
#include "flow/IAsyncFile.h"
#include "flow/network.h"
#include "flow/ActorCollection.h"
#include "flow/ChaosMetrics.h"
#include "fdbrpc/simulator.h"

// template <class AsyncFileType>
class AsyncFileChaos final : public IAsyncFile, public ReferenceCounted<AsyncFileChaos> {
private:
	Reference<IAsyncFile> file;
	bool enabled;

public:
	explicit AsyncFileChaos(Reference<IAsyncFile> file) : file(file) {
		// We only allow chaos events on storage files
		enabled = file->getFilename().find("storage-") != std::string::npos &&
		          file->getFilename().find("sqlite-wal") == std::string::npos;
	}

	void addref() override { ReferenceCounted<AsyncFileChaos>::addref(); }
	void delref() override { ReferenceCounted<AsyncFileChaos>::delref(); }

	virtual StringRef getClassName() override { return "AsyncFileReadAheadCache"_sr; }

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
		// Capture file by value in case this is destroyed during the delay
		return mapAsync(delay(diskDelay),
		                [=, file = file](Void _) -> Future<int> { return file->read(data, length, offset); });
	}

	Future<Void> write(void const* data, int length, int64_t offset) override {
		Arena arena;
		char* pdata = nullptr;
		unsigned corruptedBlock = 0;

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
					auto corruptedPos = deterministicRandom()->randomInt(0, length);
					pdata[corruptedPos] ^= (1 << deterministicRandom()->randomInt(0, 8));
					// mark the block as corrupted
					corruptedBlock = (offset + corruptedPos) / 4096;
					TraceEvent("CorruptedBlock")
					    .detail("Filename", file->getFilename())
					    .detail("Block", corruptedBlock)
					    .log();

					// increment the metric for bit flips
					auto chaosMetricsPointer = g_network->global(INetwork::enChaosMetrics);
					if (chaosMetricsPointer) {
						ChaosMetrics* chaosMetrics = static_cast<ChaosMetrics*>(chaosMetricsPointer);
						chaosMetrics->bitFlips++;
					}
				}
			}
		}

		// Wait for diskDelay before submitting the I/O
		// Capture file by value in case this is destroyed during the delay
		return mapAsync(delay(getDelay()), [=, file = file](Void _) -> Future<Void> {
			if (pdata) {
				return map(holdWhile(arena, file->write(pdata, length, offset)),
				           [corruptedBlock, file = file](auto res) {
					           if (g_network->isSimulated()) {
						           g_simulator->corruptedBlocks.emplace(file->getFilename(), corruptedBlock);
					           }
					           return res;
				           });
			}

			return file->write(data, length, offset);
		});
	}

	Future<Void> truncate(int64_t size) override {
		double diskDelay = getDelay();
		if (diskDelay == 0.0)
			return file->truncate(size);

		// Wait for diskDelay before submitting the I/O
		// Capture file by value in case this is destroyed during the delay
		return mapAsync(delay(diskDelay), [size, file = file](Void _) -> Future<Void> {
			constexpr auto maxBlockValue =
			    std::numeric_limits<decltype(g_simulator->corruptedBlocks)::key_type::second_type>::max();
			auto firstDeletedBlock =
			    g_simulator->corruptedBlocks.lower_bound(std::make_pair(file->getFilename(), size / 4096));
			auto lastFileBlock =
			    g_simulator->corruptedBlocks.upper_bound(std::make_pair(file->getFilename(), maxBlockValue));
			g_simulator->corruptedBlocks.erase(firstDeletedBlock, lastFileBlock);
			return file->truncate(size);
		});
	}

	Future<Void> sync() override {
		double diskDelay = getDelay();
		if (diskDelay == 0.0)
			return file->sync();

		// Wait for diskDelay before submitting the I/O
		// Capture file by value in case this is destroyed during the delay
		return mapAsync(delay(diskDelay), [=, file = file](Void _) -> Future<Void> { return file->sync(); });
	}

	Future<int64_t> size() const override {
		double diskDelay = getDelay();
		if (diskDelay == 0.0)
			return file->size();

		// Wait for diskDelay before submitting the I/O
		// Capture file by value in case this is destroyed during the delay
		return mapAsync(delay(diskDelay), [=, file = file](Void _) -> Future<int64_t> { return file->size(); });
	}

	int64_t debugFD() const override { return file->debugFD(); }

	std::string getFilename() const override { return file->getFilename(); }
};
