/*
 * DiskDurabilityTest.cpp
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

#include <cinttypes>
#include "fmt/format.h"
#include "fdbserver/tester/workloads.h"
#include "flow/IAsyncFile.h"
#include "fdbclient/FDBTypes.h"

struct DiskDurabilityTest : TestWorkload {
	static constexpr auto NAME = "DiskDurabilityTest";
	bool enabled;
	std::string filename;
	KeyRange range, metrics;

	explicit DiskDurabilityTest(WorkloadContext const& wcx) : TestWorkload(wcx) {
		enabled = !clientId; // only do this on the "first" client
		filename = getOption(options, "filename"_sr, "durability_test.bin"_sr).toString();
		auto prefix = getOption(options, "prefix"_sr, "/DiskDurabilityTest/"_sr);
		range = prefixRange("S"_sr.withPrefix(prefix));
		metrics = prefixRange(prefix);
	}

	Future<Void> setup(Database const& cx) override { return Void(); }
	Future<Void> start(Database const& cx) override {
		if (enabled)
			return durabilityTest(cx);
		return Void();
	}
	Future<bool> check(Database const& cx) override { return true; }
	void getMetrics(std::vector<PerfMetric>& m) override {}

	static Value encodeValue(int64_t x) {
		x = bigEndian64(x);
		return StringRef((const uint8_t*)&x, sizeof(x));
	}
	Key encodeKey(int64_t x) const { return encodeValue(x).withPrefix(range.begin); }

	static int64_t decodeValue(ValueRef k) {
		ASSERT(k.size() == sizeof(int64_t));
		return bigEndian64(*(int64_t*)k.begin());
	}
	int64_t decodeKey(KeyRef k) const { return decodeValue(k.removePrefix(range.begin)); }

	static void encodePage(uint8_t* page, int64_t value) {
		int64_t* ipage = (int64_t*)page;
		for (int i = 0; i < 4096 / 8; i++)
			ipage[i] = value + i;
	}
	static int64_t decodePage(uint8_t* page) {
		int64_t* ipage = (int64_t*)page;
		for (int i = 0; i < 4096 / 8; i++)
			if (ipage[i] != ipage[0] + i)
				return 0;
		return ipage[0];
	}

	Future<Void> durabilityTest(Database db) {
		Reference<IAsyncFile> file = co_await IAsyncFileSystem::filesystem()->open(
		    filename,
		    IAsyncFile::OPEN_CREATE | IAsyncFile::OPEN_READWRITE | IAsyncFile::OPEN_UNBUFFERED |
		        IAsyncFile::OPEN_UNCACHED | IAsyncFile::OPEN_LOCK,
		    0600);
		std::vector<uint8_t> pagedata(4096 * 128);
		uint8_t* page = (uint8_t*)((intptr_t(&pagedata[0]) | intptr_t(4095)) + 1);

		int64_t size = co_await file->size();
		bool failed = false;
		int verifyPages{ 0 };

		// Verify
		Transaction tr(db);
		while (true) {
			Error err;
			try {
				RangeResult r = co_await tr.getRange(range, GetRangeLimits(1000000));
				verifyPages = r.size();
				for (int i = 0; i < r.size(); i++) {
					int bytesRead = co_await file->read(page, 4096, decodeKey(r[i].key) * 4096);
					if (bytesRead != 4096 || decodePage(page) != decodeValue(r[i].value)) {
						printf("ValidationError\n");
						TraceEvent(SevError, "ValidationError")
						    .detail("At", decodeKey(r[i].key))
						    .detail("Expected", decodeValue(r[i].value))
						    .detail("Found", decodePage(page))
						    .detail("Read", bytesRead);
						failed = true;
					}
				}
				break;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}

		if (failed)
			throw operation_failed();

		fmt::print("Verified {0}/{1} pages\n", verifyPages, size / 4096);
		TraceEvent(SevInfo, "Verified").detail("Pages", verifyPages).detail("Of", size / 4096);

		// Run
		bool first = true;
		while (true) {
			std::vector<int64_t> targetPages;
			for (int i = deterministicRandom()->randomInt(1, 100); i > 0 && targetPages.size() < size / 4096; i--) {
				auto p = deterministicRandom()->randomInt(0, size / 4096);
				if (std::find(targetPages.begin(), targetPages.end(), p) == targetPages.end())
					targetPages.push_back(p);
			}
			for (int i = deterministicRandom()->randomInt(1, 4); i > 0; i--) {
				targetPages.push_back(size / 4096);
				size += 4096;
			}

			std::vector<int64_t> targetValues(targetPages.size());
			for (auto& v : targetValues)
				v = deterministicRandom()->randomUniqueID().first();

			tr.reset();
			while (true) {
				Error err;
				try {
					for (int i = 0; i < targetPages.size(); i++)
						tr.clear(encodeKey(targetPages[i]));

					if (!first) {
						Optional<Value> v = co_await tr.get("syncs"_sr.withPrefix(metrics.begin));
						int64_t count = v.present() ? decodeValue(v.get()) : 0;
						count++;
						tr.set("syncs"_sr.withPrefix(metrics.begin), encodeValue(count));
					}

					co_await tr.commit();
					break;
				} catch (Error& e) {
					err = e;
				}
				co_await tr.onError(err);
			}
			tr.reset();
			Future<Version> rv = tr.getReadVersion(); // hide this latency

			std::vector<Future<Void>> fresults;

			for (int i = 0; i < targetPages.size(); i++) {
				uint8_t* p = page + 4096 * i;
				encodePage(p, targetValues[i]);
				fresults.push_back(file->write(p, 4096, targetPages[i] * 4096));
			}

			co_await waitForAll(fresults);

			co_await file->sync();

			while (true) {
				Error err;
				try {
					for (int i = 0; i < targetPages.size(); i++)
						tr.set(encodeKey(targetPages[i]), encodeValue(targetValues[i]));
					co_await tr.commit();
					break;
				} catch (Error& e) {
					err = e;
				}
				co_await tr.onError(err);
			}

			first = false;
		}
	}
};
WorkloadFactory<DiskDurabilityTest> DiskDurabilityTestFactory;
