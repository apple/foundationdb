/*
 * DiskDurabilityTest.actor.cpp
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

#include <cinttypes>
#include "contrib/fmt-8.1.1/include/fmt/format.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbrpc/IAsyncFile.h"
#include "fdbclient/FDBTypes.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct DiskDurabilityTest : TestWorkload {
	bool enabled;
	std::string filename;
	KeyRange range, metrics;

	DiskDurabilityTest(WorkloadContext const& wcx) : TestWorkload(wcx) {
		enabled = !clientId; // only do this on the "first" client
		filename = getOption(options, LiteralStringRef("filename"), LiteralStringRef("durability_test.bin")).toString();
		auto prefix = getOption(options, LiteralStringRef("prefix"), LiteralStringRef("/DiskDurabilityTest/"));
		range = prefixRange(LiteralStringRef("S").withPrefix(prefix));
		metrics = prefixRange(prefix);
	}

	std::string description() const override { return "DiskDurabilityTest"; }
	Future<Void> setup(Database const& cx) override { return Void(); }
	Future<Void> start(Database const& cx) override {
		if (enabled)
			return durabilityTest(this, cx);
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

	ACTOR static Future<Void> durabilityTest(DiskDurabilityTest* self, Database db) {
		state Reference<IAsyncFile> file = wait(IAsyncFileSystem::filesystem()->open(
		    self->filename,
		    IAsyncFile::OPEN_CREATE | IAsyncFile::OPEN_READWRITE | IAsyncFile::OPEN_UNBUFFERED |
		        IAsyncFile::OPEN_UNCACHED | IAsyncFile::OPEN_LOCK,
		    0600));
		state std::vector<uint8_t> pagedata(4096 * 128);
		state uint8_t* page = (uint8_t*)((intptr_t(&pagedata[0]) | intptr_t(4095)) + 1);

		state int64_t size = wait(file->size());
		state bool failed = false;
		state int verifyPages;

		// Verify
		state Transaction tr(db);
		loop {
			try {
				state RangeResult r = wait(tr.getRange(self->range, GetRangeLimits(1000000)));
				verifyPages = r.size();
				state int i;
				for (i = 0; i < r.size(); i++) {
					int bytesRead = wait(file->read(page, 4096, self->decodeKey(r[i].key) * 4096));
					if (bytesRead != 4096 || self->decodePage(page) != self->decodeValue(r[i].value)) {
						printf("ValidationError\n");
						TraceEvent(SevError, "ValidationError")
						    .detail("At", self->decodeKey(r[i].key))
						    .detail("Expected", self->decodeValue(r[i].value))
						    .detail("Found", self->decodePage(page))
						    .detail("Read", bytesRead);
						failed = true;
					}
				}
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}

		if (failed)
			throw operation_failed();

		fmt::print("Verified {0}/{1} pages\n", verifyPages, size / 4096);
		TraceEvent(SevInfo, "Verified").detail("Pages", verifyPages).detail("Of", size / 4096);

		// Run
		state bool first = true;
		loop {
			state std::vector<int64_t> targetPages;
			for (int i = deterministicRandom()->randomInt(1, 100); i > 0 && targetPages.size() < size / 4096; i--) {
				auto p = deterministicRandom()->randomInt(0, size / 4096);
				if (!std::count(targetPages.begin(), targetPages.end(), p))
					targetPages.push_back(p);
			}
			for (int i = deterministicRandom()->randomInt(1, 4); i > 0; i--) {
				targetPages.push_back(size / 4096);
				size += 4096;
			}

			state std::vector<int64_t> targetValues(targetPages.size());
			for (auto& v : targetValues)
				v = deterministicRandom()->randomUniqueID().first();

			tr.reset();
			loop {
				try {
					for (int i = 0; i < targetPages.size(); i++)
						tr.clear(self->encodeKey(targetPages[i]));

					if (!first) {
						Optional<Value> v = wait(tr.get(LiteralStringRef("syncs").withPrefix(self->metrics.begin)));
						int64_t count = v.present() ? self->decodeValue(v.get()) : 0;
						count++;
						tr.set(LiteralStringRef("syncs").withPrefix(self->metrics.begin), self->encodeValue(count));
					}

					wait(tr.commit());
					break;
				} catch (Error& e) {
					wait(tr.onError(e));
				}
			}
			tr.reset();
			state Future<Version> rv = tr.getReadVersion(); // hide this latency

			std::vector<Future<Void>> fresults;

			for (int i = 0; i < targetPages.size(); i++) {
				uint8_t* p = page + 4096 * i;
				self->encodePage(p, targetValues[i]);
				fresults.push_back(file->write(p, 4096, targetPages[i] * 4096));
			}

			wait(waitForAll(fresults));

			wait(file->sync());

			loop {
				try {
					for (int i = 0; i < targetPages.size(); i++)
						tr.set(self->encodeKey(targetPages[i]), self->encodeValue(targetValues[i]));
					wait(tr.commit());
					break;
				} catch (Error& e) {
					wait(tr.onError(e));
				}
			}

			first = false;
		}
	}
};
WorkloadFactory<DiskDurabilityTest> DiskDurabilityTestFactory("DiskDurabilityTest");
