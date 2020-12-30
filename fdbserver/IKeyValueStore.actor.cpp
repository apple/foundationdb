#include <cinttypes>

#include "fdbserver/IKeyValueStore.h"
#include "flow/Platform.h"
#include "flow/UnitTest.h"
#include "flow/actorcompiler.h" // has to be last include

#define debug_printf

namespace {

struct SimpleCounter {
	SimpleCounter() : x(0), xt(0), t(timer()), start(t) {}
	void operator+=(int n) { x += n; }
	void operator++() { x++; }
	int64_t get() { return x; }
	double rate() {
		double t2 = timer();
		int r = (x - xt) / (t2 - t);
		xt = x;
		t = t2;
		return r;
	}
	double avgRate() { return x / (timer() - start); }
	int64_t x;
	double t;
	double start;
	int64_t xt;
	std::string toString() { return format("%lld/%.2f/%.2f", x, rate() / 1e6, avgRate() / 1e6); }
};

int randomSize(int max) {
	int n = pow(deterministicRandom()->random01(), 3) * max;
	return n;
}

StringRef randomString(Arena& arena, int len, char firstChar = 'a', char lastChar = 'z') {
	++lastChar;
	StringRef s = makeString(len, arena);
	for (int i = 0; i < len; ++i) {
		*(uint8_t*)(s.begin() + i) = (uint8_t)deterministicRandom()->randomInt(firstChar, lastChar);
	}
	return s;
}

Standalone<StringRef> randomString(int len, char firstChar = 'a', char lastChar = 'z') {
	Standalone<StringRef> s;
	(StringRef&)s = randomString(s.arena(), len, firstChar, lastChar);
	return s;
}

KeyValue randomKV(int maxKeySize = 10, int maxValueSize = 5) {
	int kLen = randomSize(1 + maxKeySize);
	int vLen = maxValueSize > 0 ? randomSize(maxValueSize) : 0;

	KeyValue kv;

	kv.key = randomString(kv.arena(), kLen, 'a', 'm');
	for (int i = 0; i < kLen; ++i) mutateString(kv.key)[i] = (uint8_t)deterministicRandom()->randomInt('a', 'm');

	if (vLen > 0) {
		kv.value = randomString(kv.arena(), vLen, 'n', 'z');
		for (int i = 0; i < vLen; ++i) mutateString(kv.value)[i] = (uint8_t)deterministicRandom()->randomInt('o', 'z');
	}

	return kv;
}

// Does a random range read, doesn't trap/report errors
ACTOR Future<Void> randomReader(IKeyValueStore* kvStore) {
	try {
		state Version v = 0;
		loop {
			wait(yield());
			if (!v || deterministicRandom()->random01() > .01) {
				v = kvStore->getLastCommittedVersion();
			}

			state KeyValue kv = randomKV(10, 0);
			state int c = deterministicRandom()->randomInt(0, 100);
			// TODO: This needs to synchronize with `kvStore` getting deleted/recreated in the main loop.
			// Standalone<RangeResultRef> ignored = wait(kvStore->readRangeAt({ kv.key, LiteralStringRef("\xff") }, v,
			// c));
		}
	} catch (Error& e) {
		if (e.code() != error_code_transaction_too_old) {
			throw e;
		}
	}

	return Void();
}

ACTOR Future<int> verifyRange(IKeyValueStore* kvStore, Key start, Key end, Version version,
                              std::map<std::pair<std::string, Version>, Optional<std::string>>* written,
                              int* pErrorCount) {
	state int errors = 0;
	if (end <= start) end = keyAfter(start);

	state std::map<std::pair<std::string, Version>, Optional<std::string>>::const_iterator it =
	    written->lower_bound(std::make_pair(start.toString(), 0));
	state std::map<std::pair<std::string, Version>, Optional<std::string>>::const_iterator itEnd =
	    written->upper_bound(std::make_pair(end.toString(), 0));
	state std::map<std::pair<std::string, Version>, Optional<std::string>>::const_iterator itLast;

	state std::vector<KeyValue> results;

	// TODO: Use a smaller row/bytes limit to meaningfully test the limit logic.
	// TODO: Reverse iteration.
	state Standalone<RangeResultRef> read = wait(kvStore->readRangeAt({ start, end }, version));

	while (1) {
		for (const auto& kv : read) {
			// Find the next written kv pair that would be present at this version
			while (1) {
				itLast = it;
				if (it == itEnd) break;
				++it;

				if (itLast->first.second <= version && itLast->second.present() &&
				    (it == itEnd || it->first.first != itLast->first.first || it->first.second > version)) {
					debug_printf("VerifyRange(@%" PRId64 ", %s, %s) Found key in written map: %s\n", version,
					             start.printable().c_str(), end.printable().c_str(), itLast->first.first.c_str());
					break;
				}
			}

			if (itLast == itEnd) {
				++errors;
				++*pErrorCount;
				printf("VerifyRange(@%" PRId64 ", %s, %s) ERROR: Tree key '%s' vs nothing in written map.\n", version,
				       start.printable().c_str(), end.printable().c_str(), kv.key.toString().c_str());
				break;
			}

			if (kv.key != itLast->first.first) {
				++errors;
				++*pErrorCount;
				printf("VerifyRange(@%" PRId64 ", %s, %s) ERROR: Tree key '%s' but expected '%s'\n", version,
				       start.printable().c_str(), end.printable().c_str(), kv.key.toString().c_str(),
				       itLast->first.first.c_str());
				break;
			}
			if (kv.value != itLast->second.get()) {
				++errors;
				++*pErrorCount;
				printf("VerifyRange(@%" PRId64 ", %s, %s) ERROR: Tree key '%s' has tree value '%s' but expected '%s'\n",
				       version, start.printable().c_str(), end.printable().c_str(), kv.key.toString().c_str(),
				       kv.value.toString().c_str(), itLast->second.get().c_str());
				break;
			}

			ASSERT(errors == 0);

			results.push_back(kv);
		}
		if (read.more) {
			Standalone<RangeResultRef> r = wait(kvStore->readRangeAt({ read.readThrough.get(), end }, version));
			read = r;
		} else {
			break;
		}
	}

	// Make sure there are no further written kv pairs that would be present at this version.
	while (1) {
		itLast = it;
		if (it == itEnd) break;
		++it;
		if (itLast->first.second <= version && itLast->second.present() &&
		    (it == itEnd || it->first.first != itLast->first.first || it->first.second > version))
			break;
	}

	if (itLast != itEnd) {
		++errors;
		++*pErrorCount;
		printf("VerifyRange(@%" PRId64 ", %s, %s) ERROR: Tree range ended but written has @%" PRId64 " '%s'\n", version,
		       start.printable().c_str(), end.printable().c_str(), itLast->first.second, itLast->first.first.c_str());
	}

	debug_printf("VerifyRangeReverse(@%" PRId64 ", %s, %s): start\n", version, start.printable().c_str(),
	             end.printable().c_str());

	// Now read the range from the tree in reverse order and compare to the saved results
	// TODO: This doesn't handle the start/end correctly.
	Standalone<RangeResultRef> r = wait(kvStore->readRangeAt({ end, start }, version, -(1 << 30)));
	read = r;

	state std::vector<KeyValue>::const_reverse_iterator rit = results.rbegin();

	while (1) {
		for (const auto& kv : read) {
			if (rit == results.rend()) {
				++errors;
				++*pErrorCount;
				printf("VerifyRangeReverse(@%" PRId64 ", %s, %s) ERROR: Tree key '%s' vs nothing in written map.\n",
				       version, start.printable().c_str(), end.printable().c_str(), kv.key.toString().c_str());
				break;
			}

			if (kv.key != rit->key) {
				++errors;
				++*pErrorCount;
				printf("VerifyRangeReverse(@%" PRId64 ", %s, %s) ERROR: Tree key '%s' but expected '%s'\n", version,
				       start.printable().c_str(), end.printable().c_str(), kv.key.toString().c_str(),
				       rit->key.toString().c_str());
				break;
			}
			if (kv.value != rit->value) {
				++errors;
				++*pErrorCount;
				printf("VerifyRangeReverse(@%" PRId64
				       ", %s, %s) ERROR: Tree key '%s' has tree value '%s' but expected '%s'\n",
				       version, start.printable().c_str(), end.printable().c_str(), kv.value.toString().c_str(),
				       kv.value.toString().c_str(), rit->value.toString().c_str());
				break;
			}

			++rit;
		}
		if (read.more) {
			Standalone<RangeResultRef> r = wait(kvStore->readRangeAt({ read.readThrough.get(), end }, version));
			read = r;
		} else {
			break;
		}
	}

	if (rit != results.rend()) {
		++errors;
		++*pErrorCount;
		printf("VerifyRangeReverse(@%" PRId64 ", %s, %s) ERROR: Tree range ended but written has '%s'\n", version,
		       start.printable().c_str(), end.printable().c_str(), rit->key.toString().c_str());
	}

	return errors;
}

// Verify the result of point reads for every set or cleared key at the given version
ACTOR Future<int> seekAll(IKeyValueStore* kvStore, Version version,
                          std::map<std::pair<std::string, Version>, Optional<std::string>>* written, int* pErrorCount) {
	state std::map<std::pair<std::string, Version>, Optional<std::string>>::const_iterator it = written->cbegin();
	state std::map<std::pair<std::string, Version>, Optional<std::string>>::const_iterator itEnd = written->cend();
	state int errors = 0;

	while (it != itEnd) {
		state std::string key = it->first.first;
		if (it->first.second == version) {
			debug_printf("Verifying @%" PRId64 " '%s'\n", version, key.c_str());
			state Arena arena;
			Optional<Value> value = wait(kvStore->readValueAt(KeyRef(arena, key), version));

			const auto& expected = it->second;
			if (expected.present()) {
				if (!value.present() || value.get() != expected.get()) {
					++errors;
					++*pErrorCount;
					if (value.present()) {
						printf("Verify ERROR: value_incorrect: for '%s' found '%s' expected '%s' @%" PRId64 "\n",
						       key.c_str(), value.get().toString().c_str(), expected.get().c_str(), version);
					} else {
						printf("Verify ERROR: value_missing: for '%s' expected '%s' @%" PRId64 "\n", key.c_str(),
						       expected.get().c_str(), version);
					}
				}
			} else {
				if (value.present()) {
					++errors;
					++*pErrorCount;
					printf("Verify ERROR: cleared_key_found: '%s' -> '%s' @%" PRId64 "\n", key.c_str(),
					       value.get().toString().c_str(), version);
				}
			}
		}
		++it;
	}
	return errors;
}

ACTOR Future<Void> verify(IKeyValueStore* kvStore, FutureStream<Version> vStream,
                          std::map<std::pair<std::string, Version>, Optional<std::string>>* written, int* pErrorCount,
                          bool serial) {
	state Future<int> fRangeAll;
	state Future<int> fRangeRandom;
	state Future<int> fSeekAll;

	// Queue of committed versions still readable from kvStore
	state std::deque<Version> committedVersions;

	try {
		loop {
			state Version v = waitNext(vStream);
			committedVersions.push_back(v);

			// Remove expired versions
			while (!committedVersions.empty() && committedVersions.front() < kvStore->getOldestVersion()) {
				committedVersions.pop_front();
			}

			// Continue if the versions list is empty, which won't wait until it reaches the oldest readable
			// kvStore version which will already be in vStream.
			if (committedVersions.empty()) {
				continue;
			}

			// Choose a random committed version.
			v = committedVersions[deterministicRandom()->randomInt(0, committedVersions.size())];

			debug_printf("Using committed version %" PRId64 "\n", v);

			debug_printf("Verifying entire key range at version %" PRId64 "\n", v);
			fRangeAll =
			    verifyRange(kvStore, LiteralStringRef(""), LiteralStringRef("\xff\xff"), v, written, pErrorCount);

			if (serial) {
				wait(success(fRangeAll));
			}

			Key begin = randomKV().key;
			Key end = randomKV().key;
			debug_printf("Verifying range (%s, %s) at version %" PRId64 "\n", begin.toString().c_str(),
			             end.toString().c_str(), v);
			fRangeRandom = verifyRange(kvStore, begin, end, v, written, pErrorCount);
			if (serial) {
				wait(success(fRangeRandom));
			}

			debug_printf("Verifying seeks to each changed key at version %" PRId64 "\n", v);
			fSeekAll = seekAll(kvStore, v, written, pErrorCount);
			if (serial) {
				wait(success(fSeekAll));
			}

			wait(success(fRangeAll) && success(fRangeRandom) && success(fSeekAll));

			printf("Verified version %" PRId64 ", %d errors\n", v, *pErrorCount);

			if (*pErrorCount != 0) break;
		}
	} catch (Error& e) {
		if (e.code() != error_code_end_of_stream && e.code() != error_code_transaction_too_old) {
			throw;
		}
	}
	return Void();
}

} // namespace

TEST_CASE("!/IKeyValueStore/correctness/") {
	state bool serialTest = deterministicRandom()->coinflip();
	state bool shortTest = deterministicRandom()->coinflip();

	state int pageSize =
	    shortTest ? 200 : (deterministicRandom()->coinflip() ? 4096 : deterministicRandom()->randomInt(200, 400));

	state int64_t targetPageOps = shortTest ? 50000 : 1000000;
	state bool pagerMemoryOnly = shortTest && (deterministicRandom()->random01() < .001);
	state int maxKeySize = deterministicRandom()->randomInt(1, pageSize * 2);
	state int maxValueSize = randomSize(pageSize * 25);
	state int maxCommitSize = shortTest ? 1000 : randomSize(std::min<int>((maxKeySize + maxValueSize) * 20000, 10e6));
	state double clearProbability = deterministicRandom()->random01() * .1;
	state double clearSingleKeyProbability = deterministicRandom()->random01();
	state double clearPostSetProbability = deterministicRandom()->random01() * .1;
	state double coldStartProbability = pagerMemoryOnly ? 0 : (deterministicRandom()->random01() * 0.3);
	state double advanceOldVersionProbability = deterministicRandom()->random01();
	state Version versionIncrement = deterministicRandom()->randomInt64(1, 1e8);
	state int maxVerificationMapEntries = 300e3;

	printf("\n");
	printf("targetPageOps: %" PRId64 "\n", targetPageOps);
	printf("pagerMemoryOnly: %d\n", pagerMemoryOnly);
	printf("serialTest: %d\n", serialTest);
	printf("shortTest: %d\n", shortTest);
	printf("pageSize: %d\n", pageSize);
	printf("maxKeySize: %d\n", maxKeySize);
	printf("maxValueSize: %d\n", maxValueSize);
	printf("maxCommitSize: %d\n", maxCommitSize);
	printf("clearProbability: %f\n", clearProbability);
	printf("clearSingleKeyProbability: %f\n", clearSingleKeyProbability);
	printf("clearPostSetProbability: %f\n", clearPostSetProbability);
	printf("coldStartProbability: %f\n", coldStartProbability);
	printf("advanceOldVersionProbability: %f\n", advanceOldVersionProbability);
	printf("versionIncrement: %" PRId64 "\n", versionIncrement);
	printf("maxVerificationMapEntries: %d\n", maxVerificationMapEntries);
	printf("\n");

	printf("Deleting existing test data...\n");
	platform::eraseDirectoryRecursive("/tmp/fakeRocks");

	printf("Initializing...\n");
	state IKeyValueStore* kvStore = keyValueStoreRocksDB("/tmp/fakeRocks", deterministicRandom()->randomUniqueID(),
	                                                     KeyValueStoreType::SSD_ROCKSDB_V1, false, false);
	wait(kvStore->init());

	state std::map<std::pair<std::string, Version>, Optional<std::string>> written;
	state std::set<Key> keys;

	state Version lastVer = kvStore->getLastCommittedVersion();
	printf("Starting from version: %" PRId64 "\n", lastVer);

	state Version version = lastVer + 1;
	kvStore->setWriteVersion(version);

	state SimpleCounter mutationBytes;
	state SimpleCounter keyBytesInserted;
	state SimpleCounter valueBytesInserted;
	state SimpleCounter sets;
	state SimpleCounter rangeClears;
	state SimpleCounter keyBytesCleared;
	state int errorCount;
	state int mutationBytesThisCommit = 0;
	state int mutationBytesTargetThisCommit = randomSize(maxCommitSize);

	state PromiseStream<Version> committedVersions;
	state Future<Void> verifyTask = verify(kvStore, committedVersions.getFuture(), &written, &errorCount, serialTest);
	state Future<Void> randomTask = serialTest ? Void() : (randomReader(kvStore) || kvStore->getError());
	if (lastVer > invalidVersion) {
		committedVersions.send(lastVer);
	}

	state Future<Void> commit = Void();
	state int64_t totalPageOps = 0;

	while (totalPageOps < targetPageOps && written.size() < maxVerificationMapEntries) {
		// Sometimes increment the version
		if (deterministicRandom()->random01() < 0.10) {
		}

		// Sometimes do a clear range
		if (deterministicRandom()->random01() < clearProbability) {
			Key start = randomKV(maxKeySize, 1).key;
			Key end = (deterministicRandom()->random01() < .01) ? keyAfter(start) : randomKV(maxKeySize, 1).key;

			// Sometimes replace start and/or end with a close actual (previously used) value
			if (deterministicRandom()->random01() < .10) {
				auto i = keys.upper_bound(start);
				if (i != keys.end()) start = *i;
			}
			if (deterministicRandom()->random01() < .10) {
				auto i = keys.upper_bound(end);
				if (i != keys.end()) end = *i;
			}

			// Do a single key clear based on probability or end being randomly chosen to be the same as begin
			// (unlikely)
			if (deterministicRandom()->random01() < clearSingleKeyProbability || end == start) {
				end = keyAfter(start);
			} else if (end < start) {
				std::swap(end, start);
			}

			// Apply clear range to verification map
			++rangeClears;
			KeyRangeRef range(start, end);
			debug_printf("      Mutation:  Clear '%s' to '%s' @%" PRId64 "\n", start.toString().c_str(),
			             end.toString().c_str(), version);
			auto e = written.lower_bound(std::make_pair(start.toString(), 0));
			if (e != written.end()) {
				auto last = e;
				auto eEnd = written.lower_bound(std::make_pair(end.toString(), 0));
				while (e != eEnd) {
					auto w = *e;
					++e;
					// If e key is different from last and last was present then insert clear for last's key at version
					if (last != eEnd &&
					    ((e == eEnd || e->first.first != last->first.first) && last->second.present())) {
						debug_printf("      Mutation:    Clearing key '%s' @%" PRId64 "\n", last->first.first.c_str(),
						             version);

						keyBytesCleared += last->first.first.size();
						mutationBytes += last->first.first.size();
						mutationBytesThisCommit += last->first.first.size();

						// If the last set was at version then just make it not present
						if (last->first.second == version) {
							last->second.reset();
						} else {
							written[std::make_pair(last->first.first, version)].reset();
						}
					}
					last = e;
				}
			}

			kvStore->clear(range);

			// Sometimes set the range start after the clear
			if (deterministicRandom()->random01() < clearPostSetProbability) {
				KeyValue kv = randomKV(0, maxValueSize);
				kv.key = range.begin;
				kvStore->set(kv);
				written[std::make_pair(kv.key.toString(), version)] = kv.value.toString();
			}
		} else {
			// Set a key
			KeyValue kv = randomKV(maxKeySize, maxValueSize);
			// Sometimes change key to a close previously used key
			if (deterministicRandom()->random01() < .01) {
				auto i = keys.upper_bound(kv.key);
				if (i != keys.end()) kv.key = StringRef(kv.arena(), *i);
			}

			debug_printf("      Mutation:  Set '%s' -> '%s' @%" PRId64 "\n", kv.key.toString().c_str(),
			             kv.value.toString().c_str(), version);

			++sets;
			keyBytesInserted += kv.key.size();
			valueBytesInserted += kv.value.size();
			mutationBytes += (kv.key.size() + kv.value.size());
			mutationBytesThisCommit += (kv.key.size() + kv.value.size());

			kvStore->set(kv);
			written[std::make_pair(kv.key.toString(), version)] = kv.value.toString();
			keys.insert(kv.key);
		}

		// Commit after any limits for this commit or the total test are reached
		if (totalPageOps >= targetPageOps || written.size() >= maxVerificationMapEntries ||
		    mutationBytesThisCommit >= mutationBytesTargetThisCommit) {

			// Wait for previous commit to finish
			wait(commit);
			printf("Committed.  Next commit %d bytes, %" PRId64 " bytes.", mutationBytesThisCommit,
			       mutationBytes.get());
			printf("  Stats:  Insert %.2f MB/s  ClearedKeys %.2f MB/s  Total %.2f\n",
			       (keyBytesInserted.rate() + valueBytesInserted.rate()) / 1e6, keyBytesCleared.rate() / 1e6,
			       mutationBytes.rate() / 1e6);

			Version v = version; // Avoid capture of version as a member of *this

			commit = map(kvStore->commit(), [=, &ops = totalPageOps](Void) {
				// Notify the background verifier that version is committed and therefore readable
				committedVersions.send(v);
				return Void();
			});

			// Increment the version.
			// TODO: We're only putting one version per commit because that's all the RocksDB engine
			// can make readable. We should either change the test logic to only read from committed
			// versions or change RocksDB to be able to read from any version.
			++version;
			kvStore->setWriteVersion(version);

			// Sometimes advance the oldest version to close the gap between the oldest and latest versions by a random
			// amount.
			if (deterministicRandom()->random01() < advanceOldVersionProbability) {
				kvStore->setOldestVersion(kvStore->getLastCommittedVersion() -
				                          deterministicRandom()->randomInt64(0, kvStore->getLastCommittedVersion() -
				                                                                    kvStore->getOldestVersion() + 1));
			}

			if (serialTest) {
				// Wait for commit, wait for verification, then start new verification
				wait(commit);
				committedVersions.sendError(end_of_stream());
				debug_printf("Waiting for verification to complete.\n");
				wait(verifyTask);
				committedVersions = PromiseStream<Version>();
				verifyTask = verify(kvStore, committedVersions.getFuture(), &written, &errorCount, serialTest);
			}

			mutationBytesThisCommit = 0;
			mutationBytesTargetThisCommit = randomSize(maxCommitSize);

			// Recover from disk at random
			if (!serialTest && deterministicRandom()->random01() < coldStartProbability) {
				printf("Recovering from disk after next commit.\n");

				// Wait for outstanding commit
				debug_printf("Waiting for outstanding commit\n");
				wait(commit);

				// Stop and wait for the verifier task
				committedVersions.sendError(end_of_stream());
				debug_printf("Waiting for verification to complete.\n");
				wait(verifyTask);

				debug_printf("Closing kvStore\n");
				Future<Void> closedFuture = kvStore->onClosed();
				kvStore->close();
				wait(closedFuture);

				printf("Reopening kvStore from disk.\n");
				kvStore = keyValueStoreRocksDB("/tmp/fakeRocks", deterministicRandom()->randomUniqueID(),
				                               KeyValueStoreType::SSD_ROCKSDB_V1, false, false);
				wait(kvStore->init());

				Version v = kvStore->getLastCommittedVersion();
				ASSERT(v == version);
				printf("Recovered from disk.  Latest version %" PRId64 "\n", v);

				// Create new promise stream and start the verifier again
				committedVersions = PromiseStream<Version>();
				verifyTask = verify(kvStore, committedVersions.getFuture(), &written, &errorCount, serialTest);
				randomTask = randomReader(kvStore) || kvStore->getError();
				committedVersions.send(v);
			}

			version += versionIncrement;
			kvStore->setWriteVersion(version);
		}

		// Check for errors
		ASSERT(errorCount == 0);
	}

	debug_printf("Waiting for outstanding commit\n");
	wait(commit);
	committedVersions.sendError(end_of_stream());
	randomTask.cancel();
	debug_printf("Waiting for verification to complete.\n");
	wait(verifyTask);

	// Check for errors
	ASSERT(errorCount == 0);

	state Future<Void> closedFuture = kvStore->onClosed();
	kvStore->dispose();
	debug_printf("Closing.\n");
	wait(closedFuture);

	return Void();
}
