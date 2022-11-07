/*
 * BlobConnectionProviderTest.actor.cpp
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

#include "fdbclient/BlobConnectionProvider.h"

#include "flow/UnitTest.h"
#include "fdbserver/Knobs.h"
#include "flow/actorcompiler.h" // has to be last include

void forceLinkBlobConnectionProviderTests() {}

struct ConnectionProviderTestSettings {
	uint32_t numProviders;
	uint32_t filesPerProvider;
	uint32_t maxFileMemory;
	uint32_t maxFileSize;
	uint32_t threads;
	bool uniformProviderChoice;
	double readWriteSplit;

	double runtime;

	int writeOps;
	int readOps;

	ConnectionProviderTestSettings() {
		numProviders = deterministicRandom()->randomSkewedUInt32(1, 1000);
		filesPerProvider =
		    1 + std::min((uint32_t)100, deterministicRandom()->randomSkewedUInt32(10, 10000) / numProviders);
		maxFileMemory = 1024 * 1024 * 1024;
		maxFileSize = maxFileMemory / (numProviders * filesPerProvider);
		maxFileSize = deterministicRandom()->randomSkewedUInt32(8, std::min((uint32_t)(16 * 1024 * 1024), maxFileSize));
		threads = deterministicRandom()->randomInt(16, 128);

		uniformProviderChoice = deterministicRandom()->coinflip();
		readWriteSplit = deterministicRandom()->randomInt(1, 10) / 10.0;

		runtime = 60.0;

		writeOps = 0;
		readOps = 0;
	}
};

struct ProviderTestData {
	Reference<BlobConnectionProvider> provider;
	std::vector<std::pair<std::string, Value>> data;
	std::unordered_set<std::string> usedNames;

	ProviderTestData() {}
	explicit ProviderTestData(Reference<BlobConnectionProvider> provider) : provider(provider) {}
};

ACTOR Future<Void> createObject(ConnectionProviderTestSettings* settings, ProviderTestData* provider) {
	// pick object name before wait so no collisions between concurrent writes
	std::string objName;
	loop {
		objName = deterministicRandom()->randomAlphaNumeric(12);
		if (provider->usedNames.insert(objName).second) {
			break;
		}
	}

	int randomDataSize = deterministicRandom()->randomInt(1, settings->maxFileSize);
	state Value data = makeString(randomDataSize);
	deterministicRandom()->randomBytes(mutateString(data), randomDataSize);

	state Reference<BackupContainerFileSystem> bstore;
	state std::string fullPath;
	std::tie(bstore, fullPath) = provider->provider->createForWrite(objName);

	if (deterministicRandom()->coinflip()) {
		state Reference<IBackupFile> file = wait(bstore->writeFile(fullPath));
		wait(file->append(data.begin(), data.size()));
		wait(file->finish());
	} else {
		std::string contents = data.toString();
		wait(bstore->writeEntireFile(fullPath, contents));
	}

	// after write, put in the readable list
	provider->data.push_back({ fullPath, data });

	return Void();
}

ACTOR Future<Void> readAndVerifyObject(ProviderTestData* provider, std::string objFullPath, Value expectedData) {
	Reference<BackupContainerFileSystem> bstore = provider->provider->getForRead(objFullPath);
	state Reference<IAsyncFile> reader = wait(bstore->readFile(objFullPath));

	state Value actualData = makeString(expectedData.size());
	int readSize = wait(reader->read(mutateString(actualData), expectedData.size(), 0));
	ASSERT_EQ(expectedData.size(), readSize);
	ASSERT(expectedData == actualData);

	return Void();
}

Future<Void> deleteObject(ProviderTestData* provider, std::string objFullPath) {
	Reference<BackupContainerFileSystem> bstore = provider->provider->getForRead(objFullPath);
	return bstore->deleteFile(objFullPath);
}

ACTOR Future<Void> workerThread(ConnectionProviderTestSettings* settings, std::vector<ProviderTestData>* providers) {
	state double endTime = now() + settings->runtime;
	try {
		while (now() < endTime) {
			// randomly pick provider
			int providerIdx;
			if (settings->uniformProviderChoice) {
				providerIdx = deterministicRandom()->randomInt(0, providers->size());
			} else {
				providerIdx = deterministicRandom()->randomSkewedUInt32(0, providers->size());
			}
			ProviderTestData* provider = &(*providers)[providerIdx];

			// randomly pick create or read
			bool doWrite = deterministicRandom()->random01() < settings->readWriteSplit;
			if (provider->usedNames.size() < settings->filesPerProvider && (provider->data.empty() || doWrite)) {
				// create an object
				wait(createObject(settings, provider));
				settings->writeOps++;
			} else if (!provider->data.empty()) {
				// read a random object
				auto& readInfo = provider->data[deterministicRandom()->randomInt(0, provider->data.size())];
				wait(readAndVerifyObject(provider, readInfo.first, readInfo.second));
				settings->readOps++;
			} else {
				// other threads are creating files up to filesPerProvider limit, but none finished yet. Just wait
				wait(delay(0.1));
			}
		}

		return Void();
	} catch (Error& e) {
		fmt::print("WorkerThread Unexpected Error {0}\n", e.name());
		throw e;
	}
}

ACTOR Future<Void> checkAndCleanUp(ProviderTestData* provider) {
	state int i;
	ASSERT(provider->usedNames.size() == provider->data.size());

	for (i = 0; i < provider->data.size(); i++) {
		auto& readInfo = provider->data[i];
		wait(readAndVerifyObject(provider, readInfo.first, readInfo.second));
		wait(deleteObject(provider, provider->data[i].first));
	}

	return Void();
}

// maybe this should be a workload instead?
TEST_CASE("/fdbserver/blob/connectionprovider") {
	state ConnectionProviderTestSettings settings;

	state std::vector<ProviderTestData> providers;
	providers.reserve(settings.numProviders);
	for (int i = 0; i < settings.numProviders; i++) {
		std::string nameStr = std::to_string(i);
		auto metadata = createRandomTestBlobMetadata(SERVER_KNOBS->BG_URL, i);
		providers.emplace_back(BlobConnectionProvider::newBlobConnectionProvider(metadata));
	}
	fmt::print("BlobConnectionProviderTest\n");

	state std::vector<Future<Void>> futures;
	futures.reserve(settings.threads);
	for (int i = 0; i < settings.threads; i++) {
		futures.push_back(workerThread(&settings, &providers));
	}

	wait(waitForAll(futures));

	fmt::print("BlobConnectionProviderTest workload phase complete with {0} files and {1} reads\n",
	           settings.writeOps,
	           settings.readOps);

	futures.clear();
	futures.reserve(providers.size());
	for (int i = 0; i < providers.size(); i++) {
		futures.push_back(checkAndCleanUp(&providers[i]));
	}

	wait(waitForAll(futures));

	fmt::print("BlobConnectionProviderTest check and cleanup phase complete\n");
	return Void();
}