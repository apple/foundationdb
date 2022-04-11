/*
 * BackupContainer.actor.cpp
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

#include <cstdlib>
#include <ostream>

// FIXME: Trim this down
#include "contrib/fmt-8.1.1/include/fmt/format.h"
#include "flow/Platform.actor.h"
#include "fdbclient/AsyncTaskThread.h"
#include "fdbclient/BackupContainer.h"
#include "fdbclient/BackupAgent.actor.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/JsonBuilder.h"
#include "flow/Arena.h"
#include "flow/Trace.h"
#include "flow/UnitTest.h"
#include "flow/Hash3.h"
#include "fdbrpc/AsyncFileReadAhead.actor.h"
#include "fdbrpc/simulator.h"
#include "flow/Platform.h"
#include "fdbclient/AsyncFileS3BlobStore.actor.h"
#include "fdbclient/BackupContainerAzureBlobStore.h"
#include "fdbclient/BackupContainerFileSystem.h"
#include "fdbclient/BackupContainerLocalDirectory.h"
#include "fdbclient/BackupContainerS3BlobStore.h"
#include "fdbclient/Status.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/KeyBackedTypes.h"
#include "fdbclient/RunTransaction.actor.h"
#include <algorithm>
#include <cinttypes>
#include <time.h>
#include "flow/actorcompiler.h" // has to be last include

namespace IBackupFile_impl {

ACTOR Future<Void> appendStringRefWithLen(Reference<IBackupFile> file, Standalone<StringRef> s) {
	state uint32_t lenBuf = bigEndian32((uint32_t)s.size());
	wait(file->append(&lenBuf, sizeof(lenBuf)));
	wait(file->append(s.begin(), s.size()));
	return Void();
}

} // namespace IBackupFile_impl

Future<Void> IBackupFile::appendStringRefWithLen(Standalone<StringRef> s) {
	return IBackupFile_impl::appendStringRefWithLen(Reference<IBackupFile>::addRef(this), s);
}

std::string IBackupContainer::ExpireProgress::toString() const {
	std::string s = step + "...";
	if (total > 0) {
		s += format("%d/%d (%.2f%%)", done, total, double(done) / total * 100);
	}
	return s;
}

void BackupFileList::toStream(FILE* fout) const {
	for (const RangeFile& f : ranges) {
		fmt::print(fout, "range {0} {1}\n", f.fileSize, f.fileName);
	}
	for (const LogFile& f : logs) {
		fmt::print(fout, "log {0} {1}\n", f.fileSize, f.fileName);
	}
	for (const KeyspaceSnapshotFile& f : snapshots) {
		fmt::print(fout, "snapshotManifest {0} {1}\n", f.totalSize, f.fileName);
	}
}

Future<Void> fetchTimes(Reference<ReadYourWritesTransaction> tr, std::map<Version, int64_t>* pVersionTimeMap) {
	std::vector<Future<Void>> futures;

	// Resolve each version in the map,
	for (auto& p : *pVersionTimeMap) {
		futures.push_back(map(timeKeeperEpochsFromVersion(p.first, tr), [=](Optional<int64_t> t) {
			if (t.present())
				pVersionTimeMap->at(p.first) = t.get();
			else
				pVersionTimeMap->erase(p.first);
			return Void();
		}));
	}

	return waitForAll(futures);
}

Future<Void> BackupDescription::resolveVersionTimes(Database cx) {
	// Populate map with versions needed
	versionTimeMap.clear();

	for (const KeyspaceSnapshotFile& m : snapshots) {
		versionTimeMap[m.beginVersion];
		versionTimeMap[m.endVersion];
	}
	if (minLogBegin.present())
		versionTimeMap[minLogBegin.get()];
	if (maxLogEnd.present())
		versionTimeMap[maxLogEnd.get()];
	if (contiguousLogEnd.present())
		versionTimeMap[contiguousLogEnd.get()];
	if (minRestorableVersion.present())
		versionTimeMap[minRestorableVersion.get()];
	if (maxRestorableVersion.present())
		versionTimeMap[maxRestorableVersion.get()];

	return runRYWTransaction(cx,
	                         [=](Reference<ReadYourWritesTransaction> tr) { return fetchTimes(tr, &versionTimeMap); });
};

std::string BackupDescription::toString() const {
	std::string info;

	info.append(format("URL: %s\n", url.c_str()));
	info.append(format("Restorable: %s\n", maxRestorableVersion.present() ? "true" : "false"));
	info.append(format("Partitioned logs: %s\n", partitioned ? "true" : "false"));

	auto formatVersion = [&](Version v) {
		std::string s;
		if (!versionTimeMap.empty()) {
			auto i = versionTimeMap.find(v);
			if (i != versionTimeMap.end())
				s = format("%lld (%s)", v, BackupAgentBase::formatTime(i->second).c_str());
			else
				s = format("%lld (unknown)", v);
		} else if (maxLogEnd.present()) {
			double days = double(maxLogEnd.get() - v) / (CLIENT_KNOBS->CORE_VERSIONSPERSECOND * 24 * 60 * 60);
			s = format("%lld (maxLogEnd %s%.2f days)", v, days < 0 ? "+" : "-", days);
		} else {
			s = format("%lld", v);
		}
		return s;
	};

	for (const KeyspaceSnapshotFile& m : snapshots) {
		info.append(
		    format("Snapshot:  startVersion=%s  endVersion=%s  totalBytes=%lld  restorable=%s  expiredPct=%.2f\n",
		           formatVersion(m.beginVersion).c_str(),
		           formatVersion(m.endVersion).c_str(),
		           m.totalSize,
		           m.restorable.orDefault(false) ? "true" : "false",
		           m.expiredPct(expiredEndVersion)));
	}

	info.append(format("SnapshotBytes: %lld\n", snapshotBytes));

	if (expiredEndVersion.present())
		info.append(format("ExpiredEndVersion:       %s\n", formatVersion(expiredEndVersion.get()).c_str()));
	if (unreliableEndVersion.present())
		info.append(format("UnreliableEndVersion:    %s\n", formatVersion(unreliableEndVersion.get()).c_str()));
	if (minLogBegin.present())
		info.append(format("MinLogBeginVersion:      %s\n", formatVersion(minLogBegin.get()).c_str()));
	if (contiguousLogEnd.present())
		info.append(format("ContiguousLogEndVersion: %s\n", formatVersion(contiguousLogEnd.get()).c_str()));
	if (maxLogEnd.present())
		info.append(format("MaxLogEndVersion:        %s\n", formatVersion(maxLogEnd.get()).c_str()));
	if (minRestorableVersion.present())
		info.append(format("MinRestorableVersion:    %s\n", formatVersion(minRestorableVersion.get()).c_str()));
	if (maxRestorableVersion.present())
		info.append(format("MaxRestorableVersion:    %s\n", formatVersion(maxRestorableVersion.get()).c_str()));

	if (!extendedDetail.empty())
		info.append("ExtendedDetail: ").append(extendedDetail);

	return info;
}

std::string BackupDescription::toJSON() const {
	JsonBuilderObject doc;

	doc.setKey("SchemaVersion", "1.0.0");
	doc.setKey("URL", url.c_str());
	doc.setKey("Restorable", maxRestorableVersion.present());
	doc.setKey("Partitioned", partitioned);

	auto formatVersion = [&](Version v) {
		JsonBuilderObject doc;
		doc.setKey("Version", v);
		if (!versionTimeMap.empty()) {
			auto i = versionTimeMap.find(v);
			if (i != versionTimeMap.end()) {
				doc.setKey("Timestamp", BackupAgentBase::formatTime(i->second));
				doc.setKey("EpochSeconds", i->second);
			}
		} else if (maxLogEnd.present()) {
			double days = double(v - maxLogEnd.get()) / (CLIENT_KNOBS->CORE_VERSIONSPERSECOND * 24 * 60 * 60);
			doc.setKey("RelativeDays", days);
		}
		return doc;
	};

	JsonBuilderArray snapshotsArray;
	for (const KeyspaceSnapshotFile& m : snapshots) {
		JsonBuilderObject snapshotDoc;
		snapshotDoc.setKey("Start", formatVersion(m.beginVersion));
		snapshotDoc.setKey("End", formatVersion(m.endVersion));
		snapshotDoc.setKey("Restorable", m.restorable.orDefault(false));
		snapshotDoc.setKey("TotalBytes", m.totalSize);
		snapshotDoc.setKey("PercentageExpired", m.expiredPct(expiredEndVersion));
		snapshotsArray.push_back(snapshotDoc);
	}
	doc.setKey("Snapshots", snapshotsArray);

	doc.setKey("TotalSnapshotBytes", snapshotBytes);

	if (expiredEndVersion.present())
		doc.setKey("ExpiredEnd", formatVersion(expiredEndVersion.get()));
	if (unreliableEndVersion.present())
		doc.setKey("UnreliableEnd", formatVersion(unreliableEndVersion.get()));
	if (minLogBegin.present())
		doc.setKey("MinLogBegin", formatVersion(minLogBegin.get()));
	if (contiguousLogEnd.present())
		doc.setKey("ContiguousLogEnd", formatVersion(contiguousLogEnd.get()));
	if (maxLogEnd.present())
		doc.setKey("MaxLogEnd", formatVersion(maxLogEnd.get()));
	if (minRestorableVersion.present())
		doc.setKey("MinRestorablePoint", formatVersion(minRestorableVersion.get()));
	if (maxRestorableVersion.present())
		doc.setKey("MaxRestorablePoint", formatVersion(maxRestorableVersion.get()));

	if (!extendedDetail.empty())
		doc.setKey("ExtendedDetail", extendedDetail);

	return doc.getJson();
}

std::string IBackupContainer::lastOpenError;

std::vector<std::string> IBackupContainer::getURLFormats() {
	return {
#ifdef BUILD_AZURE_BACKUP
		BackupContainerAzureBlobStore::getURLFormat(),
#endif
		BackupContainerLocalDirectory::getURLFormat(),
		BackupContainerS3BlobStore::getURLFormat(),
	};
}

// Get an IBackupContainer based on a container URL string
Reference<IBackupContainer> IBackupContainer::openContainer(const std::string& url,
                                                            const Optional<std::string>& proxy,
                                                            const Optional<std::string>& encryptionKeyFileName) {
	static std::map<std::string, Reference<IBackupContainer>> m_cache;

	Reference<IBackupContainer>& r = m_cache[url];
	if (r)
		return r;

	try {
		StringRef u(url);
		if (u.startsWith("file://"_sr)) {
			r = makeReference<BackupContainerLocalDirectory>(url, encryptionKeyFileName);
		} else if (u.startsWith("blobstore://"_sr)) {
			std::string resource;

			// The URL parameters contain blobstore endpoint tunables as well as possible backup-specific options.
			S3BlobStoreEndpoint::ParametersT backupParams;
			Reference<S3BlobStoreEndpoint> bstore =
			    S3BlobStoreEndpoint::fromString(url, proxy, &resource, &lastOpenError, &backupParams);

			if (resource.empty())
				throw backup_invalid_url();
			for (auto c : resource)
				if (!isalnum(c) && c != '_' && c != '-' && c != '.' && c != '/')
					throw backup_invalid_url();
			r = makeReference<BackupContainerS3BlobStore>(bstore, resource, backupParams, encryptionKeyFileName);
		}
#ifdef BUILD_AZURE_BACKUP
		else if (u.startsWith("azure://"_sr)) {
			u.eat("azure://"_sr);
			auto accountName = u.eat("@"_sr).toString();
			auto endpoint = u.eat("/"_sr).toString();
			auto containerName = u.eat("/"_sr).toString();
			r = makeReference<BackupContainerAzureBlobStore>(
			    endpoint, accountName, containerName, encryptionKeyFileName);
		}
#endif
		else {
			lastOpenError = "invalid URL prefix";
			throw backup_invalid_url();
		}

		r->encryptionKeyFileName = encryptionKeyFileName;
		r->URL = url;
		return r;
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled)
			throw;

		TraceEvent m(SevWarn, "BackupContainer");
		m.error(e);
		m.detail("Description", "Invalid container specification.  See help.");
		m.detail("URL", url);
		if (e.code() == error_code_backup_invalid_url)
			m.detail("LastOpenError", lastOpenError);

		throw;
	}
}

// Get a list of URLS to backup containers based on some a shorter URL.  This function knows about some set of supported
// URL types which support this sort of backup discovery.
ACTOR Future<std::vector<std::string>> listContainers_impl(std::string baseURL, Optional<std::string> proxy) {
	try {
		StringRef u(baseURL);
		if (u.startsWith("file://"_sr)) {
			std::vector<std::string> results = wait(BackupContainerLocalDirectory::listURLs(baseURL));
			return results;
		} else if (u.startsWith("blobstore://"_sr)) {
			std::string resource;

			S3BlobStoreEndpoint::ParametersT backupParams;
			Reference<S3BlobStoreEndpoint> bstore = S3BlobStoreEndpoint::fromString(
			    baseURL, proxy, &resource, &IBackupContainer::lastOpenError, &backupParams);

			if (!resource.empty()) {
				TraceEvent(SevWarn, "BackupContainer")
				    .detail("Description", "Invalid backup container base URL, resource aka path should be blank.")
				    .detail("URL", baseURL);
				throw backup_invalid_url();
			}

			// Create a dummy container to parse the backup-specific parameters from the URL and get a final bucket name
			BackupContainerS3BlobStore dummy(bstore, "dummy", backupParams, {});

			std::vector<std::string> results = wait(BackupContainerS3BlobStore::listURLs(bstore, dummy.getBucket()));
			return results;
		}
		// TODO: Enable this when Azure backups are ready
		/*
		else if (u.startsWith("azure://"_sr)) {
		    std::vector<std::string> results = wait(BackupContainerAzureBlobStore::listURLs(baseURL));
		    return results;
		}
		*/
		else {
			IBackupContainer::lastOpenError = "invalid URL prefix";
			throw backup_invalid_url();
		}

	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled)
			throw;

		TraceEvent m(SevWarn, "BackupContainer");
		m.error(e);
		m.detail("Description", "Invalid backup container URL prefix.  See help.");
		m.detail("URL", baseURL);
		if (e.code() == error_code_backup_invalid_url)
			m.detail("LastOpenError", IBackupContainer::lastOpenError);

		throw;
	}
}

Future<std::vector<std::string>> IBackupContainer::listContainers(const std::string& baseURL,
                                                                  const Optional<std::string>& proxy) {
	return listContainers_impl(baseURL, proxy);
}

ACTOR Future<Version> timeKeeperVersionFromDatetime(std::string datetime, Database db) {
	state KeyBackedMap<int64_t, Version> versionMap(timeKeeperPrefixRange.begin);
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(db);

	state int64_t time = BackupAgentBase::parseTime(datetime);
	if (time < 0) {
		fprintf(
		    stderr, "ERROR: Incorrect date/time or format.  Format is %s.\n", BackupAgentBase::timeFormat().c_str());
		throw backup_error();
	}

	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			state std::vector<std::pair<int64_t, Version>> results =
			    wait(versionMap.getRange(tr, 0, time, 1, Snapshot::False, Reverse::True));
			if (results.size() != 1) {
				// No key less than time was found in the database
				// Look for a key >= time.
				wait(store(results, versionMap.getRange(tr, time, std::numeric_limits<int64_t>::max(), 1)));

				if (results.size() != 1) {
					fprintf(stderr, "ERROR: Unable to calculate a version for given date/time.\n");
					throw backup_error();
				}
			}

			// Adjust version found by the delta between time and the time found and min with 0.
			auto& result = results[0];
			return std::max<Version>(0, result.second + (time - result.first) * CLIENT_KNOBS->CORE_VERSIONSPERSECOND);

		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}
}

ACTOR Future<Optional<int64_t>> timeKeeperEpochsFromVersion(Version v, Reference<ReadYourWritesTransaction> tr) {
	state KeyBackedMap<int64_t, Version> versionMap(timeKeeperPrefixRange.begin);

	// Binary search to find the closest date with a version <= v
	state int64_t min = 0;
	state int64_t max = (int64_t)now();
	state int64_t mid;
	state std::pair<int64_t, Version> found;

	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);

	loop {
		mid = (min + max + 1) / 2; // ceiling

		// Find the highest time < mid
		state std::vector<std::pair<int64_t, Version>> results =
		    wait(versionMap.getRange(tr, min, mid, 1, Snapshot::False, Reverse::True));

		if (results.size() != 1) {
			if (mid == min) {
				// There aren't any records having a version < v, so just look for any record having a time < now
				// and base a result on it
				wait(store(results, versionMap.getRange(tr, 0, (int64_t)now(), 1)));

				if (results.size() != 1) {
					// There aren't any timekeeper records to base a result on so return nothing
					return Optional<int64_t>();
				}

				found = results[0];
				break;
			}

			min = mid;
			continue;
		}

		found = results[0];

		if (v < found.second) {
			max = found.first;
		} else {
			if (found.first == min) {
				break;
			}
			min = found.first;
		}
	}

	return found.first + (v - found.second) / CLIENT_KNOBS->CORE_VERSIONSPERSECOND;
}
