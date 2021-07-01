/*
 * AgentDriver.actor.cpp
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

#include "fdbbackup/AgentDriver.h"
#include "fdbclient/BackupAgent.actor.h"
#include "fdbclient/versions.h"
#include "fdbclient/S3BlobStore.h"

#include "flow/actorcompiler.h" // must be last include

namespace {

// TODO: Add comment here
ACTOR Future<std::string> getLayerStatus(Reference<ReadYourWritesTransaction> tr,
                                         std::string name,
                                         std::string id,
                                         AgentType agentType,
                                         Database dest,
                                         bool snapshot = false) {
	// This process will write a document that looks like this:
	// { backup : { $expires : {<subdoc>}, version: <version from approximately 30 seconds from now> }
	// so that the value under 'backup' will eventually expire to null and thus be ignored by
	// readers of status.  This is because if all agents die then they can no longer clean up old
	// status docs from other dead agents.

	state Version readVer = wait(tr->getReadVersion());

	state json_spirit::mValue layersRootValue; // Will contain stuff that goes into the doc at the layers status root
	JSONDoc layersRoot(layersRootValue); // Convenient mutator / accessor for the layers root
	JSONDoc op = layersRoot.subDoc(name); // Operator object for the $expires operation
	// Create the $expires key which is where the rest of the status output will go

	state JSONDoc layerRoot = op.subDoc("$expires");
	// Set the version argument in the $expires operator object.
	op.create("version") = readVer + 120 * CLIENT_KNOBS->CORE_VERSIONSPERSECOND;

	layerRoot.create("instances_running.$sum") = 1;
	layerRoot.create("last_updated.$max") = now();

	state JSONDoc o = layerRoot.subDoc("instances." + id);

	o.create("version") = FDB_VT_VERSION;
	o.create("id") = id;
	o.create("last_updated") = now();
	o.create("memory_usage") = (int64_t)getMemoryUsage();
	o.create("resident_size") = (int64_t)getResidentMemoryUsage();
	o.create("main_thread_cpu_seconds") = getProcessorTimeThread();
	o.create("process_cpu_seconds") = getProcessorTimeProcess();
	o.create("configured_workers") = CLIENT_KNOBS->BACKUP_TASKS_PER_AGENT;

	if (agentType == AgentType::FILE) {
		static S3BlobStoreEndpoint::Stats last_stats;
		static double last_ts = 0;
		S3BlobStoreEndpoint::Stats current_stats = S3BlobStoreEndpoint::s_stats;
		JSONDoc blobstats = o.create("blob_stats");
		blobstats.create("total") = current_stats.getJSON();
		S3BlobStoreEndpoint::Stats diff = current_stats - last_stats;
		json_spirit::mObject diffObj = diff.getJSON();
		if (last_ts > 0)
			diffObj["bytes_per_second"] = double(current_stats.bytes_sent - last_stats.bytes_sent) / (now() - last_ts);
		blobstats.create("recent") = diffObj;
		last_stats = current_stats;
		last_ts = now();

		JSONDoc totalBlobStats = layerRoot.subDoc("blob_recent_io");
		for (auto& p : diffObj)
			totalBlobStats.create(p.first + ".$sum") = p.second;

		state FileBackupAgent fba;
		state std::vector<KeyBackedTag> backupTags = wait(getAllBackupTags(tr, snapshot));
		state std::vector<Future<Optional<Version>>> tagLastRestorableVersions;
		state std::vector<Future<EBackupState>> tagStates;
		state std::vector<Future<Reference<IBackupContainer>>> tagContainers;
		state std::vector<Future<int64_t>> tagRangeBytes;
		state std::vector<Future<int64_t>> tagLogBytes;
		state Future<Optional<Value>> fBackupPaused = tr->get(fba.taskBucket->getPauseKey(), snapshot);

		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);
		state std::vector<KeyBackedTag>::iterator tag;
		state std::vector<UID> backupTagUids;
		for (tag = backupTags.begin(); tag != backupTags.end(); tag++) {
			UidAndAbortedFlagT uidAndAbortedFlag = wait(tag->getOrThrow(tr, snapshot));
			BackupConfig config(uidAndAbortedFlag.first);
			backupTagUids.push_back(config.getUid());

			tagStates.push_back(config.stateEnum().getOrThrow(tr, snapshot));
			tagRangeBytes.push_back(config.rangeBytesWritten().getD(tr, snapshot, 0));
			tagLogBytes.push_back(config.logBytesWritten().getD(tr, snapshot, 0));
			tagContainers.push_back(config.backupContainer().getOrThrow(tr, snapshot));
			tagLastRestorableVersions.push_back(fba.getLastRestorable(tr, StringRef(tag->tagName), snapshot));
		}

		wait(waitForAll(tagLastRestorableVersions) && waitForAll(tagStates) && waitForAll(tagContainers) &&
		     waitForAll(tagRangeBytes) && waitForAll(tagLogBytes) && success(fBackupPaused));

		JSONDoc tagsRoot = layerRoot.subDoc("tags.$latest");
		layerRoot.create("tags.timestamp") = now();
		layerRoot.create("total_workers.$sum") =
		    fBackupPaused.get().present() ? 0 : CLIENT_KNOBS->BACKUP_TASKS_PER_AGENT;
		layerRoot.create("paused.$latest") = fBackupPaused.get().present();

		int j = 0;
		for (KeyBackedTag eachTag : backupTags) {
			EBackupState status = tagStates[j].get();
			const char* statusText = fba.getStateText(status);

			// The object for this backup tag inside this instance's subdocument
			JSONDoc tagRoot = tagsRoot.subDoc(eachTag.tagName);
			tagRoot.create("current_container") = tagContainers[j].get()->getURL();
			tagRoot.create("current_status") = statusText;
			if (tagLastRestorableVersions[j].get().present()) {
				Version last_restorable_version = tagLastRestorableVersions[j].get().get();
				double last_restorable_seconds_behind =
				    ((double)readVer - last_restorable_version) / CLIENT_KNOBS->CORE_VERSIONSPERSECOND;
				tagRoot.create("last_restorable_version") = last_restorable_version;
				tagRoot.create("last_restorable_seconds_behind") = last_restorable_seconds_behind;
			}
			tagRoot.create("running_backup") =
			    (status == EBackupState::STATE_RUNNING_DIFFERENTIAL || status == EBackupState::STATE_RUNNING);
			tagRoot.create("running_backup_is_restorable") = (status == EBackupState::STATE_RUNNING_DIFFERENTIAL);
			tagRoot.create("range_bytes_written") = tagRangeBytes[j].get();
			tagRoot.create("mutation_log_bytes_written") = tagLogBytes[j].get();
			tagRoot.create("mutation_stream_id") = backupTagUids[j].toString();

			j++;
		}
	} else if (agentType == AgentType::DB) {
		state DatabaseBackupAgent dba;
		state Reference<ReadYourWritesTransaction> tr2(new ReadYourWritesTransaction(dest));
		tr2->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr2->setOption(FDBTransactionOptions::LOCK_AWARE);
		state RangeResult tagNames = wait(tr2->getRange(dba.tagNames.range(), 10000, snapshot));
		state std::vector<Future<Optional<Key>>> backupVersion;
		state std::vector<Future<EBackupState>> backupStatus;
		state std::vector<Future<int64_t>> tagRangeBytesDR;
		state std::vector<Future<int64_t>> tagLogBytesDR;
		state Future<Optional<Value>> fDRPaused = tr->get(dba.taskBucket->getPauseKey(), snapshot);

		state std::vector<UID> drTagUids;
		for (int i = 0; i < tagNames.size(); i++) {
			backupVersion.push_back(tr2->get(tagNames[i].value.withPrefix(applyMutationsBeginRange.begin), snapshot));
			UID tagUID = BinaryReader::fromStringRef<UID>(tagNames[i].value, Unversioned());
			drTagUids.push_back(tagUID);
			backupStatus.push_back(dba.getStateValue(tr2, tagUID, snapshot));
			tagRangeBytesDR.push_back(dba.getRangeBytesWritten(tr2, tagUID, snapshot));
			tagLogBytesDR.push_back(dba.getLogBytesWritten(tr2, tagUID, snapshot));
		}

		wait(waitForAll(backupStatus) && waitForAll(backupVersion) && waitForAll(tagRangeBytesDR) &&
		     waitForAll(tagLogBytesDR) && success(fDRPaused));

		JSONDoc tagsRoot = layerRoot.subDoc("tags.$latest");
		layerRoot.create("tags.timestamp") = now();
		layerRoot.create("total_workers.$sum") = fDRPaused.get().present() ? 0 : CLIENT_KNOBS->BACKUP_TASKS_PER_AGENT;
		layerRoot.create("paused.$latest") = fDRPaused.get().present();

		for (int i = 0; i < tagNames.size(); i++) {
			std::string tagName = dba.sourceTagNames.unpack(tagNames[i].key).getString(0).toString();

			auto status = backupStatus[i].get();

			JSONDoc tagRoot = tagsRoot.create(tagName);
			tagRoot.create("running_backup") =
			    (status == EBackupState::STATE_RUNNING_DIFFERENTIAL || status == EBackupState::STATE_RUNNING);
			tagRoot.create("running_backup_is_restorable") = (status == EBackupState::STATE_RUNNING_DIFFERENTIAL);
			tagRoot.create("range_bytes_written") = tagRangeBytesDR[i].get();
			tagRoot.create("mutation_log_bytes_written") = tagLogBytesDR[i].get();
			tagRoot.create("mutation_stream_id") = drTagUids[i].toString();

			if (backupVersion[i].get().present()) {
				double seconds_behind = ((double)readVer - BinaryReader::fromStringRef<Version>(
				                                               backupVersion[i].get().get(), Unversioned())) /
				                        CLIENT_KNOBS->CORE_VERSIONSPERSECOND;
				tagRoot.create("seconds_behind") = seconds_behind;
				//TraceEvent("BackupMetrics").detail("SecondsBehind", seconds_behind);
			}

			tagRoot.create("backup_state") = BackupAgentBase::getStateText(status);
		}
	}

	std::string json = json_spirit::write_string(layersRootValue);
	return json;
}

// Get layer status document for just this layer
ACTOR Future<json_spirit::mObject> getLayerStatus(Database src, std::string rootKey) {
	state Transaction tr(src);

	loop {
		try {
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			state RangeResult kvPairs =
			    wait(tr.getRange(KeyRangeRef(rootKey, strinc(rootKey)), GetRangeLimits::ROW_LIMIT_UNLIMITED));
			json_spirit::mObject statusDoc;
			JSONDoc modifier(statusDoc);
			for (auto& kv : kvPairs) {
				json_spirit::mValue docValue;
				json_spirit::read_string(kv.value.toString(), docValue);
				modifier.absorb(docValue);
			}
			JSONDoc::expires_reference_version = (uint64_t)tr.getReadVersion().get();
			modifier.cleanOps();
			return statusDoc;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

// Check for unparseable or expired statuses and delete them.
// First checks the first doc in the key range, and if it is valid, alive and not "me" then
// returns.  Otherwise, checks the rest of the range as well.
ACTOR Future<Void> cleanupStatus(Reference<ReadYourWritesTransaction> tr,
                                 std::string rootKey,
                                 std::string name,
                                 std::string id,
                                 int limit = 1) {
	state RangeResult docs = wait(tr->getRange(KeyRangeRef(rootKey, strinc(rootKey)), limit, true));
	state bool readMore = false;
	state int i;
	for (i = 0; i < docs.size(); ++i) {
		json_spirit::mValue docValue;
		try {
			json_spirit::read_string(docs[i].value.toString(), docValue);
			JSONDoc doc(docValue);
			// Update the reference version for $expires
			JSONDoc::expires_reference_version = tr->getReadVersion().get();
			// Evaluate the operators in the document, which will reduce to nothing if it is expired.
			doc.cleanOps();
			if (!doc.has(name + ".last_updated"))
				throw Error();

			// Alive and valid.
			// If limit == 1 and id is present then read more
			if (limit == 1 && doc.has(name + ".instances." + id))
				readMore = true;
		} catch (Error& e) {
			// If doc can't be parsed or isn't alive, delete it.
			TraceEvent(SevWarn, "RemovedDeadBackupLayerStatus").detail("Key", docs[i].key);
			tr->clear(docs[i].key);
			// If limit is 1 then read more.
			if (limit == 1)
				readMore = true;
		}
		if (readMore) {
			limit = 10000;
			RangeResult docs2 = wait(tr->getRange(KeyRangeRef(rootKey, strinc(rootKey)), limit, true));
			docs = std::move(docs2);
			readMore = false;
		}
	}

	return Void();
}

// Read layer status for this layer and get the total count of agent processes (instances) then adjust the poll delay
// based on that and BACKUP_AGGREGATE_POLL_RATE
ACTOR Future<Void> updateAgentPollRate(Database src, std::string rootKey, std::string name, double* pollDelay) {
	loop {
		try {
			json_spirit::mObject status = wait(getLayerStatus(src, rootKey));
			int64_t processes = 0;
			// If instances count is present and greater than 0 then update pollDelay
			if (JSONDoc(status).tryGet<int64_t>(name + ".instances_running", processes) && processes > 0) {
				// The aggregate poll rate is the target poll rate for all agent processes in the cluster
				// The poll rate (polls/sec) for a single processes is aggregate poll rate / processes, and pollDelay is
				// the inverse of that
				*pollDelay = (double)processes / CLIENT_KNOBS->BACKUP_AGGREGATE_POLL_RATE;
			}
		} catch (Error& e) {
			TraceEvent(SevWarn, "BackupAgentPollRateUpdateError").error(e);
		}
		wait(delay(CLIENT_KNOBS->BACKUP_AGGREGATE_POLL_RATE_UPDATE_INTERVAL));
	}
}

ACTOR Future<Void> _statusUpdateActor(Database statusUpdateDest,
                                      std::string name,
                                      AgentType agentType,
                                      double* pollDelay,
                                      Database taskDest,
                                      std::string id) {
	state std::string metaKey = layerStatusMetaPrefixRange.begin.toString() + "json/" + name;
	state std::string rootKey = backupStatusPrefixRange.begin.toString() + name + "/json";
	state std::string instanceKey = rootKey + "/" + "agent-" + id;
	state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(statusUpdateDest));
	state Future<Void> pollRateUpdater;

	// Register the existence of this layer in the meta key space
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			tr->set(metaKey, rootKey);
			wait(tr->commit());
			break;
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}

	// Write status periodically
	loop {
		tr->reset();
		try {
			loop {
				try {
					tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
					tr->setOption(FDBTransactionOptions::LOCK_AWARE);
					state Future<std::string> futureStatusDoc = getLayerStatus(tr, name, id, agentType, taskDest, true);
					wait(cleanupStatus(tr, rootKey, name, id));
					std::string statusdoc = wait(futureStatusDoc);
					tr->set(instanceKey, statusdoc);
					wait(tr->commit());
					break;
				} catch (Error& e) {
					wait(tr->onError(e));
				}
			}

			wait(delay(CLIENT_KNOBS->BACKUP_STATUS_DELAY *
			           ((1.0 - CLIENT_KNOBS->BACKUP_STATUS_JITTER) +
			            2 * deterministicRandom()->random01() * CLIENT_KNOBS->BACKUP_STATUS_JITTER)));

			// Now that status was written at least once by this process (and hopefully others), start the poll rate
			// control updater if it wasn't started yet
			if (!pollRateUpdater.isValid() && pollDelay != nullptr)
				pollRateUpdater = updateAgentPollRate(statusUpdateDest, rootKey, name, pollDelay);
		} catch (Error& e) {
			TraceEvent(SevWarnAlways, "UnableToWriteStatus").error(e);
			wait(delay(10.0));
		}
	}
}

} // namespace

Future<Void> statusUpdateActor(Database statusUpdateDest,
                               std::string const& name,
                               AgentType agentType,
                               double& pollDelay,
                               Database taskDest,
                               std::string const& id) {
	return _statusUpdateActor(statusUpdateDest, name, agentType, &pollDelay, taskDest, id);
}

Optional<Database> initCluster(std::string const& clusterFile, LocalityData const& localities, bool quiet) {
	Optional<Database> db;
	auto resolvedSourceClusterFile = ClusterConnectionFile::lookupClusterFileName(clusterFile);
	Reference<ClusterConnectionFile> ccf;
	try {
		ccf = makeReference<ClusterConnectionFile>(resolvedSourceClusterFile.first);
	} catch (Error& e) {
		if (!quiet) {
			fprintf(stderr, "%s\n", ClusterConnectionFile::getErrorString(resolvedSourceClusterFile, e).c_str());
		}
		return {};
	}

	try {
		db = Database::createDatabase(ccf, -1, true, localities);
	} catch (Error& e) {
		fprintf(stderr, "ERROR: %s\n", e.what());
		fprintf(stderr, "ERROR: Unable to connect to cluster from `%s'\n", ccf->getFilename().c_str());
		return {};
	}

	return db;
}

void processLocalityArg(CSimpleOpt& args, LocalityData& localities) {
	std::string syn = args.OptionSyntax();
	if (!StringRef(syn).startsWith(LiteralStringRef("--locality_"))) {
		fprintf(stderr, "ERROR: unable to parse locality key '%s'\n", syn.c_str());
		throw invalid_option_value();
	}
	syn = syn.substr(std::string("--locality_").size());
	std::transform(syn.begin(), syn.end(), syn.begin(), ::tolower);
	localities.set(Key(syn), Value(std::string(args.OptionArg())));
}
