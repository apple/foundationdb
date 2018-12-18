/*
 * Restore.actor.cpp
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

#include "fdbserver/RestoreInterface.h"
#include "fdbclient/NativeAPI.h"
#include "fdbclient/SystemData.h"
#include "flow/actorcompiler.h"  // This must be the last #include.

// Backup agent header
#include "fdbclient/BackupAgent.h"
//#include "FileBackupAgent.h"
#include "fdbclient/ManagementAPI.h"
#include "fdbclient/MutationList.h"

#include <ctime>
#include <climits>
#include "fdbrpc/IAsyncFile.h"
#include "flow/genericactors.actor.h"
#include "flow/Hash3.h"
#include <numeric>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <algorithm>

bool debug_verbose = false;


////-- Restore code declaration START

std::map<Version, Standalone<VectorRef<MutationRef>>> kvOps;
//std::map<Version, std::vector<MutationRef>> kvOps; //TODO: Must change to standAlone before run correctness test. otherwise, you will see the mutationref memory is corrupted
std::map<Standalone<StringRef>, Standalone<StringRef>> mutationMap; //key is the unique identifier for a batch of mutation logs at the same version
std::map<Standalone<StringRef>, uint32_t> mutationPartMap; //Record the most recent
// MXX: Important: Can not use std::vector because you won't have the arena and you will hold the reference to memory that will be freed.
// Use push_back_deep() to copy data to the standalone arena.
//Standalone<VectorRef<MutationRef>> mOps;
std::vector<MutationRef> mOps;

// For convenience
typedef FileBackupAgent::ERestoreState ERestoreState;
template<> Tuple Codec<ERestoreState>::pack(ERestoreState const &val); // { return Tuple().append(val); }
template<> ERestoreState Codec<ERestoreState>::unpack(Tuple const &val); // { return (ERestoreState)val.getInt(0); }


class RestoreConfig : public KeyBackedConfig, public ReferenceCounted<RestoreConfig> {
public:
	RestoreConfig(UID uid = UID()) : KeyBackedConfig(fileRestorePrefixRange.begin, uid) {}
	RestoreConfig(Reference<Task> task) : KeyBackedConfig(fileRestorePrefixRange.begin, task) {}

	KeyBackedProperty<ERestoreState> stateEnum() {
		return configSpace.pack(LiteralStringRef(__FUNCTION__));
	}
	Future<StringRef> stateText(Reference<ReadYourWritesTransaction> tr) {
		return map(stateEnum().getD(tr), [](ERestoreState s) -> StringRef { return FileBackupAgent::restoreStateText(s); });
	}
	KeyBackedProperty<Key> addPrefix() {
		return configSpace.pack(LiteralStringRef(__FUNCTION__));
	}
	KeyBackedProperty<Key> removePrefix() {
		return configSpace.pack(LiteralStringRef(__FUNCTION__));
	}
	KeyBackedProperty<KeyRange> restoreRange() {
		return configSpace.pack(LiteralStringRef(__FUNCTION__));
	}
	KeyBackedProperty<Key> batchFuture() {
		return configSpace.pack(LiteralStringRef(__FUNCTION__));
	}
	KeyBackedProperty<Version> restoreVersion() {
		return configSpace.pack(LiteralStringRef(__FUNCTION__));
	}

	KeyBackedProperty<Reference<IBackupContainer>> sourceContainer() {
		return configSpace.pack(LiteralStringRef(__FUNCTION__));
	}
	// Get the source container as a bare URL, without creating a container instance
	KeyBackedProperty<Value> sourceContainerURL() {
		return configSpace.pack(LiteralStringRef("sourceContainer"));
	}

	// Total bytes written by all log and range restore tasks.
	KeyBackedBinaryValue<int64_t> bytesWritten() {
		return configSpace.pack(LiteralStringRef(__FUNCTION__));
	}
	// File blocks that have had tasks created for them by the Dispatch task
	KeyBackedBinaryValue<int64_t> filesBlocksDispatched() {
		return configSpace.pack(LiteralStringRef(__FUNCTION__));
	}
	// File blocks whose tasks have finished
	KeyBackedBinaryValue<int64_t> fileBlocksFinished() {
		return configSpace.pack(LiteralStringRef(__FUNCTION__));
	}
	// Total number of files in the fileMap
	KeyBackedBinaryValue<int64_t> fileCount() {
		return configSpace.pack(LiteralStringRef(__FUNCTION__));
	}
	// Total number of file blocks in the fileMap
	KeyBackedBinaryValue<int64_t> fileBlockCount() {
		return configSpace.pack(LiteralStringRef(__FUNCTION__));
	}

	// Describes a file to load blocks from during restore.  Ordered by version and then fileName to enable
	// incrementally advancing through the map, saving the version and path of the next starting point.
	struct RestoreFile {
		Version version;
		std::string fileName;
		bool isRange;  // false for log file
		int64_t blockSize;
		int64_t fileSize;
		Version endVersion;  // not meaningful for range files

		Tuple pack() const {
			return Tuple()
					.append(version)
					.append(StringRef(fileName))
					.append(isRange)
					.append(fileSize)
					.append(blockSize)
					.append(endVersion);
		}
		static RestoreFile unpack(Tuple const &t) {
			RestoreFile r;
			int i = 0;
			r.version = t.getInt(i++);
			r.fileName = t.getString(i++).toString();
			r.isRange = t.getInt(i++) != 0;
			r.fileSize = t.getInt(i++);
			r.blockSize = t.getInt(i++);
			r.endVersion = t.getInt(i++);
			return r;
		}

		std::string toString() const {
//			return "UNSET4TestHardness";
			return "version:" + std::to_string(version) + " fileName:" + fileName +" isRange:" + std::to_string(isRange)
				   + " blockSize:" + std::to_string(blockSize) + " fileSize:" + std::to_string(fileSize)
				   + " endVersion:" + std::to_string(endVersion);
		}
	};

	typedef KeyBackedSet<RestoreFile> FileSetT;
	FileSetT fileSet() {
		return configSpace.pack(LiteralStringRef(__FUNCTION__));
	}

	Future<bool> isRunnable(Reference<ReadYourWritesTransaction> tr) {
		return map(stateEnum().getD(tr), [](ERestoreState s) -> bool { return   s != ERestoreState::ABORTED
																				&& s != ERestoreState::COMPLETED
																				&& s != ERestoreState::UNITIALIZED;
		});
	}

	Future<Void> logError(Database cx, Error e, std::string const &details, void *taskInstance = nullptr) {
		if(!uid.isValid()) {
			TraceEvent(SevError, "FileRestoreErrorNoUID").error(e).detail("Description", details);
			return Void();
		}
		TraceEvent t(SevWarn, "FileRestoreError");
		t.error(e).detail("RestoreUID", uid).detail("Description", details).detail("TaskInstance", (uint64_t)taskInstance);
		// These should not happen
		if(e.code() == error_code_key_not_found)
			t.backtrace();

		return updateErrorInfo(cx, e, details);
	}

	Key mutationLogPrefix() {
		return uidPrefixKey(applyLogKeys.begin, uid);
	}

	Key applyMutationsMapPrefix() {
		return uidPrefixKey(applyMutationsKeyVersionMapRange.begin, uid);
	}

	ACTOR static Future<int64_t> getApplyVersionLag_impl(Reference<ReadYourWritesTransaction> tr, UID uid) {
		// Both of these are snapshot reads
		state Future<Optional<Value>> beginVal = tr->get(uidPrefixKey(applyMutationsBeginRange.begin, uid), true);
		state Future<Optional<Value>> endVal = tr->get(uidPrefixKey(applyMutationsEndRange.begin, uid), true);
		wait(success(beginVal) && success(endVal));

		if(!beginVal.get().present() || !endVal.get().present())
			return 0;

		Version beginVersion = BinaryReader::fromStringRef<Version>(beginVal.get().get(), Unversioned());
		Version endVersion = BinaryReader::fromStringRef<Version>(endVal.get().get(), Unversioned());
		return endVersion - beginVersion;
	}

	Future<int64_t> getApplyVersionLag(Reference<ReadYourWritesTransaction> tr) {
		return getApplyVersionLag_impl(tr, uid);
	}

	void initApplyMutations(Reference<ReadYourWritesTransaction> tr, Key addPrefix, Key removePrefix) {
		// Set these because they have to match the applyMutations values.
		this->addPrefix().set(tr, addPrefix);
		this->removePrefix().set(tr, removePrefix);

		clearApplyMutationsKeys(tr);

		// Initialize add/remove prefix, range version map count and set the map's start key to InvalidVersion
		tr->set(uidPrefixKey(applyMutationsAddPrefixRange.begin, uid), addPrefix);
		tr->set(uidPrefixKey(applyMutationsRemovePrefixRange.begin, uid), removePrefix);
		int64_t startCount = 0;
		tr->set(uidPrefixKey(applyMutationsKeyVersionCountRange.begin, uid), StringRef((uint8_t*)&startCount, 8));
		Key mapStart = uidPrefixKey(applyMutationsKeyVersionMapRange.begin, uid);
		tr->set(mapStart, BinaryWriter::toValue<Version>(invalidVersion, Unversioned()));
	}

	void clearApplyMutationsKeys(Reference<ReadYourWritesTransaction> tr) {
		tr->setOption(FDBTransactionOptions::COMMIT_ON_FIRST_PROXY);

		// Clear add/remove prefix keys
		tr->clear(uidPrefixKey(applyMutationsAddPrefixRange.begin, uid));
		tr->clear(uidPrefixKey(applyMutationsRemovePrefixRange.begin, uid));

		// Clear range version map and count key
		tr->clear(uidPrefixKey(applyMutationsKeyVersionCountRange.begin, uid));
		Key mapStart = uidPrefixKey(applyMutationsKeyVersionMapRange.begin, uid);
		tr->clear(KeyRangeRef(mapStart, strinc(mapStart)));

		// Clear any loaded mutations that have not yet been applied
		Key mutationPrefix = mutationLogPrefix();
		tr->clear(KeyRangeRef(mutationPrefix, strinc(mutationPrefix)));

		// Clear end and begin versions (intentionally in this order)
		tr->clear(uidPrefixKey(applyMutationsEndRange.begin, uid));
		tr->clear(uidPrefixKey(applyMutationsBeginRange.begin, uid));
	}

	void setApplyBeginVersion(Reference<ReadYourWritesTransaction> tr, Version ver) {
		tr->set(uidPrefixKey(applyMutationsBeginRange.begin, uid), BinaryWriter::toValue(ver, Unversioned()));
	}

	void setApplyEndVersion(Reference<ReadYourWritesTransaction> tr, Version ver) {
		tr->set(uidPrefixKey(applyMutationsEndRange.begin, uid), BinaryWriter::toValue(ver, Unversioned()));
	}

	Future<Version> getApplyEndVersion(Reference<ReadYourWritesTransaction> tr) {
		return map(tr->get(uidPrefixKey(applyMutationsEndRange.begin, uid)), [=](Optional<Value> const &value) -> Version {
			return value.present() ? BinaryReader::fromStringRef<Version>(value.get(), Unversioned()) : 0;
		});
	}

	static Future<std::string> getProgress_impl(Reference<RestoreConfig> const &restore, Reference<ReadYourWritesTransaction> const &tr);
	Future<std::string> getProgress(Reference<ReadYourWritesTransaction> tr) {
		Reference<RestoreConfig> restore = Reference<RestoreConfig>(this);
		return getProgress_impl(restore, tr);
	}

	static Future<std::string> getFullStatus_impl(Reference<RestoreConfig> const &restore, Reference<ReadYourWritesTransaction> const &tr);
	Future<std::string> getFullStatus(Reference<ReadYourWritesTransaction> tr) {
		Reference<RestoreConfig> restore = Reference<RestoreConfig>(this);
		return getFullStatus_impl(restore, tr);
	}

	std::string toString() {
		std::string ret = "[unset] TODO";
		return ret;
	}

};
class RestoreConfig;
typedef RestoreConfig::RestoreFile RestoreFile;


namespace parallelFileRestore {
	// Helper class for reading restore data from a buffer and throwing the right errors.
	struct StringRefReader {
		StringRefReader(StringRef s = StringRef(), Error e = Error()) : rptr(s.begin()), end(s.end()), failure_error(e) {}

		// Return remainder of data as a StringRef
		StringRef remainder() {
			return StringRef(rptr, end - rptr);
		}

		// Return a pointer to len bytes at the current read position and advance read pos
		const uint8_t * consume(unsigned int len) {
			if(rptr == end && len != 0)
				throw end_of_stream();
			const uint8_t *p = rptr;
			rptr += len;
			if(rptr > end)
				throw failure_error;
			return p;
		}

		// Return a T from the current read position and advance read pos
		template<typename T> const T consume() {
			return *(const T *)consume(sizeof(T));
		}

		// Functions for consuming big endian (network byte order) integers.
		// Consumes a big endian number, swaps it to little endian, and returns it.
		const int32_t  consumeNetworkInt32()  { return (int32_t)bigEndian32((uint32_t)consume< int32_t>());}
		const uint32_t consumeNetworkUInt32() { return          bigEndian32(          consume<uint32_t>());}

		bool eof() { return rptr == end; }

		const uint8_t *rptr, *end;
		Error failure_error;
	};


	ACTOR Future<Standalone<VectorRef<KeyValueRef>>> decodeRangeFileBlock(Reference<IAsyncFile> file, int64_t offset, int len) {
		state Standalone<StringRef> buf = makeString(len);
		int rLen = wait(file->read(mutateString(buf), len, offset));
		if(rLen != len)
			throw restore_bad_read();

		Standalone<VectorRef<KeyValueRef>> results({}, buf.arena());
		state StringRefReader reader(buf, restore_corrupted_data());

		try {
			// Read header, currently only decoding version 1001
			if(reader.consume<int32_t>() != 1001)
				throw restore_unsupported_file_version();

			// Read begin key, if this fails then block was invalid.
			uint32_t kLen = reader.consumeNetworkUInt32();
			const uint8_t *k = reader.consume(kLen);
			results.push_back(results.arena(), KeyValueRef(KeyRef(k, kLen), ValueRef()));

			// Read kv pairs and end key
			while(1) {
				// Read a key.
				kLen = reader.consumeNetworkUInt32();
				k = reader.consume(kLen);

				// If eof reached or first value len byte is 0xFF then a valid block end was reached.
				if(reader.eof() || *reader.rptr == 0xFF) {
					results.push_back(results.arena(), KeyValueRef(KeyRef(k, kLen), ValueRef()));
					break;
				}

				// Read a value, which must exist or the block is invalid
				uint32_t vLen = reader.consumeNetworkUInt32();
				const uint8_t *v = reader.consume(vLen);
				results.push_back(results.arena(), KeyValueRef(KeyRef(k, kLen), ValueRef(v, vLen)));

				// If eof reached or first byte of next key len is 0xFF then a valid block end was reached.
				if(reader.eof() || *reader.rptr == 0xFF)
					break;
			}

			// Make sure any remaining bytes in the block are 0xFF
			for(auto b : reader.remainder())
				if(b != 0xFF)
					throw restore_corrupted_data_padding();

			return results;

		} catch(Error &e) {
			TraceEvent(SevWarn, "FileRestoreCorruptRangeFileBlock")
				.error(e)
				.detail("Filename", file->getFilename())
				.detail("BlockOffset", offset)
				.detail("BlockLen", len)
				.detail("ErrorRelativeOffset", reader.rptr - buf.begin())
				.detail("ErrorAbsoluteOffset", reader.rptr - buf.begin() + offset);
			throw;
		}
	}


	ACTOR Future<Standalone<VectorRef<KeyValueRef>>> decodeLogFileBlock(Reference<IAsyncFile> file, int64_t offset, int len) {
		state Standalone<StringRef> buf = makeString(len);
		int rLen = wait(file->read(mutateString(buf), len, offset));
		if(rLen != len)
			throw restore_bad_read();

		Standalone<VectorRef<KeyValueRef>> results({}, buf.arena());
		state StringRefReader reader(buf, restore_corrupted_data());

		try {
			// Read header, currently only decoding version 2001
			if(reader.consume<int32_t>() != 2001)
				throw restore_unsupported_file_version();

			// Read k/v pairs.  Block ends either at end of last value exactly or with 0xFF as first key len byte.
			while(1) {
				// If eof reached or first key len bytes is 0xFF then end of block was reached.
				if(reader.eof() || *reader.rptr == 0xFF)
					break;

				// Read key and value.  If anything throws then there is a problem.
				uint32_t kLen = reader.consumeNetworkUInt32();
				const uint8_t *k = reader.consume(kLen);
				uint32_t vLen = reader.consumeNetworkUInt32();
				const uint8_t *v = reader.consume(vLen);

				results.push_back(results.arena(), KeyValueRef(KeyRef(k, kLen), ValueRef(v, vLen)));
			}

			// Make sure any remaining bytes in the block are 0xFF
			for(auto b : reader.remainder())
				if(b != 0xFF)
					throw restore_corrupted_data_padding();

			return results;

		} catch(Error &e) {
			TraceEvent(SevWarn, "FileRestoreCorruptLogFileBlock")
				.error(e)
				.detail("Filename", file->getFilename())
				.detail("BlockOffset", offset)
				.detail("BlockLen", len)
				.detail("ErrorRelativeOffset", reader.rptr - buf.begin())
				.detail("ErrorAbsoluteOffset", reader.rptr - buf.begin() + offset);
			throw;
		}
	}


}

void concatenateBackupMutation(Standalone<StringRef> val_input, Standalone<StringRef> key_input);
void registerBackupMutationForAll(Version empty);
bool isKVOpsSorted();
bool allOpsAreKnown();

////-- Restore code declaration END

static Future<Version> restoreMX(Database const &cx, RestoreRequest const &request);


ACTOR Future<Void> _restoreWorker(Database cx_input, LocalityData locality) {
	state Database cx = cx_input;
	state RestoreInterface interf;
	interf.initEndpoints();
	state Optional<RestoreInterface> leaderInterf;

	state Transaction tr(cx);
	loop {
		try {
			tr.reset();
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			Optional<Value> leader = wait(tr.get(restoreLeaderKey));
			if(leader.present()) {
				leaderInterf = BinaryReader::fromStringRef<RestoreInterface>(leader.get(), IncludeVersion());
				break;
			}
			tr.set(restoreLeaderKey, BinaryWriter::toValue(interf, IncludeVersion()));
			wait(tr.commit());
			break;
		} catch( Error &e ) {
			printf("restoreWorker select leader error\n");
			wait( tr.onError(e) );
		}
	}

	//we are not the leader, so put our interface in the agent list
	if(leaderInterf.present()) {
		loop {
			try {
				//tr.set(restoreWorkerKeyFor(interf.id()), BinaryWriter::toValue(interf, IncludeVersion()));
				tr.set(restoreWorkerKeyFor(interf.id()), restoreWorkerValue(interf));
				wait(tr.commit());
				break;
			} catch( Error &e ) {
				wait( tr.onError(e) );
			}
		}

		/*
		// Handle the dummy workload that increases a counter
		loop {
			choose {
				when(TestRequest req = waitNext(interf.test.getFuture())) {
					printf("Got Request: %d\n", req.testData);
					req.reply.send(TestReply(req.testData + 1));
					if (req.testData + 1 >= 10) {
						break;
					}
				}o
			}
		}
		 */

		// The workers' logic ends here. Should not proceed
		printf("Restore worker is about to exit now\n");
		return Void();
	}

	//we are the leader
	wait( delay(5.0) );

	state vector<RestoreInterface> agents;
	printf("MX: I'm the master\n");
	printf("Restore master waits for agents to register their workerKeys\n");
	loop {
		try {
			Standalone<RangeResultRef> agentValues = wait(tr.getRange(restoreWorkersKeys, CLIENT_KNOBS->TOO_MANY));
			ASSERT(!agentValues.more);
			if(agentValues.size()) {
				for(auto& it : agentValues) {
					agents.push_back(BinaryReader::fromStringRef<RestoreInterface>(it.value, IncludeVersion()));
				}
				break;
			}
			wait( delay(5.0) );
		} catch( Error &e ) {
			wait( tr.onError(e) );
		}
	}

	ASSERT(agents.size() > 0);

	/*
	// Handle the dummy workload that increases a counter
	state int testData = 0;
	loop {
		wait(delay(1.0));
		printf("Sending Request: %d\n", testData);
		std::vector<Future<TestReply>> replies;
		for(auto& it : agents) {
			replies.push_back( it.test.getReply(TestRequest(testData)) );
		}
		std::vector<TestReply> reps = wait( getAll(replies ));
		testData = reps[0].replyData;
		if ( testData >= 10 ) {
			break;
		}
	}
	 */

	

	printf("---MX: Perform the restore in the master now---\n");

	// ----------------Restore code START
	state int restoreId = 0;
	state int checkNum = 0;
	loop {
		state vector<RestoreRequest> restoreRequests;

		//watch for the restoreRequestTriggerKey
		state ReadYourWritesTransaction tr2(cx);

		loop {
			try {
				tr2.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr2.setOption(FDBTransactionOptions::LOCK_AWARE);
				state Future<Void> watch4RestoreRequest = tr2.watch(restoreRequestTriggerKey);
				wait(tr2.commit());
				printf("[INFO] set up watch for restoreRequestTriggerKey\n");
				wait(watch4RestoreRequest);
				printf("[INFO] restoreRequestTriggerKey watch is triggered\n");
				break;
			} catch(Error &e) {
				printf("[Error] Transaction for restore request. Error:%s\n", e.name());
				wait(tr2.onError(e));
			}
		};

		loop {
			try {
				tr2.reset();
				tr2.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr2.setOption(FDBTransactionOptions::LOCK_AWARE);

				state Optional<Value> numRequests = wait(tr2.get(restoreRequestTriggerKey));
				int num = decodeRestoreRequestTriggerValue(numRequests.get());
				//TraceEvent("RestoreRequestKey").detail("NumRequests", num);
				printf("[INFO] RestoreRequestNum:%d\n", num);


				// TODO: Create request request info. by using the same logic in the current restore
				state Standalone<RangeResultRef> restoreRequestValues = wait(tr2.getRange(restoreRequestKeys, CLIENT_KNOBS->TOO_MANY));
				printf("Restore worker get restoreRequest: %sn", restoreRequestValues.toString().c_str());

				ASSERT(!restoreRequestValues.more);

				if(restoreRequestValues.size()) {
					for ( auto &it : restoreRequestValues ) {
						printf("Now decode restore request value...\n");
						restoreRequests.push_back(decodeRestoreRequestValue(it.value));
					}
				}
				break;
			} catch(Error &e) {
				printf("[Error] Transaction for restore request. Error:%s\n", e.name());
				wait(tr2.onError(e));
			}
		};

		/*

		loop {
			state Transaction tr2(cx);
			tr2.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr2.setOption(FDBTransactionOptions::LOCK_AWARE);
			try {
				//TraceEvent("CheckRestoreRequestTrigger");
				printf("CheckRestoreRequestTrigger:%d\n", checkNum);
				checkNum++;

				state Optional<Value> numRequests = wait(tr2.get(restoreRequestTriggerKey));
				if ( !numRequests.present() ) { // restore has not been triggered yet
					TraceEvent("CheckRestoreRequestTrigger").detail("SecondsOfWait", 5);
					wait( delay(5.0) );
					continue;
				}
				int num = decodeRestoreRequestTriggerValue(numRequests.get());
				//TraceEvent("RestoreRequestKey").detail("NumRequests", num);
				printf("RestoreRequestNum:%d\n", num);

				// TODO: Create request request info. by using the same logic in the current restore
				state Standalone<RangeResultRef> restoreRequestValues = wait(tr2.getRange(restoreRequestKeys, CLIENT_KNOBS->TOO_MANY));
				printf("Restore worker get restoreRequest: %sn", restoreRequestValues.toString().c_str());

				ASSERT(!restoreRequestValues.more);

				if(restoreRequestValues.size()) {
					for ( auto &it : restoreRequestValues ) {
						printf("Now decode restore request value...\n");
						restoreRequests.push_back(decodeRestoreRequestValue(it.value));
					}
				}
				break;
			} catch( Error &e ) {
				TraceEvent("RestoreAgentLeaderErrorTr2").detail("ErrorCode", e.code()).detail("ErrorName", e.name());
				printf("RestoreAgentLeaderErrorTr2 Error code:%d name:%s\n", e.code(), e.name());
				wait( tr2.onError(e) );
			}
		}
		 */
		printf("---Print out the restore requests we received---\n");
		// Print out the requests info
		for ( auto &it : restoreRequests ) {
			printf("---RestoreRequest info:%s\n", it.toString().c_str());
		}

		// Perform the restore requests
		for ( auto &it : restoreRequests ) {
			TraceEvent("LeaderGotRestoreRequest").detail("RestoreRequestInfo", it.toString());
			Version ver = wait( restoreMX(cx, it) );
		}

		// Notify the finish of the restore by cleaning up the restore keys
		state Transaction tr3(cx);
		loop {
			tr3.reset();
			tr3.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr3.setOption(FDBTransactionOptions::LOCK_AWARE);
			try {
				tr3.clear(restoreRequestTriggerKey);
				tr3.clear(restoreRequestKeys);
				tr3.set(restoreRequestDoneKey, restoreRequestDoneValue(restoreRequests.size()));
				TraceEvent("LeaderFinishRestoreRequest");
				printf("LeaderFinishRestoreRequest\n");
				wait(tr3.commit());
				break;
			}  catch( Error &e ) {
				TraceEvent("RestoreAgentLeaderErrorTr3").detail("ErrorCode", e.code()).detail("ErrorName", e.name());
				wait( tr3.onError(e) );
			}
		};

		printf("MXRestoreEndHere RestoreID:%d\n", restoreId);
		TraceEvent("MXRestoreEndHere").detail("RestoreID", restoreId++);
		wait( delay(5.0) );
		//NOTE: we have to break the loop so that the tester.actor can receive the return of this test workload.
		//Otherwise, this special workload never returns and tester will think the test workload is stuck and the tester will timesout
		break; //TODO: this break will be removed later since we need the restore agent to run all the time!
	}

	return Void();
}

ACTOR Future<Void> restoreWorker(Reference<ClusterConnectionFile> ccf, LocalityData locality) {
	Database cx = Database::createDatabase(ccf->getFilename(), Database::API_VERSION_LATEST,locality);
	Future<Void> ret = _restoreWorker(cx, locality);
	return ret.get();
}

////--- Restore functions
ACTOR static Future<Void> _finishMX(Reference<ReadYourWritesTransaction> tr,  Reference<RestoreConfig> restore,  UID uid) {

 	//state RestoreConfig restore(task);
// 	state RestoreConfig restore(uid);
 //	restore.stateEnum().set(tr, ERestoreState::COMPLETED);
 	// Clear the file map now since it could be huge.
 //	restore.fileSet().clear(tr);

 	// TODO:  Validate that the range version map has exactly the restored ranges in it.  This means that for any restore operation
 	// the ranges to restore must be within the backed up ranges, otherwise from the restore perspective it will appear that some
 	// key ranges were missing and so the backup set is incomplete and the restore has failed.
 	// This validation cannot be done currently because Restore only supports a single restore range but backups can have many ranges.

 	// Clear the applyMutations stuff, including any unapplied mutations from versions beyond the restored version.
 //	restore.clearApplyMutationsKeys(tr);


 	try {
		printf("CheckDBlock:%s START\n", uid.toString().c_str());
		wait(checkDatabaseLock(tr, uid));
		printf("CheckDBlock:%s DONE\n", uid.toString().c_str());

 		printf("UnlockDB now. Start.\n");
 		wait(unlockDatabase(tr, uid)); //NOTE: unlockDatabase didn't commit inside the function!

 		printf("CheckDBlock:%s START\n", uid.toString().c_str());
 		wait(checkDatabaseLock(tr, uid));
 		printf("CheckDBlock:%s DONE\n", uid.toString().c_str());

 		printf("UnlockDB now. Commit.\n");
 		wait( tr->commit() );

 		printf("UnlockDB now. Done.\n");
 	} catch( Error &e ) {
 		printf("Error when we unlockDB. Error:%s\n", e.what());
 		wait(tr->onError(e));
 	}

 	return Void();
 }

 ACTOR Future<Void> applyKVOpsToDB(Database cx) {
 	state bool isPrint = false; //Debug message
 	state std::string typeStr = "";

 	if ( debug_verbose ) {
		TraceEvent("ApplyKVOPsToDB").detail("MapSize", kvOps.size());
		printf("ApplyKVOPsToDB num_of_version:%d\n", kvOps.size());
 	}
 	state std::map<Version, Standalone<VectorRef<MutationRef>>>::iterator it = kvOps.begin();
 	state int count = 0;
 	for ( ; it != kvOps.end(); ++it ) {

 		if ( debug_verbose ) {
			TraceEvent("ApplyKVOPsToDB\t").detail("Version", it->first).detail("OpNum", it->second.size());
 		}
 		printf("ApplyKVOPsToDB Version:%08lx num_of_ops:%d\n",  it->first, it->second.size());

 		state MutationRef m;
 		state int index = 0;
 		for ( ; index < it->second.size(); ++index ) {
 			m = it->second[index];
 			if (  m.type >= MutationRef::Type::SetValue && m.type <= MutationRef::Type::MAX_ATOMIC_OP )
 				typeStr = typeString[m.type];
 			else {
 				printf("ApplyKVOPsToDB MutationType:%d is out of range\n", m.type);
 			}

 			state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));

 			loop {
 				try {
 					tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
 					tr->setOption(FDBTransactionOptions::LOCK_AWARE);

 					if ( m.type == MutationRef::SetValue ) {
 						tr->set(m.param1, m.param2);
 					} else if ( m.type == MutationRef::ClearRange ) {
 						KeyRangeRef mutationRange(m.param1, m.param2);
 						tr->clear(mutationRange);
 					} else {
 						printf("[WARNING] mtype:%d (%s) unhandled\n", m.type, typeStr.c_str());
 					}

 					wait(tr->commit());
					++count;
 					break;
 				} catch(Error &e) {
 					printf("ApplyKVOPsToDB transaction error:%s. Type:%d, Param1:%s, Param2:%s\n", e.what(),
 							m.type, getHexString(m.param1).c_str(), getHexString(m.param2).c_str());
 					wait(tr->onError(e));
 				}
 			}

 			if ( isPrint ) {
 				printf("\tApplyKVOPsToDB Version:%016lx MType:%s K:%s, V:%s K_size:%d V_size:%d\n", it->first, typeStr.c_str(),
 					   getHexString(m.param1).c_str(), getHexString(m.param2).c_str(), m.param1.size(), m.param2.size());

 				TraceEvent("ApplyKVOPsToDB\t\t").detail("Version", it->first)
 						.detail("MType", m.type).detail("MTypeStr", typeStr)
 						.detail("MKey", getHexString(m.param1))
 						.detail("MValueSize", m.param2.size())
 						.detail("MValue", getHexString(m.param2));
 			}
 		}
 	}

 	printf("ApplyKVOPsToDB number of kv mutations:%d\n", count);

 	return Void();
}


//--- Extract backup range and log file and get the mutation list
ACTOR static Future<Void> _executeApplyRangeFileToDB(Database cx, Reference<RestoreConfig> restore_input,
 													 RestoreFile rangeFile_input, int64_t readOffset_input, int64_t readLen_input,
 													 Reference<IBackupContainer> bc, KeyRange restoreRange, Key addPrefix, Key removePrefix
 													 ) {
	state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx)); // Used to clear the range where the KV will be applied.

 	TraceEvent("ExecuteApplyRangeFileToDB_MX").detail("RestoreRange", restoreRange.contents().toString()).detail("AddPrefix", addPrefix.printable()).detail("RemovePrefix", removePrefix.printable());

 	state Reference<RestoreConfig> restore = restore_input;
 	state RestoreFile rangeFile = rangeFile_input;
 	state int64_t readOffset = readOffset_input;
 	state int64_t readLen = readLen_input;


 	TraceEvent("FileRestoreRangeStart_MX")
 			.suppressFor(60)
 			.detail("RestoreUID", restore->getUid())
 			.detail("FileName", rangeFile.fileName)
 			.detail("FileVersion", rangeFile.version)
 			.detail("FileSize", rangeFile.fileSize)
 			.detail("ReadOffset", readOffset)
 			.detail("ReadLen", readLen)
 			.detail("TaskInstance", (uint64_t)this);
 	//MX: the set of key value version is rangeFile.version. the key-value set in the same range file has the same version

 	TraceEvent("ReadFileStart").detail("Filename", rangeFile.fileName);
 	state Reference<IAsyncFile> inFile = wait(bc->readFile(rangeFile.fileName));
 	TraceEvent("ReadFileFinish").detail("Filename", rangeFile.fileName).detail("FileRefValid", inFile.isValid());


 	state Standalone<VectorRef<KeyValueRef>> blockData = wait(parallelFileRestore::decodeRangeFileBlock(inFile, readOffset, readLen));
 	TraceEvent("ExtractApplyRangeFileToDB_MX").detail("BlockDataVectorSize", blockData.contents().size())
 			.detail("RangeFirstKey", blockData.front().key.printable()).detail("RangeLastKey", blockData.back().key.printable());

 	// First and last key are the range for this file
 	state KeyRange fileRange = KeyRangeRef(blockData.front().key, blockData.back().key);
 	printf("[INFO] RangeFile:%s KeyRange:%s, restoreRange:%s\n",
 			rangeFile.fileName.c_str(), fileRange.toString().c_str(), restoreRange.toString().c_str());

 	// If fileRange doesn't intersect restore range then we're done.
 	if(!fileRange.intersects(restoreRange)) {
 		TraceEvent("ExtractApplyRangeFileToDB_MX").detail("NoIntersectRestoreRange", "FinishAndReturn");
 		return Void();
 	}

 	// We know the file range intersects the restore range but there could still be keys outside the restore range.
 	// Find the subvector of kv pairs that intersect the restore range.  Note that the first and last keys are just the range endpoints for this file
 	int rangeStart = 1;
 	int rangeEnd = blockData.size() - 1;
 	// Slide start forward, stop if something in range is found
	// Move rangeStart and rangeEnd until they is within restoreRange
 	while(rangeStart < rangeEnd && !restoreRange.contains(blockData[rangeStart].key))
 		++rangeStart;
 	// Side end backward, stop if something in range is found
 	while(rangeEnd > rangeStart && !restoreRange.contains(blockData[rangeEnd - 1].key))
 		--rangeEnd;

 	// MX: now data only contains the kv mutation within restoreRange
 	state VectorRef<KeyValueRef> data = blockData.slice(rangeStart, rangeEnd);
 	printf("[INFO] RangeFile:%s blockData entry size:%d recovered data size:%d\n", rangeFile.fileName.c_str(), blockData.size(), data.size());

 	// Shrink file range to be entirely within restoreRange and translate it to the new prefix
 	// First, use the untranslated file range to create the shrunk original file range which must be used in the kv range version map for applying mutations
 	state KeyRange originalFileRange = KeyRangeRef(std::max(fileRange.begin, restoreRange.begin), std::min(fileRange.end,   restoreRange.end));

 	// Now shrink and translate fileRange
 	Key fileEnd = std::min(fileRange.end,   restoreRange.end);
 	if(fileEnd == (removePrefix == StringRef() ? normalKeys.end : strinc(removePrefix)) ) {
 		fileEnd = addPrefix == StringRef() ? normalKeys.end : strinc(addPrefix);
 	} else {
 		fileEnd = fileEnd.removePrefix(removePrefix).withPrefix(addPrefix);
 	}
 	fileRange = KeyRangeRef(std::max(fileRange.begin, restoreRange.begin).removePrefix(removePrefix).withPrefix(addPrefix),fileEnd);

 	state int start = 0;
 	state int end = data.size();
 	state int dataSizeLimit = BUGGIFY ? g_random->randomInt(256 * 1024, 10e6) : CLIENT_KNOBS->RESTORE_WRITE_TX_SIZE;
 	state int kvCount = 0;

 	tr->reset();
 	//MX: This is where the key-value pair in range file is applied into DB
 	TraceEvent("ExtractApplyRangeFileToDB_MX").detail("Progress", "StartApplyKVToDB").detail("DataSize", data.size()).detail("DataSizeLimit", dataSizeLimit);
 	loop {
 		try {
 			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
 			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

 			state int i = start;
 			state int txBytes = 0;
 			state int iend = start;

 			// find iend that results in the desired transaction size
 			for(; iend < end && txBytes < dataSizeLimit; ++iend) {
 				txBytes += data[iend].key.expectedSize();
 				txBytes += data[iend].value.expectedSize();
 			}

 			// Clear the range we are about to set.
 			// If start == 0 then use fileBegin for the start of the range, else data[start]
 			// If iend == end then use fileEnd for the end of the range, else data[iend]
 			state KeyRange trRange = KeyRangeRef((start == 0 ) ? fileRange.begin : data[start].key.removePrefix(removePrefix).withPrefix(addPrefix)
 					, (iend == end) ? fileRange.end   : data[iend ].key.removePrefix(removePrefix).withPrefix(addPrefix));

 			// Clear the range before we set it.
 			tr->clear(trRange);

 			for(; i < iend; ++i) {
 //				tr->setOption(FDBTransactionOptions::NEXT_WRITE_NO_WRITE_CONFLICT_RANGE);
 //				tr->set(data[i].key.removePrefix(removePrefix).withPrefix(addPrefix), data[i].value);
 				//MXX: print out the key value version, and operations.
 //				printf("RangeFile [key:%s, value:%s, version:%ld, op:set]\n", data[i].key.printable().c_str(), data[i].value.printable().c_str(), rangeFile.version);
// 				TraceEvent("PrintRangeFile_MX").detail("Key", data[i].key.printable()).detail("Value", data[i].value.printable())
// 					.detail("Version", rangeFile.version).detail("Op", "set");
////				printf("PrintRangeFile_MX: mType:set param1:%s param2:%s param1_size:%d, param2_size:%d\n",
////						getHexString(data[i].key.c_str(), getHexString(data[i].value).c_str(), data[i].key.size(), data[i].value.size());

				//NOTE: Should NOT removePrefix and addPrefix for the backup data!
				// In other words, the following operation is wrong:  data[i].key.removePrefix(removePrefix).withPrefix(addPrefix)
 				MutationRef m(MutationRef::Type::SetValue, data[i].key, data[i].value); //ASSUME: all operation in range file is set.
				++kvCount;

 				// TODO: we can commit the kv operation into DB.
 				// Right now, we cache all kv operations into kvOps, and apply all kv operations later in one place
 				if ( kvOps.find(rangeFile.version) == kvOps.end() ) { // Create the map's key if mutation m is the first on to be inserted
 					//kvOps.insert(std::make_pair(rangeFile.version, Standalone<VectorRef<MutationRef>>(VectorRef<MutationRef>())));
 					kvOps.insert(std::make_pair(rangeFile.version, VectorRef<MutationRef>()));
 				}

 				ASSERT(kvOps.find(rangeFile.version) != kvOps.end());
				kvOps[rangeFile.version].push_back_deep(kvOps[rangeFile.version].arena(), m);

 			}

 			// Add to bytes written count
 //			restore.bytesWritten().atomicOp(tr, txBytes, MutationRef::Type::AddValue);
 //
 			state Future<Void> checkLock = checkDatabaseLock(tr, restore->getUid());

 			wait( checkLock );

 			wait(tr->commit());

 			TraceEvent("FileRestoreCommittedRange_MX")
 					.suppressFor(60)
 					.detail("RestoreUID", restore->getUid())
 					.detail("FileName", rangeFile.fileName)
 					.detail("FileVersion", rangeFile.version)
 					.detail("FileSize", rangeFile.fileSize)
 					.detail("ReadOffset", readOffset)
 					.detail("ReadLen", readLen)
 //					.detail("CommitVersion", tr->getCommittedVersion())
 					.detail("BeginRange", printable(trRange.begin))
 					.detail("EndRange", printable(trRange.end))
 					.detail("StartIndex", start)
 					.detail("EndIndex", i)
 					.detail("DataSize", data.size())
 					.detail("Bytes", txBytes)
 					.detail("OriginalFileRange", printable(originalFileRange));


 			TraceEvent("ExtraApplyRangeFileToDB_ENDMX").detail("KVOpsMapSizeMX", kvOps.size()).detail("MutationSize", kvOps[rangeFile.version].size());

 			// Commit succeeded, so advance starting point
 			start = i;

 			if(start == end) {
 				TraceEvent("ExtraApplyRangeFileToDB_MX").detail("Progress", "DoneApplyKVToDB");
 				printf("[INFO] RangeFile:%s: the number of kv operations = %d\n", rangeFile.fileName.c_str(), kvCount);
 				return Void();
 			}
 			tr->reset();
 		} catch(Error &e) {
 			if(e.code() == error_code_transaction_too_large)
 				dataSizeLimit /= 2;
 			else
 				wait(tr->onError(e));
 		}
 	}


 }

 ACTOR static Future<Void> _executeApplyMutationLogFileToDB(Database cx, Reference<RestoreConfig> restore_input,
 														   RestoreFile logFile_input, int64_t readOffset_input, int64_t readLen_input,
 														   Reference<IBackupContainer> bc, KeyRange restoreRange, Key addPrefix, Key removePrefix
 														   ) {
 	state Reference<RestoreConfig> restore = restore_input;

 	state RestoreFile logFile = logFile_input;
 	state int64_t readOffset = readOffset_input;
 	state int64_t readLen = readLen_input;

 	TraceEvent("FileRestoreLogStart_MX")
 			.suppressFor(60)
 			.detail("RestoreUID", restore->getUid())
 			.detail("FileName", logFile.fileName)
 			.detail("FileBeginVersion", logFile.version)
 			.detail("FileEndVersion", logFile.endVersion)
 			.detail("FileSize", logFile.fileSize)
 			.detail("ReadOffset", readOffset)
 			.detail("ReadLen", readLen)
 			.detail("TaskInstance", (uint64_t)this);

 	state Key mutationLogPrefix = restore->mutationLogPrefix();
 	TraceEvent("ReadLogFileStart").detail("LogFileName", logFile.fileName);
 	state Reference<IAsyncFile> inFile = wait(bc->readFile(logFile.fileName));
 	TraceEvent("ReadLogFileFinish").detail("LogFileName", logFile.fileName).detail("FileInfo", logFile.toString());


 	printf("Parse log file:%s\n", logFile.fileName.c_str());
 	state Standalone<VectorRef<KeyValueRef>> data = wait(parallelFileRestore::decodeLogFileBlock(inFile, readOffset, readLen));
 	//state Standalone<VectorRef<MutationRef>> data = wait(fileBackup::decodeLogFileBlock_MX(inFile, readOffset, readLen)); //Decode log file
 	TraceEvent("ReadLogFileFinish").detail("LogFileName", logFile.fileName).detail("DecodedDataSize", data.contents().size());
 	printf("ReadLogFile, raw data size:%d\n", data.size());

 	state int start = 0;
 	state int end = data.size();
 	state int dataSizeLimit = BUGGIFY ? g_random->randomInt(256 * 1024, 10e6) : CLIENT_KNOBS->RESTORE_WRITE_TX_SIZE;
	state int kvCount = 0;


 //	tr->reset();
 	loop {
 //		try {
 			printf("Process start:%d where end=%d\n", start, end);
 			if(start == end)
 				return Void();

 //			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
 //			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

 			state int i = start;
 			state int txBytes = 0;
 			for(; i < end && txBytes < dataSizeLimit; ++i) {
 				Key k = data[i].key.withPrefix(mutationLogPrefix);
 				ValueRef v = data[i].value;
 //				tr->set(k, v);
 				txBytes += k.expectedSize();
 				txBytes += v.expectedSize();
 				//MXX: print out the key value version, and operations.
 				//printf("LogFile [key:%s, value:%s, version:%ld, op:NoOp]\n", k.printable().c_str(), v.printable().c_str(), logFile.version);
 //				printf("LogFile [KEY:%s, VALUE:%s, VERSION:%ld, op:NoOp]\n", getHexString(k).c_str(), getHexString(v).c_str(), logFile.version);
 //				printBackupMutationRefValueHex(v, " |\t");
 /*
 				TraceEvent("PrintMutationLogFile_MX").detail("Key",  getHexString(k)).detail("Value", getHexString(v))
 						.detail("Version", logFile.version).detail("Op", "NoOps");

 				printf("||Register backup mutation:file:%s, data:%d\n", logFile.fileName.c_str(), i);
 				registerBackupMutation(data[i].value, logFile.version);
 */
 //				printf("[DEBUG]||Concatenate backup mutation:fileInfo:%s, data:%d\n", logFile.toString().c_str(), i);
 				concatenateBackupMutation(data[i].value, data[i].key);
 //				//TODO: Decode the value to get the mutation type. Use NoOp to distinguish from range kv for now.
 //				MutationRef m(MutationRef::Type::NoOp, data[i].key, data[i].value); //ASSUME: all operation in log file is NoOp.
 //				if ( kvOps.find(logFile.version) == kvOps.end() ) {
 //					kvOps.insert(std::make_pair(logFile.version, std::vector<MutationRef>()));
 //				} else {
 //					kvOps[logFile.version].push_back(m);
 //				}
 			}

 //			state Future<Void> checkLock = checkDatabaseLock(tr, restore.getUid());

 //			wait( checkLock );

 			// Add to bytes written count
 //			restore.bytesWritten().atomicOp(tr, txBytes, MutationRef::Type::AddValue);

 //			wait(tr->commit());

 			TraceEvent("FileRestoreCommittedLog")
 					.suppressFor(60)
 					.detail("RestoreUID", restore->getUid())
 					.detail("FileName", logFile.fileName)
 					.detail("FileBeginVersion", logFile.version)
 					.detail("FileEndVersion", logFile.endVersion)
 					.detail("FileSize", logFile.fileSize)
 					.detail("ReadOffset", readOffset)
 					.detail("ReadLen", readLen)
 //					.detail("CommitVersion", tr->getCommittedVersion())
 					.detail("StartIndex", start)
 					.detail("EndIndex", i)
 					.detail("DataSize", data.size())
 					.detail("Bytes", txBytes);
 //					.detail("TaskInstance", (uint64_t)this);

 			TraceEvent("ExtractApplyLogFileToDBEnd_MX").detail("KVOpsMapSizeMX", kvOps.size()).detail("MutationSize", kvOps[logFile.version].size());

 			// Commit succeeded, so advance starting point
 			start = i;
 //			tr->reset();
 //		} catch(Error &e) {
 //			if(e.code() == error_code_transaction_too_large)
 //				dataSizeLimit /= 2;
 //			else
 //				wait(tr->onError(e));
 //		}
 	}

 }


ACTOR static Future<Void> prepareRestore(Database cx, Reference<ReadYourWritesTransaction> tr, Key tagName, Key backupURL,
		Version restoreVersion, Key addPrefix, Key removePrefix, KeyRange restoreRange, bool lockDB, UID uid,
		Reference<RestoreConfig> restore_input) {
 	ASSERT(restoreRange.contains(removePrefix) || removePrefix.size() == 0);

 	printf("prepareRestore: the current db lock status is as below\n");
	wait(checkDatabaseLock(tr, uid));

 	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
 	tr->setOption(FDBTransactionOptions::LOCK_AWARE);

 	printf("MX:Prepare restore for the tag:%s\n", tagName.toString().c_str());
 	// Get old restore config for this tag
 	state KeyBackedTag tag = makeRestoreTag(tagName.toString());
 	state Optional<UidAndAbortedFlagT> oldUidAndAborted = wait(tag.get(tr));
 	TraceEvent("PrepareRestoreMX").detail("OldUidAndAbortedPresent", oldUidAndAborted.present());
 	if(oldUidAndAborted.present()) {
 		if (oldUidAndAborted.get().first == uid) {
 			if (oldUidAndAborted.get().second) {
 				throw restore_duplicate_uid();
 			}
 			else {
 				return Void();
 			}
 		}

 		state Reference<RestoreConfig> oldRestore = Reference<RestoreConfig>(new RestoreConfig(oldUidAndAborted.get().first));

 		// Make sure old restore for this tag is not runnable
 		bool runnable = wait(oldRestore->isRunnable(tr));

 		if (runnable) {
 			throw restore_duplicate_tag();
 		}

 		// Clear the old restore config
 		oldRestore->clear(tr);
 	}

 	KeyRange restoreIntoRange = KeyRangeRef(restoreRange.begin, restoreRange.end).removePrefix(removePrefix).withPrefix(addPrefix);
 	Standalone<RangeResultRef> existingRows = wait(tr->getRange(restoreIntoRange, 1));
 	if (existingRows.size() > 0) {
 		throw restore_destination_not_empty();
 	}

 	// Make new restore config
 	state Reference<RestoreConfig> restore = Reference<RestoreConfig>(new RestoreConfig(uid));

 	// Point the tag to the new uid
	printf("MX:Point the tag:%s to the new uid:%s\n", tagName.toString().c_str(), uid.toString().c_str());
 	tag.set(tr, {uid, false});

 	Reference<IBackupContainer> bc = IBackupContainer::openContainer(backupURL.toString());

 	// Configure the new restore
 	restore->tag().set(tr, tagName.toString());
 	restore->sourceContainer().set(tr, bc);
 	restore->stateEnum().set(tr, ERestoreState::QUEUED);
 	restore->restoreVersion().set(tr, restoreVersion);
 	restore->restoreRange().set(tr, restoreRange);
 	// this also sets restore.add/removePrefix.
 	restore->initApplyMutations(tr, addPrefix, removePrefix);
	printf("MX:Configure new restore config to :%s\n", restore->toString().c_str());
	restore_input = restore;
	printf("MX:Assign the global restoreConfig to :%s\n", restore_input->toString().c_str());

 	TraceEvent("PrepareRestoreMX").detail("RestoreConfigConstruct", "Done");

	printf("MX: lockDB:%d before we finish prepareRestore()\n", lockDB);
 	if (lockDB)
 		wait(lockDatabase(tr, uid));
 	else
 		wait(checkDatabaseLock(tr, uid));


 	return Void();
 }

 // ACTOR static Future<Void> _executeMX(Database cx,  Reference<Task> task, UID uid, RestoreRequest request) is rename to this function
 ACTOR static Future<Void> extractBackupData(Database cx, Reference<RestoreConfig> restore_input, UID uid, RestoreRequest request) {
 	state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
 	state Reference<RestoreConfig> restore = restore_input;
 	state Version restoreVersion;
 	state Reference<IBackupContainer> bc;
 	state Key addPrefix = request.addPrefix;
 	state Key removePrefix = request.removePrefix;
 	state KeyRange restoreRange = request.range;

 	TraceEvent("ExecuteMX");

 	loop {
 		try {
 			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
 			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

 			//wait(checkTaskVersion(tr->getDatabase(), task, name, version));
 			Version _restoreVersion = wait(restore->restoreVersion().getOrThrow(tr)); //Failed
 			restoreVersion = _restoreVersion;
 			TraceEvent("ExecuteMX").detail("RestoreVersion", restoreVersion);

 			ERestoreState oldState = wait(restore->stateEnum().getD(tr));
 			TraceEvent("ExecuteMX").detail("OldState", oldState);
 			printf("Restore state:%d\n", oldState);
 			if(oldState != ERestoreState::QUEUED && oldState != ERestoreState::STARTING) {
 				wait(restore->logError(cx, restore_error(), format("StartFullRestore: Encountered unexpected state(%d)", oldState), this));
 				TraceEvent("StartFullRestoreMX").detail("Error", "Encounter unexpected state");
 				return Void();
 			}
 			restore->stateEnum().set(tr, ERestoreState::STARTING);
 			TraceEvent("ExecuteMX").detail("StateEnum", "Done");
 			restore->fileSet().clear(tr);
 			restore->fileBlockCount().clear(tr);
 			restore->fileCount().clear(tr);
 			TraceEvent("ExecuteMX").detail("Clear", "Done");
 			Reference<IBackupContainer> _bc = wait(restore->sourceContainer().getOrThrow(tr));
 			TraceEvent("ExecuteMX").detail("BackupContainer", "Done");
 			bc = _bc;

 			wait(tr->commit());
 			break;
 		} catch(Error &e) {
 			TraceEvent("ExecuteMXErrorTr").detail("ErrorName", e.name());
 			wait(tr->onError(e));
 			TraceEvent("ExecuteMXErrorTrDone");
 		}
 	}

 	TraceEvent("ExecuteMX").detail("GetRestoreSet", restoreVersion);

 	//MX: Get restore file set from BackupContainer
 	Optional<RestorableFileSet> restorable = wait(bc->getRestoreSet(restoreVersion));
 	printf("MX:ExtraRestoreData,restoreFileset, present:%d\n", restorable.present());

 	TraceEvent("ExecuteMX").detail("Restorable", restorable.present());

 	if(!restorable.present())
 		throw restore_missing_data();

 	// First version for which log data should be applied
 	//	Params.firstVersion().set(task, restorable.get().snapshot.beginVersion);

 	// Convert the two lists in restorable (logs and ranges) to a single list of RestoreFiles.
 	// Order does not matter, they will be put in order when written to the restoreFileMap below.
 	state std::vector<RestoreConfig::RestoreFile> files;

 	for(const RangeFile &f : restorable.get().ranges) {
// 		TraceEvent("FoundRangeFileMX").detail("FileInfo", f.toString());
 		printf("FoundRangeFileMX, fileInfo:%s\n", f.toString().c_str());
 		files.push_back({f.version, f.fileName, true, f.blockSize, f.fileSize});
 	}
 	for(const LogFile &f : restorable.get().logs) {
// 		TraceEvent("FoundLogFileMX").detail("FileInfo", f.toString());
		printf("FoundLogFileMX, fileInfo:%s\n", f.toString().c_str());
 		files.push_back({f.beginVersion, f.fileName, false, f.blockSize, f.fileSize, f.endVersion});
 	}

 	state std::vector<RestoreConfig::RestoreFile>::iterator start = files.begin();
 	state std::vector<RestoreConfig::RestoreFile>::iterator end = files.end();

 	tr->reset();
 	while(start != end) {
 		try {
 			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
 			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

 			state std::vector<RestoreConfig::RestoreFile>::iterator i = start;

 			state int txBytes = 0;
 			state int nFileBlocks = 0;
 			state int nFiles = 0;
 			auto fileSet = restore->fileSet();
 			for(; i != end && txBytes < 1e6; ++i) {
 				txBytes += fileSet.insert(tr, *i);
 				nFileBlocks += (i->fileSize + i->blockSize - 1) / i->blockSize;
 				++nFiles;
 			}

 			// Record the restore progress into system space
			restore->fileCount().atomicOp(tr, nFiles, MutationRef::Type::AddValue);
			restore->fileBlockCount().atomicOp(tr, nFileBlocks, MutationRef::Type::AddValue);

 			wait(tr->commit());

 			TraceEvent("FileRestoreLoadedFilesMX")
 					.detail("RestoreUID", restore->getUid())
 					.detail("FileCount", nFiles)
 					.detail("FileBlockCount", nFileBlocks)
 					.detail("TransactionBytes", txBytes)
 					.detail("TaskInstance", (uint64_t)this);

 			start = i;
 			tr->reset();
 		} catch(Error &e) {
 			wait(tr->onError(e));
 		}
 	}

 	//Apply range and log files to DB
 	TraceEvent("ApplyBackupFileToDB").detail("FileSize", files.size());
 	printf("ApplyBackupFileToDB, FileSize:%d\n", files.size());
 	state int64_t beginBlock = 0;
 	state int64_t j = 0;
 	state int64_t readLen = 0;
 	state int64_t readOffset = 0;
 	state RestoreConfig::RestoreFile f;
 	state int fi = 0;
 	//Get the mutation log into the kvOps first
 	printf("Extra mutation logs...\n");
 	state std::vector<Future<Void>> futures;
 	for ( fi = 0; fi < files.size(); ++fi ) {
 		f = files[fi];
 		if ( !f.isRange ) {
 			TraceEvent("ExtractLogFileToDB_MX").detail("FileInfo", f.toString());
 			printf("ExtractMutationLogs: id:%d fileInfo:%s\n", fi, f.toString().c_str());
 			beginBlock = 0;
 			j = beginBlock *f.blockSize;
 			readLen = 0;
 			// For each block of the file
 			for(; j < f.fileSize; j += f.blockSize) {
 				readOffset = j;
 				readLen = std::min<int64_t>(f.blockSize, f.fileSize - j);
 				printf("ExtractMutationLogs: id:%d fileInfo:%s, readOffset:%d\n", fi, f.toString().c_str(), readOffset);

 				//futures.push_back(_executeApplyMutationLogFileToDB(cx, task, f, readOffset, readLen, bc, restoreRange, addPrefix, removePrefix));
 				wait( _executeApplyMutationLogFileToDB(cx, restore, f, readOffset, readLen, bc, restoreRange, addPrefix, removePrefix) );

 				// Increment beginBlock for the file
 				++beginBlock;
 				TraceEvent("ApplyLogFileToDB_MX_Offset").detail("FileInfo", f.toString()).detail("ReadOffset", readOffset).detail("ReadLen", readLen);
 			}
 		}
 	}
 	printf("Wait for  futures of concatenate mutation logs, start waiting\n");
 //	wait(waitForAll(futures));
 	printf("Wait for  futures of concatenate mutation logs, finish waiting\n");

 	printf("Now parse concatenated mutation log and register it to kvOps, mutationMap size:%d start...\n", mutationMap.size());
 	registerBackupMutationForAll(Version());
 	printf("Now parse concatenated mutation log and register it to kvOps, mutationMap size:%d done...\n", mutationMap.size());

 	//Get the range file into the kvOps later
 	printf("ApplyRangeFiles\n");
 	futures.clear();
 	for ( fi = 0; fi < files.size(); ++fi ) {
 		f = files[fi];
 		printf("ApplyRangeFiles:id:%d\n", fi);
 		if ( f.isRange ) {
 //			TraceEvent("ApplyRangeFileToDB_MX").detail("FileInfo", f.toString());
 			printf("ApplyRangeFileToDB_MX FileInfo:%s\n", f.toString().c_str());
 			beginBlock = 0;
 			j = beginBlock *f.blockSize;
 			readLen = 0;
 			// For each block of the file
 			for(; j < f.fileSize; j += f.blockSize) {
 				readOffset = j;
 				readLen = std::min<int64_t>(f.blockSize, f.fileSize - j);
 				futures.push_back( _executeApplyRangeFileToDB(cx, restore, f, readOffset, readLen, bc, restoreRange, addPrefix, removePrefix) );

 				// Increment beginBlock for the file
 				++beginBlock;
// 				TraceEvent("ApplyRangeFileToDB_MX").detail("FileInfo", f.toString()).detail("ReadOffset", readOffset).detail("ReadLen", readLen);
 			}
 		}
 	}
 	if ( futures.size() != 0 ) {
 		printf("Wait for  futures of applyRangeFiles, start waiting\n");
 		wait(waitForAll(futures));
 		printf("Wait for  futures of applyRangeFiles, finish waiting\n");
 	}

 //	printf("Now print KVOps\n");
 //	printKVOps();

 //	printf("Now sort KVOps in increasing order of commit version\n");
 //	sort(kvOps.begin(), kvOps.end()); //sort in increasing order of key using default less_than comparator
 	if ( isKVOpsSorted() ) {
 		printf("[CORRECT] KVOps is sorted by version\n");
 	} else {
 		printf("[ERROR]!!! KVOps is NOT sorted by version\n");
 //		assert( 0 );
 	}

 	if ( allOpsAreKnown() ) {
 		printf("[CORRECT] KVOps all operations are known.\n");
 	} else {
 		printf("[ERROR]!!! KVOps has unknown mutation op. Exit...\n");
 //		assert( 0 );
 	}

 	printf("Now apply KVOps to DB. start...\n");
 	printf("DB lock status:%d\n");
 	tr->reset();
 	wait(checkDatabaseLock(tr, uid));
	wait(tr->commit());

	//Apply the kv operations to DB
 	wait( applyKVOpsToDB(cx) );
 	printf("Now apply KVOps to DB, Done\n");
 //	filterAndSortMutationOps();




 	return Void();
 }

ACTOR static Future<Version> restoreMX(Database cx, RestoreRequest request) {
	state Key tagName = request.tagName;
	state Key url = request.url;
	state bool waitForComplete = request.waitForComplete;
	state Version targetVersion = request.targetVersion;
	state bool verbose = request.verbose;
	state KeyRange range = request.range;
	state Key addPrefix = request.addPrefix;
	state Key removePrefix = request.removePrefix;
	state bool lockDB = request.lockDB;
	state UID randomUid = request.randomUid;

	//MX: Lock DB if it is not locked
	printf("[INFO] RestoreRequest lockDB:%d\n", lockDB);
	if ( lockDB == false ) {
		printf("[INFO] RestoreRequest lockDB:%d; we will forcely lock db\n", lockDB);
		lockDB = true;
	}


	state Reference<IBackupContainer> bc = IBackupContainer::openContainer(url.toString());
	state BackupDescription desc = wait(bc->describeBackup());

	wait(desc.resolveVersionTimes(cx));

	printf("Backup Description\n%s", desc.toString().c_str());
	printf("MX: Restore for url:%s, lockDB:%d\n", url.toString().c_str(), lockDB);
	if(targetVersion == invalidVersion && desc.maxRestorableVersion.present())
		targetVersion = desc.maxRestorableVersion.get();

	Optional<RestorableFileSet> restoreSet = wait(bc->getRestoreSet(targetVersion));

	//Above is the restore master code
	//Below is the agent code
	TraceEvent("RestoreMX").detail("StartRestoreForRequest", request.toString());
	printf("RestoreMX: start restore for request: %s\n", request.toString().c_str());

	if(!restoreSet.present()) {
		TraceEvent(SevWarn, "FileBackupAgentRestoreNotPossible")
				.detail("BackupContainer", bc->getURL())
				.detail("TargetVersion", targetVersion);
		fprintf(stderr, "ERROR: Restore version %lld is not possible from %s\n", targetVersion, bc->getURL().c_str());
		throw restore_invalid_version();
	} else {
		printf("---To restore from the following files: num_logs_file:%d num_range_files:%d---\n",
				restoreSet.get().logs.size(), restoreSet.get().ranges.size());
		for (int i = 0; i < restoreSet.get().logs.size(); ++i) {
			printf("log file:%s\n", restoreSet.get().logs[i].toString().c_str());
		}
		for (int i = 0; i < restoreSet.get().ranges.size(); ++i) {
			printf("range file:%s\n", restoreSet.get().ranges[i].toString().c_str());
		}

	}

	if (verbose) {
		printf("Restoring backup to version: %lld\n", (long long) targetVersion);
		TraceEvent("RestoreBackupMX").detail("TargetVersion", (long long) targetVersion);
	}



	state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
	state Reference<RestoreConfig> restoreConfig(new RestoreConfig(randomUid));
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			wait(prepareRestore(cx, tr, tagName, url, targetVersion, addPrefix, removePrefix, range, lockDB, randomUid, restoreConfig));
			printf("MX:After prepareRestore() restoreConfig becomes :%s\n", restoreConfig->toString().c_str());
			printf("MX: TargetVersion:%ld (0x%lx)\n", targetVersion, targetVersion);

			TraceEvent("SetApplyEndVersion_MX").detail("TargetVersion", targetVersion);
			restoreConfig->setApplyEndVersion(tr, targetVersion); //MX: TODO: This may need to be set at correct position and may be set multiple times?

			wait(tr->commit());
			// MX: Now execute the restore: Step 1 get the restore files (range and mutation log) name
			// At the end of extractBackupData, we apply the mutation to DB
			wait( extractBackupData(cx, restoreConfig, randomUid, request) );
			printf("Finish my restore now!\n");

			//Unlock DB
			TraceEvent("RestoreMX").detail("UnlockDB", "Start");
			//state RestoreConfig restore(task);

			// MX: Unlock DB after restore
			state Reference<ReadYourWritesTransaction> tr_unlockDB(new ReadYourWritesTransaction(cx));
			printf("Finish restore cleanup. Start\n");
			wait( _finishMX(tr_unlockDB, restoreConfig, randomUid) );
			printf("Finish restore cleanup. Done\n");

			TraceEvent("RestoreMX").detail("UnlockDB", "Done");



			break;
		} catch(Error &e) {
			if(e.code() != error_code_restore_duplicate_tag) {
				wait(tr->onError(e));
			}
		}
	}



	//TODO: _finish() task: Make sure the restore is finished.

	//TODO: Uncomment the following code later

	return targetVersion;
}

struct cmpForKVOps {
	bool operator()(const Version& a, const Version& b) const {
		return a < b;
	}
};


// Helper class for reading restore data from a buffer and throwing the right errors.
struct StringRefReaderMX {
	StringRefReaderMX(StringRef s = StringRef(), Error e = Error()) : rptr(s.begin()), end(s.end()), failure_error(e) {}

	// Return remainder of data as a StringRef
	StringRef remainder() {
		return StringRef(rptr, end - rptr);
	}

	// Return a pointer to len bytes at the current read position and advance read pos
	//Consume a little-Endian data. Since we only run on little-Endian machine, the data on storage is little Endian
	const uint8_t * consume(unsigned int len) {
		if(rptr == end && len != 0)
			throw end_of_stream();
		const uint8_t *p = rptr;
		rptr += len;
		if(rptr > end)
			throw failure_error;
		return p;
	}

	// Return a T from the current read position and advance read pos
	template<typename T> const T consume() {
		return *(const T *)consume(sizeof(T));
	}

	// Functions for consuming big endian (network byte order) integers.
	// Consumes a big endian number, swaps it to little endian, and returns it.
	const int32_t  consumeNetworkInt32()  { return (int32_t)bigEndian32((uint32_t)consume< int32_t>());}
	const uint32_t consumeNetworkUInt32() { return          bigEndian32(          consume<uint32_t>());}

	const int64_t  consumeNetworkInt64()  { return (int64_t)bigEndian64((uint32_t)consume< int64_t>());}
	const uint64_t consumeNetworkUInt64() { return          bigEndian64(          consume<uint64_t>());}

	bool eof() { return rptr == end; }

	const uint8_t *rptr, *end;
	Error failure_error;
};

//-------Helper functions
std::string getHexString(StringRef input) {
	std::stringstream ss;
	for (int i = 0; i<input.size(); i++) {
		if ( i % 4 == 0 )
			ss << " ";
		if ( i == 12 ) { //The end of 12bytes, which is the version size for value
			ss << "|";
		}
		if ( i == (12 + 12) ) { //The end of version + header
			ss << "@";
		}
		ss << std::setfill('0') << std::setw(2) << std::hex << (int) input[i]; // [] operator moves the pointer in step of unit8
	}
	return ss.str();
}

std::string getHexKey(StringRef input, int skip) {
	std::stringstream ss;
	for (int i = 0; i<skip; i++) {
		if ( i % 4 == 0 )
			ss << " ";
		ss << std::setfill('0') << std::setw(2) << std::hex << (int) input[i]; // [] operator moves the pointer in step of unit8
	}
	ss << "||";

	//hashvalue
	ss << std::setfill('0') << std::setw(2) << std::hex << (int) input[skip]; // [] operator moves the pointer in step of unit8
	ss << "|";

	// commitversion in 64bit
	int count = 0;
	for (int i = skip+1; i<input.size() && i < skip+1+8; i++) {
		if ( count++ % 4 == 0 )
			ss << " ";
		ss << std::setfill('0') << std::setw(2) << std::hex << (int) input[i]; // [] operator moves the pointer in step of unit8
	}
	// part value
	count = 0;
	for (int i = skip+1+8; i<input.size(); i++) {
		if ( count++ % 4 == 0 )
			ss << " ";
		ss << std::setfill('0') << std::setw(2) << std::hex << (int) input[i]; // [] operator moves the pointer in step of unit8
	}
	return ss.str();
}


void printMutationListRefHex(MutationListRef m, std::string prefix) {
	MutationListRef::Iterator iter = m.begin();
	for ( ;iter != m.end(); ++iter) {
		printf("%s mType:%04x param1:%s param2:%s param1_size:%d, param2_size:%d\n", prefix.c_str(), iter->type,
			   getHexString(iter->param1).c_str(), getHexString(iter->param2).c_str(), iter->param1.size(), iter->param2.size());
	}
}

//TODO: Print out the backup mutation log value. The backup log value (i.e., the value in the kv pair) has the following format
//version(12B)|mutationRef|MutationRef|....
//A mutationRef has the format: |type_4B|param1_size_4B|param2_size_4B|param1|param2.
//Note: The data is stored in little endian! You need to convert it to BigEndian so that you know how long the param1 and param2 is and how to format them!
void printBackupMutationRefValueHex(Standalone<StringRef> val_input, std::string prefix) {
	std::stringstream ss;
	const int version_size = 12;
	const int header_size = 12;
	StringRef val = val_input.contents();
	StringRefReaderMX reader(val, restore_corrupted_data());

	int count_size = 0;
	// Get the version
	uint64_t version = reader.consume<uint64_t>();
	count_size += 8;
	uint32_t val_length_decode = reader.consume<uint32_t>();
	count_size += 4;

	printf("----------------------------------------------------------\n");
	printf("To decode value:%s\n", getHexString(val).c_str());
	if ( val_length_decode != (val.size() - 12) ) {
		fprintf(stderr, "%s[PARSE ERROR]!!! val_length_decode:%d != val.size:%d\n", prefix.c_str(), val_length_decode, val.size());
	} else {
		if ( debug_verbose ) {
			printf("%s[PARSE SUCCESS] val_length_decode:%d == (val.size:%d - 12)\n", prefix.c_str(), val_length_decode, val.size());
		}
	}

	// Get the mutation header
	while (1) {
		// stop when reach the end of the string
		if(reader.eof() ) { //|| *reader.rptr == 0xFFCheckRestoreRequestDoneErrorMX
			//printf("Finish decode the value\n");
			break;
		}


		uint32_t type = reader.consume<uint32_t>();//reader.consumeNetworkUInt32();
		uint32_t kLen = reader.consume<uint32_t>();//reader.consumeNetworkUInt32();
		uint32_t vLen = reader.consume<uint32_t>();//reader.consumeNetworkUInt32();
		const uint8_t *k = reader.consume(kLen);
		const uint8_t *v = reader.consume(vLen);
		count_size += 4 * 3 + kLen + vLen;

		if ( kLen < 0 || kLen > val.size() || vLen < 0 || vLen > val.size() ) {
			fprintf(stderr, "%s[PARSE ERROR]!!!! kLen:%d(0x%04x) vLen:%d(0x%04x)\n", prefix.c_str(), kLen, kLen, vLen, vLen);
		}

		if ( debug_verbose ) {
			printf("%s---DedodeBackupMutation: Type:%d K:%s V:%s k_size:%d v_size:%d\n", prefix.c_str(),
				   type,  getHexString(KeyRef(k, kLen)).c_str(), getHexString(KeyRef(v, vLen)).c_str(), kLen, vLen);
		}

	}
	if ( debug_verbose ) {
		printf("----------------------------------------------------------\n");
	}
}

void printBackupLogKeyHex(Standalone<StringRef> key_input, std::string prefix) {
	std::stringstream ss;
	const int version_size = 12;
	const int header_size = 12;
	StringRef val = key_input.contents();
	StringRefReaderMX reader(val, restore_corrupted_data());

	int count_size = 0;
	// Get the version
	uint64_t version = reader.consume<uint64_t>();
	count_size += 8;
	uint32_t val_length_decode = reader.consume<uint32_t>();
	count_size += 4;

	printf("----------------------------------------------------------\n");
	printf("To decode value:%s\n", getHexString(val).c_str());
	if ( val_length_decode != (val.size() - 12) ) {
		fprintf(stderr, "%s[PARSE ERROR]!!! val_length_decode:%d != val.size:%d\n", prefix.c_str(), val_length_decode, val.size());
	} else {
		printf("%s[PARSE SUCCESS] val_length_decode:%d == (val.size:%d - 12)\n", prefix.c_str(), val_length_decode, val.size());
	}

	// Get the mutation header
	while (1) {
		// stop when reach the end of the string
		if(reader.eof() ) { //|| *reader.rptr == 0xFF
			//printf("Finish decode the value\n");
			break;
		}


		uint32_t type = reader.consume<uint32_t>();//reader.consumeNetworkUInt32();
		uint32_t kLen = reader.consume<uint32_t>();//reader.consumeNetworkUInt32();
		uint32_t vLen = reader.consume<uint32_t>();//reader.consumeNetworkUInt32();
		const uint8_t *k = reader.consume(kLen);
		const uint8_t *v = reader.consume(vLen);
		count_size += 4 * 3 + kLen + vLen;

		if ( kLen < 0 || kLen > val.size() || vLen < 0 || vLen > val.size() ) {
			printf("%s[PARSE ERROR]!!!! kLen:%d(0x%04x) vLen:%d(0x%04x)\n", prefix.c_str(), kLen, kLen, vLen, vLen);
		}

		printf("%s---DedoceBackupMutation: Type:%d K:%s V:%s k_size:%d v_size:%d\n", prefix.c_str(),
			   type,  getHexString(KeyRef(k, kLen)).c_str(), getHexString(KeyRef(v, vLen)).c_str(), kLen, vLen);

	}
	printf("----------------------------------------------------------\n");
}

void printKVOps() {
	std::string typeStr = "MSet";
	TraceEvent("PrintKVOPs").detail("MapSize", kvOps.size());
	printf("PrintKVOPs num_of_version:%d\n", kvOps.size());
	for ( auto it = kvOps.begin(); it != kvOps.end(); ++it ) {
		TraceEvent("PrintKVOPs\t").detail("Version", it->first).detail("OpNum", it->second.size());
		printf("PrintKVOPs Version:%08lx num_of_ops:%d\n",  it->first, it->second.size());
		for ( auto m = it->second.begin(); m != it->second.end(); ++m ) {
			if (  m->type >= MutationRef::Type::SetValue && m->type <= MutationRef::Type::MAX_ATOMIC_OP )
				typeStr = typeString[m->type];
			else {
				printf("PrintKVOPs MutationType:%d is out of range\n", m->type);
			}

			printf("\tPrintKVOPs Version:%016lx MType:%s K:%s, V:%s K_size:%d V_size:%d\n", it->first, typeStr.c_str(),
				   getHexString(m->param1).c_str(), getHexString(m->param2).c_str(), m->param1.size(), m->param2.size());

			TraceEvent("PrintKVOPs\t\t").detail("Version", it->first)
					.detail("MType", m->type).detail("MTypeStr", typeStr)
					.detail("MKey", getHexString(m->param1))
					.detail("MValueSize", m->param2.size())
					.detail("MValue", getHexString(m->param2));
		}
	}
}

// Sanity check if KVOps is sorted
bool isKVOpsSorted() {
	bool ret = true;
	auto prev = kvOps.begin();
	for ( auto it = kvOps.begin(); it != kvOps.end(); ++it ) {
		if ( prev->first > it->first ) {
			ret = false;
			break;
		}
		prev = it;
	}
	return ret;
}

bool allOpsAreKnown() {
	bool ret = true;
	for ( auto it = kvOps.begin(); it != kvOps.end(); ++it ) {
		for ( auto m = it->second.begin(); m != it->second.end(); ++m ) {
			if ( m->type == MutationRef::SetValue || m->type == MutationRef::ClearRange  )
				continue;
			else {
				printf("[ERROR] Unknown mutation type:%d\n", m->type);
				ret = false;
			}
		}

	}

	return ret;
}



//version_input is the file version
void registerBackupMutation(Standalone<StringRef> val_input, Version file_version) {
	std::string prefix = "||\t";
	std::stringstream ss;
	const int version_size = 12;
	const int header_size = 12;
	StringRef val = val_input.contents();
	StringRefReaderMX reader(val, restore_corrupted_data());

	int count_size = 0;
	// Get the version
	uint64_t version = reader.consume<uint64_t>();
	count_size += 8;
	uint32_t val_length_decode = reader.consume<uint32_t>();
	count_size += 4;

	if ( kvOps.find(file_version) == kvOps.end() ) {
		//kvOps.insert(std::make_pair(rangeFile.version, Standalone<VectorRef<MutationRef>>(VectorRef<MutationRef>())));
		kvOps.insert(std::make_pair(file_version, VectorRef<MutationRef>()));
	}

	printf("----------------------------------------------------------Register Backup Mutation into KVOPs version:%08lx\n", file_version);
	printf("To decode value:%s\n", getHexString(val).c_str());
	if ( val_length_decode != (val.size() - 12) ) {
		printf("[PARSE ERROR]!!! val_length_decode:%d != val.size:%d\n",  val_length_decode, val.size());
	} else {
		printf("[PARSE SUCCESS] val_length_decode:%d == (val.size:%d - 12)\n", val_length_decode, val.size());
	}

	// Get the mutation header
	while (1) {
		// stop when reach the end of the string
		if(reader.eof() ) { //|| *reader.rptr == 0xFF
			//printf("Finish decode the value\n");
			break;
		}


		uint32_t type = reader.consume<uint32_t>();//reader.consumeNetworkUInt32();
		uint32_t kLen = reader.consume<uint32_t>();//reader.consumeNetworkUInkvOps[t32();
		uint32_t vLen = reader.consume<uint32_t>();//reader.consumeNetworkUInt32();
		const uint8_t *k = reader.consume(kLen);
		const uint8_t *v = reader.consume(vLen);
		count_size += 4 * 3 + kLen + vLen;

		MutationRef m((MutationRef::Type) type, KeyRef(k, kLen), KeyRef(v, vLen)); //ASSUME: all operation in range file is set.
		kvOps[file_version].push_back_deep(kvOps[file_version].arena(), m);

		//		if ( kLen < 0 || kLen > val.size() || vLen < 0 || vLen > val.size() ) {
		//			printf("%s[PARSE ERROR]!!!! kLen:%d(0x%04x) vLen:%d(0x%04x)\n", prefix.c_str(), kLen, kLen, vLen, vLen);
		//		}
		//
		if ( debug_verbose ) {
			printf("%s---RegisterBackupMutation: Type:%d K:%s V:%s k_size:%d v_size:%d\n", prefix.c_str(),
				   type,  getHexString(KeyRef(k, kLen)).c_str(), getHexString(KeyRef(v, vLen)).c_str(), kLen, vLen);
		}

	}
	//	printf("----------------------------------------------------------\n");
}

//key_input format: [logRangeMutation.first][hash_value_of_commit_version:1B][bigEndian64(commitVersion)][bigEndian32(part)]
void concatenateBackupMutation(Standalone<StringRef> val_input, Standalone<StringRef> key_input) {
	std::string prefix = "||\t";
	std::stringstream ss;
	const int version_size = 12;
	const int header_size = 12;
	StringRef val = val_input.contents();
	StringRefReaderMX reader(val, restore_corrupted_data());
	StringRefReaderMX readerKey(key_input, restore_corrupted_data()); //read key_input!
	int logRangeMutationFirstLength = key_input.size() - 1 - 8 - 4;

	if ( logRangeMutationFirstLength < 0 ) {
		printf("[ERROR]!!! logRangeMutationFirstLength:%d < 0, key_input.size:%d\n", logRangeMutationFirstLength, key_input.size());
	}

	if ( debug_verbose ) {
		printf("[DEBUG] Process key_input:%s\n", getHexKey(key_input, logRangeMutationFirstLength).c_str());
	}

	//PARSE key
	Standalone<StringRef> id_old = key_input.substr(0, key_input.size() - 4); //Used to sanity check the decoding of key is correct
	Standalone<StringRef> partStr = key_input.substr(key_input.size() - 4, 4); //part
	StringRefReaderMX readerPart(partStr, restore_corrupted_data());
	uint32_t part_direct = readerPart.consumeNetworkUInt32(); //Consume a bigEndian value
	if ( debug_verbose  ) {
		printf("[DEBUG] Process prefix:%s and partStr:%s part_direct:%08x fromm key_input:%s, size:%d\n",
			   getHexKey(id_old, logRangeMutationFirstLength).c_str(),
			   getHexString(partStr).c_str(),
			   part_direct,
			   getHexKey(key_input, logRangeMutationFirstLength).c_str(),
			   key_input.size());
	}

	StringRef longRangeMutationFirst;

	if ( logRangeMutationFirstLength > 0 ) {
		printf("readerKey consumes %dB\n", logRangeMutationFirstLength);
		longRangeMutationFirst = StringRef(readerKey.consume(logRangeMutationFirstLength), logRangeMutationFirstLength);
	}

	uint8_t hashValue = readerKey.consume<uint8_t>();
	uint64_t commitVersion = readerKey.consumeNetworkUInt64(); // Consume big Endian value encoded in log file, commitVersion is in littleEndian
	uint64_t commitVersionBE = bigEndian64(commitVersion);
	uint32_t part = readerKey.consumeNetworkUInt32(); //Consume big Endian value encoded in log file
	uint32_t partBE = bigEndian32(part);
	Standalone<StringRef> id2 = longRangeMutationFirst.withSuffix(StringRef(&hashValue,1)).withSuffix(StringRef((uint8_t*) &commitVersion, 8));

	//Use commitVersion as id
	Standalone<StringRef> id = StringRef((uint8_t*) &commitVersion, 8);

	if ( debug_verbose ) {
		printf("[DEBUG] key_input_size:%d longRangeMutationFirst:%s hashValue:%02x commitVersion:%016lx (BigEndian:%016lx) part:%08x (BigEndian:%08x), part_direct:%08x mutationMap.size:%d\n",
			   key_input.size(), longRangeMutationFirst.printable().c_str(), hashValue,
			   commitVersion, commitVersionBE,
			   part, partBE,
			   part_direct, mutationMap.size());
	}

	if ( mutationMap.find(id) == mutationMap.end() ) {
		mutationMap.insert(std::make_pair(id, val_input));
		if ( part_direct != 0 ) {
			printf("[ERROR]!!! part:%d != 0 for key_input:%s\n", part, getHexString(key_input).c_str());
		}
		mutationPartMap.insert(std::make_pair(id, part));
	} else { // concatenate the val string
		mutationMap[id] = mutationMap[id].contents().withSuffix(val_input.contents()); //Assign the new Areana to the map's value
		if ( part_direct != (mutationPartMap[id] + 1) ) {
			printf("[ERROR]!!! current part id:%d new part_direct:%d is not the next integer of key_input:%s\n", mutationPartMap[id], part_direct, getHexString(key_input).c_str());
		}
		if ( part_direct != part ) {
			printf("part_direct:%08x != part:%08x\n", part_direct, part);
		}
		mutationPartMap[id] = part;
	}
}

void registerBackupMutationForAll(Version empty) {
	std::string prefix = "||\t";
	std::stringstream ss;
	const int version_size = 12;
	const int header_size = 12;
	int kvCount = 0;

	for ( auto& m: mutationMap ) {
		StringRef k = m.first.contents();
		StringRefReaderMX readerVersion(k, restore_corrupted_data());
		uint64_t commitVerison = readerVersion.consume<uint64_t>(); // Consume little Endian data


		StringRef val = m.second.contents();
		StringRefReaderMX reader(val, restore_corrupted_data());

		int count_size = 0;
		// Get the include version in the batch commit, which is not the commitVersion.
		// commitVersion is in the key
		uint64_t includeVersion = reader.consume<uint64_t>();
		count_size += 8;
		uint32_t val_length_decode = reader.consume<uint32_t>(); //Parse little endian value, confirmed it is correct!
		count_size += 4;

		if ( kvOps.find(commitVerison) == kvOps.end() ) {
			kvOps.insert(std::make_pair(commitVerison, VectorRef<MutationRef>()));
		}

		if ( debug_verbose ) {
			printf("----------------------------------------------------------Register Backup Mutation into KVOPs version:%08lx\n", commitVerison);
			printf("To decode value:%s\n", getHexString(val).c_str());
		}
		if ( val_length_decode != (val.size() - 12) ) {
			//IF we see val.size() == 10000, It means val should be concatenated! The concatenation may fail to copy the data
			fprintf(stderr, "[PARSE ERROR]!!! val_length_decode:%d != val.size:%d\n",  val_length_decode, val.size());
		} else {
			if ( debug_verbose ) {
				printf("[PARSE SUCCESS] val_length_decode:%d == (val.size:%d - 12)\n", val_length_decode, val.size());
			}
		}

		// Get the mutation header
		while (1) {
			// stop when reach the end of the string
			if(reader.eof() ) { //|| *reader.rptr == 0xFF
				//printf("Finish decode the value\n");
				break;
			}


			uint32_t type = reader.consume<uint32_t>();//reader.consumeNetworkUInt32();
			uint32_t kLen = reader.consume<uint32_t>();//reader.consumeNetworkUInkvOps[t32();
			uint32_t vLen = reader.consume<uint32_t>();//reader.consumeNetworkUInt32();
			const uint8_t *k = reader.consume(kLen);
			const uint8_t *v = reader.consume(vLen);
			count_size += 4 * 3 + kLen + vLen;

			MutationRef m((MutationRef::Type) type, KeyRef(k, kLen), KeyRef(v, vLen));
			kvOps[commitVerison].push_back_deep(kvOps[commitVerison].arena(), m);
			kvCount++;

			//		if ( kLen < 0 || kLen > val.size() || vLen < 0 || vLen > val.size() ) {
			//			printf("%s[PARSE ERROR]!!!! kLen:%d(0x%04x) vLen:%d(0x%04x)\n", prefix.c_str(), kLen, kLen, vLen, vLen);
			//		}
			//
			if ( debug_verbose ) {
				printf("%s---RegisterBackupMutation: Version:%016lx Type:%d K:%s V:%s k_size:%d v_size:%d\n", prefix.c_str(),
					   commitVerison, type,  getHexString(KeyRef(k, kLen)).c_str(), getHexString(KeyRef(v, vLen)).c_str(), kLen, vLen);
			}

		}
		//	printf("----------------------------------------------------------\n");
	}

	printf("[INFO] All mutation log files produces %d mutation operations\n", kvCount);

}






////---------------Helper Functions and Class copied from old file---------------


ACTOR Future<std::string> RestoreConfig::getProgress_impl(Reference<RestoreConfig> restore, Reference<ReadYourWritesTransaction> tr) {
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);

	state Future<int64_t> fileCount = restore->fileCount().getD(tr);
	state Future<int64_t> fileBlockCount = restore->fileBlockCount().getD(tr);
	state Future<int64_t> fileBlocksDispatched = restore->filesBlocksDispatched().getD(tr);
	state Future<int64_t> fileBlocksFinished = restore->fileBlocksFinished().getD(tr);
	state Future<int64_t> bytesWritten = restore->bytesWritten().getD(tr);
	state Future<StringRef> status = restore->stateText(tr);
	state Future<Version> lag = restore->getApplyVersionLag(tr);
	state Future<std::string> tag = restore->tag().getD(tr);
	state Future<std::pair<std::string, Version>> lastError = restore->lastError().getD(tr);

	// restore might no longer be valid after the first wait so make sure it is not needed anymore.
	state UID uid = restore->getUid();
	wait(success(fileCount) && success(fileBlockCount) && success(fileBlocksDispatched) && success(fileBlocksFinished) && success(bytesWritten) && success(status) && success(lag) && success(tag) && success(lastError));

	std::string errstr = "None";
	if(lastError.get().second != 0)
		errstr = format("'%s' %llds ago.\n", lastError.get().first.c_str(), (tr->getReadVersion().get() - lastError.get().second) / CLIENT_KNOBS->CORE_VERSIONSPERSECOND );

	TraceEvent("FileRestoreProgress")
		.detail("RestoreUID", uid)
		.detail("Tag", tag.get())
		.detail("State", status.get().toString())
		.detail("FileCount", fileCount.get())
		.detail("FileBlocksFinished", fileBlocksFinished.get())
		.detail("FileBlocksTotal", fileBlockCount.get())
		.detail("FileBlocksInProgress", fileBlocksDispatched.get() - fileBlocksFinished.get())
		.detail("BytesWritten", bytesWritten.get())
		.detail("ApplyLag", lag.get())
		.detail("TaskInstance", (uint64_t)this);


	return format("Tag: %s  UID: %s  State: %s  Blocks: %lld/%lld  BlocksInProgress: %lld  Files: %lld  BytesWritten: %lld  ApplyVersionLag: %lld  LastError: %s",
					tag.get().c_str(),
					uid.toString().c_str(),
					status.get().toString().c_str(),
					fileBlocksFinished.get(),
					fileBlockCount.get(),
					fileBlocksDispatched.get() - fileBlocksFinished.get(),
					fileCount.get(),
					bytesWritten.get(),
					lag.get(),
					errstr.c_str()
				);
}
