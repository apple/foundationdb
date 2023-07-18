/*
 * OldTLogServer.actor.cpp
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

#include "flow/Hash3.h"
#include "flow/UnitTest.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/Notified.h"
#include "fdbclient/KeyRangeMap.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "fdbserver/TLogInterface.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/IKeyValueStore.h"
#include "flow/ActorCollection.h"
#include "fdbrpc/FailureMonitor.h"
#include "fdbserver/IDiskQueue.h"
#include "fdbrpc/sim_validation.h"
#include "fdbrpc/Stats.h"
#include "fdbserver/ServerDBInfo.h"
#include "fdbserver/LogSystem.h"
#include "fdbserver/WaitFailure.h"
#include "flow/actorcompiler.h" // This must be the last #include.

using std::max;
using std::min;

namespace oldTLog_4_6 {

typedef int16_t OldTag;

OldTag convertTag(Tag tag) {
	if (tag == invalidTag || tag.locality == tagLocalityTxs)
		return invalidTagOld;
	if (tag == txsTag)
		return txsTagOld;
	ASSERT(tag.id >= 0);
	return tag.id;
}

Tag convertOldTag(OldTag tag) {
	if (tag == invalidTagOld)
		return invalidTag;
	if (tag == txsTagOld)
		return txsTag;
	ASSERT(tag >= 0);
	return Tag(tagLocalityUpgraded, tag);
}

struct OldTagMessagesRef {
	OldTag tag;
	VectorRef<int> messageOffsets;

	OldTagMessagesRef() {}
	OldTagMessagesRef(Arena& a, const OldTagMessagesRef& from)
	  : tag(from.tag), messageOffsets(a, from.messageOffsets) {}

	size_t expectedSize() const { return messageOffsets.expectedSize(); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, tag, messageOffsets);
	}
};

struct TLogQueueEntryRef {
	UID id;
	Version version;
	Version knownCommittedVersion;
	StringRef messages;
	VectorRef<OldTagMessagesRef> tags;

	TLogQueueEntryRef() : version(0), knownCommittedVersion(0) {}
	TLogQueueEntryRef(Arena& a, TLogQueueEntryRef const& from)
	  : id(from.id), version(from.version), knownCommittedVersion(from.knownCommittedVersion),
	    messages(a, from.messages), tags(a, from.tags) {}

	template <class Ar>
	void serialize(Ar& ar) {
		if (ar.protocolVersion().hasMultiGenerationTLog()) {
			serializer(ar, version, messages, tags, knownCommittedVersion, id);
		} else if (ar.isDeserializing) {
			serializer(ar, version, messages, tags);
			knownCommittedVersion = 0;
			id = UID();
		}
	}
	size_t expectedSize() const { return messages.expectedSize() + tags.expectedSize(); }
};

typedef Standalone<TLogQueueEntryRef> TLogQueueEntry;

struct TLogQueue final : public IClosable {
public:
	TLogQueue(IDiskQueue* queue, UID dbgid) : queue(queue), dbgid(dbgid) {}

	// Each packet in the queue is
	//    uint32_t payloadSize
	//    uint8_t payload[payloadSize]  (begins with uint64_t protocolVersion via IncludeVersion)
	//    uint8_t validFlag

	// TLogQueue is a durable queue of TLogQueueEntry objects with an interface similar to IDiskQueue

	// TLogQueue pushes (but not commits) are atomic - after commit fails to return, a prefix of entire calls to push
	// are durable.  This is
	//    implemented on top of the weaker guarantee of IDiskQueue::commit (that a prefix of bytes is durable) using
	//    validFlag and by padding any incomplete packet with zeros after recovery.

	// Before calling push, pop, or commit, the user must call readNext() until it throws
	//    end_of_stream(). It may not be called again thereafter.
	Future<TLogQueueEntry> readNext() { return readNext(this); }

	void push(TLogQueueEntryRef const& qe) {
		BinaryWriter wr(Unversioned()); // outer framing is not versioned
		wr << uint32_t(0);
		IncludeVersion(ProtocolVersion::withTLogQueueEntryRef()).write(wr); // payload is versioned
		wr << qe;
		wr << uint8_t(1);
		*(uint32_t*)wr.getData() = wr.getLength() - sizeof(uint32_t) - sizeof(uint8_t);
		auto loc = queue->push(wr.toValue());
		//TraceEvent("TLogQueueVersionWritten", dbgid).detail("Size", wr.getLength() - sizeof(uint32_t) - sizeof(uint8_t)).detail("Loc", loc);
		version_location[qe.version] = loc;
	}
	void pop(Version upTo) {
		// Keep only the given and all subsequent version numbers
		// Find the first version >= upTo
		auto v = version_location.lower_bound(upTo);
		if (v == version_location.begin())
			return;

		if (v == version_location.end()) {
			v = version_location.lastItem();
		} else {
			v.decrementNonEnd();
		}

		queue->pop(v->value);
		version_location.erase(version_location.begin(),
		                       v); // ... and then we erase that previous version and all prior versions
	}
	Future<Void> commit() { return queue->commit(); }

	// Implements IClosable
	Future<Void> getError() const override { return queue->getError(); }
	Future<Void> onClosed() const override { return queue->onClosed(); }
	void dispose() override {
		queue->dispose();
		delete this;
	}
	void close() override {
		queue->close();
		delete this;
	}

private:
	IDiskQueue* queue;
	Map<Version, IDiskQueue::location>
	    version_location; // For the version of each entry that was push()ed, the end location of the serialized bytes
	UID dbgid;

	ACTOR static Future<TLogQueueEntry> readNext(TLogQueue* self) {
		state TLogQueueEntry result;
		state int zeroFillSize = 0;

		loop {
			Standalone<StringRef> h = wait(self->queue->readNext(sizeof(uint32_t)));
			if (h.size() != sizeof(uint32_t)) {
				if (h.size()) {
					CODE_PROBE(true, "Zero fill within size field", probe::decoration::rare);
					int payloadSize = 0;
					memcpy(&payloadSize, h.begin(), h.size());
					zeroFillSize = sizeof(uint32_t) - h.size(); // zero fill the size itself
					zeroFillSize += payloadSize + 1; // and then the contents and valid flag
				}
				break;
			}

			state uint32_t payloadSize = *(uint32_t*)h.begin();
			ASSERT(payloadSize < (100 << 20));

			Standalone<StringRef> e = wait(self->queue->readNext(payloadSize + 1));
			if (e.size() != payloadSize + 1) {
				CODE_PROBE(true, "Zero fill within payload", probe::decoration::rare);
				zeroFillSize = payloadSize + 1 - e.size();
				break;
			}

			if (e[payloadSize]) {
				Arena a = e.arena();
				ArenaReader ar(a, e.substr(0, payloadSize), IncludeVersion());
				ar >> result;
				self->version_location[result.version] = self->queue->getNextReadLocation();
				return result;
			}
		}
		if (zeroFillSize) {
			CODE_PROBE(true, "Fixing a partial commit at the end of the tlog queue", probe::decoration::rare);
			for (int i = 0; i < zeroFillSize; i++)
				self->queue->push(StringRef((const uint8_t*)"", 1));
		}
		throw end_of_stream();
	}
};

////// Persistence format (for self->persistentData)

// Immutable keys
static const KeyValueRef persistFormat("Format"_sr, "FoundationDB/LogServer/2/3"_sr);
static const KeyRangeRef persistFormatReadableRange("FoundationDB/LogServer/2/3"_sr, "FoundationDB/LogServer/2/4"_sr);
static const KeyRangeRef persistRecoveryCountKeys = KeyRangeRef("DbRecoveryCount/"_sr, "DbRecoveryCount0"_sr);

// Updated on updatePersistentData()
static const KeyRangeRef persistCurrentVersionKeys = KeyRangeRef("version/"_sr, "version0"_sr);
static const KeyRange persistTagMessagesKeys = prefixRange("TagMsg/"_sr);
static const KeyRange persistTagPoppedKeys = prefixRange("TagPop/"_sr);

static Key persistTagMessagesKey(UID id, OldTag tag, Version version) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes(persistTagMessagesKeys.begin);
	wr << id;
	wr << tag;
	wr << bigEndian64(version);
	return wr.toValue();
}

static Key persistTagPoppedKey(UID id, OldTag tag) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes(persistTagPoppedKeys.begin);
	wr << id;
	wr << tag;
	return wr.toValue();
}

static Value persistTagPoppedValue(Version popped) {
	return BinaryWriter::toValue(popped, Unversioned());
}

static OldTag decodeTagPoppedKey(KeyRef id, KeyRef key) {
	OldTag s;
	BinaryReader rd(key.removePrefix(persistTagPoppedKeys.begin).removePrefix(id), Unversioned());
	rd >> s;
	return s;
}

static Version decodeTagPoppedValue(ValueRef value) {
	return BinaryReader::fromStringRef<Version>(value, Unversioned());
}

static StringRef stripTagMessagesKey(StringRef key) {
	return key.substr(sizeof(UID) + sizeof(OldTag) + persistTagMessagesKeys.begin.size());
}

static Version decodeTagMessagesKey(StringRef key) {
	return bigEndian64(BinaryReader::fromStringRef<Version>(stripTagMessagesKey(key), Unversioned()));
}

struct TLogData : NonCopyable {
	AsyncTrigger newLogData;
	Deque<UID> queueOrder;
	std::map<UID, Reference<struct LogData>> id_data;

	UID dbgid;
	UID workerID;

	IKeyValueStore* persistentData;
	IDiskQueue* rawPersistentQueue;
	TLogQueue* persistentQueue;

	int64_t diskQueueCommitBytes;
	AsyncVar<bool>
	    largeDiskQueueCommitBytes; // becomes true when diskQueueCommitBytes is greater than MAX_QUEUE_COMMIT_BYTES

	Reference<AsyncVar<ServerDBInfo> const> dbInfo;

	NotifiedVersion queueCommitEnd;
	Version queueCommitBegin;
	AsyncTrigger newVersion;

	int64_t instanceID;
	int64_t bytesInput;
	int64_t bytesDurable;
	int activePeekStreams = 0;

	Version prevVersion;

	struct PeekTrackerData {
		std::map<int, Promise<Version>> sequence_version;
		double lastUpdate;
	};

	std::map<UID, PeekTrackerData> peekTracker;
	WorkerCache<TLogInterface> tlogCache;

	Future<Void> updatePersist; // SOMEDAY: integrate the recovery and update storage so that only one of them is
	                            // committing to persistant data.

	PromiseStream<Future<Void>> sharedActors;
	bool terminated;

	TLogData(UID dbgid,
	         UID workerID,
	         IKeyValueStore* persistentData,
	         IDiskQueue* persistentQueue,
	         Reference<AsyncVar<ServerDBInfo> const> const& dbInfo)
	  : dbgid(dbgid), workerID(workerID), persistentData(persistentData), rawPersistentQueue(persistentQueue),
	    persistentQueue(new TLogQueue(persistentQueue, dbgid)), diskQueueCommitBytes(0),
	    largeDiskQueueCommitBytes(false), dbInfo(dbInfo), queueCommitEnd(0), queueCommitBegin(0),
	    instanceID(deterministicRandom()->randomUniqueID().first()), bytesInput(0), bytesDurable(0), prevVersion(0),
	    updatePersist(Void()), terminated(false) {}
};

struct LogData : NonCopyable, public ReferenceCounted<LogData> {
	struct TagData {
		std::deque<std::pair<Version, LengthPrefixedStringRef>> version_messages;
		bool nothing_persistent; // true means tag is *known* to have no messages in persistentData.  false means
		                         // nothing.
		bool popped_recently; // `popped` has changed since last updatePersistentData
		Version popped; // see popped version tracking contract below
		bool update_version_sizes;

		TagData(Version popped, bool nothing_persistent, bool popped_recently, OldTag tag)
		  : nothing_persistent(nothing_persistent), popped_recently(popped_recently), popped(popped),
		    update_version_sizes(tag != txsTagOld) {}

		TagData(TagData&& r) noexcept
		  : version_messages(std::move(r.version_messages)), nothing_persistent(r.nothing_persistent),
		    popped_recently(r.popped_recently), popped(r.popped), update_version_sizes(r.update_version_sizes) {}
		void operator=(TagData&& r) noexcept {
			version_messages = std::move(r.version_messages);
			nothing_persistent = r.nothing_persistent;
			popped_recently = r.popped_recently;
			popped = r.popped;
			update_version_sizes = r.update_version_sizes;
		}

		// Erase messages not needed to update *from* versions >= before (thus, messages with toversion <= before)
		ACTOR Future<Void> eraseMessagesBefore(TagData* self,
		                                       Version before,
		                                       int64_t* gBytesErased,
		                                       Reference<LogData> tlogData,
		                                       TaskPriority taskID) {
			while (!self->version_messages.empty() && self->version_messages.front().first < before) {
				Version version = self->version_messages.front().first;
				std::pair<int, int>& sizes = tlogData->version_sizes[version];
				int64_t messagesErased = 0;

				while (!self->version_messages.empty() && self->version_messages.front().first == version) {
					auto const& m = self->version_messages.front();
					++messagesErased;

					if (self->update_version_sizes) {
						sizes.first -= m.second.expectedSize();
					}

					self->version_messages.pop_front();
				}

				int64_t bytesErased = (messagesErased * sizeof(std::pair<Version, LengthPrefixedStringRef>) *
				                       SERVER_KNOBS->VERSION_MESSAGES_OVERHEAD_FACTOR_1024THS) >>
				                      10;
				tlogData->bytesDurable += bytesErased;
				*gBytesErased += bytesErased;
				wait(yield(taskID));
			}

			return Void();
		}

		Future<Void> eraseMessagesBefore(Version before,
		                                 int64_t* gBytesErased,
		                                 Reference<LogData> tlogData,
		                                 TaskPriority taskID) {
			return eraseMessagesBefore(this, before, gBytesErased, tlogData, taskID);
		}
	};

	/*
	Popped version tracking contract needed by log system to implement ILogCursor::popped():

	    - Log server tracks for each (possible) tag a popped_version
	    Impl: TagData::popped (in memory) and persistTagPoppedKeys (in persistentData)
	    - popped_version(tag) is <= the maximum version for which log server (or a predecessor) is ever asked to pop the
	tag Impl: Only increased by tLogPop() in response to either a pop request or recovery from a predecessor
	    - popped_version(tag) is > the maximum version for which log server is unable to peek messages due to previous
	pops (on this server or a predecessor) Impl: Increased by tLogPop() atomically with erasing messages from memory;
	persisted by updatePersistentData() atomically with erasing messages from store; messages are not erased from queue
	where popped_version is not persisted
	    - LockTLogReply returns all tags which either have messages, or which have nonzero popped_versions
	    Impl: tag_data is present for all such tags
	    - peek(tag, v) returns the popped_version for tag if that is greater than v
	    Impl: Check tag_data->popped (after all waits)
	*/

	bool stopped, initialized;
	DBRecoveryCount recoveryCount;

	VersionMetricHandle persistentDataVersion,
	    persistentDataDurableVersion; // The last version number in the portion of the log (written|durable) to
	                                  // persistentData
	NotifiedVersion version, queueCommittedVersion;
	Version queueCommittingVersion;
	Version knownCommittedVersion;

	Deque<std::pair<Version, Standalone<VectorRef<uint8_t>>>> messageBlocks;
	Map<OldTag, TagData> tag_data;

	Map<Version, std::pair<int, int>> version_sizes;

	CounterCollection cc;
	Counter bytesInput;
	Counter bytesDurable;

	UID logId;
	Version newPersistentDataVersion;
	Future<Void> removed;
	TLogInterface tli;
	PromiseStream<Future<Void>> addActor;
	TLogData* tLogData;
	Promise<bool> recoverySuccessful;
	Future<Void> recovery;

	explicit LogData(TLogData* tLogData, TLogInterface interf)
	  : stopped(false), initialized(false), recoveryCount(), queueCommittingVersion(0), knownCommittedVersion(0),
	    cc("TLog", interf.id().toString()), bytesInput("BytesInput", cc), bytesDurable("BytesDurable", cc),
	    logId(interf.id()), newPersistentDataVersion(invalidVersion), tli(interf), tLogData(tLogData),
	    recovery(Void()) {
		startRole(Role::TRANSACTION_LOG,
		          interf.id(),
		          tLogData->workerID,
		          { { "SharedTLog", tLogData->dbgid.shortString() } },
		          "Restored");
		addActor.send(traceRole(Role::TRANSACTION_LOG, interf.id()));

		persistentDataVersion.init("TLog.PersistentDataVersion"_sr, cc.getId());
		persistentDataDurableVersion.init("TLog.PersistentDataDurableVersion"_sr, cc.getId());
		version.initMetric("TLog.Version"_sr, cc.getId());
		queueCommittedVersion.initMetric("TLog.QueueCommittedVersion"_sr, cc.getId());

		specialCounter(cc, "Version", [this]() { return this->version.get(); });
		specialCounter(cc, "SharedBytesInput", [tLogData]() { return tLogData->bytesInput; });
		specialCounter(cc, "SharedBytesDurable", [tLogData]() { return tLogData->bytesDurable; });
		specialCounter(cc, "ActivePeekStreams", [tLogData]() { return tLogData->activePeekStreams; });
	}

	~LogData() {
		tLogData->bytesDurable += bytesInput.getValue() - bytesDurable.getValue();
		TraceEvent("TLogBytesWhenRemoved", tli.id())
		    .detail("SharedBytesInput", tLogData->bytesInput)
		    .detail("SharedBytesDurable", tLogData->bytesDurable)
		    .detail("LocalBytesInput", bytesInput.getValue())
		    .detail("LocalBytesDurable", bytesDurable.getValue());

		ASSERT_ABORT(tLogData->bytesDurable <= tLogData->bytesInput);
		endRole(Role::TRANSACTION_LOG, tli.id(), "Error", true);

		if (!tLogData->terminated) {
			Key logIdKey = BinaryWriter::toValue(logId, Unversioned());
			tLogData->persistentData->clear(singleKeyRange(logIdKey.withPrefix(persistCurrentVersionKeys.begin)));
			tLogData->persistentData->clear(singleKeyRange(logIdKey.withPrefix(persistRecoveryCountKeys.begin)));
			Key msgKey = logIdKey.withPrefix(persistTagMessagesKeys.begin);
			tLogData->persistentData->clear(KeyRangeRef(msgKey, strinc(msgKey)));
			Key poppedKey = logIdKey.withPrefix(persistTagPoppedKeys.begin);
			tLogData->persistentData->clear(KeyRangeRef(poppedKey, strinc(poppedKey)));
		}
	}

	LogEpoch epoch() const { return recoveryCount; }
};

ACTOR Future<Void> tLogLock(TLogData* self, ReplyPromise<TLogLockResult> reply, Reference<LogData> logData) {
	state Version stopVersion = logData->version.get();

	CODE_PROBE(true, "TLog stopped by recovering master");
	CODE_PROBE(logData->stopped, "LogData already stopped");
	CODE_PROBE(!logData->stopped, "LogData not yet stopped", probe::decoration::rare);

	TraceEvent("TLogStop", logData->logId)
	    .detail("Ver", stopVersion)
	    .detail("IsStopped", logData->stopped)
	    .detail("QueueCommitted", logData->queueCommittedVersion.get());

	logData->stopped = true;
	if (logData->recoverySuccessful.canBeSet()) {
		logData->recoverySuccessful.send(false);
	}

	// Lock once the current version has been committed
	wait(logData->queueCommittedVersion.whenAtLeast(stopVersion));

	ASSERT(stopVersion == logData->version.get());

	TLogLockResult result;
	result.end = stopVersion;
	result.knownCommittedVersion = logData->knownCommittedVersion;

	TraceEvent("TLogStop2", self->dbgid)
	    .detail("LogId", logData->logId)
	    .detail("Ver", stopVersion)
	    .detail("IsStopped", logData->stopped)
	    .detail("QueueCommitted", logData->queueCommittedVersion.get());

	reply.send(result);
	return Void();
}

void updatePersistentPopped(TLogData* self, Reference<LogData> logData, OldTag tag, LogData::TagData& data) {
	if (!data.popped_recently)
		return;
	self->persistentData->set(
	    KeyValueRef(persistTagPoppedKey(logData->logId, tag), persistTagPoppedValue(data.popped)));
	data.popped_recently = false;

	if (data.nothing_persistent)
		return;

	self->persistentData->clear(KeyRangeRef(persistTagMessagesKey(logData->logId, tag, Version(0)),
	                                        persistTagMessagesKey(logData->logId, tag, data.popped)));
	if (data.popped > logData->persistentDataVersion)
		data.nothing_persistent = true;
}

ACTOR Future<Void> updatePersistentData(TLogData* self, Reference<LogData> logData, Version newPersistentDataVersion) {
	// PERSIST: Changes self->persistentDataVersion and writes and commits the relevant changes
	ASSERT(newPersistentDataVersion <= logData->version.get());
	ASSERT(newPersistentDataVersion <= logData->queueCommittedVersion.get());
	ASSERT(newPersistentDataVersion > logData->persistentDataVersion);
	ASSERT(logData->persistentDataVersion == logData->persistentDataDurableVersion);

	//TraceEvent("UpdatePersistentData", self->dbgid).detail("Seq", newPersistentDataSeq);

	state bool anyData = false;
	state Map<OldTag, LogData::TagData>::iterator tag;
	// For all existing tags
	for (tag = logData->tag_data.begin(); tag != logData->tag_data.end(); ++tag) {
		state Version currentVersion = 0;
		// Clear recently popped versions from persistentData if necessary
		updatePersistentPopped(self, logData, tag->key, tag->value);
		// Transfer unpopped messages with version numbers less than newPersistentDataVersion to persistentData
		state std::deque<std::pair<Version, LengthPrefixedStringRef>>::iterator msg =
		    tag->value.version_messages.begin();
		while (msg != tag->value.version_messages.end() && msg->first <= newPersistentDataVersion) {
			currentVersion = msg->first;
			anyData = true;
			tag->value.nothing_persistent = false;
			BinaryWriter wr(Unversioned());

			for (; msg != tag->value.version_messages.end() && msg->first == currentVersion; ++msg)
				wr << msg->second.toStringRef();

			self->persistentData->set(
			    KeyValueRef(persistTagMessagesKey(logData->logId, tag->key, currentVersion), wr.toValue()));

			Future<Void> f = yield(TaskPriority::UpdateStorage);
			if (!f.isReady()) {
				wait(f);
				msg = std::upper_bound(tag->value.version_messages.begin(),
				                       tag->value.version_messages.end(),
				                       std::make_pair(currentVersion, LengthPrefixedStringRef()),
				                       [](const auto& l, const auto& r) { return l.first < r.first; });
			}
		}

		wait(yield(TaskPriority::UpdateStorage));
	}

	self->persistentData->set(
	    KeyValueRef(BinaryWriter::toValue(logData->logId, Unversioned()).withPrefix(persistCurrentVersionKeys.begin),
	                BinaryWriter::toValue(newPersistentDataVersion, Unversioned())));
	logData->persistentDataVersion = newPersistentDataVersion;

	wait(self->persistentData->commit()); // SOMEDAY: This seems to be running pretty often, should we slow it down???
	wait(delay(0, TaskPriority::UpdateStorage));

	// Now that the changes we made to persistentData are durable, erase the data we moved from memory and the queue,
	// increase bytesDurable accordingly, and update persistentDataDurableVersion.

	CODE_PROBE(anyData, "TLog moved data to persistentData");
	logData->persistentDataDurableVersion = newPersistentDataVersion;

	for (tag = logData->tag_data.begin(); tag != logData->tag_data.end(); ++tag) {
		wait(tag->value.eraseMessagesBefore(
		    newPersistentDataVersion + 1, &self->bytesDurable, logData, TaskPriority::UpdateStorage));
		wait(yield(TaskPriority::UpdateStorage));
	}

	logData->version_sizes.erase(logData->version_sizes.begin(),
	                             logData->version_sizes.lower_bound(logData->persistentDataDurableVersion));

	wait(yield(TaskPriority::UpdateStorage));

	while (!logData->messageBlocks.empty() && logData->messageBlocks.front().first <= newPersistentDataVersion) {
		int64_t bytesErased =
		    int64_t(logData->messageBlocks.front().second.size()) * SERVER_KNOBS->TLOG_MESSAGE_BLOCK_OVERHEAD_FACTOR;
		logData->bytesDurable += bytesErased;
		self->bytesDurable += bytesErased;
		logData->messageBlocks.pop_front();
		wait(yield(TaskPriority::UpdateStorage));
	}

	if (logData->bytesDurable.getValue() > logData->bytesInput.getValue() || self->bytesDurable > self->bytesInput) {
		TraceEvent(SevError, "BytesDurableTooLarge", logData->logId)
		    .detail("SharedBytesInput", self->bytesInput)
		    .detail("SharedBytesDurable", self->bytesDurable)
		    .detail("LocalBytesInput", logData->bytesInput.getValue())
		    .detail("LocalBytesDurable", logData->bytesDurable.getValue());
	}

	ASSERT(logData->bytesDurable.getValue() <= logData->bytesInput.getValue());
	ASSERT(self->bytesDurable <= self->bytesInput);

	if (self->queueCommitEnd.get() > 0)
		self->persistentQueue->pop(newPersistentDataVersion +
		                           1); // SOMEDAY: this can cause a slow task (~0.5ms), presumably from erasing too many
		                               // versions. Should we limit the number of versions cleared at a time?

	return Void();
}

// This function (and updatePersistentData, which is called by this function) run at a low priority and can soak up all
// CPU resources. For this reason, they employ aggressive use of yields to avoid causing slow tasks that could introduce
// latencies for more important work (e.g. commits).
ACTOR Future<Void> updateStorage(TLogData* self) {
	while (self->queueOrder.size() && !self->id_data.count(self->queueOrder.front())) {
		self->queueOrder.pop_front();
	}

	if (!self->queueOrder.size()) {
		wait(delay(BUGGIFY ? SERVER_KNOBS->BUGGIFY_TLOG_STORAGE_MIN_UPDATE_INTERVAL
		                   : SERVER_KNOBS->TLOG_STORAGE_MIN_UPDATE_INTERVAL,
		           TaskPriority::UpdateStorage));
		return Void();
	}

	state Reference<LogData> logData = self->id_data[self->queueOrder.front()];
	state Version prevVersion = 0;
	state Version nextVersion = 0;
	state int totalSize = 0;

	if (logData->stopped) {
		if (self->bytesInput - self->bytesDurable >= SERVER_KNOBS->TLOG_SPILL_THRESHOLD) {
			while (logData->persistentDataDurableVersion != logData->version.get()) {
				std::vector<std::pair<std::deque<std::pair<Version, LengthPrefixedStringRef>>::iterator,
				                      std::deque<std::pair<Version, LengthPrefixedStringRef>>::iterator>>
				    iters;
				for (auto tag = logData->tag_data.begin(); tag != logData->tag_data.end(); ++tag)
					iters.push_back(
					    std::make_pair(tag->value.version_messages.begin(), tag->value.version_messages.end()));

				nextVersion = 0;
				while (totalSize < SERVER_KNOBS->UPDATE_STORAGE_BYTE_LIMIT ||
				       nextVersion <= logData->persistentDataVersion) {
					nextVersion = logData->version.get();
					for (auto& it : iters)
						if (it.first != it.second)
							nextVersion = std::min(nextVersion, it.first->first + 1);

					if (nextVersion == logData->version.get())
						break;

					for (auto& it : iters) {
						while (it.first != it.second && it.first->first < nextVersion) {
							totalSize += it.first->second.expectedSize();
							++it.first;
						}
					}
				}

				wait(logData->queueCommittedVersion.whenAtLeast(nextVersion));
				wait(delay(0, TaskPriority::UpdateStorage));

				//TraceEvent("TlogUpdatePersist", self->dbgid).detail("LogId", logData->logId).detail("NextVersion", nextVersion).detail("Version", logData->version.get()).detail("PersistentDataDurableVer", logData->persistentDataDurableVersion).detail("QueueCommitVer", logData->queueCommittedVersion.get()).detail("PersistDataVer", logData->persistentDataVersion);
				if (nextVersion > logData->persistentDataVersion) {
					self->updatePersist = updatePersistentData(self, logData, nextVersion);
					wait(self->updatePersist);
				} else {
					wait(delay(BUGGIFY ? SERVER_KNOBS->BUGGIFY_TLOG_STORAGE_MIN_UPDATE_INTERVAL
					                   : SERVER_KNOBS->TLOG_STORAGE_MIN_UPDATE_INTERVAL,
					           TaskPriority::UpdateStorage));
				}

				if (logData->removed.isReady()) {
					break;
				}
			}

			if (logData->persistentDataDurableVersion == logData->version.get()) {
				self->queueOrder.pop_front();
			}
			wait(delay(0.0, TaskPriority::UpdateStorage));
		} else {
			wait(delay(BUGGIFY ? SERVER_KNOBS->BUGGIFY_TLOG_STORAGE_MIN_UPDATE_INTERVAL
			                   : SERVER_KNOBS->TLOG_STORAGE_MIN_UPDATE_INTERVAL,
			           TaskPriority::UpdateStorage));
		}
	} else if (logData->initialized) {
		ASSERT(self->queueOrder.size() == 1);
		state Map<Version, std::pair<int, int>>::iterator sizeItr = logData->version_sizes.begin();
		while (totalSize < SERVER_KNOBS->UPDATE_STORAGE_BYTE_LIMIT && sizeItr != logData->version_sizes.end() &&
		       (logData->bytesInput.getValue() - logData->bytesDurable.getValue() - totalSize >=
		            SERVER_KNOBS->TLOG_SPILL_THRESHOLD ||
		        sizeItr->value.first == 0)) {
			wait(yield(TaskPriority::UpdateStorage));

			++sizeItr;
			nextVersion = sizeItr == logData->version_sizes.end() ? logData->version.get() : sizeItr->key;

			state Map<OldTag, LogData::TagData>::iterator tag;
			for (tag = logData->tag_data.begin(); tag != logData->tag_data.end(); ++tag) {
				auto it = std::lower_bound(tag->value.version_messages.begin(),
				                           tag->value.version_messages.end(),
				                           std::make_pair(prevVersion, LengthPrefixedStringRef()),
				                           [](const auto& l, const auto& r) { return l.first < r.first; });
				for (; it != tag->value.version_messages.end() && it->first < nextVersion; ++it) {
					totalSize += it->second.expectedSize();
				}

				wait(yield(TaskPriority::UpdateStorage));
			}

			prevVersion = nextVersion;
		}

		nextVersion = std::max<Version>(nextVersion, logData->persistentDataVersion);

		//TraceEvent("UpdateStorageVer", logData->logId).detail("NextVersion", nextVersion).detail("PersistentDataVersion", logData->persistentDataVersion).detail("TotalSize", totalSize);

		wait(logData->queueCommittedVersion.whenAtLeast(nextVersion));
		wait(delay(0, TaskPriority::UpdateStorage));

		if (nextVersion > logData->persistentDataVersion) {
			self->updatePersist = updatePersistentData(self, logData, nextVersion);
			wait(self->updatePersist);
		}

		if (totalSize < SERVER_KNOBS->UPDATE_STORAGE_BYTE_LIMIT) {
			wait(delay(BUGGIFY ? SERVER_KNOBS->BUGGIFY_TLOG_STORAGE_MIN_UPDATE_INTERVAL
			                   : SERVER_KNOBS->TLOG_STORAGE_MIN_UPDATE_INTERVAL,
			           TaskPriority::UpdateStorage));
		} else {
			// recovery wants to commit to persistant data when updatePersistentData is not active, this delay ensures
			// that immediately after updatePersist returns another one has not been started yet.
			wait(delay(0.0, TaskPriority::UpdateStorage));
		}
	} else {
		wait(delay(BUGGIFY ? SERVER_KNOBS->BUGGIFY_TLOG_STORAGE_MIN_UPDATE_INTERVAL
		                   : SERVER_KNOBS->TLOG_STORAGE_MIN_UPDATE_INTERVAL,
		           TaskPriority::UpdateStorage));
	}
	return Void();
}

ACTOR Future<Void> updateStorageLoop(TLogData* self) {
	wait(delay(0, TaskPriority::UpdateStorage));

	loop {
		wait(updateStorage(self));
	}
}

void commitMessages(Reference<LogData> self,
                    Version version,
                    Arena arena,
                    StringRef messages,
                    VectorRef<OldTagMessagesRef> tags,
                    int64_t& bytesInput) {
	// SOMEDAY: This method of copying messages is reasonably memory efficient, but it's still a lot of bytes copied.
	// Find a way to do the memory allocation right as we receive the messages in the network layer.

	int64_t addedBytes = 0;
	int64_t expectedBytes = 0;

	if (!messages.size()) {
		return;
	}

	StringRef messages1; // the first block of messages, if they aren't all stored contiguously.  otherwise empty

	// Grab the last block in the blocks list so we can share its arena
	// We pop all of the elements of it to create a "fresh" vector that starts at the end of the previous vector
	Standalone<VectorRef<uint8_t>> block;
	if (self->messageBlocks.empty()) {
		block = Standalone<VectorRef<uint8_t>>();
		block.reserve(block.arena(), std::max<int64_t>(SERVER_KNOBS->TLOG_MESSAGE_BLOCK_BYTES, messages.size()));
	} else {
		block = self->messageBlocks.back().second;
	}

	block.pop_front(block.size());

	// If the current batch of messages doesn't fit entirely in the remainder of the last block in the list
	if (messages.size() + block.size() > block.capacity()) {
		// Find how many messages will fit
		LengthPrefixedStringRef r((uint32_t*)messages.begin());
		uint8_t const* end = messages.begin() + block.capacity() - block.size();
		while (r.toStringRef().end() <= end) {
			r = LengthPrefixedStringRef((uint32_t*)r.toStringRef().end());
		}

		// Fill up the rest of this block
		int bytes = (uint8_t*)r.getLengthPtr() - messages.begin();
		if (bytes) {
			CODE_PROBE(true, "Splitting commit messages across multiple blocks");
			messages1 = StringRef(block.end(), bytes);
			block.append(block.arena(), messages.begin(), bytes);
			self->messageBlocks.emplace_back(version, block);
			addedBytes += int64_t(block.size()) * SERVER_KNOBS->TLOG_MESSAGE_BLOCK_OVERHEAD_FACTOR;
			messages = messages.substr(bytes);
		}

		// Make a new block
		block = Standalone<VectorRef<uint8_t>>();
		block.reserve(block.arena(), std::max<int64_t>(SERVER_KNOBS->TLOG_MESSAGE_BLOCK_BYTES, messages.size()));
	}

	// Copy messages into block
	ASSERT(messages.size() <= block.capacity() - block.size());
	block.append(block.arena(), messages.begin(), messages.size());
	self->messageBlocks.emplace_back(version, block);
	addedBytes += int64_t(block.size()) * SERVER_KNOBS->TLOG_MESSAGE_BLOCK_OVERHEAD_FACTOR;
	messages = StringRef(block.end() - messages.size(), messages.size());

	for (auto tag = tags.begin(); tag != tags.end(); ++tag) {
		int64_t tagMessages = 0;

		auto tsm = self->tag_data.find(tag->tag);
		if (tsm == self->tag_data.end()) {
			tsm = self->tag_data.insert(
			    mapPair(std::move(OldTag(tag->tag)), LogData::TagData(Version(0), true, true, tag->tag)), false);
		}

		if (version >= tsm->value.popped) {
			for (int m = 0; m < tag->messageOffsets.size(); ++m) {
				int offs = tag->messageOffsets[m];
				uint8_t const* p =
				    offs < messages1.size() ? messages1.begin() + offs : messages.begin() + offs - messages1.size();
				tsm->value.version_messages.emplace_back(version, LengthPrefixedStringRef((uint32_t*)p));
				if (tsm->value.version_messages.back().second.expectedSize() > SERVER_KNOBS->MAX_MESSAGE_SIZE) {
					TraceEvent(SevWarnAlways, "LargeMessage")
					    .detail("Size", tsm->value.version_messages.back().second.expectedSize());
				}
				if (tag->tag != txsTagOld)
					expectedBytes += tsm->value.version_messages.back().second.expectedSize();

				++tagMessages;
			}
		}

		// The factor of VERSION_MESSAGES_OVERHEAD is intended to be an overestimate of the actual memory used to store
		// this data in a std::deque. In practice, this number is probably something like 528/512 ~= 1.03, but this
		// could vary based on the implementation. There will also be a fixed overhead per std::deque, but its size
		// should be trivial relative to the size of the TLog queue and can be thought of as increasing the capacity of
		// the queue slightly.
		addedBytes += (tagMessages * sizeof(std::pair<Version, LengthPrefixedStringRef>) *
		               SERVER_KNOBS->VERSION_MESSAGES_OVERHEAD_FACTOR_1024THS) >>
		              10;
	}

	self->version_sizes[version] = std::make_pair(expectedBytes, expectedBytes);
	self->bytesInput += addedBytes;
	bytesInput += addedBytes;

	//TraceEvent("TLogPushed", self->dbgid).detail("Bytes", addedBytes).detail("MessageBytes", messages.size()).detail("Tags", tags.size()).detail("ExpectedBytes", expectedBytes).detail("MCount", mCount).detail("TCount", tCount);
}

Version poppedVersion(Reference<LogData> self, OldTag tag) {
	auto mapIt = self->tag_data.find(tag);
	if (mapIt == self->tag_data.end())
		return Version(0);
	return mapIt->value.popped;
}

std::deque<std::pair<Version, LengthPrefixedStringRef>>& get_version_messages(Reference<LogData> self, OldTag tag) {
	auto mapIt = self->tag_data.find(tag);
	if (mapIt == self->tag_data.end()) {
		static std::deque<std::pair<Version, LengthPrefixedStringRef>> empty;
		return empty;
	}
	return mapIt->value.version_messages;
};

ACTOR Future<Void> tLogPop(TLogData* self, TLogPopRequest req, Reference<LogData> logData) {
	OldTag oldTag = convertTag(req.tag);
	auto ti = logData->tag_data.find(oldTag);
	if (ti == logData->tag_data.end()) {
		ti = logData->tag_data.insert(mapPair(oldTag, LogData::TagData(req.to, true, true, oldTag)));
	} else if (req.to > ti->value.popped) {
		ti->value.popped = req.to;
		ti->value.popped_recently = true;
		// if (to.epoch == self->epoch())
		if (req.to > logData->persistentDataDurableVersion)
			wait(ti->value.eraseMessagesBefore(req.to, &self->bytesDurable, logData, TaskPriority::TLogPop));
	}

	req.reply.send(Void());
	return Void();
}

void peekMessagesFromMemory(Reference<LogData> self,
                            Tag tag,
                            Version reqBegin,
                            BinaryWriter& messages,
                            Version& endVersion) {
	OldTag oldTag = convertTag(tag);
	ASSERT(!messages.getLength());

	auto& deque = get_version_messages(self, oldTag);
	Version begin = std::max(reqBegin, self->persistentDataDurableVersion + 1);
	auto it = std::lower_bound(deque.begin(),
	                           deque.end(),
	                           std::make_pair(begin, LengthPrefixedStringRef()),
	                           [](const auto& l, const auto& r) { return l.first < r.first; });

	Version currentVersion = -1;
	for (; it != deque.end(); ++it) {
		if (it->first != currentVersion) {
			if (messages.getLength() >= SERVER_KNOBS->DESIRED_TOTAL_BYTES) {
				endVersion = it->first;
				//TraceEvent("TLogPeekMessagesReached2", self->dbgid);
				break;
			}

			currentVersion = it->first;
			messages << int32_t(-1) << currentVersion;
		}

		BinaryReader rd(it->second.getLengthPtr(), it->second.expectedSize() + 4, Unversioned());
		while (!rd.empty()) {
			int32_t messageLength;
			uint32_t subVersion;
			rd >> messageLength >> subVersion;
			messageLength += sizeof(uint16_t) + sizeof(Tag);
			messages << messageLength << subVersion << uint16_t(1) << tag;
			messageLength -= (sizeof(subVersion) + sizeof(uint16_t) + sizeof(Tag));
			messages.serializeBytes(rd.readBytes(messageLength), messageLength);
		}
	}
}

// Common logics to peek TLog and create TLogPeekReply that serves both streaming peek or normal peek request
ACTOR template <typename PromiseType>
Future<Void> tLogPeekMessages(PromiseType replyPromise,
                              TLogData* self,
                              Reference<LogData> logData,
                              Version reqBegin,
                              Tag reqTag,
                              bool reqReturnIfBlocked = false,
                              bool reqOnlySpilled = false,
                              Optional<std::pair<UID, int>> reqSequence = Optional<std::pair<UID, int>>()) {
	state BinaryWriter messages(Unversioned());
	state BinaryWriter messages2(Unversioned());
	state int sequence = -1;
	state UID peekId;
	state OldTag oldTag = convertTag(reqTag);

	if (reqSequence.present()) {
		try {
			peekId = reqSequence.get().first;
			sequence = reqSequence.get().second;
			if (sequence >= SERVER_KNOBS->PARALLEL_GET_MORE_REQUESTS &&
			    self->peekTracker.find(peekId) == self->peekTracker.end()) {
				throw operation_obsolete();
			}
			if (sequence > 0) {
				auto& trackerData = self->peekTracker[peekId];
				trackerData.lastUpdate = now();
				Version ver = wait(trackerData.sequence_version[sequence].getFuture());
				reqBegin = std::max(ver, reqBegin);
				wait(yield());
			}
		} catch (Error& e) {
			if (e.code() == error_code_timed_out || e.code() == error_code_operation_obsolete) {
				replyPromise.sendError(e);
				return Void();
			} else {
				throw;
			}
		}
	}

	if (reqReturnIfBlocked && logData->version.get() < reqBegin) {
		replyPromise.sendError(end_of_stream());
		return Void();
	}

	//TraceEvent("TLogPeekMessages0", self->dbgid).detail("ReqBeginEpoch", reqBegin.epoch).detail("ReqBeginSeq", reqBegin.sequence).detail("Epoch", self->epoch()).detail("PersistentDataSeq", self->persistentDataSequence).detail("Tag1", reqTag1).detail("Tag2", reqTag2);
	// Wait until we have something to return that the caller doesn't already have
	if (logData->version.get() < reqBegin) {
		wait(logData->version.whenAtLeast(reqBegin));
		wait(delay(SERVER_KNOBS->TLOG_PEEK_DELAY, g_network->getCurrentTask()));
	}

	state Version endVersion = logData->version.get() + 1;

	Version poppedVer = poppedVersion(logData, oldTag);
	if (poppedVer > reqBegin) {
		TLogPeekReply rep;
		rep.maxKnownVersion = logData->version.get();
		rep.minKnownCommittedVersion = 0;
		rep.popped = poppedVer;
		rep.end = poppedVer;
		rep.onlySpilled = false;

		if (reqSequence.present()) {
			auto& trackerData = self->peekTracker[peekId];
			auto& sequenceData = trackerData.sequence_version[sequence + 1];
			trackerData.lastUpdate = now();
			if (trackerData.sequence_version.size() && sequence + 1 < trackerData.sequence_version.begin()->first) {
				replyPromise.sendError(operation_obsolete());
				if (!sequenceData.isSet())
					sequenceData.sendError(operation_obsolete());
				return Void();
			}
			if (sequenceData.isSet()) {
				if (sequenceData.getFuture().get() != rep.end) {
					CODE_PROBE(true, "tlog peek second attempt ended at a different version", probe::decoration::rare);
					replyPromise.sendError(operation_obsolete());
					return Void();
				}
			} else {
				sequenceData.send(rep.end);
			}
			rep.begin = reqBegin;
		}

		replyPromise.send(rep);
		return Void();
	}

	// grab messages from disk
	//TraceEvent("TLogPeekMessages", self->dbgid).detail("ReqBeginEpoch", reqBegin.epoch).detail("ReqBeginSeq", reqBegin.sequence).detail("Epoch", self->epoch()).detail("PersistentDataSeq", self->persistentDataSequence).detail("Tag1", reqTag1).detail("Tag2", reqTag2);
	if (reqBegin <= logData->persistentDataDurableVersion) {
		// Just in case the durable version changes while we are waiting for the read, we grab this data from memory. We
		// may or may not actually send it depending on whether we get enough data from disk. SOMEDAY: Only do this if
		// an initial attempt to read from disk results in insufficient data and the required data is no longer in
		// memory SOMEDAY: Should we only send part of the messages we collected, to actually limit the size of the
		// result?

		peekMessagesFromMemory(logData, reqTag, reqBegin, messages2, endVersion);

		RangeResult kvs = wait(self->persistentData->readRange(
		    KeyRangeRef(persistTagMessagesKey(logData->logId, oldTag, reqBegin),
		                persistTagMessagesKey(logData->logId, oldTag, logData->persistentDataDurableVersion + 1)),
		    SERVER_KNOBS->DESIRED_TOTAL_BYTES,
		    SERVER_KNOBS->DESIRED_TOTAL_BYTES));

		//TraceEvent("TLogPeekResults", self->dbgid).detail("ForAddress", replyPromise.getEndpoint().getPrimaryAddress()).detail("Tag1Results", s1).detail("Tag2Results", s2).detail("Tag1ResultsLim", kv1.size()).detail("Tag2ResultsLim", kv2.size()).detail("Tag1ResultsLast", kv1.size() ? kv1[0].key : "").detail("Tag2ResultsLast", kv2.size() ? kv2[0].key : "").detail("Limited", limited).detail("NextEpoch", next_pos.epoch).detail("NextSeq", next_pos.sequence).detail("NowEpoch", self->epoch()).detail("NowSeq", self->sequence.getNextSequence());

		for (auto& kv : kvs) {
			auto ver = decodeTagMessagesKey(kv.key);
			messages << int32_t(-1) << ver;

			BinaryReader rd(kv.value, Unversioned());
			while (!rd.empty()) {
				int32_t messageLength;
				uint32_t subVersion;
				rd >> messageLength >> subVersion;
				messageLength += sizeof(uint16_t) + sizeof(Tag);
				messages << messageLength << subVersion << uint16_t(1) << reqTag;
				messageLength -= (sizeof(subVersion) + sizeof(uint16_t) + sizeof(Tag));
				messages.serializeBytes(rd.readBytes(messageLength), messageLength);
			}
		}

		if (kvs.expectedSize() >= SERVER_KNOBS->DESIRED_TOTAL_BYTES)
			endVersion = decodeTagMessagesKey(kvs.end()[-1].key) + 1;
		else
			messages.serializeBytes(messages2.toValue());
	} else {
		peekMessagesFromMemory(logData, reqTag, reqBegin, messages, endVersion);
		//TraceEvent("TLogPeekResults", self->dbgid).detail("ForAddress", replyPromise.getEndpoint().getPrimaryAddress()).detail("MessageBytes", messages.getLength()).detail("NextEpoch", next_pos.epoch).detail("NextSeq", next_pos.sequence).detail("NowSeq", self->sequence.getNextSequence());
	}

	TLogPeekReply reply;
	reply.maxKnownVersion = logData->version.get();
	reply.minKnownCommittedVersion = 0;
	reply.onlySpilled = false;
	reply.messages = StringRef(reply.arena, messages.toValue());
	reply.end = endVersion;

	//TraceEvent("TlogPeek", self->dbgid).detail("LogId", logData->logId).detail("EndVer", reply.end).detail("MsgBytes", reply.messages.expectedSize()).detail("ForAddress", replyPromise.getEndpoint().getPrimaryAddress());

	if (reqSequence.present()) {
		auto& trackerData = self->peekTracker[peekId];
		trackerData.lastUpdate = now();
		auto& sequenceData = trackerData.sequence_version[sequence + 1];
		if (sequenceData.isSet()) {
			if (sequenceData.getFuture().get() != reply.end) {
				CODE_PROBE(true, "tlog peek second attempt ended at a different version (2)", probe::decoration::rare);
				replyPromise.sendError(operation_obsolete());
				return Void();
			}
		} else {
			sequenceData.send(reply.end);
		}
		reply.begin = reqBegin;
	}

	replyPromise.send(reply);
	return Void();
}

// This actor keep pushing TLogPeekStreamReply until it's removed from the cluster or should recover
ACTOR Future<Void> tLogPeekStream(TLogData* self, TLogPeekStreamRequest req, Reference<LogData> logData) {
	self->activePeekStreams++;

	state Version begin = req.begin;
	state bool onlySpilled = false;
	req.reply.setByteLimit(std::min(SERVER_KNOBS->MAXIMUM_PEEK_BYTES, req.limitBytes));
	loop {
		state TLogPeekStreamReply reply;
		state Promise<TLogPeekReply> promise;
		state Future<TLogPeekReply> future(promise.getFuture());
		try {
			wait(req.reply.onReady() && store(reply.rep, future) &&
			     tLogPeekMessages(promise, self, logData, begin, req.tag, req.returnIfBlocked, onlySpilled));

			reply.rep.begin = begin;
			req.reply.send(reply);
			begin = reply.rep.end;
			onlySpilled = reply.rep.onlySpilled;
			if (reply.rep.end > logData->version.get()) {
				wait(delay(SERVER_KNOBS->TLOG_PEEK_DELAY, g_network->getCurrentTask()));
			} else {
				wait(delay(0, g_network->getCurrentTask()));
			}
		} catch (Error& e) {
			self->activePeekStreams--;
			TraceEvent(SevDebug, "TLogPeekStreamEnd", logData->logId)
			    .errorUnsuppressed(e)
			    .detail("PeerAddr", req.reply.getEndpoint().getPrimaryAddress());

			if (e.code() == error_code_end_of_stream || e.code() == error_code_operation_obsolete) {
				req.reply.sendError(e);
				return Void();
			} else {
				throw;
			}
		}
	}
}

ACTOR Future<Void> doQueueCommit(TLogData* self, Reference<LogData> logData) {
	state Version ver = logData->version.get();
	state Version commitNumber = self->queueCommitBegin + 1;
	self->queueCommitBegin = commitNumber;
	logData->queueCommittingVersion = ver;

	Future<Void> c = self->persistentQueue->commit();
	self->diskQueueCommitBytes = 0;
	self->largeDiskQueueCommitBytes.set(false);

	wait(c);
	wait(self->queueCommitEnd.whenAtLeast(commitNumber - 1));

	// Calling check_yield instead of yield to avoid a destruction ordering problem in simulation
	if (g_network->check_yield(g_network->getCurrentTask())) {
		wait(delay(0, g_network->getCurrentTask()));
	}

	ASSERT(ver > logData->queueCommittedVersion.get());

	logData->queueCommittedVersion.set(ver);
	self->queueCommitEnd.set(commitNumber);

	//TraceEvent("TLogCommitDurable", self->dbgid).detail("Version", ver);

	return Void();
}

ACTOR Future<Void> commitQueue(TLogData* self) {
	state Reference<LogData> logData;

	loop {
		int foundCount = 0;
		for (auto it : self->id_data) {
			if (!it.second->stopped) {
				logData = it.second;
				foundCount++;
			}
		}

		ASSERT(foundCount < 2);
		if (foundCount == 0) {
			wait(self->newLogData.onTrigger());
			continue;
		}

		TraceEvent("CommitQueueNewLog", self->dbgid)
		    .detail("LogId", logData->logId)
		    .detail("Version", logData->version.get())
		    .detail("Committing", logData->queueCommittingVersion)
		    .detail("Commmitted", logData->queueCommittedVersion.get());

		loop {
			if (logData->stopped && logData->version.get() == std::max(logData->queueCommittingVersion,
			                                                           logData->queueCommittedVersion.get())) {
				wait(logData->queueCommittedVersion.whenAtLeast(logData->version.get()));
				break;
			}

			choose {
				when(wait(logData->version.whenAtLeast(
				    std::max(logData->queueCommittingVersion, logData->queueCommittedVersion.get()) + 1))) {
					while (self->queueCommitBegin != self->queueCommitEnd.get() &&
					       !self->largeDiskQueueCommitBytes.get()) {
						wait(self->queueCommitEnd.whenAtLeast(self->queueCommitBegin) ||
						     self->largeDiskQueueCommitBytes.onChange());
					}
					self->sharedActors.send(doQueueCommit(self, logData));
				}
				when(wait(self->newLogData.onTrigger())) {}
			}
		}
	}
}

ACTOR Future<Void> rejoinClusterController(TLogData* self,
                                           TLogInterface tli,
                                           DBRecoveryCount recoveryCount,
                                           Future<Void> registerWithCC) {
	state LifetimeToken lastMasterLifetime;
	loop {
		auto const& inf = self->dbInfo->get();
		bool isDisplaced =
		    !std::count(inf.priorCommittedLogServers.begin(), inf.priorCommittedLogServers.end(), tli.id());
		isDisplaced = isDisplaced && inf.recoveryCount >= recoveryCount &&
		              inf.recoveryState != RecoveryState::UNINITIALIZED && !inf.logSystemConfig.hasTLog(tli.id());
		if (isDisplaced) {
			TraceEvent("TLogDisplaced", tli.id())
			    .detail("Reason", "DBInfoDoesNotContain")
			    .detail("RecoveryCount", recoveryCount)
			    .detail("InfRecoveryCount", inf.recoveryCount)
			    .detail("RecoveryState", (int)inf.recoveryState)
			    .detail("LogSysConf", describe(inf.logSystemConfig.tLogs))
			    .detail("PriorLogs", describe(inf.priorCommittedLogServers))
			    .detail("OldLogGens", inf.logSystemConfig.oldTLogs.size());
			if (BUGGIFY)
				wait(delay(SERVER_KNOBS->BUGGIFY_WORKER_REMOVED_MAX_LAG * deterministicRandom()->random01()));
			throw worker_removed();
		}

		if (registerWithCC.isReady()) {
			if (!lastMasterLifetime.isEqual(self->dbInfo->get().masterLifetime)) {
				// The TLogRejoinRequest is needed to establish communications with a new master, which doesn't have our
				// TLogInterface
				TLogRejoinRequest req;
				req.myInterface = tli;
				TraceEvent("TLogRejoining", tli.id())
				    .detail("ClusterController", self->dbInfo->get().clusterInterface.id())
				    .detail("DbInfoMasterLifeTime", self->dbInfo->get().masterLifetime.toString())
				    .detail("LastMasterLifeTime", lastMasterLifetime.toString());
				choose {
					when(TLogRejoinReply rep = wait(
					         brokenPromiseToNever(self->dbInfo->get().clusterInterface.tlogRejoin.getReply(req)))) {
						if (rep.masterIsRecovered)
							lastMasterLifetime = self->dbInfo->get().masterLifetime;
					}
					when(wait(self->dbInfo->onChange())) {}
				}
			} else {
				wait(self->dbInfo->onChange());
			}
		} else {
			wait(registerWithCC || self->dbInfo->onChange());
		}
	}
}

ACTOR Future<Void> cleanupPeekTrackers(TLogData* self) {
	loop {
		double minTimeUntilExpiration = SERVER_KNOBS->PEEK_TRACKER_EXPIRATION_TIME;
		auto it = self->peekTracker.begin();
		while (it != self->peekTracker.end()) {
			double timeUntilExpiration = it->second.lastUpdate + SERVER_KNOBS->PEEK_TRACKER_EXPIRATION_TIME - now();
			if (timeUntilExpiration < 1.0e-6) {
				for (auto seq : it->second.sequence_version) {
					if (!seq.second.isSet()) {
						seq.second.sendError(timed_out());
					}
				}
				it = self->peekTracker.erase(it);
			} else {
				minTimeUntilExpiration = std::min(minTimeUntilExpiration, timeUntilExpiration);
				++it;
			}
		}

		wait(delay(minTimeUntilExpiration));
	}
}

void getQueuingMetrics(TLogData* self, TLogQueuingMetricsRequest const& req) {
	TLogQueuingMetricsReply reply;
	reply.localTime = now();
	reply.instanceID = self->instanceID;
	reply.bytesInput = self->bytesInput;
	reply.bytesDurable = self->bytesDurable;
	reply.storageBytes = self->persistentData->getStorageBytes();
	reply.kcv = self->prevVersion;
	req.reply.send(reply);
}

ACTOR Future<Void> serveTLogInterface(TLogData* self,
                                      TLogInterface tli,
                                      Reference<LogData> logData,
                                      PromiseStream<Void> warningCollectorInput) {
	loop choose {
		when(TLogPeekRequest req = waitNext(tli.peekMessages.getFuture())) {
			logData->addActor.send(tLogPeekMessages(
			    req.reply, self, logData, req.begin, req.tag, req.returnIfBlocked, req.onlySpilled, req.sequence));
		}
		when(TLogPeekStreamRequest req = waitNext(tli.peekStreamMessages.getFuture())) {
			TraceEvent(SevDebug, "TLogPeekStream", logData->logId)
			    .detail("Token", tli.peekStreamMessages.getEndpoint().token);
			logData->addActor.send(tLogPeekStream(self, req, logData));
		}
		when(TLogPopRequest req = waitNext(tli.popMessages.getFuture())) {
			logData->addActor.send(tLogPop(self, req, logData));
		}
		when(TLogCommitRequest req = waitNext(tli.commit.getFuture())) {
			ASSERT(logData->stopped);
			req.reply.sendError(tlog_stopped());
		}
		when(ReplyPromise<TLogLockResult> reply = waitNext(tli.lock.getFuture())) {
			logData->addActor.send(tLogLock(self, reply, logData));
		}
		when(TLogQueuingMetricsRequest req = waitNext(tli.getQueuingMetrics.getFuture())) {
			getQueuingMetrics(self, req);
		}
		when(TLogConfirmRunningRequest req = waitNext(tli.confirmRunning.getFuture())) {
			if (req.debugID.present()) {
				UID tlogDebugID = nondeterministicRandom()->randomUniqueID();
				g_traceBatch.addAttach("TransactionAttachID", req.debugID.get().first(), tlogDebugID.first());
				g_traceBatch.addEvent("TransactionDebug", tlogDebugID.first(), "TLogServer.TLogConfirmRunningRequest");
			}
			ASSERT(logData->stopped);
			req.reply.sendError(tlog_stopped());
		}
	}
}

void removeLog(TLogData* self, Reference<LogData> logData) {
	TraceEvent("TLogRemoved", logData->logId)
	    .detail("Input", logData->bytesInput.getValue())
	    .detail("Durable", logData->bytesDurable.getValue());
	logData->stopped = true;
	if (logData->recoverySuccessful.canBeSet()) {
		logData->recoverySuccessful.send(false);
	}

	logData->addActor = PromiseStream<Future<Void>>(); // there could be items still in the promise stream if one of the
	                                                   // actors threw an error immediately
	self->id_data.erase(logData->logId);

	if (self->id_data.size()) {
		return;
	} else {
		throw worker_removed();
	}
}

ACTOR Future<Void> tLogCore(TLogData* self, Reference<LogData> logData) {
	if (logData->removed.isReady()) {
		wait(delay(0)); // to avoid iterator invalidation in restorePersistentState when removed is already ready
		ASSERT(logData->removed.isError());

		if (logData->removed.getError().code() != error_code_worker_removed) {
			throw logData->removed.getError();
		}

		removeLog(self, logData);
		return Void();
	}

	TraceEvent("NewLogData", self->dbgid).detail("LogId", logData->logId);
	logData->initialized = true;
	self->newLogData.trigger();

	state PromiseStream<Void> warningCollectorInput;
	state Future<Void> warningCollector =
	    timeoutWarningCollector(warningCollectorInput.getFuture(), 1.0, "TLogQueueCommitSlow", self->dbgid);
	state Future<Void> error = actorCollection(logData->addActor.getFuture());

	logData->addActor.send(logData->recovery);
	logData->addActor.send(waitFailureServer(logData->tli.waitFailure.getFuture()));
	logData->addActor.send(logData->removed);
	// FIXME: update tlogMetrics to include new information, or possibly only have one copy for the shared instance
	logData->addActor.send(logData->cc.traceCounters("TLogMetrics",
	                                                 logData->logId,
	                                                 SERVER_KNOBS->STORAGE_LOGGING_DELAY,
	                                                 logData->logId.toString() + "/TLogMetrics",
	                                                 [self = self](TraceEvent& te) {
		                                                 StorageBytes sbTlog = self->persistentData->getStorageBytes();
		                                                 te.detail("KvstoreBytesUsed", sbTlog.used);
		                                                 te.detail("KvstoreBytesFree", sbTlog.free);
		                                                 te.detail("KvstoreBytesAvailable", sbTlog.available);
		                                                 te.detail("KvstoreBytesTotal", sbTlog.total);
		                                                 te.detail("KvstoreBytesTemp", sbTlog.temp);

		                                                 StorageBytes sbQueue =
		                                                     self->rawPersistentQueue->getStorageBytes();
		                                                 te.detail("QueueDiskBytesUsed", sbQueue.used);
		                                                 te.detail("QueueDiskBytesFree", sbQueue.free);
		                                                 te.detail("QueueDiskBytesAvailable", sbQueue.available);
		                                                 te.detail("QueueDiskBytesTotal", sbQueue.total);
		                                                 te.detail("QueueDiskBytesTemp", sbQueue.temp);
	                                                 }));

	logData->addActor.send(serveTLogInterface(self, logData->tli, logData, warningCollectorInput));

	try {
		wait(error);
		throw internal_error();
	} catch (Error& e) {
		if (e.code() != error_code_worker_removed)
			throw;

		removeLog(self, logData);
		return Void();
	}
}

ACTOR Future<Void> restorePersistentState(TLogData* self, LocalityData locality) {
	state double startt = now();
	state Reference<LogData> logData;
	state KeyRange tagKeys;
	// PERSIST: Read basic state from persistentData; replay persistentQueue but don't erase it

	TraceEvent("TLogRestorePersistentState", self->dbgid).log();

	IKeyValueStore* storage = self->persistentData;
	state Future<Optional<Value>> fFormat = storage->readValue(persistFormat.key);
	state Future<RangeResult> fVers = storage->readRange(persistCurrentVersionKeys);
	state Future<RangeResult> fRecoverCounts = storage->readRange(persistRecoveryCountKeys);

	// FIXME: metadata in queue?

	wait(waitForAll(std::vector{ fFormat }));
	wait(waitForAll(std::vector{ fVers, fRecoverCounts }));

	if (fFormat.get().present() && !persistFormatReadableRange.contains(fFormat.get().get())) {
		TraceEvent(SevError, "UnsupportedDBFormat", self->dbgid)
		    .detail("Format", fFormat.get().get())
		    .detail("Expected", persistFormat.value.toString());
		throw worker_recovery_failed();
	}

	if (!fFormat.get().present()) {
		RangeResult v = wait(self->persistentData->readRange(KeyRangeRef(StringRef(), "\xff"_sr), 1));
		if (!v.size()) {
			CODE_PROBE(
			    true, "The DB is completely empty, so it was never initialized.  Delete it.", probe::decoration::rare);
			throw worker_removed();
		} else {
			// This should never happen
			TraceEvent(SevError, "NoDBFormatKey", self->dbgid).detail("FirstKey", v[0].key);
			ASSERT(false);
			throw worker_recovery_failed();
		}
	}

	state std::vector<Future<ErrorOr<Void>>> removed;

	ASSERT(fVers.get().size() == fRecoverCounts.get().size());

	state int idx = 0;
	state Promise<Void> registerWithCC;
	for (idx = 0; idx < fVers.get().size(); idx++) {
		state KeyRef rawId = fVers.get()[idx].key.removePrefix(persistCurrentVersionKeys.begin);
		UID id1 = BinaryReader::fromStringRef<UID>(rawId, Unversioned());
		UID id2 = BinaryReader::fromStringRef<UID>(
		    fRecoverCounts.get()[idx].key.removePrefix(persistRecoveryCountKeys.begin), Unversioned());
		ASSERT(id1 == id2);

		TLogInterface recruited(id1, self->dbgid, locality);
		recruited.initEndpoints();

		DUMPTOKEN(recruited.peekMessages);
		DUMPTOKEN(recruited.peekStreamMessages);
		DUMPTOKEN(recruited.popMessages);
		DUMPTOKEN(recruited.commit);
		DUMPTOKEN(recruited.lock);
		DUMPTOKEN(recruited.getQueuingMetrics);
		DUMPTOKEN(recruited.confirmRunning);
		DUMPTOKEN(recruited.waitFailure);
		DUMPTOKEN(recruited.recoveryFinished);
		DUMPTOKEN(recruited.disablePopRequest);
		DUMPTOKEN(recruited.enablePopRequest);
		DUMPTOKEN(recruited.snapRequest);

		logData = Reference<LogData>(new LogData(self, recruited));
		logData->stopped = true;
		self->id_data[id1] = logData;

		Version ver = BinaryReader::fromStringRef<Version>(fVers.get()[idx].value, Unversioned());
		logData->persistentDataVersion = ver;
		logData->persistentDataDurableVersion = ver;
		logData->version.set(ver);
		logData->recoveryCount =
		    BinaryReader::fromStringRef<DBRecoveryCount>(fRecoverCounts.get()[idx].value, Unversioned());
		logData->removed = rejoinClusterController(self, recruited, logData->recoveryCount, registerWithCC.getFuture());
		removed.push_back(errorOr(logData->removed));

		TraceEvent("TLogRestorePersistentStateVer", id1).detail("Ver", ver);

		// Restore popped keys.  Pop operations that took place after the last (committed) updatePersistentDataVersion
		// might be lost, but that is fine because we will get the corresponding data back, too.
		tagKeys = prefixRange(rawId.withPrefix(persistTagPoppedKeys.begin));
		loop {
			if (logData->removed.isReady())
				break;
			RangeResult data = wait(self->persistentData->readRange(tagKeys, BUGGIFY ? 3 : 1 << 30, 1 << 20));
			if (!data.size())
				break;
			((KeyRangeRef&)tagKeys) = KeyRangeRef(keyAfter(data.back().key, tagKeys.arena()), tagKeys.end);

			for (auto& kv : data) {
				OldTag tag = decodeTagPoppedKey(rawId, kv.key);
				Version popped = decodeTagPoppedValue(kv.value);
				TraceEvent("TLogRestorePop", logData->logId).detail("Tag", tag).detail("To", popped);
				ASSERT(logData->tag_data.find(tag) == logData->tag_data.end());
				logData->tag_data.insert(mapPair(tag, LogData::TagData(popped, false, false, tag)));
			}
		}
	}

	state Future<Void> allRemoved = waitForAll(removed);
	state Version lastVer = 0;
	state UID lastId = UID(1, 1); // initialized so it will not compare equal to a default UID
	state double recoverMemoryLimit = SERVER_KNOBS->TARGET_BYTES_PER_TLOG + SERVER_KNOBS->SPRING_BYTES_TLOG;
	if (BUGGIFY)
		recoverMemoryLimit =
		    std::max<double>(SERVER_KNOBS->BUGGIFY_RECOVER_MEMORY_LIMIT, SERVER_KNOBS->TLOG_SPILL_THRESHOLD);

	try {
		loop {
			if (allRemoved.isReady()) {
				CODE_PROBE(true, "all tlogs removed during queue recovery", probe::decoration::rare);
				throw worker_removed();
			}
			choose {
				when(TLogQueueEntry qe = wait(self->persistentQueue->readNext())) {
					if (!self->queueOrder.size() || self->queueOrder.back() != qe.id)
						self->queueOrder.push_back(qe.id);
					if (qe.id != lastId) {
						lastId = qe.id;
						auto it = self->id_data.find(qe.id);
						if (it != self->id_data.end()) {
							logData = it->second;
						} else {
							logData = Reference<LogData>();
						}
					} else {
						ASSERT(qe.version >= lastVer);
						lastVer = qe.version;
					}

					//TraceEvent("TLogRecoveredQE", self->dbgid).detail("LogId", qe.id).detail("Ver", qe.version).detail("MessageBytes", qe.messages.size()).detail("Tags", qe.tags.size())
					//	.detail("Tag0", qe.tags.size() ? qe.tags[0].tag : invalidTag).detail("Version",
					// logData->version.get());

					if (logData) {
						logData->knownCommittedVersion =
						    std::max(logData->knownCommittedVersion, qe.knownCommittedVersion);
						if (qe.version > logData->version.get()) {
							commitMessages(logData, qe.version, qe.arena(), qe.messages, qe.tags, self->bytesInput);
							logData->version.set(qe.version);
							logData->queueCommittedVersion.set(qe.version);

							while (self->bytesInput - self->bytesDurable >= recoverMemoryLimit) {
								CODE_PROBE(
								    true, "Flush excess data during TLog queue recovery", probe::decoration::rare);
								TraceEvent("FlushLargeQueueDuringRecovery", self->dbgid)
								    .detail("BytesInput", self->bytesInput)
								    .detail("BytesDurable", self->bytesDurable)
								    .detail("Version", logData->version.get())
								    .detail("PVer", logData->persistentDataVersion);

								choose {
									when(wait(updateStorage(self))) {}
									when(wait(allRemoved)) {
										throw worker_removed();
									}
								}
							}
						}
					}
				}
				when(wait(allRemoved)) {
					throw worker_removed();
				}
			}
		}
	} catch (Error& e) {
		if (e.code() != error_code_end_of_stream)
			throw;
	}

	TraceEvent("TLogRestorePersistentStateDone", self->dbgid).detail("Took", now() - startt);
	CODE_PROBE(now() - startt >= 1.0, "TLog recovery took more than 1 second");

	for (auto it : self->id_data) {
		if (it.second->queueCommittedVersion.get() == 0) {
			TraceEvent("TLogZeroVersion", self->dbgid).detail("LogId", it.first);
			it.second->queueCommittedVersion.set(it.second->version.get());
		}
		self->sharedActors.send(tLogCore(self, it.second));
	}

	if (registerWithCC.canBeSet())
		registerWithCC.send(Void());
	return Void();
}

ACTOR Future<Void> tLog(IKeyValueStore* persistentData,
                        IDiskQueue* persistentQueue,
                        Reference<AsyncVar<ServerDBInfo> const> db,
                        LocalityData locality,
                        UID tlogId,
                        UID workerID) {
	state TLogData self(tlogId, workerID, persistentData, persistentQueue, db);
	state Future<Void> error = actorCollection(self.sharedActors.getFuture());

	TraceEvent("SharedTlog", tlogId).detail("Version", "4.6");

	try {
		wait(ioTimeoutError(persistentData->init(), SERVER_KNOBS->TLOG_MAX_CREATE_DURATION, "TLogInit"));
		wait(restorePersistentState(&self, locality));

		self.sharedActors.send(cleanupPeekTrackers(&self));
		self.sharedActors.send(commitQueue(&self));
		self.sharedActors.send(updateStorageLoop(&self));

		wait(error);
		throw internal_error();
	} catch (Error& e) {
		TraceEvent("TLogError", tlogId).errorUnsuppressed(e);

		for (auto& it : self.id_data) {
			if (it.second->recoverySuccessful.canBeSet()) {
				it.second->recoverySuccessful.send(false);
			}
		}

		throw;
	}
}
} // namespace oldTLog_4_6
