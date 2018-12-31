/*
 * TLogServer.actor.cpp
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

#include "flow/Hash3.h"
#include "flow/Stats.h"
#include "flow/UnitTest.h"
#include "fdbclient/NativeAPI.h"
#include "fdbclient/Notified.h"
#include "fdbclient/KeyRangeMap.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/WorkerInterface.h"
#include "fdbserver/TLogInterface.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/IKeyValueStore.h"
#include "flow/ActorCollection.h"
#include "fdbrpc/FailureMonitor.h"
#include "fdbserver/IDiskQueue.h"
#include "fdbrpc/sim_validation.h"
#include "fdbserver/ServerDBInfo.h"
#include "fdbserver/LogSystem.h"
#include "fdbserver/WaitFailure.h"
#include "fdbserver/RecoveryState.h"
#include "flow/actorcompiler.h"  // This must be the last #include.

using std::pair;
using std::make_pair;
using std::min;
using std::max;

struct TLogQueueEntryRef {
	UID id;
	Version version;
	Version knownCommittedVersion;
	StringRef messages;

	TLogQueueEntryRef() : version(0), knownCommittedVersion(0) {}
	TLogQueueEntryRef(Arena &a, TLogQueueEntryRef const &from)
	  : version(from.version), knownCommittedVersion(from.knownCommittedVersion), id(from.id), messages(a, from.messages) {
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, version, messages, knownCommittedVersion, id);
	}
	size_t expectedSize() const {
		return messages.expectedSize();
	}
};

struct AlternativeTLogQueueEntryRef {
	UID id;
	Version version;
	Version knownCommittedVersion;
	std::vector<TagsAndMessage>* alternativeMessages;

	AlternativeTLogQueueEntryRef() : version(0), knownCommittedVersion(0), alternativeMessages(NULL) {}

	template <class Ar>
	void serialize(Ar& ar) {
		ASSERT(!ar.isDeserializing && alternativeMessages);
		uint32_t msgSize = expectedSize();
		serializer(ar, version, msgSize);
		for(auto& msg : *alternativeMessages) {
			ar.serializeBytes( msg.message );
		}
		serializer(ar, knownCommittedVersion, id);
	}

	uint32_t expectedSize() const {
		uint32_t msgSize = 0;
		for(auto& msg : *alternativeMessages) {
			msgSize += msg.message.size();
		}
		return msgSize;
	}
};

typedef Standalone<TLogQueueEntryRef> TLogQueueEntry;
struct LogData;
struct TLogData;

struct TLogQueue : public IClosable {
public:
	TLogQueue( IDiskQueue* queue, UID dbgid ) : queue(queue), dbgid(dbgid) {}

	// Each packet in the queue is
	//    uint32_t payloadSize
	//    uint8_t payload[payloadSize]  (begins with uint64_t protocolVersion via IncludeVersion)
	//    uint8_t validFlag

	// TLogQueue is a durable queue of TLogQueueEntry objects with an interface similar to IDiskQueue

	// TLogQueue pushes (but not commits) are atomic - after commit fails to return, a prefix of entire calls to push are durable.  This is
	//    implemented on top of the weaker guarantee of IDiskQueue::commit (that a prefix of bytes is durable) using validFlag and by
	//    padding any incomplete packet with zeros after recovery.

	// Before calling push, pop, or commit, the user must call readNext() until it throws
	//    end_of_stream(). It may not be called again thereafter.
	Future<TLogQueueEntry> readNext( TLogData* tLog ) {
		return readNext( this, tLog );
	}

	template <class T>
	void push( T const& qe, Reference<LogData> logData );
	void pop( Version upTo, Reference<LogData> logData );
	Future<Void> commit() { return queue->commit(); }

	// Implements IClosable
	virtual Future<Void> getError() { return queue->getError(); }
	virtual Future<Void> onClosed() { return queue->onClosed(); }
	virtual void dispose() { queue->dispose(); delete this; }
	virtual void close() { queue->close(); delete this; }

private:
	IDiskQueue* queue;
	UID dbgid;

	void updateVersionSizes( const TLogQueueEntry& result, TLogData* tLog );

	ACTOR static Future<TLogQueueEntry> readNext( TLogQueue* self, TLogData* tLog ) {
		state TLogQueueEntry result;
		state int zeroFillSize = 0;

		loop {
			Standalone<StringRef> h = wait( self->queue->readNext( sizeof(uint32_t) ) );
			if (h.size() != sizeof(uint32_t)) {
				if (h.size()) {
					TEST( true );  // Zero fill within size field
					int payloadSize = 0;
					memcpy(&payloadSize, h.begin(), h.size());
					zeroFillSize = sizeof(uint32_t)-h.size(); // zero fill the size itself
					zeroFillSize += payloadSize+1;  // and then the contents and valid flag
				}
				break;
			}

			state uint32_t payloadSize = *(uint32_t*)h.begin();
			ASSERT( payloadSize < (100<<20) );

			Standalone<StringRef> e = wait( self->queue->readNext( payloadSize+1 ) );
			if (e.size() != payloadSize+1) {
				TEST( true ); // Zero fill within payload
				zeroFillSize = payloadSize+1 - e.size();
				break;
			}

			if (e[payloadSize]) {
				Arena a = e.arena();
				ArenaReader ar( a, e.substr(0, payloadSize), IncludeVersion() );
				ar >> result;
				self->updateVersionSizes(result, tLog);
				return result;
			}
		}
		if (zeroFillSize) {
			TEST( true );  // Fixing a partial commit at the end of the tlog queue
			for(int i=0; i<zeroFillSize; i++)
				self->queue->push( StringRef((const uint8_t*)"",1) );
		}
		throw end_of_stream();
	}
};

////// Persistence format (for self->persistentData)

// Immutable keys
static const KeyValueRef persistFormat( LiteralStringRef( "Format" ), LiteralStringRef("FoundationDB/LogServer/2/4") );
static const KeyRangeRef persistFormatReadableRange( LiteralStringRef("FoundationDB/LogServer/2/3"), LiteralStringRef("FoundationDB/LogServer/2/5") );
static const KeyRangeRef persistRecoveryCountKeys = KeyRangeRef( LiteralStringRef( "DbRecoveryCount/" ), LiteralStringRef( "DbRecoveryCount0" ) );

// Updated on updatePersistentData()
static const KeyRangeRef persistCurrentVersionKeys = KeyRangeRef( LiteralStringRef( "version/" ), LiteralStringRef( "version0" ) );
static const KeyRangeRef persistKnownCommittedVersionKeys = KeyRangeRef( LiteralStringRef( "knownCommitted/" ), LiteralStringRef( "knownCommitted0" ) );
static const KeyRangeRef persistLocalityKeys = KeyRangeRef( LiteralStringRef( "Locality/" ), LiteralStringRef( "Locality0" ) );
static const KeyRangeRef persistLogRouterTagsKeys = KeyRangeRef( LiteralStringRef( "LogRouterTags/" ), LiteralStringRef( "LogRouterTags0" ) );
static const KeyRange persistTagMessagesKeys = prefixRange(LiteralStringRef("TagMsg/"));
static const KeyRange persistTagPoppedKeys = prefixRange(LiteralStringRef("TagPop/"));

static Key persistTagMessagesKey( UID id, Tag tag, Version version ) {
	BinaryWriter wr( Unversioned() );
	wr.serializeBytes(persistTagMessagesKeys.begin);
	wr << id;
	wr << tag;
	wr << bigEndian64( version );
	return wr.toStringRef();
}

static Key persistTagPoppedKey( UID id, Tag tag ) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes( persistTagPoppedKeys.begin );
	wr << id;
	wr << tag;
	return wr.toStringRef();
}

static Value persistTagPoppedValue( Version popped ) {
	return BinaryWriter::toValue( popped, Unversioned() );
}

static Tag decodeTagPoppedKey( KeyRef id, KeyRef key ) {
	Tag s;
	BinaryReader rd( key.removePrefix(persistTagPoppedKeys.begin).removePrefix(id), Unversioned() );
	rd >> s;
	return s;
}

static Version decodeTagPoppedValue( ValueRef value ) {
	return BinaryReader::fromStringRef<Version>( value, Unversioned() );
}

static StringRef stripTagMessagesKey( StringRef key ) {
	return key.substr( sizeof(UID) + sizeof(Tag) + persistTagMessagesKeys.begin.size() );
}

static Version decodeTagMessagesKey( StringRef key ) {
	return bigEndian64( BinaryReader::fromStringRef<Version>( stripTagMessagesKey(key), Unversioned() ) );
}

struct TLogData : NonCopyable {
	AsyncTrigger newLogData;
	Deque<UID> queueOrder;
	std::map<UID, Reference<struct LogData>> id_data;

	UID dbgid;

	IKeyValueStore* persistentData;
	IDiskQueue* rawPersistentQueue;
	TLogQueue *persistentQueue;

	int64_t diskQueueCommitBytes;
	AsyncVar<bool> largeDiskQueueCommitBytes; //becomes true when diskQueueCommitBytes is greater than MAX_QUEUE_COMMIT_BYTES

	Reference<AsyncVar<ServerDBInfo>> dbInfo;

	NotifiedVersion queueCommitEnd;
	Version queueCommitBegin;

	int64_t instanceID;
	int64_t bytesInput;
	int64_t bytesDurable;
	int64_t overheadBytesInput;
	int64_t overheadBytesDurable;

	struct PeekTrackerData {
		std::map<int, Promise<Version>> sequence_version;
		double lastUpdate;
	};

	std::map<UID, PeekTrackerData> peekTracker;
	WorkerCache<TLogInterface> tlogCache;

	PromiseStream<Future<Void>> sharedActors;
	Promise<Void> terminated;
	FlowLock concurrentLogRouterReads;
	FlowLock persistentDataCommitLock;

	TLogData(UID dbgid, IKeyValueStore* persistentData, IDiskQueue * persistentQueue, Reference<AsyncVar<ServerDBInfo>> const& dbInfo)
			: dbgid(dbgid), instanceID(g_random->randomUniqueID().first()),
			  persistentData(persistentData), rawPersistentQueue(persistentQueue), persistentQueue(new TLogQueue(persistentQueue, dbgid)),
			  dbInfo(dbInfo), queueCommitBegin(0), queueCommitEnd(0),
			  diskQueueCommitBytes(0), largeDiskQueueCommitBytes(false), bytesInput(0), bytesDurable(0), overheadBytesInput(0), overheadBytesDurable(0),
			  concurrentLogRouterReads(SERVER_KNOBS->CONCURRENT_LOG_ROUTER_READS)
		{
		}
};

struct LogData : NonCopyable, public ReferenceCounted<LogData> {
	struct TagData : NonCopyable, public ReferenceCounted<TagData> {
		std::deque<std::pair<Version, LengthPrefixedStringRef>> versionMessages;
		bool nothingPersistent;				// true means tag is *known* to have no messages in persistentData.  false means nothing.
		bool poppedRecently;					// `popped` has changed since last updatePersistentData
		Version popped;				// see popped version tracking contract below
		bool unpoppedRecovered;
		Tag tag;

		TagData( Tag tag, Version popped, bool nothingPersistent, bool poppedRecently, bool unpoppedRecovered ) : tag(tag), nothingPersistent(nothingPersistent), popped(popped), poppedRecently(poppedRecently), unpoppedRecovered(unpoppedRecovered) {}

		TagData(TagData&& r) noexcept(true) : versionMessages(std::move(r.versionMessages)), nothingPersistent(r.nothingPersistent), poppedRecently(r.poppedRecently), popped(r.popped), tag(r.tag), unpoppedRecovered(r.unpoppedRecovered) {}
		void operator= (TagData&& r) noexcept(true) {
			versionMessages = std::move(r.versionMessages);
			nothingPersistent = r.nothingPersistent;
			poppedRecently = r.poppedRecently;
			popped = r.popped;
			tag = r.tag;
			unpoppedRecovered = r.unpoppedRecovered;
		}

		// Erase messages not needed to update *from* versions >= before (thus, messages with toversion <= before)
		ACTOR Future<Void> eraseMessagesBefore( TagData *self, Version before, TLogData *tlogData, Reference<LogData> logData, int taskID ) {
			while(!self->versionMessages.empty() && self->versionMessages.front().first < before) {
				Version version = self->versionMessages.front().first;
				std::pair<int,int> &sizes = logData->version_sizes[version];
				int64_t messagesErased = 0;

				while(!self->versionMessages.empty() && self->versionMessages.front().first == version) {
					auto const& m = self->versionMessages.front();
					++messagesErased;

					if(self->tag != txsTag) {
						sizes.first -= m.second.expectedSize();
					} else {
						sizes.second -= m.second.expectedSize();
					}

					self->versionMessages.pop_front();
				}

				int64_t bytesErased = messagesErased * SERVER_KNOBS->VERSION_MESSAGES_ENTRY_BYTES_WITH_OVERHEAD;
				logData->bytesDurable += bytesErased;
				tlogData->bytesDurable += bytesErased;
				tlogData->overheadBytesDurable += bytesErased;
				wait(yield(taskID));
			}

			return Void();
		}

		Future<Void> eraseMessagesBefore(Version before, TLogData *tlogData, Reference<LogData> logData, int taskID) {
			return eraseMessagesBefore(this, before, tlogData, logData, taskID);
		}
	};

	Map<Version, IDiskQueue::location> versionLocation;  // For the version of each entry that was push()ed, the end location of the serialized bytes

	/*
	Popped version tracking contract needed by log system to implement ILogCursor::popped():

		- Log server tracks for each (possible) tag a popped_version
		Impl: TagData::popped (in memory) and persistTagPoppedKeys (in persistentData)
		- popped_version(tag) is <= the maximum version for which log server (or a predecessor) is ever asked to pop the tag
		Impl: Only increased by tLogPop() in response to either a pop request or recovery from a predecessor
		- popped_version(tag) is > the maximum version for which log server is unable to peek messages due to previous pops (on this server or a predecessor)
		Impl: Increased by tLogPop() atomically with erasing messages from memory; persisted by updatePersistentData() atomically with erasing messages from store; messages are not erased from queue where popped_version is not persisted
		- LockTLogReply returns all tags which either have messages, or which have nonzero popped_versions
		Impl: tag_data is present for all such tags
		- peek(tag, v) returns the popped_version for tag if that is greater than v
		Impl: Check tag_data->popped (after all waits)
	*/

	AsyncTrigger stopCommit;
	bool stopped, initialized;
	DBRecoveryCount recoveryCount;

	VersionMetricHandle persistentDataVersion, persistentDataDurableVersion;  // The last version number in the portion of the log (written|durable) to persistentData
	NotifiedVersion version, queueCommittedVersion;
	Version queueCommittingVersion;
	Version knownCommittedVersion, durableKnownCommittedVersion, minKnownCommittedVersion;

	Deque<std::pair<Version, Standalone<VectorRef<uint8_t>>>> messageBlocks;
	std::vector<std::vector<Reference<TagData>>> tag_data; //tag.locality | tag.id
	int unpoppedRecoveredTags;

	Reference<TagData> getTagData(Tag tag) {
		int idx = tag.locality >= 0 ? 2*tag.locality : 1-(2*tag.locality);
		if(idx >= tag_data.size()) {
			tag_data.resize(idx+1);
		}
		if(tag.id >= tag_data[idx].size()) {
			tag_data[idx].resize(tag.id+1);
		}
		return tag_data[idx][tag.id];
	}

	//only callable after getTagData returns a null reference
	Reference<TagData> createTagData(Tag tag, Version popped, bool nothingPersistent, bool poppedRecently, bool unpoppedRecovered) {
		if(tag.locality != tagLocalityLogRouter && allTags.size() && !allTags.count(tag) && popped <= recoveredAt) {
			popped = recoveredAt + 1;
		}
		Reference<TagData> newTagData = Reference<TagData>( new TagData(tag, popped, nothingPersistent, poppedRecently, unpoppedRecovered) );
		int idx = tag.locality >= 0 ? 2*tag.locality : 1-(2*tag.locality);
		tag_data[idx][tag.id] = newTagData;
		return newTagData;
	}

	Map<Version, std::pair<int,int>> version_sizes;

	CounterCollection cc;
	Counter bytesInput;
	Counter bytesDurable;

	UID logId;
	Version newPersistentDataVersion;
	Future<Void> removed;
	PromiseStream<Future<Void>> addActor;
	TLogData* tLogData;
	Promise<Void> recoveryComplete, committingQueue;
	Version unrecoveredBefore, recoveredAt;

	Reference<AsyncVar<Reference<ILogSystem>>> logSystem;
	Tag remoteTag;
	bool isPrimary;
	int logRouterTags;
	Version logRouterPoppedVersion, logRouterPopToVersion;
	int8_t locality;
	UID recruitmentID;
	std::set<Tag> allTags;
	Future<Void> terminated;

	explicit LogData(TLogData* tLogData, TLogInterface interf, Tag remoteTag, bool isPrimary, int logRouterTags, UID recruitmentID, std::vector<Tag> tags) : tLogData(tLogData), knownCommittedVersion(0), logId(interf.id()),
			cc("TLog", interf.id().toString()), bytesInput("BytesInput", cc), bytesDurable("BytesDurable", cc), remoteTag(remoteTag), isPrimary(isPrimary), logRouterTags(logRouterTags), recruitmentID(recruitmentID),
			logSystem(new AsyncVar<Reference<ILogSystem>>()), logRouterPoppedVersion(0), durableKnownCommittedVersion(0), minKnownCommittedVersion(0), allTags(tags.begin(), tags.end()), terminated(tLogData->terminated.getFuture()),
			// These are initialized differently on init() or recovery
			recoveryCount(), stopped(false), initialized(false), queueCommittingVersion(0), newPersistentDataVersion(invalidVersion), unrecoveredBefore(1), recoveredAt(1), unpoppedRecoveredTags(0),
			logRouterPopToVersion(0), locality(tagLocalityInvalid)
	{
		startRole(Role::TRANSACTION_LOG, interf.id(), UID());

		persistentDataVersion.init(LiteralStringRef("TLog.PersistentDataVersion"), cc.id);
		persistentDataDurableVersion.init(LiteralStringRef("TLog.PersistentDataDurableVersion"), cc.id);
		version.initMetric(LiteralStringRef("TLog.Version"), cc.id);
		queueCommittedVersion.initMetric(LiteralStringRef("TLog.QueueCommittedVersion"), cc.id);

		specialCounter(cc, "Version", [this](){ return this->version.get(); });
		specialCounter(cc, "SharedBytesInput", [tLogData](){ return tLogData->bytesInput; });
		specialCounter(cc, "SharedBytesDurable", [tLogData](){ return tLogData->bytesDurable; });
		specialCounter(cc, "SharedOverheadBytesInput", [tLogData](){ return tLogData->overheadBytesInput; });
		specialCounter(cc, "SharedOverheadBytesDurable", [tLogData](){ return tLogData->overheadBytesDurable; });
		specialCounter(cc, "KvstoreBytesUsed", [tLogData](){ return tLogData->persistentData->getStorageBytes().used; });
		specialCounter(cc, "KvstoreBytesFree", [tLogData](){ return tLogData->persistentData->getStorageBytes().free; });
		specialCounter(cc, "KvstoreBytesAvailable", [tLogData](){ return tLogData->persistentData->getStorageBytes().available; });
		specialCounter(cc, "KvstoreBytesTotal", [tLogData](){ return tLogData->persistentData->getStorageBytes().total; });
		specialCounter(cc, "QueueDiskBytesUsed", [tLogData](){ return tLogData->rawPersistentQueue->getStorageBytes().used; });
		specialCounter(cc, "QueueDiskBytesFree", [tLogData](){ return tLogData->rawPersistentQueue->getStorageBytes().free; });
		specialCounter(cc, "QueueDiskBytesAvailable", [tLogData](){ return tLogData->rawPersistentQueue->getStorageBytes().available; });
		specialCounter(cc, "QueueDiskBytesTotal", [tLogData](){ return tLogData->rawPersistentQueue->getStorageBytes().total; });
	}

	~LogData() {
		endRole(Role::TRANSACTION_LOG, logId, "Error", true);

		if(!terminated.isReady()) {
			tLogData->bytesDurable += bytesInput.getValue() - bytesDurable.getValue();
			TraceEvent("TLogBytesWhenRemoved", logId).detail("SharedBytesInput", tLogData->bytesInput).detail("SharedBytesDurable", tLogData->bytesDurable).detail("LocalBytesInput", bytesInput.getValue()).detail("LocalBytesDurable", bytesDurable.getValue());

			ASSERT_ABORT(tLogData->bytesDurable <= tLogData->bytesInput);

			Key logIdKey = BinaryWriter::toValue(logId,Unversioned());
			tLogData->persistentData->clear( singleKeyRange(logIdKey.withPrefix(persistCurrentVersionKeys.begin)) );
			tLogData->persistentData->clear( singleKeyRange(logIdKey.withPrefix(persistKnownCommittedVersionKeys.begin)) );
			tLogData->persistentData->clear( singleKeyRange(logIdKey.withPrefix(persistLocalityKeys.begin)) );
			tLogData->persistentData->clear( singleKeyRange(logIdKey.withPrefix(persistLogRouterTagsKeys.begin)) );
			tLogData->persistentData->clear( singleKeyRange(logIdKey.withPrefix(persistRecoveryCountKeys.begin)) );
			Key msgKey = logIdKey.withPrefix(persistTagMessagesKeys.begin);
			tLogData->persistentData->clear( KeyRangeRef( msgKey, strinc(msgKey) ) );
			Key poppedKey = logIdKey.withPrefix(persistTagPoppedKeys.begin);
			tLogData->persistentData->clear( KeyRangeRef( poppedKey, strinc(poppedKey) ) );
		}
	}

	LogEpoch epoch() const { return recoveryCount; }
};

template <class T>
void TLogQueue::push( T const& qe, Reference<LogData> logData ) {
	BinaryWriter wr( Unversioned() );  // outer framing is not versioned
	wr << uint32_t(0);
	IncludeVersion().write(wr);  // payload is versioned
	wr << qe;
	wr << uint8_t(1);
	*(uint32_t*)wr.getData() = wr.getLength() - sizeof(uint32_t) - sizeof(uint8_t);
	auto loc = queue->push( wr.toStringRef() );
	//TraceEvent("TLogQueueVersionWritten", dbgid).detail("Size", wr.getLength() - sizeof(uint32_t) - sizeof(uint8_t)).detail("Loc", loc);
	logData->versionLocation[qe.version] = loc;
}
void TLogQueue::pop( Version upTo, Reference<LogData> logData ) {
	// Keep only the given and all subsequent version numbers
	// Find the first version >= upTo
	auto v = logData->versionLocation.lower_bound(upTo);
	if (v == logData->versionLocation.begin()) return;

	if(v == logData->versionLocation.end()) {
		v = logData->versionLocation.lastItem();
	}
	else {
		v.decrementNonEnd();
	}

	queue->pop( v->value );
	logData->versionLocation.erase( logData->versionLocation.begin(), v );  // ... and then we erase that previous version and all prior versions
}
void TLogQueue::updateVersionSizes( const TLogQueueEntry& result, TLogData* tLog ) {
	auto it = tLog->id_data.find(result.id);
	if(it != tLog->id_data.end()) {
		it->second->versionLocation[result.version] = queue->getNextReadLocation();
	}
}

ACTOR Future<Void> tLogLock( TLogData* self, ReplyPromise< TLogLockResult > reply, Reference<LogData> logData ) {
	state Version stopVersion = logData->version.get();

	TEST(true); // TLog stopped by recovering master
	TEST( logData->stopped );
	TEST( !logData->stopped );

	TraceEvent("TLogStop", logData->logId).detail("Ver", stopVersion).detail("IsStopped", logData->stopped).detail("QueueCommitted", logData->queueCommittedVersion.get());

	logData->stopped = true;
	if(!logData->recoveryComplete.isSet()) {
		logData->recoveryComplete.sendError(end_of_stream());
	}

	// Lock once the current version has been committed
	wait( logData->queueCommittedVersion.whenAtLeast( stopVersion ) );

	ASSERT(stopVersion == logData->version.get());

	TLogLockResult result;
	result.end = stopVersion;
	result.knownCommittedVersion = logData->knownCommittedVersion;

	TraceEvent("TLogStop2", self->dbgid).detail("LogId", logData->logId).detail("Ver", stopVersion).detail("IsStopped", logData->stopped).detail("QueueCommitted", logData->queueCommittedVersion.get()).detail("KnownCommitted", result.knownCommittedVersion);

	reply.send( result );
	return Void();
}

void updatePersistentPopped( TLogData* self, Reference<LogData> logData, Reference<LogData::TagData> data ) {
	if (!data->poppedRecently) return;
	self->persistentData->set(KeyValueRef( persistTagPoppedKey(logData->logId, data->tag), persistTagPoppedValue(data->popped) ));
	data->poppedRecently = false;

	if (data->nothingPersistent) return;

	self->persistentData->clear( KeyRangeRef(
		persistTagMessagesKey( logData->logId, data->tag, Version(0) ),
		persistTagMessagesKey( logData->logId, data->tag, data->popped ) ) );
	if (data->popped > logData->persistentDataVersion)
		data->nothingPersistent = true;
}

ACTOR Future<Void> updatePersistentData( TLogData* self, Reference<LogData> logData, Version newPersistentDataVersion ) {
	// PERSIST: Changes self->persistentDataVersion and writes and commits the relevant changes
	ASSERT( newPersistentDataVersion <= logData->version.get() );
	ASSERT( newPersistentDataVersion <= logData->queueCommittedVersion.get() );
	ASSERT( newPersistentDataVersion > logData->persistentDataVersion );
	ASSERT( logData->persistentDataVersion == logData->persistentDataDurableVersion );

	//TraceEvent("UpdatePersistentData", self->dbgid).detail("Seq", newPersistentDataSeq);

	state bool anyData = false;

	// For all existing tags
	state int tagLocality = 0;
	state int tagId = 0;

	for(tagLocality = 0; tagLocality < logData->tag_data.size(); tagLocality++) {
		for(tagId = 0; tagId < logData->tag_data[tagLocality].size(); tagId++) {
			state Reference<LogData::TagData> tagData = logData->tag_data[tagLocality][tagId];
			if(tagData) {
				state Version currentVersion = 0;
				// Clear recently popped versions from persistentData if necessary
				updatePersistentPopped( self, logData, tagData );
				// Transfer unpopped messages with version numbers less than newPersistentDataVersion to persistentData
				state std::deque<std::pair<Version, LengthPrefixedStringRef>>::iterator msg = tagData->versionMessages.begin();
				while(msg != tagData->versionMessages.end() && msg->first <= newPersistentDataVersion) {
					currentVersion = msg->first;
					anyData = true;
					tagData->nothingPersistent = false;
					BinaryWriter wr( Unversioned() );

					for(; msg != tagData->versionMessages.end() && msg->first == currentVersion; ++msg)
						wr << msg->second.toStringRef();

					self->persistentData->set( KeyValueRef( persistTagMessagesKey( logData->logId, tagData->tag, currentVersion ), wr.toStringRef() ) );

					Future<Void> f = yield(TaskUpdateStorage);
					if(!f.isReady()) {
						wait(f);
						msg = std::upper_bound(tagData->versionMessages.begin(), tagData->versionMessages.end(), std::make_pair(currentVersion, LengthPrefixedStringRef()), CompareFirst<std::pair<Version, LengthPrefixedStringRef>>());
					}
				}

				wait(yield(TaskUpdateStorage));
			}
		}
	}

	self->persistentData->set( KeyValueRef( BinaryWriter::toValue(logData->logId,Unversioned()).withPrefix(persistCurrentVersionKeys.begin), BinaryWriter::toValue(newPersistentDataVersion, Unversioned()) ) );
	self->persistentData->set( KeyValueRef( BinaryWriter::toValue(logData->logId,Unversioned()).withPrefix(persistKnownCommittedVersionKeys.begin), BinaryWriter::toValue(logData->knownCommittedVersion, Unversioned()) ) );
	logData->persistentDataVersion = newPersistentDataVersion;

	wait( self->persistentData->commit() ); // SOMEDAY: This seems to be running pretty often, should we slow it down???
	wait( delay(0, TaskUpdateStorage) );

	// Now that the changes we made to persistentData are durable, erase the data we moved from memory and the queue, increase bytesDurable accordingly, and update persistentDataDurableVersion.

	TEST(anyData);  // TLog moved data to persistentData
	logData->persistentDataDurableVersion = newPersistentDataVersion;

	for(tagLocality = 0; tagLocality < logData->tag_data.size(); tagLocality++) {
		for(tagId = 0; tagId < logData->tag_data[tagLocality].size(); tagId++) {
			if(logData->tag_data[tagLocality][tagId]) {
				wait(logData->tag_data[tagLocality][tagId]->eraseMessagesBefore( newPersistentDataVersion+1, self, logData, TaskUpdateStorage ));
				wait(yield(TaskUpdateStorage));
			}
		}
	}

	logData->version_sizes.erase(logData->version_sizes.begin(), logData->version_sizes.lower_bound(logData->persistentDataDurableVersion));

	wait(yield(TaskUpdateStorage));

	while(!logData->messageBlocks.empty() && logData->messageBlocks.front().first <= newPersistentDataVersion) {
		int64_t bytesErased = int64_t(logData->messageBlocks.front().second.size()) * SERVER_KNOBS->TLOG_MESSAGE_BLOCK_OVERHEAD_FACTOR;
		logData->bytesDurable += bytesErased;
		self->bytesDurable += bytesErased;
		logData->messageBlocks.pop_front();
		wait(yield(TaskUpdateStorage));
	}

	if(logData->bytesDurable.getValue() > logData->bytesInput.getValue() || self->bytesDurable > self->bytesInput) {
		TraceEvent(SevError, "BytesDurableTooLarge", logData->logId).detail("SharedBytesInput", self->bytesInput).detail("SharedBytesDurable", self->bytesDurable).detail("LocalBytesInput", logData->bytesInput.getValue()).detail("LocalBytesDurable", logData->bytesDurable.getValue());
	}

	ASSERT(logData->bytesDurable.getValue() <= logData->bytesInput.getValue());
	ASSERT(self->bytesDurable <= self->bytesInput);

	if( self->queueCommitEnd.get() > 0 )
		self->persistentQueue->pop( newPersistentDataVersion+1, logData ); // SOMEDAY: this can cause a slow task (~0.5ms), presumably from erasing too many versions. Should we limit the number of versions cleared at a time?

	return Void();
}

// This function (and updatePersistentData, which is called by this function) run at a low priority and can soak up all CPU resources.
// For this reason, they employ aggressive use of yields to avoid causing slow tasks that could introduce latencies for more important
// work (e.g. commits).
ACTOR Future<Void> updateStorage( TLogData* self ) {
	while(self->queueOrder.size() && !self->id_data.count(self->queueOrder.front())) {
		self->queueOrder.pop_front();
	}

	if(!self->queueOrder.size()) {
		wait( delay(BUGGIFY ? SERVER_KNOBS->BUGGIFY_TLOG_STORAGE_MIN_UPDATE_INTERVAL : SERVER_KNOBS->TLOG_STORAGE_MIN_UPDATE_INTERVAL, TaskUpdateStorage) );
		return Void();
	}

	state Reference<LogData> logData = self->id_data[self->queueOrder.front()];
	state Version nextVersion = 0;
	state int totalSize = 0;

	state FlowLock::Releaser commitLockReleaser;

	if(logData->stopped) {
		if (self->bytesInput - self->bytesDurable >= SERVER_KNOBS->TLOG_SPILL_THRESHOLD) {
			while(logData->persistentDataDurableVersion != logData->version.get()) {
				totalSize = 0;
				Map<Version, std::pair<int,int>>::iterator sizeItr = logData->version_sizes.begin();
				nextVersion = logData->version.get();
				while( totalSize < SERVER_KNOBS->UPDATE_STORAGE_BYTE_LIMIT && sizeItr != logData->version_sizes.end() )
				{
					totalSize += sizeItr->value.first + sizeItr->value.second;
					++sizeItr;
					nextVersion = sizeItr == logData->version_sizes.end() ? logData->version.get() : sizeItr->key;
				}

				wait( logData->queueCommittedVersion.whenAtLeast( nextVersion ) );
				wait( delay(0, TaskUpdateStorage) );

				//TraceEvent("TlogUpdatePersist", self->dbgid).detail("LogId", logData->logId).detail("NextVersion", nextVersion).detail("Version", logData->version.get()).detail("PersistentDataDurableVer", logData->persistentDataDurableVersion).detail("QueueCommitVer", logData->queueCommittedVersion.get()).detail("PersistDataVer", logData->persistentDataVersion);
				if (nextVersion > logData->persistentDataVersion) {
					wait( self->persistentDataCommitLock.take() );
					commitLockReleaser = FlowLock::Releaser(self->persistentDataCommitLock);
					wait( updatePersistentData(self, logData, nextVersion) );
					commitLockReleaser.release();
				} else {
					wait( delay(BUGGIFY ? SERVER_KNOBS->BUGGIFY_TLOG_STORAGE_MIN_UPDATE_INTERVAL : SERVER_KNOBS->TLOG_STORAGE_MIN_UPDATE_INTERVAL, TaskUpdateStorage) );
				}

				if( logData->removed.isReady() ) {
					break;
				}
			}

			if(logData->persistentDataDurableVersion == logData->version.get()) {
				self->queueOrder.pop_front();
			}
			wait( delay(0.0, TaskUpdateStorage) );
		} else {
			wait( delay(BUGGIFY ? SERVER_KNOBS->BUGGIFY_TLOG_STORAGE_MIN_UPDATE_INTERVAL : SERVER_KNOBS->TLOG_STORAGE_MIN_UPDATE_INTERVAL, TaskUpdateStorage) );
		}
	}
	else if(logData->initialized) {
		ASSERT(self->queueOrder.size() == 1);
		if(logData->version_sizes.empty()) {
			nextVersion = logData->version.get();
		} else {
			Map<Version, std::pair<int,int>>::iterator sizeItr = logData->version_sizes.begin();
			while( totalSize < SERVER_KNOBS->UPDATE_STORAGE_BYTE_LIMIT && sizeItr != logData->version_sizes.end()
					&& (logData->bytesInput.getValue() - logData->bytesDurable.getValue() - totalSize >= SERVER_KNOBS->TLOG_SPILL_THRESHOLD || sizeItr->value.first == 0) )
			{
				totalSize += sizeItr->value.first + sizeItr->value.second;
				++sizeItr;
				nextVersion = sizeItr == logData->version_sizes.end() ? logData->version.get() : sizeItr->key;
			}
		}

		//TraceEvent("UpdateStorageVer", logData->logId).detail("NextVersion", nextVersion).detail("PersistentDataVersion", logData->persistentDataVersion).detail("TotalSize", totalSize);

		wait( logData->queueCommittedVersion.whenAtLeast( nextVersion ) );
		wait( delay(0, TaskUpdateStorage) );

		if (nextVersion > logData->persistentDataVersion) {
			wait( self->persistentDataCommitLock.take() );
			commitLockReleaser = FlowLock::Releaser(self->persistentDataCommitLock);
			wait( updatePersistentData(self, logData, nextVersion) );
			commitLockReleaser.release();
		}

		if( totalSize < SERVER_KNOBS->UPDATE_STORAGE_BYTE_LIMIT ) {
			wait( delay(BUGGIFY ? SERVER_KNOBS->BUGGIFY_TLOG_STORAGE_MIN_UPDATE_INTERVAL : SERVER_KNOBS->TLOG_STORAGE_MIN_UPDATE_INTERVAL, TaskUpdateStorage) );
		}
		else {
			//recovery wants to commit to persistant data when updatePersistentData is not active, this delay ensures that immediately after
			//updatePersist returns another one has not been started yet.
			wait( delay(0.0, TaskUpdateStorage) );
		}
	} else {
		wait( delay(BUGGIFY ? SERVER_KNOBS->BUGGIFY_TLOG_STORAGE_MIN_UPDATE_INTERVAL : SERVER_KNOBS->TLOG_STORAGE_MIN_UPDATE_INTERVAL, TaskUpdateStorage) );
	}
	return Void();
}

ACTOR Future<Void> updateStorageLoop( TLogData* self ) {
	wait(delay(0, TaskUpdateStorage));

	loop {
		wait( updateStorage(self) );
	}
}

void commitMessages( TLogData* self, Reference<LogData> logData, Version version, const std::vector<TagsAndMessage>& taggedMessages ) {
	// SOMEDAY: This method of copying messages is reasonably memory efficient, but it's still a lot of bytes copied.  Find a
	// way to do the memory allocation right as we receive the messages in the network layer.

	int64_t addedBytes = 0;
	int64_t overheadBytes = 0;
	int expectedBytes = 0;
	int txsBytes = 0;

	if(!taggedMessages.size()) {
		return;
	}

	int msgSize = 0;
	for(auto& i : taggedMessages) {
		msgSize += i.message.size();
	}

	// Grab the last block in the blocks list so we can share its arena
	// We pop all of the elements of it to create a "fresh" vector that starts at the end of the previous vector
	Standalone<VectorRef<uint8_t>> block;
	if(logData->messageBlocks.empty()) {
		block = Standalone<VectorRef<uint8_t>>();
		block.reserve(block.arena(), std::max<int64_t>(SERVER_KNOBS->TLOG_MESSAGE_BLOCK_BYTES, msgSize));
	}
	else {
		block = logData->messageBlocks.back().second;
	}

	block.pop_front(block.size());

	for(auto& msg : taggedMessages) {
		if(msg.message.size() > block.capacity() - block.size()) {
			logData->messageBlocks.push_back( std::make_pair(version, block) );
			addedBytes += int64_t(block.size()) * SERVER_KNOBS->TLOG_MESSAGE_BLOCK_OVERHEAD_FACTOR;
			block = Standalone<VectorRef<uint8_t>>();
			block.reserve(block.arena(), std::max<int64_t>(SERVER_KNOBS->TLOG_MESSAGE_BLOCK_BYTES, msgSize));
		}

		block.append(block.arena(), msg.message.begin(), msg.message.size());
		for(auto tag : msg.tags) {
			if(logData->locality == tagLocalitySatellite) {
				if(!(tag == txsTag || tag.locality == tagLocalityLogRouter)) {
					continue;
				}
			} else if(!(logData->locality == tagLocalitySpecial || logData->locality == tag.locality || tag.locality < 0)) {
				continue;
			}

			if(tag.locality == tagLocalityLogRouter) {
				if(!logData->logRouterTags) {
					continue;
				}
				tag.id = tag.id % logData->logRouterTags;
			}
			Reference<LogData::TagData> tagData = logData->getTagData(tag);
			if(!tagData) {
				tagData = logData->createTagData(tag, 0, true, true, false);
			}

			if (version >= tagData->popped) {
				tagData->versionMessages.push_back(std::make_pair(version, LengthPrefixedStringRef((uint32_t*)(block.end() - msg.message.size()))));
				if(tagData->versionMessages.back().second.expectedSize() > SERVER_KNOBS->MAX_MESSAGE_SIZE) {
					TraceEvent(SevWarnAlways, "LargeMessage").detail("Size", tagData->versionMessages.back().second.expectedSize());
				}
				if (tag != txsTag) {
					expectedBytes += tagData->versionMessages.back().second.expectedSize();
				} else {
					txsBytes += tagData->versionMessages.back().second.expectedSize();
				}

				// The factor of VERSION_MESSAGES_OVERHEAD is intended to be an overestimate of the actual memory used to store this data in a std::deque.
				// In practice, this number is probably something like 528/512 ~= 1.03, but this could vary based on the implementation.
				// There will also be a fixed overhead per std::deque, but its size should be trivial relative to the size of the TLog
				// queue and can be thought of as increasing the capacity of the queue slightly.
				overheadBytes += SERVER_KNOBS->VERSION_MESSAGES_ENTRY_BYTES_WITH_OVERHEAD;
			}
		}

		msgSize -= msg.message.size();
	}
	logData->messageBlocks.push_back( std::make_pair(version, block) );
	addedBytes += int64_t(block.size()) * SERVER_KNOBS->TLOG_MESSAGE_BLOCK_OVERHEAD_FACTOR;
	addedBytes += overheadBytes;

	logData->version_sizes[version] = std::make_pair(expectedBytes, txsBytes);
	logData->bytesInput += addedBytes;
	self->bytesInput += addedBytes;
	self->overheadBytesInput += overheadBytes;

	//TraceEvent("TLogPushed", self->dbgid).detail("Bytes", addedBytes).detail("MessageBytes", messages.size()).detail("Tags", tags.size()).detail("ExpectedBytes", expectedBytes).detail("MCount", mCount).detail("TCount", tCount);
}

void commitMessages( TLogData *self, Reference<LogData> logData, Version version, Arena arena, StringRef messages ) {
	ArenaReader rd( arena, messages, Unversioned() );
	int32_t messageLength, rawLength;
	uint16_t tagCount;
	uint32_t sub;
	std::vector<TagsAndMessage> msgs;
	while(!rd.empty()) {
		TagsAndMessage tagsAndMsg;
		rd.checkpoint();
		rd >> messageLength >> sub >> tagCount;
		tagsAndMsg.tags.resize(tagCount);
		for(int i = 0; i < tagCount; i++) {
			rd >> tagsAndMsg.tags[i];
		}
		rawLength = messageLength + sizeof(messageLength);
		rd.rewind();
		tagsAndMsg.message = StringRef((uint8_t const*)rd.readBytes(rawLength), rawLength);
		msgs.push_back(std::move(tagsAndMsg));
	}
	commitMessages(self, logData, version, msgs);
}

Version poppedVersion( Reference<LogData> self, Tag tag) {
	auto tagData = self->getTagData(tag);
	if (!tagData) {
		return self->recoveredAt;
	}
	return tagData->popped;
}

std::deque<std::pair<Version, LengthPrefixedStringRef>> & getVersionMessages( Reference<LogData> self, Tag tag ) {
	auto tagData = self->getTagData(tag);
	if (!tagData) {
		static std::deque<std::pair<Version, LengthPrefixedStringRef>> empty;
		return empty;
	}
	return tagData->versionMessages;
};

ACTOR Future<Void> tLogPop( TLogData* self, TLogPopRequest req, Reference<LogData> logData ) {
	auto tagData = logData->getTagData(req.tag);
	if (!tagData) {
		tagData = logData->createTagData(req.tag, req.to, true, true, false);
	} else if (req.to > tagData->popped) {
		tagData->popped = req.to;
		tagData->poppedRecently = true;

		if(tagData->unpoppedRecovered && req.to > logData->recoveredAt) {
			tagData->unpoppedRecovered = false;
			logData->unpoppedRecoveredTags--;
			TraceEvent("TLogPoppedTag", logData->logId).detail("Tags", logData->unpoppedRecoveredTags).detail("Tag", req.tag.toString()).detail("DurableKCVer", logData->durableKnownCommittedVersion).detail("RecoveredAt", logData->recoveredAt);
			if(logData->unpoppedRecoveredTags == 0 && logData->durableKnownCommittedVersion >= logData->recoveredAt && logData->recoveryComplete.canBeSet()) {
				logData->recoveryComplete.send(Void());
			}
		}

		if ( req.to > logData->persistentDataDurableVersion )
			wait(tagData->eraseMessagesBefore( req.to, self, logData, TaskTLogPop ));
		//TraceEvent("TLogPop", self->dbgid).detail("Tag", req.tag).detail("To", req.to);
	}

	req.reply.send(Void());
	return Void();
}

void peekMessagesFromMemory( Reference<LogData> self, TLogPeekRequest const& req, BinaryWriter& messages, Version& endVersion ) {
	ASSERT( !messages.getLength() );

	auto& deque = getVersionMessages(self, req.tag);
	//TraceEvent("TLogPeekMem", self->dbgid).detail("Tag", printable(req.tag1)).detail("PDS", self->persistentDataSequence).detail("PDDS", self->persistentDataDurableSequence).detail("Oldest", map1.empty() ? 0 : map1.begin()->key ).detail("OldestMsgCount", map1.empty() ? 0 : map1.begin()->value.size());

	Version begin = std::max( req.begin, self->persistentDataDurableVersion+1 );
	auto it = std::lower_bound(deque.begin(), deque.end(), std::make_pair(begin, LengthPrefixedStringRef()), CompareFirst<std::pair<Version, LengthPrefixedStringRef>>());

	Version currentVersion = -1;
	for(; it != deque.end(); ++it) {
		if(it->first != currentVersion) {
			if (messages.getLength() >= SERVER_KNOBS->DESIRED_TOTAL_BYTES) {
				endVersion = currentVersion + 1;
				//TraceEvent("TLogPeekMessagesReached2", self->dbgid);
				break;
			}

			currentVersion = it->first;
			messages << int32_t(-1) << currentVersion;
		}

		messages << it->second.toStringRef();
	}
}

ACTOR Future<Void> tLogPeekMessages( TLogData* self, TLogPeekRequest req, Reference<LogData> logData ) {
	state BinaryWriter messages(Unversioned());
	state BinaryWriter messages2(Unversioned());
	state int sequence = -1;
	state UID peekId;

	if(req.sequence.present()) {
		try {
			peekId = req.sequence.get().first;
			sequence = req.sequence.get().second;
			if(sequence > 0) {
				auto& trackerData = self->peekTracker[peekId];
				auto seqBegin = trackerData.sequence_version.begin();
				while(trackerData.sequence_version.size() && seqBegin->first <= sequence - SERVER_KNOBS->PARALLEL_GET_MORE_REQUESTS) {
					if(seqBegin->second.canBeSet()) {
						seqBegin->second.sendError(timed_out());
					}
					trackerData.sequence_version.erase(seqBegin);
					seqBegin = trackerData.sequence_version.begin();
				}

				if(trackerData.sequence_version.size() && sequence < seqBegin->first) {
					throw timed_out();
				}

				trackerData.lastUpdate = now();
				Version ver = wait(trackerData.sequence_version[sequence].getFuture());
				req.begin = ver;
				wait(yield());
			}
		} catch( Error &e ) {
			if(e.code() == error_code_timed_out) {
				req.reply.sendError(timed_out());
				return Void();
			} else {
				throw;
			}
		}
	}

	if( req.returnIfBlocked && logData->version.get() < req.begin ) {
		req.reply.sendError(end_of_stream());
		return Void();
	}

	//TraceEvent("TLogPeekMessages0", self->dbgid).detail("ReqBeginEpoch", req.begin.epoch).detail("ReqBeginSeq", req.begin.sequence).detail("Epoch", self->epoch()).detail("PersistentDataSeq", self->persistentDataSequence).detail("Tag1", printable(req.tag1)).detail("Tag2", printable(req.tag2));
	// Wait until we have something to return that the caller doesn't already have
	if( logData->version.get() < req.begin ) {
		wait( logData->version.whenAtLeast( req.begin ) );
		wait( delay(SERVER_KNOBS->TLOG_PEEK_DELAY, g_network->getCurrentTask()) );
	}

	if( req.tag.locality == tagLocalityLogRouter ) {
		wait( self->concurrentLogRouterReads.take() );
		state FlowLock::Releaser globalReleaser(self->concurrentLogRouterReads);
		wait( delay(0.0, TaskLowPriority) );
	}

	Version poppedVer = poppedVersion(logData, req.tag);
	if(poppedVer > req.begin) {
		TLogPeekReply rep;
		rep.maxKnownVersion = logData->version.get();
		rep.minKnownCommittedVersion = logData->minKnownCommittedVersion;
		rep.popped = poppedVer;
		rep.end = poppedVer;

		if(req.sequence.present()) {
			auto& trackerData = self->peekTracker[peekId];
			trackerData.lastUpdate = now();
			if(trackerData.sequence_version.size() && sequence+1 < trackerData.sequence_version.begin()->first) {
				req.reply.sendError(timed_out());
				return Void();
			}
			auto& sequenceData = trackerData.sequence_version[sequence+1];
			if(sequenceData.isSet()) {
				if(sequenceData.getFuture().get() != rep.end) {
					TEST(true); //tlog peek second attempt ended at a different version
					req.reply.sendError(timed_out());
					return Void();
				}
			} else {
				sequenceData.send(rep.end);
			}
			rep.begin = req.begin;
		}

		req.reply.send( rep );
		return Void();
	}

	state Version endVersion = logData->version.get() + 1;

	//grab messages from disk
	//TraceEvent("TLogPeekMessages", self->dbgid).detail("ReqBeginEpoch", req.begin.epoch).detail("ReqBeginSeq", req.begin.sequence).detail("Epoch", self->epoch()).detail("PersistentDataSeq", self->persistentDataSequence).detail("Tag1", printable(req.tag1)).detail("Tag2", printable(req.tag2));
	if( req.begin <= logData->persistentDataDurableVersion ) {
		// Just in case the durable version changes while we are waiting for the read, we grab this data from memory.  We may or may not actually send it depending on
		// whether we get enough data from disk.
		// SOMEDAY: Only do this if an initial attempt to read from disk results in insufficient data and the required data is no longer in memory
		// SOMEDAY: Should we only send part of the messages we collected, to actually limit the size of the result?

		peekMessagesFromMemory( logData, req, messages2, endVersion );

		Standalone<VectorRef<KeyValueRef>> kvs = wait(
			self->persistentData->readRange(KeyRangeRef(
				persistTagMessagesKey(logData->logId, req.tag, req.begin),
				persistTagMessagesKey(logData->logId, req.tag, logData->persistentDataDurableVersion + 1)), SERVER_KNOBS->DESIRED_TOTAL_BYTES, SERVER_KNOBS->DESIRED_TOTAL_BYTES));

		//TraceEvent("TLogPeekResults", self->dbgid).detail("ForAddress", req.reply.getEndpoint().address).detail("Tag1Results", s1).detail("Tag2Results", s2).detail("Tag1ResultsLim", kv1.size()).detail("Tag2ResultsLim", kv2.size()).detail("Tag1ResultsLast", kv1.size() ? printable(kv1[0].key) : "").detail("Tag2ResultsLast", kv2.size() ? printable(kv2[0].key) : "").detail("Limited", limited).detail("NextEpoch", next_pos.epoch).detail("NextSeq", next_pos.sequence).detail("NowEpoch", self->epoch()).detail("NowSeq", self->sequence.getNextSequence());

		for (auto &kv : kvs) {
			auto ver = decodeTagMessagesKey(kv.key);
			messages << int32_t(-1) << ver;
			messages.serializeBytes(kv.value);
		}

		if (kvs.expectedSize() >= SERVER_KNOBS->DESIRED_TOTAL_BYTES)
			endVersion = decodeTagMessagesKey(kvs.end()[-1].key) + 1;
		else
			messages.serializeBytes( messages2.toStringRef() );
	} else {
		peekMessagesFromMemory( logData, req, messages, endVersion );
		//TraceEvent("TLogPeekResults", self->dbgid).detail("ForAddress", req.reply.getEndpoint().address).detail("MessageBytes", messages.getLength()).detail("NextEpoch", next_pos.epoch).detail("NextSeq", next_pos.sequence).detail("NowSeq", self->sequence.getNextSequence());
	}

	TLogPeekReply reply;
	reply.maxKnownVersion = logData->version.get();
	reply.minKnownCommittedVersion = logData->minKnownCommittedVersion;
	reply.messages = messages.toStringRef();
	reply.end = endVersion;

	//TraceEvent("TlogPeek", self->dbgid).detail("LogId", logData->logId).detail("EndVer", reply.end).detail("MsgBytes", reply.messages.expectedSize()).detail("ForAddress", req.reply.getEndpoint().address);

	if(req.sequence.present()) {
		auto& trackerData = self->peekTracker[peekId];
		trackerData.lastUpdate = now();
		if(trackerData.sequence_version.size() && sequence+1 < trackerData.sequence_version.begin()->first) {
			req.reply.sendError(timed_out());
			return Void();
		}
		auto& sequenceData = trackerData.sequence_version[sequence+1];
		if(sequenceData.isSet()) {
			if(sequenceData.getFuture().get() != reply.end) {
				TEST(true); //tlog peek second attempt ended at a different version
				req.reply.sendError(timed_out());
				return Void();
			}
		} else {
			sequenceData.send(reply.end);
		}
		reply.begin = req.begin;
	}

	req.reply.send( reply );
	return Void();
}

ACTOR Future<Void> doQueueCommit( TLogData* self, Reference<LogData> logData ) {
	state Version ver = logData->version.get();
	state Version commitNumber = self->queueCommitBegin+1;
	state Version knownCommittedVersion = logData->knownCommittedVersion;
	self->queueCommitBegin = commitNumber;
	logData->queueCommittingVersion = ver;

	Future<Void> c = self->persistentQueue->commit();
	self->diskQueueCommitBytes = 0;
	self->largeDiskQueueCommitBytes.set(false);

	wait(c);
	wait(self->queueCommitEnd.whenAtLeast(commitNumber-1));

	//Calling check_yield instead of yield to avoid a destruction ordering problem in simulation
	if(g_network->check_yield(g_network->getCurrentTask())) {
		wait(delay(0, g_network->getCurrentTask()));
	}

	ASSERT( ver > logData->queueCommittedVersion.get() );

	logData->durableKnownCommittedVersion = knownCommittedVersion;
	if(logData->unpoppedRecoveredTags == 0 && knownCommittedVersion >= logData->recoveredAt && logData->recoveryComplete.canBeSet()) {
		TraceEvent("TLogRecoveryComplete", logData->logId).detail("Tags", logData->unpoppedRecoveredTags).detail("DurableKCVer", logData->durableKnownCommittedVersion).detail("RecoveredAt", logData->recoveredAt);
		logData->recoveryComplete.send(Void());
	}

	//TraceEvent("TLogCommitDurable", self->dbgid).detail("Version", ver);
	if(logData->logSystem->get() && (!logData->isPrimary || logData->logRouterPoppedVersion < logData->logRouterPopToVersion)) {
		logData->logRouterPoppedVersion = ver;
		logData->logSystem->get()->pop(ver, logData->remoteTag, knownCommittedVersion, logData->locality);
	}

	logData->queueCommittedVersion.set(ver);
	self->queueCommitEnd.set(commitNumber);

	return Void();
}

ACTOR Future<Void> commitQueue( TLogData* self ) {
	state Reference<LogData> logData;

	loop {
		int foundCount = 0;
		for(auto it : self->id_data) {
			if(!it.second->stopped) {
				 logData = it.second;
				 foundCount++;
			}
		}

		ASSERT(foundCount < 2);
		if(!foundCount) {
			wait( self->newLogData.onTrigger() );
			continue;
		}

		TraceEvent("CommitQueueNewLog", self->dbgid).detail("LogId", logData->logId).detail("Version", logData->version.get()).detail("Committing", logData->queueCommittingVersion).detail("Commmitted", logData->queueCommittedVersion.get());
		if(logData->committingQueue.canBeSet()) {
			logData->committingQueue.send(Void());
		}

		loop {
			if(logData->stopped && logData->version.get() == std::max(logData->queueCommittingVersion, logData->queueCommittedVersion.get())) {
				wait( logData->queueCommittedVersion.whenAtLeast(logData->version.get() ) );
				break;
			}

			choose {
				when(wait( logData->version.whenAtLeast( std::max(logData->queueCommittingVersion, logData->queueCommittedVersion.get()) + 1 ) ) ) {
					while( self->queueCommitBegin != self->queueCommitEnd.get() && !self->largeDiskQueueCommitBytes.get() ) {
						wait( self->queueCommitEnd.whenAtLeast(self->queueCommitBegin) || self->largeDiskQueueCommitBytes.onChange() );
					}
					self->sharedActors.send(doQueueCommit(self, logData));
				}
				when(wait(self->newLogData.onTrigger())) {}
			}
		}
	}
}

ACTOR Future<Void> tLogCommit(
		TLogData* self,
		TLogCommitRequest req,
		Reference<LogData> logData,
		PromiseStream<Void> warningCollectorInput ) {
	state Optional<UID> tlogDebugID;
	if(req.debugID.present())
	{
		tlogDebugID = g_nondeterministic_random->randomUniqueID();
		g_traceBatch.addAttach("CommitAttachID", req.debugID.get().first(), tlogDebugID.get().first());
		g_traceBatch.addEvent("CommitDebug", tlogDebugID.get().first(), "TLog.tLogCommit.BeforeWaitForVersion");
	}

	logData->minKnownCommittedVersion = std::max(logData->minKnownCommittedVersion, req.minKnownCommittedVersion);

	wait( logData->version.whenAtLeast( req.prevVersion ) );

	//Calling check_yield instead of yield to avoid a destruction ordering problem in simulation
	if(g_network->check_yield(g_network->getCurrentTask())) {
		wait(delay(0, g_network->getCurrentTask()));
	}

	state double waitStartT = 0;
	while( self->bytesInput - self->bytesDurable >= SERVER_KNOBS->TLOG_HARD_LIMIT_BYTES && !logData->stopped ) {
		if (now() - waitStartT >= 1) {
			TraceEvent(SevWarn, "TLogUpdateLag", logData->logId)
				.detail("Version", logData->version.get())
				.detail("PersistentDataVersion", logData->persistentDataVersion)
				.detail("PersistentDataDurableVersion", logData->persistentDataDurableVersion);
			waitStartT = now();
		}
		wait( delayJittered(.005, TaskTLogCommit) );
	}

	if(logData->stopped) {
		req.reply.sendError( tlog_stopped() );
		return Void();
	}

	if (logData->version.get() == req.prevVersion) {  // Not a duplicate (check relies on no waiting between here and self->version.set() below!)
		if(req.debugID.present())
			g_traceBatch.addEvent("CommitDebug", tlogDebugID.get().first(), "TLog.tLogCommit.Before");

		//TraceEvent("TLogCommit", logData->logId).detail("Version", req.version);
		commitMessages(self, logData, req.version, req.arena, req.messages);

		logData->knownCommittedVersion = std::max(logData->knownCommittedVersion, req.knownCommittedVersion);

		// Log the changes to the persistent queue, to be committed by commitQueue()
		TLogQueueEntryRef qe;
		qe.version = req.version;
		qe.knownCommittedVersion = logData->knownCommittedVersion;
		qe.messages = req.messages;
		qe.id = logData->logId;
		self->persistentQueue->push( qe, logData );

		self->diskQueueCommitBytes += qe.expectedSize();
		if( self->diskQueueCommitBytes > SERVER_KNOBS->MAX_QUEUE_COMMIT_BYTES ) {
			self->largeDiskQueueCommitBytes.set(true);
		}

		// Notifies the commitQueue actor to commit persistentQueue, and also unblocks tLogPeekMessages actors
		logData->version.set( req.version );

		if(req.debugID.present())
			g_traceBatch.addEvent("CommitDebug", tlogDebugID.get().first(), "TLog.tLogCommit.AfterTLogCommit");
	}
	// Send replies only once all prior messages have been received and committed.
	state Future<Void> stopped = logData->stopCommit.onTrigger();
	wait( timeoutWarning( logData->queueCommittedVersion.whenAtLeast( req.version ) || stopped, 0.1, warningCollectorInput ) );

	if(stopped.isReady()) {
		ASSERT(logData->stopped);
		req.reply.sendError( tlog_stopped() );
		return Void();
	}

	if(req.debugID.present())
		g_traceBatch.addEvent("CommitDebug", tlogDebugID.get().first(), "TLog.tLogCommit.After");

	req.reply.send( logData->durableKnownCommittedVersion );
	return Void();
}

ACTOR Future<Void> initPersistentState( TLogData* self, Reference<LogData> logData ) {
	wait( self->persistentDataCommitLock.take() );
	state FlowLock::Releaser commitLockReleaser(self->persistentDataCommitLock);

	// PERSIST: Initial setup of persistentData for a brand new tLog for a new database
	state IKeyValueStore *storage = self->persistentData;
	wait(storage->init());
	storage->set( persistFormat );
	storage->set( KeyValueRef( BinaryWriter::toValue(logData->logId,Unversioned()).withPrefix(persistCurrentVersionKeys.begin), BinaryWriter::toValue(logData->version.get(), Unversioned()) ) );
	storage->set( KeyValueRef( BinaryWriter::toValue(logData->logId,Unversioned()).withPrefix(persistKnownCommittedVersionKeys.begin), BinaryWriter::toValue(logData->knownCommittedVersion, Unversioned()) ) );
	storage->set( KeyValueRef( BinaryWriter::toValue(logData->logId,Unversioned()).withPrefix(persistLocalityKeys.begin), BinaryWriter::toValue(logData->locality, Unversioned()) ) );
	storage->set( KeyValueRef( BinaryWriter::toValue(logData->logId,Unversioned()).withPrefix(persistLogRouterTagsKeys.begin), BinaryWriter::toValue(logData->logRouterTags, Unversioned()) ) );
	storage->set( KeyValueRef( BinaryWriter::toValue(logData->logId,Unversioned()).withPrefix(persistRecoveryCountKeys.begin), BinaryWriter::toValue(logData->recoveryCount, Unversioned()) ) );

	for(auto tag : logData->allTags) {
		ASSERT(!logData->getTagData(tag));
		logData->createTagData(tag, 0, true, true, true);
		updatePersistentPopped( self, logData, logData->getTagData(tag) );
	}

	TraceEvent("TLogInitCommit", logData->logId);
	wait( self->persistentData->commit() );
	return Void();
}

ACTOR Future<Void> rejoinMasters( TLogData* self, TLogInterface tli, DBRecoveryCount recoveryCount, Future<Void> registerWithMaster, bool isPrimary ) {
	state UID lastMasterID(0,0);
	loop {
		auto const& inf = self->dbInfo->get();
		bool isDisplaced = !std::count( inf.priorCommittedLogServers.begin(), inf.priorCommittedLogServers.end(), tli.id() );
		if(isPrimary) {
			isDisplaced = isDisplaced && inf.recoveryCount >= recoveryCount && inf.recoveryState != RecoveryState::UNINITIALIZED;
		} else {
			isDisplaced = isDisplaced && ( ( inf.recoveryCount > recoveryCount && inf.recoveryState != RecoveryState::UNINITIALIZED ) || ( inf.recoveryCount == recoveryCount && inf.recoveryState == RecoveryState::FULLY_RECOVERED ) );
		}
		if(isDisplaced) {
			for(auto& log : inf.logSystemConfig.tLogs) {
				 if( std::count( log.tLogs.begin(), log.tLogs.end(), tli.id() ) ) {
					isDisplaced = false;
					break;
				 }
			}
		}
		if(isDisplaced) {
			for(auto& old : inf.logSystemConfig.oldTLogs) {
				for(auto& log : old.tLogs) {
					 if( std::count( log.tLogs.begin(), log.tLogs.end(), tli.id() ) ) {
						isDisplaced = false;
						break;
					 }
				}
			}
		}
		if ( isDisplaced )
		{
			TraceEvent("TLogDisplaced", tli.id()).detail("Reason", "DBInfoDoesNotContain").detail("RecoveryCount", recoveryCount).detail("InfRecoveryCount", inf.recoveryCount).detail("RecoveryState", (int)inf.recoveryState)
				.detail("LogSysConf", describe(inf.logSystemConfig.tLogs)).detail("PriorLogs", describe(inf.priorCommittedLogServers)).detail("OldLogGens", inf.logSystemConfig.oldTLogs.size());
			if (BUGGIFY) wait( delay( SERVER_KNOBS->BUGGIFY_WORKER_REMOVED_MAX_LAG * g_random->random01() ) );
			throw worker_removed();
		}

		if( registerWithMaster.isReady() ) {
			if ( self->dbInfo->get().master.id() != lastMasterID) {
				// The TLogRejoinRequest is needed to establish communications with a new master, which doesn't have our TLogInterface
				TLogRejoinRequest req(tli);
				TraceEvent("TLogRejoining", self->dbgid).detail("Master", self->dbInfo->get().master.id());
				choose {
					when ( bool success = wait( brokenPromiseToNever( self->dbInfo->get().master.tlogRejoin.getReply( req ) ) ) ) {
						if (success)
							lastMasterID = self->dbInfo->get().master.id();
					}
					when ( wait( self->dbInfo->onChange() ) ) { }
				}
			} else {
				wait( self->dbInfo->onChange() );
			}
		} else {
			wait( registerWithMaster || self->dbInfo->onChange() );
		}
	}
}

ACTOR Future<Void> respondToRecovered( TLogInterface tli, Promise<Void> recoveryComplete ) {
	state bool finishedRecovery = true;
	try {
		wait( recoveryComplete.getFuture() );
	} catch( Error &e ) {
		if(e.code() != error_code_end_of_stream) {
			throw;
		}
		finishedRecovery = false;
	}
	TraceEvent("TLogRespondToRecovered", tli.id()).detail("Finished", finishedRecovery);
	loop {
		TLogRecoveryFinishedRequest req = waitNext( tli.recoveryFinished.getFuture() );
		if(finishedRecovery) {
			req.reply.send(Void());
		} else {
			req.reply.send(Never());
		}
	}
}

ACTOR Future<Void> cleanupPeekTrackers( TLogData* self ) {
	loop {
		double minTimeUntilExpiration = SERVER_KNOBS->PEEK_TRACKER_EXPIRATION_TIME;
		auto it = self->peekTracker.begin();
		while(it != self->peekTracker.end()) {
			double timeUntilExpiration = it->second.lastUpdate + SERVER_KNOBS->PEEK_TRACKER_EXPIRATION_TIME - now();
			if(timeUntilExpiration < 1.0e-6) {
				for(auto seq : it->second.sequence_version) {
					if(!seq.second.isSet()) {
						seq.second.sendError(timed_out());
					}
				}
				it = self->peekTracker.erase(it);
			} else {
				minTimeUntilExpiration = std::min(minTimeUntilExpiration, timeUntilExpiration);
				++it;
			}
		}

		wait( delay(minTimeUntilExpiration) );
	}
}

void getQueuingMetrics( TLogData* self, Reference<LogData> logData, TLogQueuingMetricsRequest const& req ) {
	TLogQueuingMetricsReply reply;
	reply.localTime = now();
	reply.instanceID = self->instanceID;
	reply.bytesInput = self->bytesInput;
	reply.bytesDurable = self->bytesDurable;
	reply.storageBytes = self->persistentData->getStorageBytes();
	//FIXME: Add the knownCommittedVersion to this message and change ratekeeper to use that version.
	reply.v = logData->durableKnownCommittedVersion;
	req.reply.send( reply );
}

ACTOR Future<Void> serveTLogInterface( TLogData* self, TLogInterface tli, Reference<LogData> logData, PromiseStream<Void> warningCollectorInput ) {
	state Future<Void> dbInfoChange = Void();

	loop choose {
		when( wait( dbInfoChange ) ) {
			dbInfoChange = self->dbInfo->onChange();
			bool found = false;
			if(self->dbInfo->get().recoveryState >= RecoveryState::ACCEPTING_COMMITS) {
				for(auto& logs : self->dbInfo->get().logSystemConfig.tLogs) {
					if( std::count( logs.tLogs.begin(), logs.tLogs.end(), logData->logId ) ) {
						found = true;
						break;
					}
				}
			}
			if(found && self->dbInfo->get().logSystemConfig.recruitmentID == logData->recruitmentID) {
				logData->logSystem->set(ILogSystem::fromServerDBInfo( self->dbgid, self->dbInfo->get() ));
				if(!logData->isPrimary) {
					logData->logSystem->get()->pop(logData->logRouterPoppedVersion, logData->remoteTag, logData->durableKnownCommittedVersion, logData->locality);
				}

				if(!logData->isPrimary && logData->stopped) {
					TraceEvent("TLogAlreadyStopped", self->dbgid);
					logData->removed = logData->removed && logData->logSystem->get()->endEpoch();
				}
			} else {
				logData->logSystem->set(Reference<ILogSystem>());
			}
		}
		when( TLogPeekRequest req = waitNext( tli.peekMessages.getFuture() ) ) {
			logData->addActor.send( tLogPeekMessages( self, req, logData ) );
		}
		when( TLogPopRequest req = waitNext( tli.popMessages.getFuture() ) ) {
			logData->addActor.send( tLogPop( self, req, logData ) );
		}
		when( TLogCommitRequest req = waitNext( tli.commit.getFuture() ) ) {
			//TraceEvent("TLogCommitReq", logData->logId).detail("Ver", req.version).detail("PrevVer", req.prevVersion).detail("LogVer", logData->version.get());
			ASSERT(logData->isPrimary);
			TEST(logData->stopped); // TLogCommitRequest while stopped
			if (!logData->stopped)
				logData->addActor.send( tLogCommit( self, req, logData, warningCollectorInput ) );
			else
				req.reply.sendError( tlog_stopped() );
		}
		when( ReplyPromise< TLogLockResult > reply = waitNext( tli.lock.getFuture() ) ) {
			logData->addActor.send( tLogLock(self, reply, logData) );
		}
		when (TLogQueuingMetricsRequest req = waitNext(tli.getQueuingMetrics.getFuture())) {
			getQueuingMetrics(self, logData, req);
		}
		when (TLogConfirmRunningRequest req = waitNext(tli.confirmRunning.getFuture())){
			if (req.debugID.present() ) {
				UID tlogDebugID = g_nondeterministic_random->randomUniqueID();
				g_traceBatch.addAttach("TransactionAttachID", req.debugID.get().first(), tlogDebugID.first());
				g_traceBatch.addEvent("TransactionDebug", tlogDebugID.first(), "TLogServer.TLogConfirmRunningRequest");
			}
			if (!logData->stopped)
				req.reply.send(Void());
			else
				req.reply.sendError( tlog_stopped() );
		}
	}
}

void removeLog( TLogData* self, Reference<LogData> logData ) {
	TraceEvent("TLogRemoved", logData->logId).detail("Input", logData->bytesInput.getValue()).detail("Durable", logData->bytesDurable.getValue());
	logData->stopped = true;
	if(!logData->recoveryComplete.isSet()) {
		logData->recoveryComplete.sendError(end_of_stream());
	}

	logData->addActor = PromiseStream<Future<Void>>(); //there could be items still in the promise stream if one of the actors threw an error immediately
	self->id_data.erase(logData->logId);

	if(self->id_data.size()) {
		return;
	} else {
		throw worker_removed();
	}
}

ACTOR Future<Void> pullAsyncData( TLogData* self, Reference<LogData> logData, std::vector<Tag> tags, Version beginVersion, Optional<Version> endVersion, bool poppedIsKnownCommitted, bool parallelGetMore ) {
	state Future<Void> dbInfoChange = Void();
	state Reference<ILogSystem::IPeekCursor> r;
	state Version tagAt = beginVersion;
	state Version lastVer = 0;

	while (!endVersion.present() || logData->version.get() < endVersion.get()) {
		loop {
			choose {
				when(wait( r ? r->getMore(TaskTLogCommit) : Never() ) ) {
					break;
				}
				when( wait( dbInfoChange ) ) {
					if( logData->logSystem->get() ) {
						r = logData->logSystem->get()->peek( logData->logId, tagAt, endVersion, tags, parallelGetMore );
					} else {
						r = Reference<ILogSystem::IPeekCursor>();
					}
					dbInfoChange = logData->logSystem->onChange();
				}
			}
		}

		state double waitStartT = 0;
		while( self->bytesInput - self->bytesDurable >= SERVER_KNOBS->TLOG_HARD_LIMIT_BYTES && !logData->stopped ) {
			if (now() - waitStartT >= 1) {
				TraceEvent(SevWarn, "TLogUpdateLag", logData->logId)
					.detail("Version", logData->version.get())
					.detail("PersistentDataVersion", logData->persistentDataVersion)
					.detail("PersistentDataDurableVersion", logData->persistentDataDurableVersion);
				waitStartT = now();
			}
			wait( delayJittered(.005, TaskTLogCommit) );
		}

		state Version ver = 0;
		state std::vector<TagsAndMessage> messages;
		loop {
			if(logData->stopped) {
				return Void();
			}

			state bool foundMessage = r->hasMessage();
			if (!foundMessage || r->version().version != ver) {
				ASSERT(r->version().version > lastVer);
				if (ver) {
					if(endVersion.present() && ver > endVersion.get()) {
						return Void();
					}

					if(poppedIsKnownCommitted) {
						logData->knownCommittedVersion = std::max(logData->knownCommittedVersion, r->popped());
					}

					commitMessages(self, logData, ver, messages);

					if(self->terminated.isSet()) {
						return Void();
					}

					// Log the changes to the persistent queue, to be committed by commitQueue()
					AlternativeTLogQueueEntryRef qe;
					qe.version = ver;
					qe.knownCommittedVersion = logData->knownCommittedVersion;
					qe.alternativeMessages = &messages;
					qe.id = logData->logId;
					self->persistentQueue->push( qe, logData );

					self->diskQueueCommitBytes += qe.expectedSize();
					if( self->diskQueueCommitBytes > SERVER_KNOBS->MAX_QUEUE_COMMIT_BYTES ) {
						self->largeDiskQueueCommitBytes.set(true);
					}

					// Notifies the commitQueue actor to commit persistentQueue, and also unblocks tLogPeekMessages actors
					logData->version.set( ver );
					wait( yield(TaskTLogCommit) );
				}
				lastVer = ver;
				ver = r->version().version;
				messages.clear();

				if (!foundMessage) {
					ver--;
					if(ver > logData->version.get()) {
						if(endVersion.present() && ver > endVersion.get()) {
							return Void();
						}

						if(poppedIsKnownCommitted) {
							logData->knownCommittedVersion = std::max(logData->knownCommittedVersion, r->popped());
						}

						if(self->terminated.isSet()) {
							return Void();
						}

						// Log the changes to the persistent queue, to be committed by commitQueue()
						TLogQueueEntryRef qe;
						qe.version = ver;
						qe.knownCommittedVersion = logData->knownCommittedVersion;
						qe.messages = StringRef();
						qe.id = logData->logId;
						self->persistentQueue->push( qe, logData );

						self->diskQueueCommitBytes += qe.expectedSize();
						if( self->diskQueueCommitBytes > SERVER_KNOBS->MAX_QUEUE_COMMIT_BYTES ) {
							self->largeDiskQueueCommitBytes.set(true);
						}

						// Notifies the commitQueue actor to commit persistentQueue, and also unblocks tLogPeekMessages actors
						logData->version.set( ver );
						wait( yield(TaskTLogCommit) );
					}
					break;
				}
			}

			messages.push_back( TagsAndMessage(r->getMessageWithTags(), r->getTags()) );
			r->nextMessage();
		}

		tagAt = std::max( r->version().version, logData->version.get() + 1 );
	}
	return Void();
}

ACTOR Future<Void> tLogCore( TLogData* self, Reference<LogData> logData, TLogInterface tli, bool pulledRecoveryVersions ) {
	if(logData->removed.isReady()) {
		wait(delay(0)); //to avoid iterator invalidation in restorePersistentState when removed is already ready
		ASSERT(logData->removed.isError());

		if(logData->removed.getError().code() != error_code_worker_removed) {
			throw logData->removed.getError();
		}

		removeLog(self, logData);
		return Void();
	}

	state PromiseStream<Void> warningCollectorInput;
	state Future<Void> warningCollector = timeoutWarningCollector( warningCollectorInput.getFuture(), 1.0, "TLogQueueCommitSlow", self->dbgid );
	state Future<Void> error = actorCollection( logData->addActor.getFuture() );

	logData->addActor.send( waitFailureServer( tli.waitFailure.getFuture()) );
	logData->addActor.send( logData->removed );
	//FIXME: update tlogMetrics to include new information, or possibly only have one copy for the shared instance
	logData->addActor.send( traceCounters("TLogMetrics", logData->logId, SERVER_KNOBS->STORAGE_LOGGING_DELAY, &logData->cc, logData->logId.toString() + "/TLogMetrics"));
	logData->addActor.send( serveTLogInterface(self, tli, logData, warningCollectorInput) );

	if(!logData->isPrimary) {
		std::vector<Tag> tags;
		tags.push_back(logData->remoteTag);
		logData->addActor.send( pullAsyncData(self, logData, tags, pulledRecoveryVersions ? logData->recoveredAt + 1 : logData->unrecoveredBefore, Optional<Version>(), true, false) );
	}

	try {
		wait( error );
		throw internal_error();
	} catch( Error &e ) {
		if( e.code() != error_code_worker_removed )
			throw;

		removeLog(self, logData);
		return Void();
	}
}

ACTOR Future<Void> checkEmptyQueue(TLogData* self) {
	TraceEvent("TLogCheckEmptyQueueBegin", self->dbgid);
	try {
		TLogQueueEntry r = wait( self->persistentQueue->readNext(self) );
		throw internal_error();
	} catch (Error& e) {
		if (e.code() != error_code_end_of_stream) throw;
		TraceEvent("TLogCheckEmptyQueueEnd", self->dbgid);
		return Void();
	}
}

ACTOR Future<Void> checkRecovered(TLogData* self) {
	TraceEvent("TLogCheckRecoveredBegin", self->dbgid);
	Optional<Value> v = wait( self->persistentData->readValue(StringRef()) );
	TraceEvent("TLogCheckRecoveredEnd", self->dbgid);
	return Void();
}

ACTOR Future<Void> restorePersistentState( TLogData* self, LocalityData locality, Promise<Void> oldLog, Promise<Void> recovered, PromiseStream<InitializeTLogRequest> tlogRequests ) {
	state double startt = now();
	state Reference<LogData> logData;
	state KeyRange tagKeys;
	// PERSIST: Read basic state from persistentData; replay persistentQueue but don't erase it

	TraceEvent("TLogRestorePersistentState", self->dbgid);

	state IKeyValueStore *storage = self->persistentData;
	wait(storage->init());
	state Future<Optional<Value>> fFormat = storage->readValue(persistFormat.key);
	state Future<Standalone<VectorRef<KeyValueRef>>> fVers = storage->readRange(persistCurrentVersionKeys);
	state Future<Standalone<VectorRef<KeyValueRef>>> fKnownCommitted = storage->readRange(persistKnownCommittedVersionKeys);
	state Future<Standalone<VectorRef<KeyValueRef>>> fLocality = storage->readRange(persistLocalityKeys);
	state Future<Standalone<VectorRef<KeyValueRef>>> fLogRouterTags = storage->readRange(persistLogRouterTagsKeys);
	state Future<Standalone<VectorRef<KeyValueRef>>> fRecoverCounts = storage->readRange(persistRecoveryCountKeys);

	// FIXME: metadata in queue?

	wait( waitForAll( (vector<Future<Optional<Value>>>(), fFormat ) ) );
	wait( waitForAll( (vector<Future<Standalone<VectorRef<KeyValueRef>>>>(), fVers, fKnownCommitted, fLocality, fLogRouterTags, fRecoverCounts) ) );

	if (fFormat.get().present() && !persistFormatReadableRange.contains( fFormat.get().get() )) {
		//FIXME: remove when we no longer need to test upgrades from 4.X releases
		if(g_network->isSimulated()) {
			TraceEvent("ElapsedTime").detail("SimTime", now()).detail("RealTime", 0).detail("RandomUnseed", 0);
			flushAndExit(0);
		}

		TraceEvent(SevError, "UnsupportedDBFormat", self->dbgid).detail("Format", printable(fFormat.get().get())).detail("Expected", persistFormat.value.toString());
		throw worker_recovery_failed();
	}

	if (!fFormat.get().present()) {
		Standalone<VectorRef<KeyValueRef>> v = wait( self->persistentData->readRange( KeyRangeRef(StringRef(), LiteralStringRef("\xff")), 1 ) );
		if (!v.size()) {
			TEST(true); // The DB is completely empty, so it was never initialized.  Delete it.
			throw worker_removed();
		} else {
			// This should never happen
			TraceEvent(SevError, "NoDBFormatKey", self->dbgid).detail("FirstKey", printable(v[0].key));
			ASSERT( false );
			throw worker_recovery_failed();
		}
	}

	state std::vector<Future<ErrorOr<Void>>> removed;

	if(fFormat.get().get() == LiteralStringRef("FoundationDB/LogServer/2/3")) {
		//FIXME: need for upgrades from 5.X to 6.0, remove once this upgrade path is no longer needed
		if(recovered.canBeSet()) recovered.send(Void());
		oldLog.send(Void());
		while(!tlogRequests.isEmpty()) {
			tlogRequests.getFuture().pop().reply.sendError(recruitment_failed());
		}

		wait( oldTLog::tLog(self->persistentData, self->rawPersistentQueue, self->dbInfo, locality, self->dbgid) );
		throw internal_error();
	}

	ASSERT(fVers.get().size() == fRecoverCounts.get().size());

	state std::map<UID, int8_t> id_locality;
	for(auto it : fLocality.get()) {
		id_locality[ BinaryReader::fromStringRef<UID>(it.key.removePrefix(persistLocalityKeys.begin), Unversioned())] = BinaryReader::fromStringRef<int8_t>( it.value, Unversioned() );
	}

	state std::map<UID, int> id_logRouterTags;
	for(auto it : fLogRouterTags.get()) {
		id_logRouterTags[ BinaryReader::fromStringRef<UID>(it.key.removePrefix(persistLogRouterTagsKeys.begin), Unversioned())] = BinaryReader::fromStringRef<int>( it.value, Unversioned() );
	}

	state std::map<UID, Version> id_knownCommitted;
	for(auto it : fKnownCommitted.get()) {
		id_knownCommitted[ BinaryReader::fromStringRef<UID>(it.key.removePrefix(persistKnownCommittedVersionKeys.begin), Unversioned())] = BinaryReader::fromStringRef<Version>( it.value, Unversioned() );
	}

	state int idx = 0;
	state Promise<Void> registerWithMaster;
	state std::map<UID, TLogInterface> id_interf;
	for(idx = 0; idx < fVers.get().size(); idx++) {
		state KeyRef rawId = fVers.get()[idx].key.removePrefix(persistCurrentVersionKeys.begin);
		UID id1 = BinaryReader::fromStringRef<UID>( rawId, Unversioned() );
		UID id2 = BinaryReader::fromStringRef<UID>( fRecoverCounts.get()[idx].key.removePrefix(persistRecoveryCountKeys.begin), Unversioned() );
		ASSERT(id1 == id2);

		TLogInterface recruited(id1, self->dbgid, locality);
		recruited.initEndpoints();

		DUMPTOKEN( recruited.peekMessages );
		DUMPTOKEN( recruited.popMessages );
		DUMPTOKEN( recruited.commit );
		DUMPTOKEN( recruited.lock );
		DUMPTOKEN( recruited.getQueuingMetrics );
		DUMPTOKEN( recruited.confirmRunning );

		//We do not need the remoteTag, because we will not be loading any additional data
		logData = Reference<LogData>( new LogData(self, recruited, Tag(), true, id_logRouterTags[id1], UID(), std::vector<Tag>()) );
		logData->locality = id_locality[id1];
		logData->stopped = true;
		self->id_data[id1] = logData;
		id_interf[id1] = recruited;

		logData->knownCommittedVersion = id_knownCommitted[id1];
		Version ver = BinaryReader::fromStringRef<Version>( fVers.get()[idx].value, Unversioned() );
		logData->persistentDataVersion = ver;
		logData->persistentDataDurableVersion = ver;
		logData->version.set(ver);
		logData->recoveryCount = BinaryReader::fromStringRef<DBRecoveryCount>( fRecoverCounts.get()[idx].value, Unversioned() );
		logData->removed = rejoinMasters(self, recruited, logData->recoveryCount, registerWithMaster.getFuture(), false);
		removed.push_back(errorOr(logData->removed));

		TraceEvent("TLogRestorePersistentStateVer", id1).detail("Ver", ver);

		// Restore popped keys.  Pop operations that took place after the last (committed) updatePersistentDataVersion might be lost, but
		// that is fine because we will get the corresponding data back, too.
		tagKeys = prefixRange( rawId.withPrefix(persistTagPoppedKeys.begin) );
		loop {
			if(logData->removed.isReady()) break;
			Standalone<VectorRef<KeyValueRef>> data = wait( self->persistentData->readRange( tagKeys, BUGGIFY ? 3 : 1<<30, 1<<20 ) );
			if (!data.size()) break;
			((KeyRangeRef&)tagKeys) = KeyRangeRef( keyAfter(data.back().key, tagKeys.arena()), tagKeys.end );

			for(auto &kv : data) {
				Tag tag = decodeTagPoppedKey(rawId, kv.key);
				Version popped = decodeTagPoppedValue(kv.value);
				TraceEvent("TLogRestorePopped", logData->logId).detail("Tag", tag.toString()).detail("To", popped);
				auto tagData = logData->getTagData(tag);
				ASSERT( !tagData );
				logData->createTagData(tag, popped, false, false, false);
			}
		}
	}

	state Future<Void> allRemoved = waitForAll(removed);
	state UID lastId = UID(1,1); //initialized so it will not compare equal to a default UID
	state double recoverMemoryLimit = SERVER_KNOBS->TLOG_RECOVER_MEMORY_LIMIT;
	if (BUGGIFY) recoverMemoryLimit = std::max<double>(SERVER_KNOBS->BUGGIFY_RECOVER_MEMORY_LIMIT, SERVER_KNOBS->TLOG_SPILL_THRESHOLD);

	try {
		loop {
			if(allRemoved.isReady()) {
				TEST(true); //all tlogs removed during queue recovery
				throw worker_removed();
			}
			choose {
				when( TLogQueueEntry qe = wait( self->persistentQueue->readNext(self) ) ) {
					if(!self->queueOrder.size() || self->queueOrder.back() != qe.id) self->queueOrder.push_back(qe.id);
					if(qe.id != lastId) {
						lastId = qe.id;
						auto it = self->id_data.find(qe.id);
						if(it != self->id_data.end()) {
							logData = it->second;
						} else {
							logData = Reference<LogData>();
						}
					}

					//TraceEvent("TLogRecoveredQE", self->dbgid).detail("LogId", qe.id).detail("Ver", qe.version).detail("MessageBytes", qe.messages.size()).detail("Tags", qe.tags.size())
					//	.detail("Tag0", qe.tags.size() ? qe.tags[0].tag : invalidTag).detail("Version", logData->version.get());

					if(logData) {
						logData->knownCommittedVersion = std::max(logData->knownCommittedVersion, qe.knownCommittedVersion);
						if( qe.version > logData->version.get() ) {
							commitMessages(self, logData, qe.version, qe.arena(), qe.messages);
							logData->version.set( qe.version );
							logData->queueCommittedVersion.set( qe.version );

							while (self->bytesInput - self->bytesDurable >= recoverMemoryLimit) {
								TEST(true);  // Flush excess data during TLog queue recovery
								TraceEvent("FlushLargeQueueDuringRecovery", self->dbgid).detail("BytesInput", self->bytesInput).detail("BytesDurable", self->bytesDurable).detail("Version", logData->version.get()).detail("PVer", logData->persistentDataVersion);

								choose {
									when( wait( updateStorage(self) ) ) {}
									when( wait( allRemoved ) ) { throw worker_removed(); }
								}
							}
						}
					}
				}
				when( wait( allRemoved ) ) { throw worker_removed(); }
			}
		}
	} catch (Error& e) {
		if (e.code() != error_code_end_of_stream) throw;
	}

	TraceEvent("TLogRestorePersistentStateDone", self->dbgid).detail("Took", now()-startt);
	TEST( now()-startt >= 1.0 );  // TLog recovery took more than 1 second

	for(auto it : self->id_data) {
		if(it.second->queueCommittedVersion.get() == 0) {
			TraceEvent("TLogZeroVersion", self->dbgid).detail("LogId", it.first);
			it.second->queueCommittedVersion.set(it.second->version.get());
		}
		it.second->recoveryComplete.sendError(end_of_stream());
		self->sharedActors.send( tLogCore( self, it.second, id_interf[it.first], false ) );
	}

	if(registerWithMaster.canBeSet()) registerWithMaster.send(Void());
	return Void();
}

bool tlogTerminated( TLogData* self, IKeyValueStore* persistentData, TLogQueue* persistentQueue, Error const& e ) {
	// Dispose the IKVS (destroying its data permanently) only if this shutdown is definitely permanent.  Otherwise just close it.
	if (e.code() == error_code_worker_removed || e.code() == error_code_recruitment_failed) {
		persistentData->dispose();
		persistentQueue->dispose();
	} else {
		persistentData->close();
		persistentQueue->close();
	}

	if ( e.code() == error_code_worker_removed ||
		 e.code() == error_code_recruitment_failed ||
		 e.code() == error_code_file_not_found )
	{
		TraceEvent("TLogTerminated", self->dbgid).error(e, true);
		return true;
	} else
		return false;
}

ACTOR Future<Void> updateLogSystem(TLogData* self, Reference<LogData> logData, LogSystemConfig recoverFrom, Reference<AsyncVar<Reference<ILogSystem>>> logSystem) {
	loop {
		bool found = false;
		if(self->dbInfo->get().logSystemConfig.recruitmentID == logData->recruitmentID) {
			if( self->dbInfo->get().logSystemConfig.isNextGenerationOf(recoverFrom) ) {
				logSystem->set(ILogSystem::fromOldLogSystemConfig( logData->logId, self->dbInfo->get().myLocality, self->dbInfo->get().logSystemConfig ));
				found = true;
			} else if( self->dbInfo->get().logSystemConfig.isEqualIds(recoverFrom) ) {
				logSystem->set(ILogSystem::fromLogSystemConfig( logData->logId, self->dbInfo->get().myLocality, self->dbInfo->get().logSystemConfig, false, true ));
				found = true;
			}
			else if( self->dbInfo->get().recoveryState >= RecoveryState::ACCEPTING_COMMITS ) {
				logSystem->set(ILogSystem::fromLogSystemConfig( logData->logId, self->dbInfo->get().myLocality, self->dbInfo->get().logSystemConfig, true ));
				found = true;
			}
		}
		if( !found ) {
			logSystem->set(Reference<ILogSystem>());
		} else {
			logData->logSystem->get()->pop(logData->logRouterPoppedVersion, logData->remoteTag, logData->durableKnownCommittedVersion, logData->locality);
		}
		TraceEvent("TLogUpdate", self->dbgid).detail("LogId", logData->logId).detail("RecruitmentID", logData->recruitmentID).detail("DbRecruitmentID", self->dbInfo->get().logSystemConfig.recruitmentID).detail("RecoverFrom", recoverFrom.toString()).detail("DbInfo", self->dbInfo->get().logSystemConfig.toString()).detail("Found", found).detail("LogSystem", (bool) logSystem->get() ).detail("RecoveryState", (int)self->dbInfo->get().recoveryState);
		for(auto it : self->dbInfo->get().logSystemConfig.oldTLogs) {
			TraceEvent("TLogUpdateOld", self->dbgid).detail("LogId", logData->logId).detail("DbInfo", it.toString());
		}
		wait( self->dbInfo->onChange() );
	}
}

ACTOR Future<Void> tLogStart( TLogData* self, InitializeTLogRequest req, LocalityData locality ) {
	state TLogInterface recruited(self->dbgid, locality);
	recruited.locality = locality;
	recruited.initEndpoints();

	DUMPTOKEN( recruited.peekMessages );
	DUMPTOKEN( recruited.popMessages );
	DUMPTOKEN( recruited.commit );
	DUMPTOKEN( recruited.lock );
	DUMPTOKEN( recruited.getQueuingMetrics );
	DUMPTOKEN( recruited.confirmRunning );

	for(auto it : self->id_data) {
		if( !it.second->stopped ) {
			TraceEvent("TLogStoppedByNewRecruitment", self->dbgid).detail("StoppedId", it.first.toString()).detail("RecruitedId", recruited.id()).detail("EndEpoch", it.second->logSystem->get().getPtr() != 0);
			if(!it.second->isPrimary && it.second->logSystem->get()) {
				it.second->removed = it.second->removed && it.second->logSystem->get()->endEpoch();
			}
			if(it.second->committingQueue.canBeSet()) {
				it.second->committingQueue.sendError(worker_removed());
			}
		}
		it.second->stopped = true;
		if(!it.second->recoveryComplete.isSet()) {
			it.second->recoveryComplete.sendError(end_of_stream());
		}
		it.second->stopCommit.trigger();
	}

	state Reference<LogData> logData = Reference<LogData>( new LogData(self, recruited, req.remoteTag, req.isPrimary, req.logRouterTags, req.recruitmentID, req.allTags) );
	self->id_data[recruited.id()] = logData;
	logData->locality = req.locality;
	logData->recoveryCount = req.epoch;
	logData->removed = rejoinMasters(self, recruited, req.epoch, Future<Void>(Void()), req.isPrimary);
	self->queueOrder.push_back(recruited.id());

	TraceEvent("TLogStart", logData->logId);
	state Future<Void> updater;
	state bool pulledRecoveryVersions = false;
	try {
		if( logData->removed.isReady() ) {
			throw logData->removed.getError();
		}

		if (req.recoverFrom.logSystemType == 2) {
			logData->unrecoveredBefore = req.startVersion;
			logData->recoveredAt = req.recoverAt;
			logData->knownCommittedVersion = req.startVersion - 1;
			logData->persistentDataVersion = logData->unrecoveredBefore - 1;
			logData->persistentDataDurableVersion = logData->unrecoveredBefore - 1;
			logData->queueCommittedVersion.set( logData->unrecoveredBefore - 1 );
			logData->version.set( logData->unrecoveredBefore - 1 );

			logData->unpoppedRecoveredTags = req.allTags.size();
			wait( initPersistentState( self, logData ) || logData->removed );

			TraceEvent("TLogRecover", self->dbgid).detail("LogId", logData->logId).detail("At", req.recoverAt).detail("Known", req.knownCommittedVersion).detail("Unrecovered", logData->unrecoveredBefore).detail("Tags", describe(req.recoverTags)).detail("Locality", req.locality).detail("LogRouterTags", logData->logRouterTags);

			if(logData->recoveryComplete.isSet()) {
				throw worker_removed();
			}

			updater = updateLogSystem(self, logData, req.recoverFrom, logData->logSystem);

			logData->initialized = true;
			self->newLogData.trigger();

			if((req.isPrimary || req.recoverFrom.logRouterTags == 0) && !logData->stopped && logData->unrecoveredBefore <= req.recoverAt) {
				if(req.recoverFrom.logRouterTags > 0 && req.locality != tagLocalitySatellite) {
					logData->logRouterPopToVersion = req.recoverAt;
					std::vector<Tag> tags;
					tags.push_back(logData->remoteTag);
					wait(pullAsyncData(self, logData, tags, logData->unrecoveredBefore, req.recoverAt, true, false) || logData->removed);
				} else if(!req.recoverTags.empty()) {
					ASSERT(logData->unrecoveredBefore > req.knownCommittedVersion);
					wait(pullAsyncData(self, logData, req.recoverTags, req.knownCommittedVersion + 1, req.recoverAt, false, true) || logData->removed);
				}
				pulledRecoveryVersions = true;
				logData->knownCommittedVersion = req.recoverAt;
			}

			if((req.isPrimary || req.recoverFrom.logRouterTags == 0) && logData->version.get() < req.recoverAt && !logData->stopped) {
				// Log the changes to the persistent queue, to be committed by commitQueue()
				TLogQueueEntryRef qe;
				qe.version = req.recoverAt;
				qe.knownCommittedVersion = logData->knownCommittedVersion;
				qe.messages = StringRef();
				qe.id = logData->logId;
				self->persistentQueue->push( qe, logData );

				self->diskQueueCommitBytes += qe.expectedSize();
				if( self->diskQueueCommitBytes > SERVER_KNOBS->MAX_QUEUE_COMMIT_BYTES ) {
					self->largeDiskQueueCommitBytes.set(true);
				}

				logData->version.set( req.recoverAt );
			}

			if(logData->recoveryComplete.isSet()) {
				throw worker_removed();
			}

			logData->addActor.send( respondToRecovered( recruited, logData->recoveryComplete ) );
		} else {
			// Brand new tlog, initialization has already been done by caller
			wait( initPersistentState( self, logData ) || logData->removed );

			if(logData->recoveryComplete.isSet()) {
				throw worker_removed();
			}

			logData->initialized = true;
			self->newLogData.trigger();

			logData->recoveryComplete.send(Void());
		}
		wait(logData->committingQueue.getFuture() || logData->removed );
	} catch( Error &e ) {
		if(e.code() != error_code_actor_cancelled) {
			req.reply.sendError(e);
		}

		if( e.code() != error_code_worker_removed ) {
			throw;
		}

		wait( delay(0.0) ); // if multiple recruitment requests were already in the promise stream make sure they are all started before any are removed

		removeLog(self, logData);
		return Void();
	}

	req.reply.send( recruited );

	TraceEvent("TLogReady", logData->logId).detail("AllTags", describe(req.allTags)).detail("Locality", logData->locality);

	updater = Void();
	wait( tLogCore( self, logData, recruited, pulledRecoveryVersions ) );
	return Void();
}

// New tLog (if !recoverFrom.size()) or restore from network
ACTOR Future<Void> tLog( IKeyValueStore* persistentData, IDiskQueue* persistentQueue, Reference<AsyncVar<ServerDBInfo>> db, LocalityData locality, PromiseStream<InitializeTLogRequest> tlogRequests, UID tlogId, bool restoreFromDisk, Promise<Void> oldLog, Promise<Void> recovered ) {
	state TLogData self( tlogId, persistentData, persistentQueue, db );
	state Future<Void> error = actorCollection( self.sharedActors.getFuture() );

	TraceEvent("SharedTlog", tlogId);
	// FIXME: Pass the worker id instead of stubbing it
	startRole(Role::SHARED_TRANSACTION_LOG, tlogId, UID());
	try {
		if(restoreFromDisk) {
			wait( restorePersistentState( &self, locality, oldLog, recovered, tlogRequests ) );
		} else {
			wait( checkEmptyQueue(&self) && checkRecovered(&self) );
		}

		//Disk errors need a chance to kill this actor.
		wait(delay(0.000001));

		if(recovered.canBeSet()) recovered.send(Void());

		self.sharedActors.send( cleanupPeekTrackers(&self) );
		self.sharedActors.send( commitQueue(&self) );
		self.sharedActors.send( updateStorageLoop(&self) );

		loop {
			choose {
				when ( InitializeTLogRequest req = waitNext(tlogRequests.getFuture() ) ) {
					if( !self.tlogCache.exists( req.recruitmentID ) ) {
						self.tlogCache.set( req.recruitmentID, req.reply.getFuture() );
						self.sharedActors.send( self.tlogCache.removeOnReady( req.recruitmentID, tLogStart( &self, req, locality ) ) );
					} else {
						forwardPromise( req.reply, self.tlogCache.get( req.recruitmentID ) );
					}
				}
				when ( wait( error ) ) { throw internal_error(); }
			}
		}
	} catch (Error& e) {
		self.terminated.send(Void());
		TraceEvent("TLogError", tlogId).error(e, true);
		endRole(Role::SHARED_TRANSACTION_LOG, tlogId, "Error", true);
		if(recovered.canBeSet()) recovered.send(Void());

		while(!tlogRequests.isEmpty()) {
			tlogRequests.getFuture().pop().reply.sendError(recruitment_failed());
		}

		for( auto& it : self.id_data ) {
			if(!it.second->recoveryComplete.isSet()) {
				it.second->recoveryComplete.sendError(end_of_stream());
			}
		}

		if (tlogTerminated( &self, persistentData, self.persistentQueue, e )) {
			return Void();
		} else {
			throw;
		}
	}
}

// UNIT TESTS
struct DequeAllocatorStats {
	static int64_t allocatedBytes;
};

int64_t DequeAllocatorStats::allocatedBytes = 0;

template <class T>
struct DequeAllocator : std::allocator<T> {
	template<typename U>
	struct rebind {
		typedef DequeAllocator<U> other;
	};

	DequeAllocator() {}

	template<typename U>
	DequeAllocator(DequeAllocator<U> const& u) : std::allocator<T>(u) {}

	T* allocate(std::size_t n, std::allocator<void>::const_pointer hint = 0) {
		DequeAllocatorStats::allocatedBytes += n * sizeof(T);
		//fprintf(stderr, "Allocating %lld objects for %lld bytes (total allocated: %lld)\n", n, n * sizeof(T), DequeAllocatorStats::allocatedBytes);
		return std::allocator<T>::allocate(n, hint);
	}
	void deallocate(T* p, std::size_t n) {
		DequeAllocatorStats::allocatedBytes -= n * sizeof(T);
		//fprintf(stderr, "Deallocating %lld objects for %lld bytes (total allocated: %lld)\n", n, n * sizeof(T), DequeAllocatorStats::allocatedBytes);
		return std::allocator<T>::deallocate(p, n);
	}
};

TEST_CASE("/fdbserver/tlogserver/VersionMessagesOverheadFactor" ) {

	typedef std::pair<Version, LengthPrefixedStringRef> TestType; // type used by versionMessages

	for(int i = 1; i < 9; ++i) {
		for(int j = 0; j < 20; ++j) {
			DequeAllocatorStats::allocatedBytes = 0;
			DequeAllocator<TestType> allocator;
			std::deque<TestType, DequeAllocator<TestType>> d(allocator);

			int numElements = g_random->randomInt(pow(10, i-1), pow(10, i));
			for(int k = 0; k < numElements; ++k) {
				d.push_back(TestType());
			}

			int removedElements = 0;//g_random->randomInt(0, numElements); // FIXME: the overhead factor does not accurately account for removal!
			for(int k = 0; k < removedElements; ++k) {
				d.pop_front();
			}

			int64_t dequeBytes = DequeAllocatorStats::allocatedBytes + sizeof(std::deque<TestType>);
			int64_t insertedBytes = (numElements-removedElements) * sizeof(TestType);
			double overheadFactor = std::max<double>(insertedBytes, dequeBytes-10000) / insertedBytes; // We subtract 10K here as an estimated upper bound for the fixed cost of an std::deque
			//fprintf(stderr, "%d elements (%d inserted, %d removed):\n", numElements-removedElements, numElements, removedElements);
			//fprintf(stderr, "Allocated %lld bytes to store %lld bytes (%lf overhead factor)\n", dequeBytes, insertedBytes, overheadFactor);
			ASSERT(overheadFactor * 1024 <= SERVER_KNOBS->VERSION_MESSAGES_OVERHEAD_FACTOR_1024THS);
		}
	}

	return Void();
}
