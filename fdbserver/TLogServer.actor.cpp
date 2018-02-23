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

#include "flow/actorcompiler.h"
#include "flow/Hash3.h"
#include "flow/Stats.h"
#include "flow/UnitTest.h"
#include "fdbclient/NativeAPI.h"
#include "fdbclient/Notified.h"
#include "fdbclient/KeyRangeMap.h"
#include "fdbclient/SystemData.h"
#include "WorkerInterface.h"
#include "TLogInterface.h"
#include "Knobs.h"
#include "IKeyValueStore.h"
#include "flow/ActorCollection.h"
#include "fdbrpc/FailureMonitor.h"
#include "IDiskQueue.h"
#include "fdbrpc/sim_validation.h"
#include "ServerDBInfo.h"
#include "LogSystem.h"
#include "WaitFailure.h"

using std::pair;
using std::make_pair;
using std::min;
using std::max;

struct TLogQueueEntryRef {
	UID id;
	Version version;
	Version knownCommittedVersion;
	StringRef messages;
	VectorRef< TagMessagesRef > tags;

	TLogQueueEntryRef() : version(0), knownCommittedVersion(0) {}
	TLogQueueEntryRef(Arena &a, TLogQueueEntryRef const &from)
	  : version(from.version), knownCommittedVersion(from.knownCommittedVersion), id(from.id), messages(a, from.messages), tags(a, from.tags) {
	}

	template <class Ar>
	void serialize(Ar& ar) {
		if( ar.protocolVersion() >= 0x0FDB00A460010001) {
			ar & version & messages & tags & knownCommittedVersion & id;
		} else if(ar.isDeserializing) {
			ar & version & messages & tags;
			knownCommittedVersion = 0;
			id = UID();
		}
	}
	size_t expectedSize() const {
		return messages.expectedSize() + tags.expectedSize();
	}
};

typedef Standalone<TLogQueueEntryRef> TLogQueueEntry;

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
	Future<TLogQueueEntry> readNext() {
		return readNext( this );
	}

	void push( TLogQueueEntryRef const& qe ) {
		BinaryWriter wr( Unversioned() );  // outer framing is not versioned
		wr << uint32_t(0);
		IncludeVersion().write(wr);  // payload is versioned
		wr << qe;
		wr << uint8_t(1);
		*(uint32_t*)wr.getData() = wr.getLength() - sizeof(uint32_t) - sizeof(uint8_t);
		auto loc = queue->push( wr.toStringRef() );
		//TraceEvent("TLogQueueVersionWritten", dbgid).detail("Size", wr.getLength() - sizeof(uint32_t) - sizeof(uint8_t)).detail("Loc", loc);
		version_location[qe.version] = loc;
	}
	void pop( Version upTo ) {
		// Keep only the given and all subsequent version numbers
		// Find the first version >= upTo
		auto v = version_location.lower_bound(upTo);
		if (v == version_location.begin()) return;

		if(v == version_location.end()) {
			v = version_location.lastItem();
		}
		else {
			v.decrementNonEnd();
		}

		queue->pop( v->value );
		version_location.erase( version_location.begin(), v );  // ... and then we erase that previous version and all prior versions
	}
	Future<Void> commit() { return queue->commit(); }

	// Implements IClosable
	virtual Future<Void> getError() { return queue->getError(); }
	virtual Future<Void> onClosed() { return queue->onClosed(); }
	virtual void dispose() { queue->dispose(); delete this; }
	virtual void close() { queue->close(); delete this; }

private:
	IDiskQueue* queue;
	Map<Version, IDiskQueue::location> version_location;  // For the version of each entry that was push()ed, the end location of the serialized bytes
	UID dbgid;

	ACTOR static Future<TLogQueueEntry> readNext( TLogQueue* self ) {
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
				self->version_location[result.version] = self->queue->getNextReadLocation();
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

struct LengthPrefixedStringRef {
	// Represents a pointer to a string which is prefixed by a 4-byte length
	// A LengthPrefixedStringRef is only pointer-sized (8 bytes vs 12 bytes for StringRef), but the corresponding string is 4 bytes bigger, and
	// substring operations aren't efficient as they are with StringRef.  It's a good choice when there might be lots of references to the same
	// exact string.

	uint32_t* length;

	StringRef toStringRef() const { ASSERT(length); return StringRef( (uint8_t*)(length+1), *length ); }
	int expectedSize() const { ASSERT(length); return *length; }
	uint32_t* getLengthPtr() const { return length; }

	LengthPrefixedStringRef() : length(NULL) {}
	LengthPrefixedStringRef(uint32_t* length) : length(length) {}
};

template<class T>
struct CompareFirst {
	bool operator() (T const& lhs, T const& rhs) const {
		return lhs.first < rhs.first;
	}
};

////// Persistence format (for self->persistentData)

// Immutable keys
static const KeyValueRef persistFormat( LiteralStringRef( "Format" ), LiteralStringRef("FoundationDB/LogServer/2/3") );
static const KeyRangeRef persistFormatReadableRange( LiteralStringRef("FoundationDB/LogServer/2/2"), LiteralStringRef("FoundationDB/LogServer/2/4") );
static const KeyRangeRef persistRecoveryCountKeys = KeyRangeRef( LiteralStringRef( "DbRecoveryCount/" ), LiteralStringRef( "DbRecoveryCount0" ) );

// Updated on updatePersistentData()
static const KeyRangeRef persistCurrentVersionKeys = KeyRangeRef( LiteralStringRef( "version/" ), LiteralStringRef( "version0" ) );
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
	AsyncTrigger newVersion;

	int64_t instanceID;
	int64_t bytesInput;
	int64_t bytesDurable;

	Version prevVersion;

	struct peekTrackerData {
		std::map<int, Promise<Version>> sequence_version;
		double lastUpdate;
	};

	std::map<UID, peekTrackerData> peekTracker;
	WorkerCache<TLogInterface> tlogCache;

	Future<Void> updatePersist; //SOMEDAY: integrate the recovery and update storage so that only one of them is committing to persistant data.
	Future<Void> oldLogServer;

	PromiseStream<Future<Void>> sharedActors;
	bool terminated;

	TLogData(UID dbgid, IKeyValueStore* persistentData, IDiskQueue * persistentQueue, Reference<AsyncVar<ServerDBInfo>> const& dbInfo)
			: dbgid(dbgid), instanceID(g_random->randomUniqueID().first()),
			  persistentData(persistentData), rawPersistentQueue(persistentQueue), persistentQueue(new TLogQueue(persistentQueue, dbgid)),
			  dbInfo(dbInfo), queueCommitBegin(0), queueCommitEnd(0), prevVersion(0),
			  diskQueueCommitBytes(0), largeDiskQueueCommitBytes(false),
			  bytesInput(0), bytesDurable(0), updatePersist(Void()), terminated(false)
		{
		}
};

struct LogData : NonCopyable, public ReferenceCounted<LogData> {
	struct TagData {
		std::deque<std::pair<Version, LengthPrefixedStringRef>> version_messages;
		bool nothing_persistent;				// true means tag is *known* to have no messages in persistentData.  false means nothing.
		bool popped_recently;					// `popped` has changed since last updatePersistentData
		Version popped;				// see popped version tracking contract below
		bool update_version_sizes;

		TagData( Version popped, bool nothing_persistent, bool popped_recently, Tag tag ) : nothing_persistent(nothing_persistent), popped(popped), popped_recently(popped_recently), update_version_sizes(tag != txsTag) {}

		TagData(TagData&& r) noexcept(true) : version_messages(std::move(r.version_messages)), nothing_persistent(r.nothing_persistent), popped_recently(r.popped_recently), popped(r.popped), update_version_sizes(r.update_version_sizes) {}
		void operator= (TagData&& r) noexcept(true) {
			version_messages = std::move(r.version_messages);
			nothing_persistent = r.nothing_persistent;
			popped_recently = r.popped_recently;
			popped = r.popped;
			update_version_sizes = r.update_version_sizes;
		}

		// Erase messages not needed to update *from* versions >= before (thus, messages with toversion <= before)
		ACTOR Future<Void> eraseMessagesBefore( TagData *self, Version before, int64_t* gBytesErased, Reference<LogData> tlogData, int taskID ) {
			while(!self->version_messages.empty() && self->version_messages.front().first < before) {
				Version version = self->version_messages.front().first;
				std::pair<int, int> &sizes = tlogData->version_sizes[version];
				int64_t messagesErased = 0;

				while(!self->version_messages.empty() && self->version_messages.front().first == version) {
					auto const& m = self->version_messages.front();
					++messagesErased;

					if(self->update_version_sizes) {
						sizes.first -= m.second.expectedSize();
					}

					self->version_messages.pop_front();
				}

				int64_t bytesErased = (messagesErased * sizeof(std::pair<Version, LengthPrefixedStringRef>) * SERVER_KNOBS->VERSION_MESSAGES_OVERHEAD_FACTOR_1024THS) >> 10;
				tlogData->bytesDurable += bytesErased;
				*gBytesErased += bytesErased;
				Void _ = wait(yield(taskID));
			}

			return Void();
		}

		Future<Void> eraseMessagesBefore(Version before, int64_t* gBytesErased, Reference<LogData> tlogData, int taskID) {
			return eraseMessagesBefore(this, before, gBytesErased, tlogData, taskID);
		}
	};

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

	bool stopped, initialized;
	DBRecoveryCount recoveryCount;

	VersionMetricHandle persistentDataVersion, persistentDataDurableVersion;  // The last version number in the portion of the log (written|durable) to persistentData
	NotifiedVersion version, queueCommittedVersion;
	Version queueCommittingVersion;
	Version knownCommittedVersion;

	Deque<std::pair<Version, Standalone<VectorRef<uint8_t>>>> messageBlocks;
	Map< Tag, TagData > tag_data;

	Map<Version, std::pair<int,int>> version_sizes;

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

	explicit LogData(TLogData* tLogData, TLogInterface interf) : tLogData(tLogData), knownCommittedVersion(0), tli(interf), logId(interf.id()),
			cc("TLog", interf.id().toString()),
			bytesInput("bytesInput", cc),
			bytesDurable("bytesDurable", cc),
			// These are initialized differently on init() or recovery
			recoveryCount(), stopped(false), initialized(false), queueCommittingVersion(0), newPersistentDataVersion(invalidVersion), recovery(Void())
	{
		startRole(interf.id(), UID(), "TLog");

		persistentDataVersion.init(LiteralStringRef("TLog.PersistentDataVersion"), cc.id);
		persistentDataDurableVersion.init(LiteralStringRef("TLog.PersistentDataDurableVersion"), cc.id);
		version.initMetric(LiteralStringRef("TLog.Version"), cc.id);
		queueCommittedVersion.initMetric(LiteralStringRef("TLog.QueueCommittedVersion"), cc.id);

		specialCounter(cc, "version", [this](){ return this->version.get(); });
		specialCounter(cc, "sharedBytesInput", [tLogData](){ return tLogData->bytesInput; });
		specialCounter(cc, "sharedBytesDurable", [tLogData](){ return tLogData->bytesDurable; });
		specialCounter(cc, "kvstoreBytesUsed", [tLogData](){ return tLogData->persistentData->getStorageBytes().used; });
		specialCounter(cc, "kvstoreBytesFree", [tLogData](){ return tLogData->persistentData->getStorageBytes().free; });
		specialCounter(cc, "kvstoreBytesAvailable", [tLogData](){ return tLogData->persistentData->getStorageBytes().available; });
		specialCounter(cc, "kvstoreBytesTotal", [tLogData](){ return tLogData->persistentData->getStorageBytes().total; });
		specialCounter(cc, "queueDiskBytesUsed", [tLogData](){ return tLogData->rawPersistentQueue->getStorageBytes().used; });
		specialCounter(cc, "queueDiskBytesFree", [tLogData](){ return tLogData->rawPersistentQueue->getStorageBytes().free; });
		specialCounter(cc, "queueDiskBytesAvailable", [tLogData](){ return tLogData->rawPersistentQueue->getStorageBytes().available; });
		specialCounter(cc, "queueDiskBytesTotal", [tLogData](){ return tLogData->rawPersistentQueue->getStorageBytes().total; });
	}

	~LogData() {
		tLogData->bytesDurable += bytesInput.getValue() - bytesDurable.getValue();
		TraceEvent("TLogBytesWhenRemoved", tli.id()).detail("sharedBytesInput", tLogData->bytesInput).detail("sharedBytesDurable", tLogData->bytesDurable).detail("localBytesInput", bytesInput.getValue()).detail("localBytesDurable", bytesDurable.getValue());

		ASSERT(tLogData->bytesDurable <= tLogData->bytesInput);
		endRole(tli.id(), "TLog", "Error", true);

		if(!tLogData->terminated) {
			Key logIdKey = BinaryWriter::toValue(logId,Unversioned());
			tLogData->persistentData->clear( singleKeyRange(logIdKey.withPrefix(persistCurrentVersionKeys.begin)) );
			tLogData->persistentData->clear( singleKeyRange(logIdKey.withPrefix(persistRecoveryCountKeys.begin)) );
			Key msgKey = logIdKey.withPrefix(persistTagMessagesKeys.begin);
			tLogData->persistentData->clear( KeyRangeRef( msgKey, strinc(msgKey) ) );
			Key poppedKey = logIdKey.withPrefix(persistTagPoppedKeys.begin);
			tLogData->persistentData->clear( KeyRangeRef( poppedKey, strinc(poppedKey) ) );
		}
	}

	LogEpoch epoch() const { return recoveryCount; }
};

ACTOR Future<Void> tLogLock( TLogData* self, ReplyPromise< TLogLockResult > reply, Reference<LogData> logData ) {
	state Version stopVersion = logData->version.get();

	TEST(true); // TLog stopped by recovering master
	TEST( logData->stopped );
	TEST( !logData->stopped );

	TraceEvent("TLogStop", logData->logId).detail("Ver", stopVersion).detail("isStopped", logData->stopped).detail("queueCommitted", logData->queueCommittedVersion.get());

	logData->stopped = true;
	if(logData->recoverySuccessful.canBeSet()) {
		logData->recoverySuccessful.send(false);
	}

	// Lock once the current version has been committed
	Void _ = wait( logData->queueCommittedVersion.whenAtLeast( stopVersion ) );

	ASSERT(stopVersion == logData->version.get());

	TLogLockResult result;
	result.end = stopVersion;
	result.knownCommittedVersion = logData->knownCommittedVersion;
	for( auto & tag : logData->tag_data )
		result.tags.push_back( tag.key );

	TraceEvent("TLogStop2", self->dbgid).detail("logId", logData->logId).detail("Ver", stopVersion).detail("isStopped", logData->stopped).detail("queueCommitted", logData->queueCommittedVersion.get()).detail("tags", describe(result.tags));


	reply.send( result );
	return Void();
}

void updatePersistentPopped( TLogData* self, Reference<LogData> logData, Tag tag, LogData::TagData& data ) {
	if (!data.popped_recently) return;
	self->persistentData->set(KeyValueRef( persistTagPoppedKey(logData->logId, tag), persistTagPoppedValue(data.popped) ));
	data.popped_recently = false;

	if (data.nothing_persistent) return;

	self->persistentData->clear( KeyRangeRef(
		persistTagMessagesKey( logData->logId, tag, Version(0) ),
		persistTagMessagesKey( logData->logId, tag, data.popped ) ) );
	if (data.popped > logData->persistentDataVersion)
		data.nothing_persistent = true;
}

ACTOR Future<Void> updatePersistentData( TLogData* self, Reference<LogData> logData, Version newPersistentDataVersion ) {
	// PERSIST: Changes self->persistentDataVersion and writes and commits the relevant changes
	ASSERT( newPersistentDataVersion <= logData->version.get() );
	ASSERT( newPersistentDataVersion <= logData->queueCommittedVersion.get() );
	ASSERT( newPersistentDataVersion > logData->persistentDataVersion );
	ASSERT( logData->persistentDataVersion == logData->persistentDataDurableVersion );

	//TraceEvent("updatePersistentData", self->dbgid).detail("seq", newPersistentDataSeq);

	state bool anyData = false;
	state Map<Tag, LogData::TagData>::iterator tag;
	// For all existing tags
	for(tag = logData->tag_data.begin(); tag != logData->tag_data.end(); ++tag) {
		state Version currentVersion = 0;
		// Clear recently popped versions from persistentData if necessary
		updatePersistentPopped( self, logData, tag->key, tag->value );
		// Transfer unpopped messages with version numbers less than newPersistentDataVersion to persistentData
		state std::deque<std::pair<Version, LengthPrefixedStringRef>>::iterator msg = tag->value.version_messages.begin();
		while(msg != tag->value.version_messages.end() && msg->first <= newPersistentDataVersion) {
			currentVersion = msg->first;
			anyData = true;
			tag->value.nothing_persistent = false;
			BinaryWriter wr( Unversioned() );

			for(; msg != tag->value.version_messages.end() && msg->first == currentVersion; ++msg)
				wr << msg->second.toStringRef();

			self->persistentData->set( KeyValueRef( persistTagMessagesKey( logData->logId, tag->key, currentVersion ), wr.toStringRef() ) );

			Future<Void> f = yield(TaskUpdateStorage);
			if(!f.isReady()) {
				Void _ = wait(f);
				msg = std::upper_bound(tag->value.version_messages.begin(), tag->value.version_messages.end(), std::make_pair(currentVersion, LengthPrefixedStringRef()), CompareFirst<std::pair<Version, LengthPrefixedStringRef>>());
			}
		}

		Void _ = wait(yield(TaskUpdateStorage));
	}

	self->persistentData->set( KeyValueRef( BinaryWriter::toValue(logData->logId,Unversioned()).withPrefix(persistCurrentVersionKeys.begin), BinaryWriter::toValue(newPersistentDataVersion, Unversioned()) ) );
	logData->persistentDataVersion = newPersistentDataVersion;

	Void _ = wait( self->persistentData->commit() ); // SOMEDAY: This seems to be running pretty often, should we slow it down???
	Void _ = wait( delay(0, TaskUpdateStorage) );

	// Now that the changes we made to persistentData are durable, erase the data we moved from memory and the queue, increase bytesDurable accordingly, and update persistentDataDurableVersion.

	TEST(anyData);  // TLog moved data to persistentData
	logData->persistentDataDurableVersion = newPersistentDataVersion;

	for(tag = logData->tag_data.begin(); tag != logData->tag_data.end(); ++tag) {
		Void _ = wait(tag->value.eraseMessagesBefore( newPersistentDataVersion+1, &self->bytesDurable, logData, TaskUpdateStorage ));
		Void _ = wait(yield(TaskUpdateStorage));
	}

	logData->version_sizes.erase(logData->version_sizes.begin(), logData->version_sizes.lower_bound(logData->persistentDataDurableVersion));

	Void _ = wait(yield(TaskUpdateStorage));

	while(!logData->messageBlocks.empty() && logData->messageBlocks.front().first <= newPersistentDataVersion) {
		int64_t bytesErased = int64_t(logData->messageBlocks.front().second.size()) * SERVER_KNOBS->TLOG_MESSAGE_BLOCK_OVERHEAD_FACTOR;
		logData->bytesDurable += bytesErased;
		self->bytesDurable += bytesErased;
		logData->messageBlocks.pop_front();
		Void _ = wait(yield(TaskUpdateStorage));
	}

	if(logData->bytesDurable.getValue() > logData->bytesInput.getValue() || self->bytesDurable > self->bytesInput) {
		TraceEvent(SevError, "BytesDurableTooLarge", logData->logId).detail("sharedBytesInput", self->bytesInput).detail("sharedBytesDurable", self->bytesDurable).detail("localBytesInput", logData->bytesInput.getValue()).detail("localBytesDurable", logData->bytesDurable.getValue());
	}

	ASSERT(logData->bytesDurable.getValue() <= logData->bytesInput.getValue());
	ASSERT(self->bytesDurable <= self->bytesInput);

	if( self->queueCommitEnd.get() > 0 )
		self->persistentQueue->pop( newPersistentDataVersion+1 ); // SOMEDAY: this can cause a slow task (~0.5ms), presumably from erasing too many versions. Should we limit the number of versions cleared at a time?

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
		Void _ = wait( delay(BUGGIFY ? SERVER_KNOBS->BUGGIFY_TLOG_STORAGE_MIN_UPDATE_INTERVAL : SERVER_KNOBS->TLOG_STORAGE_MIN_UPDATE_INTERVAL, TaskUpdateStorage) );
		return Void();
	}

	state Reference<LogData> logData = self->id_data[self->queueOrder.front()];
	state Version prevVersion = 0;
	state Version nextVersion = 0;
	state int totalSize = 0;

	if(logData->stopped) {
		if (self->bytesInput - self->bytesDurable >= SERVER_KNOBS->TLOG_SPILL_THRESHOLD) {
			while(logData->persistentDataDurableVersion != logData->version.get()) {
				std::vector<std::pair<std::deque<std::pair<Version, LengthPrefixedStringRef>>::iterator, std::deque<std::pair<Version, LengthPrefixedStringRef>>::iterator>> iters;
				for(auto tag = logData->tag_data.begin(); tag != logData->tag_data.end(); ++tag)
					iters.push_back(std::make_pair(tag->value.version_messages.begin(), tag->value.version_messages.end()));

				nextVersion = 0;
				while( totalSize < SERVER_KNOBS->UPDATE_STORAGE_BYTE_LIMIT || nextVersion <= logData->persistentDataVersion ) {
					nextVersion = logData->version.get();
					for( auto &it : iters )
						if(it.first != it.second)
							nextVersion = std::min( nextVersion, it.first->first + 1 );

					if(nextVersion == logData->version.get())
						break;

					for( auto &it : iters ) {
						while (it.first != it.second && it.first->first < nextVersion) {
							totalSize += it.first->second.expectedSize();
							++it.first;
						}
					}
				}

				Void _ = wait( logData->queueCommittedVersion.whenAtLeast( nextVersion ) );
				Void _ = wait( delay(0, TaskUpdateStorage) );

				//TraceEvent("TlogUpdatePersist", self->dbgid).detail("logId", logData->logId).detail("nextVersion", nextVersion).detail("version", logData->version.get()).detail("persistentDataDurableVer", logData->persistentDataDurableVersion).detail("queueCommitVer", logData->queueCommittedVersion.get()).detail("persistDataVer", logData->persistentDataVersion);
				if (nextVersion > logData->persistentDataVersion) {
					self->updatePersist = updatePersistentData(self, logData, nextVersion);
					Void _ = wait( self->updatePersist );
				} else {
					Void _ = wait( delay(BUGGIFY ? SERVER_KNOBS->BUGGIFY_TLOG_STORAGE_MIN_UPDATE_INTERVAL : SERVER_KNOBS->TLOG_STORAGE_MIN_UPDATE_INTERVAL, TaskUpdateStorage) );
				}

				if( logData->removed.isReady() ) {
					break;
				}
			}

			if(logData->persistentDataDurableVersion == logData->version.get()) {
				self->queueOrder.pop_front();
			}
			Void _ = wait( delay(0.0, TaskUpdateStorage) );
		} else {
			Void _ = wait( delay(BUGGIFY ? SERVER_KNOBS->BUGGIFY_TLOG_STORAGE_MIN_UPDATE_INTERVAL : SERVER_KNOBS->TLOG_STORAGE_MIN_UPDATE_INTERVAL, TaskUpdateStorage) );
		}
	}
	else if(logData->initialized) {
		ASSERT(self->queueOrder.size() == 1);
		state Map<Version, std::pair<int, int>>::iterator sizeItr = logData->version_sizes.begin();
		while( totalSize < SERVER_KNOBS->UPDATE_STORAGE_BYTE_LIMIT && sizeItr != logData->version_sizes.end()
				&& (logData->bytesInput.getValue() - logData->bytesDurable.getValue() - totalSize >= SERVER_KNOBS->TLOG_SPILL_THRESHOLD || sizeItr->value.first == 0) )
		{
			Void _ = wait( yield(TaskUpdateStorage) );

			++sizeItr;
			nextVersion = sizeItr == logData->version_sizes.end() ? logData->version.get() : sizeItr->key;

			state Map<Tag, LogData::TagData>::iterator tag;
			for(tag = logData->tag_data.begin(); tag != logData->tag_data.end(); ++tag) {
				auto it = std::lower_bound(tag->value.version_messages.begin(), tag->value.version_messages.end(), std::make_pair(prevVersion, LengthPrefixedStringRef()), CompareFirst<std::pair<Version, LengthPrefixedStringRef>>());
				for(; it != tag->value.version_messages.end() && it->first < nextVersion; ++it) {
					totalSize += it->second.expectedSize();
				}

				Void _ = wait(yield(TaskUpdateStorage));
			}

			prevVersion = nextVersion;
		}

		nextVersion = std::max<Version>(nextVersion, logData->persistentDataVersion);

		//TraceEvent("UpdateStorageVer", logData->logId).detail("nextVersion", nextVersion).detail("persistentDataVersion", logData->persistentDataVersion).detail("totalSize", totalSize);

		Void _ = wait( logData->queueCommittedVersion.whenAtLeast( nextVersion ) );
		Void _ = wait( delay(0, TaskUpdateStorage) );

		if (nextVersion > logData->persistentDataVersion) {
			self->updatePersist = updatePersistentData(self, logData, nextVersion);
			Void _ = wait( self->updatePersist );
		}

		if( totalSize < SERVER_KNOBS->UPDATE_STORAGE_BYTE_LIMIT ) {
			Void _ = wait( delay(BUGGIFY ? SERVER_KNOBS->BUGGIFY_TLOG_STORAGE_MIN_UPDATE_INTERVAL : SERVER_KNOBS->TLOG_STORAGE_MIN_UPDATE_INTERVAL, TaskUpdateStorage) );
		}
		else {
			//recovery wants to commit to persistant data when updatePersistentData is not active, this delay ensures that immediately after
			//updatePersist returns another one has not been started yet.
			Void _ = wait( delay(0.0, TaskUpdateStorage) );
		}
	} else {
		Void _ = wait( delay(BUGGIFY ? SERVER_KNOBS->BUGGIFY_TLOG_STORAGE_MIN_UPDATE_INTERVAL : SERVER_KNOBS->TLOG_STORAGE_MIN_UPDATE_INTERVAL, TaskUpdateStorage) );
	}
	return Void();
}

ACTOR Future<Void> updateStorageLoop( TLogData* self ) {
	Void _ = wait(delay(0, TaskUpdateStorage));

	loop {
		Void _ = wait( updateStorage(self) );
	}
}

void commitMessages( Reference<LogData> self, Version version, Arena arena, StringRef messages, VectorRef< TagMessagesRef > tags, int64_t& bytesInput) {
	// SOMEDAY: This method of copying messages is reasonably memory efficient, but it's still a lot of bytes copied.  Find a
	// way to do the memory allocation right as we receive the messages in the network layer.

	int64_t addedBytes = 0;
	int64_t expectedBytes = 0;

	if(!messages.size()) {
		return;
	}

	StringRef messages1;  // the first block of messages, if they aren't all stored contiguously.  otherwise empty

	// Grab the last block in the blocks list so we can share its arena
	// We pop all of the elements of it to create a "fresh" vector that starts at the end of the previous vector
	Standalone<VectorRef<uint8_t>> block;
	if(self->messageBlocks.empty()) {
		block = Standalone<VectorRef<uint8_t>>();
		block.reserve(block.arena(), std::max<int64_t>(SERVER_KNOBS->TLOG_MESSAGE_BLOCK_BYTES, messages.size()));
	}
	else {
		block = self->messageBlocks.back().second;
	}

	block.pop_front(block.size());

	// If the current batch of messages doesn't fit entirely in the remainder of the last block in the list
	if(messages.size() + block.size() > block.capacity()) {
		// Find how many messages will fit
		LengthPrefixedStringRef r((uint32_t*)messages.begin());
		uint8_t const* end = messages.begin() + block.capacity() - block.size();
		while(r.toStringRef().end() <= end) {
			r = LengthPrefixedStringRef( (uint32_t*)r.toStringRef().end() );
		}

		// Fill up the rest of this block
		int bytes = (uint8_t*)r.getLengthPtr()-messages.begin();
		if (bytes) {
			TEST(true); // Splitting commit messages across multiple blocks
			messages1 = StringRef(block.end(), bytes);
			block.append(block.arena(), messages.begin(), bytes);
			self->messageBlocks.push_back( std::make_pair(version, block) );
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
	self->messageBlocks.push_back( std::make_pair(version, block) );
	addedBytes += int64_t(block.size()) * SERVER_KNOBS->TLOG_MESSAGE_BLOCK_OVERHEAD_FACTOR;
	messages = StringRef(block.end()-messages.size(), messages.size());

	for(auto tag = tags.begin(); tag != tags.end(); ++tag) {
		int64_t tagMessages = 0;

		auto tsm = self->tag_data.find(tag->tag);
		if (tsm == self->tag_data.end()) {
			tsm = self->tag_data.insert( mapPair(std::move(Tag(tag->tag)), LogData::TagData(Version(0), true, true, tag->tag) ), false );
		}

		if (version >= tsm->value.popped) {
			for(int m = 0; m < tag->messageOffsets.size(); ++m) {
				int offs = tag->messageOffsets[m];
				uint8_t const* p = offs < messages1.size() ? messages1.begin() + offs : messages.begin() + offs - messages1.size();
				tsm->value.version_messages.push_back(std::make_pair(version, LengthPrefixedStringRef((uint32_t*)p)));
				if(tsm->value.version_messages.back().second.expectedSize() > SERVER_KNOBS->MAX_MESSAGE_SIZE) {
					TraceEvent(SevWarnAlways, "LargeMessage").detail("Size", tsm->value.version_messages.back().second.expectedSize());
				}
				if (tag->tag != txsTag)
					expectedBytes += tsm->value.version_messages.back().second.expectedSize();

				++tagMessages;
			}
		}

		// The factor of VERSION_MESSAGES_OVERHEAD is intended to be an overestimate of the actual memory used to store this data in a std::deque.
		// In practice, this number is probably something like 528/512 ~= 1.03, but this could vary based on the implementation.
		// There will also be a fixed overhead per std::deque, but its size should be trivial relative to the size of the TLog
		// queue and can be thought of as increasing the capacity of the queue slightly.
		addedBytes += (tagMessages * sizeof(std::pair<Version, LengthPrefixedStringRef>) * SERVER_KNOBS->VERSION_MESSAGES_OVERHEAD_FACTOR_1024THS) >> 10;
	}

	self->version_sizes[version] = make_pair(expectedBytes, expectedBytes);
	self->bytesInput += addedBytes;
	bytesInput += addedBytes;

	//TraceEvent("TLogPushed", self->dbgid).detail("Bytes", addedBytes).detail("MessageBytes", messages.size()).detail("Tags", tags.size()).detail("expectedBytes", expectedBytes).detail("mCount", mCount).detail("tCount", tCount);
}

Version poppedVersion( Reference<LogData> self, Tag tag) {
	auto mapIt = self->tag_data.find(tag);
	if (mapIt == self->tag_data.end())
		return Version(0);
	return mapIt->value.popped;
}

std::deque<std::pair<Version, LengthPrefixedStringRef>> & get_version_messages( Reference<LogData> self, Tag tag ) {
	auto mapIt = self->tag_data.find(tag);
	if (mapIt == self->tag_data.end()) {
		static std::deque<std::pair<Version, LengthPrefixedStringRef>> empty;
		return empty;
	}
	return mapIt->value.version_messages;
};

ACTOR Future<Void> tLogPop( TLogData* self, TLogPopRequest req, Reference<LogData> logData ) {
	auto ti = logData->tag_data.find(req.tag);
	if (ti == logData->tag_data.end()) {
		ti = logData->tag_data.insert( mapPair(std::move(Tag(req.tag)), LogData::TagData(req.to, true, true, req.tag)) );
	} else if (req.to > ti->value.popped) {
		ti->value.popped = req.to;
		ti->value.popped_recently = true;
		//if (to.epoch == self->epoch())
		if ( req.to > logData->persistentDataDurableVersion )
			Void _ = wait(ti->value.eraseMessagesBefore( req.to, &self->bytesDurable, logData, TaskTLogPop ));
		//TraceEvent("TLogPop", self->dbgid).detail("Tag", req.tag).detail("To", req.to);
	}

	req.reply.send(Void());
	return Void();
}

void peekMessagesFromMemory( Reference<LogData> self, TLogPeekRequest const& req, BinaryWriter& messages, Version& endVersion ) {
	ASSERT( !messages.getLength() );

	auto& deque = get_version_messages(self, req.tag);
	//TraceEvent("tLogPeekMem", self->dbgid).detail("Tag", printable(req.tag1)).detail("pDS", self->persistentDataSequence).detail("pDDS", self->persistentDataDurableSequence).detail("Oldest", map1.empty() ? 0 : map1.begin()->key ).detail("OldestMsgCount", map1.empty() ? 0 : map1.begin()->value.size());

	Version begin = std::max( req.begin, self->persistentDataDurableVersion+1 );
	auto it = std::lower_bound(deque.begin(), deque.end(), std::make_pair(begin, LengthPrefixedStringRef()), CompareFirst<std::pair<Version, LengthPrefixedStringRef>>());

	Version currentVersion = -1;
	for(; it != deque.end(); ++it) {
		if(it->first != currentVersion) {
			if (messages.getLength() >= SERVER_KNOBS->DESIRED_TOTAL_BYTES) {
				endVersion = it->first;
				//TraceEvent("tLogPeekMessagesReached2", self->dbgid);
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
				trackerData.lastUpdate = now();
				Version ver = wait(trackerData.sequence_version[sequence].getFuture());
				req.begin = ver;
				Void _ = wait(yield());
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

	//TraceEvent("tLogPeekMessages0", self->dbgid).detail("reqBeginEpoch", req.begin.epoch).detail("reqBeginSeq", req.begin.sequence).detail("epoch", self->epoch()).detail("persistentDataSeq", self->persistentDataSequence).detail("Tag1", printable(req.tag1)).detail("Tag2", printable(req.tag2));
	// Wait until we have something to return that the caller doesn't already have
	if( logData->version.get() < req.begin ) {
		Void _ = wait( logData->version.whenAtLeast( req.begin ) );
		Void _ = wait( delay(SERVER_KNOBS->TLOG_PEEK_DELAY, g_network->getCurrentTask()) );
	}

	state Version endVersion = logData->version.get() + 1;

	//grab messages from disk
	//TraceEvent("tLogPeekMessages", self->dbgid).detail("reqBeginEpoch", req.begin.epoch).detail("reqBeginSeq", req.begin.sequence).detail("epoch", self->epoch()).detail("persistentDataSeq", self->persistentDataSequence).detail("Tag1", printable(req.tag1)).detail("Tag2", printable(req.tag2));
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

	Version poppedVer = poppedVersion(logData, req.tag);

	TLogPeekReply reply;
	reply.maxKnownVersion = logData->version.get();
	if(poppedVer > req.begin) {
		reply.popped = poppedVer;
		reply.end = poppedVer;
	} else {
		reply.messages = messages.toStringRef();
		reply.end = endVersion;
	}
	//TraceEvent("TlogPeek", self->dbgid).detail("logId", logData->logId).detail("endVer", reply.end).detail("msgBytes", reply.messages.expectedSize()).detail("ForAddress", req.reply.getEndpoint().address);

	if(req.sequence.present()) {
		auto& trackerData = self->peekTracker[peekId];
		trackerData.lastUpdate = now();
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
	}

	req.reply.send( reply );
	return Void();
}

ACTOR Future<Void> doQueueCommit( TLogData* self, Reference<LogData> logData ) {
	state Version ver = logData->version.get();
	state Version commitNumber = self->queueCommitBegin+1;
	self->queueCommitBegin = commitNumber;
	logData->queueCommittingVersion = ver;

	Future<Void> c = self->persistentQueue->commit();
	self->diskQueueCommitBytes = 0;
	self->largeDiskQueueCommitBytes.set(false);

	Void _ = wait(c);
	Void _ = wait(self->queueCommitEnd.whenAtLeast(commitNumber-1));

	//Calling check_yield instead of yield to avoid a destruction ordering problem in simulation
	if(g_network->check_yield(g_network->getCurrentTask())) {
		Void _ = wait(delay(0, g_network->getCurrentTask()));
	}

	ASSERT( ver > logData->queueCommittedVersion.get() );

	logData->queueCommittedVersion.set(ver);
	self->queueCommitEnd.set(commitNumber);

	TraceEvent("TLogCommitDurable", self->dbgid).detail("Version", ver);

	return Void();
}

ACTOR Future<Void> commitQueue( TLogData* self ) {
	state Reference<LogData> logData;

	loop {
		bool foundCount = 0;
		for(auto it : self->id_data) {
			if(!it.second->stopped) {
				 logData = it.second;
				 foundCount++;
			}
		}

		ASSERT(foundCount < 2);
		if(!foundCount) {
			Void _ = wait( self->newLogData.onTrigger() );
			continue;
		}

		TraceEvent("commitQueueNewLog", self->dbgid).detail("logId", logData->logId).detail("version", logData->version.get()).detail("committing", logData->queueCommittingVersion).detail("commmitted", logData->queueCommittedVersion.get());

		loop {
			if(logData->stopped && logData->version.get() == std::max(logData->queueCommittingVersion, logData->queueCommittedVersion.get())) {
				Void _ = wait( logData->queueCommittedVersion.whenAtLeast(logData->version.get() ) );
				break;
			}

			choose {
				when(Void _ = wait( logData->version.whenAtLeast( std::max(logData->queueCommittingVersion, logData->queueCommittedVersion.get()) + 1 ) ) ) {
					while( self->queueCommitBegin != self->queueCommitEnd.get() && !self->largeDiskQueueCommitBytes.get() ) {
						Void _ = wait( self->queueCommitEnd.whenAtLeast(self->queueCommitBegin) || self->largeDiskQueueCommitBytes.onChange() );
					}
					self->sharedActors.send(doQueueCommit(self, logData));
				}
				when(Void _ = wait(self->newLogData.onTrigger())) {}
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

	logData->knownCommittedVersion = std::max(logData->knownCommittedVersion, req.knownCommittedVersion);

	Void _ = wait( logData->version.whenAtLeast( req.prevVersion ) );

	//Calling check_yield instead of yield to avoid a destruction ordering problem in simulation
	if(g_network->check_yield(g_network->getCurrentTask())) {
		Void _ = wait(delay(0, g_network->getCurrentTask()));
	}

	if(logData->stopped) {
		req.reply.sendError( tlog_stopped() );
		return Void();
	}

	if (logData->version.get() == req.prevVersion) {  // Not a duplicate (check relies on no waiting between here and self->version.set() below!)
		if(req.debugID.present())
			g_traceBatch.addEvent("CommitDebug", tlogDebugID.get().first(), "TLog.tLogCommit.Before");

		TraceEvent("TLogCommit", logData->logId).detail("Version", req.version);
		commitMessages(logData, req.version, req.arena, req.messages, req.tags, self->bytesInput);

		// Log the changes to the persistent queue, to be committed by commitQueue()
		TLogQueueEntryRef qe;
		qe.version = req.version;
		qe.knownCommittedVersion = req.knownCommittedVersion;
		qe.messages = req.messages;
		qe.tags = req.tags;
		qe.id = logData->logId;
		self->persistentQueue->push( qe );

		self->diskQueueCommitBytes += qe.expectedSize();
		if( self->diskQueueCommitBytes > SERVER_KNOBS->MAX_QUEUE_COMMIT_BYTES ) {
			self->largeDiskQueueCommitBytes.set(true);
		}

		// Notifies the commitQueue actor to commit persistentQueue, and also unblocks tLogPeekMessages actors
		self->prevVersion = logData->version.get();
		logData->version.set( req.version );
		self->newVersion.trigger();

		if(req.debugID.present())
			g_traceBatch.addEvent("CommitDebug", tlogDebugID.get().first(), "TLog.tLogCommit.AfterTLogCommit");
	}
	// Send replies only once all prior messages have been received and committed.
	Void _ = wait( timeoutWarning( logData->queueCommittedVersion.whenAtLeast( req.version ), 0.1, warningCollectorInput ) );

	if(req.debugID.present())
		g_traceBatch.addEvent("CommitDebug", tlogDebugID.get().first(), "TLog.tLogCommit.After");

	req.reply.send( Void() );
	return Void();
}

ACTOR Future<Void> initPersistentState( TLogData* self, Reference<LogData> logData ) {
	// PERSIST: Initial setup of persistentData for a brand new tLog for a new database
	IKeyValueStore *storage = self->persistentData;
	storage->set( persistFormat );
	storage->set( KeyValueRef( BinaryWriter::toValue(logData->logId,Unversioned()).withPrefix(persistCurrentVersionKeys.begin), BinaryWriter::toValue(logData->version.get(), Unversioned()) ) );
	storage->set( KeyValueRef( BinaryWriter::toValue(logData->logId,Unversioned()).withPrefix(persistRecoveryCountKeys.begin), BinaryWriter::toValue(logData->recoveryCount, Unversioned()) ) );

	TraceEvent("TLogInitCommit", logData->logId);
	Void _ = wait( self->updatePersist );
	Void _ = wait( self->persistentData->commit() );
	return Void();
}

ACTOR Future<Void> rejoinMasters( TLogData* self, TLogInterface tli, DBRecoveryCount recoveryCount, Future<Void> registerWithMaster ) {
	state UID lastMasterID(0,0);
	loop {
		auto const& inf = self->dbInfo->get();
		bool isDisplaced = inf.recoveryCount >= recoveryCount && inf.recoveryState != 0 &&
			 !std::count( inf.logSystemConfig.tLogs.begin(), inf.logSystemConfig.tLogs.end(), tli.id() ) &&
			 !std::count( inf.priorCommittedLogServers.begin(), inf.priorCommittedLogServers.end(), tli.id() );
		for(int i = 0; i < inf.logSystemConfig.oldTLogs.size() && isDisplaced; i++) {
			isDisplaced = !std::count( inf.logSystemConfig.oldTLogs[i].tLogs.begin(), inf.logSystemConfig.oldTLogs[i].tLogs.end(), tli.id() );
		}
		if ( isDisplaced )
		{
			TraceEvent("TLogDisplaced", tli.id()).detail("Reason", "DBInfoDoesNotContain").detail("recoveryCount", recoveryCount).detail("infRecoveryCount", inf.recoveryCount).detail("recoveryState", inf.recoveryState)
				.detail("logSysConf", describe(inf.logSystemConfig.tLogs)).detail("priorLogs", describe(inf.priorCommittedLogServers)).detail("oldLogGens", inf.logSystemConfig.oldTLogs.size());
			if (BUGGIFY) Void _ = wait( delay( SERVER_KNOBS->BUGGIFY_WORKER_REMOVED_MAX_LAG * g_random->random01() ) );
			throw worker_removed();
		}

		if( registerWithMaster.isReady() ) {
			if ( self->dbInfo->get().master.id() != lastMasterID) {
				// The TLogRejoinRequest is needed to establish communications with a new master, which doesn't have our TLogInterface
				TLogRejoinRequest req;
				req.myInterface = tli;
				TraceEvent("TLogRejoining", self->dbgid).detail("Master", self->dbInfo->get().master.id());
				choose {
					when ( bool success = wait( brokenPromiseToNever( self->dbInfo->get().master.tlogRejoin.getReply( req ) ) ) ) {
						if (success)
							lastMasterID = self->dbInfo->get().master.id();
					}
					when ( Void _ = wait( self->dbInfo->onChange() ) ) { }
				}
			} else {
				Void _ = wait( self->dbInfo->onChange() );
			}
		} else {
			Void _ = wait( registerWithMaster || self->dbInfo->onChange() );
		}
	}
}

ACTOR Future<Void> respondToRecovered( TLogInterface tli, Promise<bool> recoverySuccessful, Future<Void> recovery ) {
	Void _ = wait( success(recoverySuccessful.getFuture()) || recovery );
	ASSERT(recoverySuccessful.isSet());
	state bool finishedRecovery = recoverySuccessful.getFuture().get();

	if(!finishedRecovery) {
		recovery = Void();
	}

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

		Void _ = wait( delay(minTimeUntilExpiration) );
	}
}

void getQueuingMetrics( TLogData* self, TLogQueuingMetricsRequest const& req ) {
	TLogQueuingMetricsReply reply;
	reply.localTime = now();
	reply.instanceID = self->instanceID;
	reply.bytesInput = self->bytesInput;
	reply.bytesDurable = self->bytesDurable;
	reply.storageBytes = self->persistentData->getStorageBytes();
	reply.v = self->prevVersion;
	req.reply.send( reply );
}

ACTOR Future<Void> serveTLogInterface( TLogData* self, TLogInterface tli, Reference<LogData> logData, PromiseStream<Void> warningCollectorInput ) {
	loop choose {
		when( TLogPeekRequest req = waitNext( tli.peekMessages.getFuture() ) ) {
			logData->addActor.send( tLogPeekMessages( self, req, logData ) );
		}
		when( TLogPopRequest req = waitNext( tli.popMessages.getFuture() ) ) {
			logData->addActor.send( tLogPop( self, req, logData ) );
		}
		when( TLogCommitRequest req = waitNext( tli.commit.getFuture() ) ) {
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
			getQueuingMetrics(self, req);
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
	TraceEvent("TLogRemoved", logData->logId).detail("input", logData->bytesInput.getValue()).detail("durable", logData->bytesDurable.getValue());
	logData->stopped = true;
	if(logData->recoverySuccessful.canBeSet()) {
		logData->recoverySuccessful.send(false);
	}

	logData->addActor = PromiseStream<Future<Void>>(); //there could be items still in the promise stream if one of the actors threw an error immediately
	self->id_data.erase(logData->logId);

	if(self->id_data.size() || (self->oldLogServer.isValid() && !self->oldLogServer.isReady())) {
		return;
	} else {
		throw worker_removed();
	}
}

ACTOR Future<Void> tLogCore( TLogData* self, Reference<LogData> logData ) {
	if(logData->removed.isReady()) {
		Void _ = wait(delay(0)); //to avoid iterator invalidation in restorePersistentState when removed is already ready
		ASSERT(logData->removed.isError());

		if(logData->removed.getError().code() != error_code_worker_removed) {
			throw logData->removed.getError();
		}

		removeLog(self, logData);
		return Void();
	}

	TraceEvent("newLogData", self->dbgid).detail("logId", logData->logId);
	logData->initialized = true;
	self->newLogData.trigger();

	state PromiseStream<Void> warningCollectorInput;
	state Future<Void> warningCollector = timeoutWarningCollector( warningCollectorInput.getFuture(), 1.0, "TLogQueueCommitSlow", self->dbgid );
	state Future<Void> error = actorCollection( logData->addActor.getFuture() );

	logData->addActor.send( logData->recovery );
	logData->addActor.send( waitFailureServer(logData->tli.waitFailure.getFuture()) );
	logData->addActor.send( logData->removed );
	//FIXME: update tlogMetrics to include new information, or possibly only have one copy for the shared instance
	logData->addActor.send( traceCounters("TLogMetrics", logData->logId, SERVER_KNOBS->STORAGE_LOGGING_DELAY, &logData->cc, logData->logId.toString() + "/TLogMetrics"));
	logData->addActor.send( serveTLogInterface(self, logData->tli, logData, warningCollectorInput) );

	try {
		Void _ = wait( error );
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
		TLogQueueEntry r = wait( self->persistentQueue->readNext() );
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

	IKeyValueStore *storage = self->persistentData;
	state Future<Optional<Value>> fFormat = storage->readValue(persistFormat.key);
	state Future<Standalone<VectorRef<KeyValueRef>>> fVers = storage->readRange(persistCurrentVersionKeys);
	state Future<Standalone<VectorRef<KeyValueRef>>> fRecoverCounts = storage->readRange(persistRecoveryCountKeys);

	// FIXME: metadata in queue?

	Void _ = wait( waitForAll( (vector<Future<Optional<Value>>>(), fFormat ) ) );
	Void _ = wait( waitForAll( (vector<Future<Standalone<VectorRef<KeyValueRef>>>>(), fVers, fRecoverCounts) ) );

	if (fFormat.get().present() && !persistFormatReadableRange.contains( fFormat.get().get() )) {
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

	if(fFormat.get().get() == LiteralStringRef("FoundationDB/LogServer/2/2")) {
		TLogInterface recruited;
		recruited.uniqueID = self->dbgid;
		recruited.locality = locality;
		recruited.initEndpoints();

		DUMPTOKEN( recruited.peekMessages );
		DUMPTOKEN( recruited.popMessages );
		DUMPTOKEN( recruited.commit );
		DUMPTOKEN( recruited.lock );
		DUMPTOKEN( recruited.getQueuingMetrics );
		DUMPTOKEN( recruited.confirmRunning );

		//FIXME: need for upgrades from 4.X to 5.0, remove once this upgrade path is no longer needed
		if(recovered.canBeSet()) recovered.send(Void());
		oldLog.send(Void());
		while(!tlogRequests.isEmpty()) {
			tlogRequests.getFuture().pop().reply.sendError(recruitment_failed());
		}

		Void _ = wait( oldTLog::tLog(self->persistentData, self->rawPersistentQueue, recruited, self->dbInfo) );
		throw internal_error();
	}

	ASSERT(fVers.get().size() == fRecoverCounts.get().size());

	state int idx = 0;
	state Promise<Void> registerWithMaster;
	for(idx = 0; idx < fVers.get().size(); idx++) {
		state KeyRef rawId = fVers.get()[idx].key.removePrefix(persistCurrentVersionKeys.begin);
		UID id1 = BinaryReader::fromStringRef<UID>( rawId, Unversioned() );
		UID id2 = BinaryReader::fromStringRef<UID>( fRecoverCounts.get()[idx].key.removePrefix(persistRecoveryCountKeys.begin), Unversioned() );
		ASSERT(id1 == id2);

		TLogInterface recruited;
		recruited.uniqueID = id1;
		recruited.locality = locality;
		recruited.initEndpoints();

		DUMPTOKEN( recruited.peekMessages );
		DUMPTOKEN( recruited.popMessages );
		DUMPTOKEN( recruited.commit );
		DUMPTOKEN( recruited.lock );
		DUMPTOKEN( recruited.getQueuingMetrics );
		DUMPTOKEN( recruited.confirmRunning );

		logData = Reference<LogData>( new LogData(self, recruited) );
		logData->stopped = true;
		self->id_data[id1] = logData;

		Version ver = BinaryReader::fromStringRef<Version>( fVers.get()[idx].value, Unversioned() );
		logData->persistentDataVersion = ver;
		logData->persistentDataDurableVersion = ver;
		logData->version.set(ver);
		logData->recoveryCount = BinaryReader::fromStringRef<DBRecoveryCount>( fRecoverCounts.get()[idx].value, Unversioned() );
		logData->removed = rejoinMasters(self, recruited, logData->recoveryCount, registerWithMaster.getFuture());
		removed.push_back(errorOr(logData->removed));

		TraceEvent("TLogRestorePersistentStateVer", id1).detail("ver", ver);

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
				TraceEvent("TLogRestorePop", logData->logId).detail("Tag", tag).detail("To", popped);
				ASSERT( logData->tag_data.find(tag) == logData->tag_data.end() );
				logData->tag_data.insert( mapPair( std::move(Tag(tag)), LogData::TagData( popped, false, false, tag )) );
			}
		}
	}

	state Future<Void> allRemoved = waitForAll(removed);
	state Version lastVer = 0;
	state UID lastId = UID(1,1); //initialized so it will not compare equal to a default UID
	state double recoverMemoryLimit = SERVER_KNOBS->TARGET_BYTES_PER_TLOG + SERVER_KNOBS->SPRING_BYTES_TLOG;
	if (BUGGIFY) recoverMemoryLimit = std::max<double>(SERVER_KNOBS->BUGGIFY_RECOVER_MEMORY_LIMIT, SERVER_KNOBS->TLOG_SPILL_THRESHOLD);

	try {
		loop {
			if(allRemoved.isReady()) {
				TEST(true); //all tlogs removed during queue recovery
				throw worker_removed();
			}
			choose {
				when( TLogQueueEntry qe = wait( self->persistentQueue->readNext() ) ) {
					if(!self->queueOrder.size() || self->queueOrder.back() != qe.id) self->queueOrder.push_back(qe.id);
					if(qe.id != lastId) {
						lastId = qe.id;
						auto it = self->id_data.find(qe.id);
						if(it != self->id_data.end()) {
							logData = it->second;
						} else {
							logData = Reference<LogData>();
						}
					} else {
						ASSERT( qe.version >= lastVer );
						lastVer = qe.version;
					}

					//TraceEvent("TLogRecoveredQE", self->dbgid).detail("logId", qe.id).detail("ver", qe.version).detail("MessageBytes", qe.messages.size()).detail("Tags", qe.tags.size())
					//	.detail("Tag0", qe.tags.size() ? qe.tags[0].tag : invalidTag).detail("version", logData->version.get());

					if(logData) {
						logData->knownCommittedVersion = std::max(logData->knownCommittedVersion, qe.knownCommittedVersion);
						if( qe.version > logData->version.get() ) {
							commitMessages(logData, qe.version, qe.arena(), qe.messages, qe.tags, self->bytesInput);
							logData->version.set( qe.version );
							logData->queueCommittedVersion.set( qe.version );

							while (self->bytesInput - self->bytesDurable >= recoverMemoryLimit) {
								TEST(true);  // Flush excess data during TLog queue recovery
								TraceEvent("FlushLargeQueueDuringRecovery", self->dbgid).detail("BytesInput", self->bytesInput).detail("BytesDurable", self->bytesDurable).detail("Version", logData->version.get()).detail("PVer", logData->persistentDataVersion);

								choose {
									when( Void _ = wait( updateStorage(self) ) ) {}
									when( Void _ = wait( allRemoved ) ) { throw worker_removed(); }
								}
							}
						}
					}
				}
				when( Void _ = wait( allRemoved ) ) { throw worker_removed(); }
			}
		}
	} catch (Error& e) {
		if (e.code() != error_code_end_of_stream) throw;
	}

	TraceEvent("TLogRestorePersistentStateDone", self->dbgid).detail("Took", now()-startt);
	TEST( now()-startt >= 1.0 );  // TLog recovery took more than 1 second

	for(auto it : self->id_data) {
		if(it.second->queueCommittedVersion.get() == 0) {
			TraceEvent("TLogZeroVersion", self->dbgid).detail("logId", it.first);
			it.second->queueCommittedVersion.set(it.second->version.get());
		}
		self->sharedActors.send( tLogCore( self, it.second ) );
	}

	if(registerWithMaster.canBeSet()) registerWithMaster.send(Void());
	return Void();
}

bool tlogTerminated( TLogData* self, IKeyValueStore* persistentData, TLogQueue* persistentQueue, Error const& e ) {
	// Dispose the IKVS (destroying its data permanently) only if this shutdown is definitely permanent.  Otherwise just close it.
	self->terminated = true;
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

ACTOR Future<Void> recoverTagFromLogSystem( TLogData* self, Reference<LogData> logData, Version beginVersion, Version endVersion, Tag tag, Reference<AsyncVar<int>> uncommittedBytes, Reference<AsyncVar<Reference<ILogSystem>>> logSystem, int taskID ) {
	state Future<Void> dbInfoChange = Void();
	state Reference<ILogSystem::IPeekCursor> r;
	state Version tagAt = beginVersion;
	state Version tagPopped = 0;
	state Version lastVer = 0;

	TraceEvent("LogRecoveringTagBegin", logData->logId).detail("Tag", tag).detail("recoverAt", endVersion);

	while (tagAt <= endVersion) {
		loop {
			choose {
				when(Void _ = wait( r ? r->getMore(taskID) : Never() ) ) {
					break;
				}
				when( Void _ = wait( dbInfoChange ) ) {
					if(r) tagPopped = std::max(tagPopped, r->popped());
					if( logSystem->get() )
						r = logSystem->get()->peek( tagAt, tag );
					else
						r = Reference<ILogSystem::IPeekCursor>();
					dbInfoChange = logSystem->onChange();
				}
			}
		}

		//TraceEvent("LogRecoveringTagResults", logData->logId).detail("Tag", tag);

		Version ver = 0;
		BinaryWriter wr( Unversioned() );
		int writtenBytes = 0;
		while (true) {
			bool foundMessage = r->hasMessage();
			//TraceEvent("LogRecoveringMsg", logData->logId).detail("Tag", tag).detail("foundMessage", foundMessage).detail("ver", r->version().toString());
			if (!foundMessage || r->version().version != ver) {
				ASSERT(r->version().version > lastVer);
				if (ver) {
					//TraceEvent("LogRecoveringTagVersion", logData->logId).detail("Tag", tag).detail("Ver", ver).detail("Bytes", wr.getLength());
					writtenBytes += 100 + wr.getLength();
					self->persistentData->set( KeyValueRef( persistTagMessagesKey( logData->logId, tag, ver ), wr.toStringRef() ) );
				}
				lastVer = ver;
				ver = r->version().version;
				wr = BinaryWriter( Unversioned() );
				if (!foundMessage || ver > endVersion)
					break;
			}

			// FIXME: This logic duplicates stuff in LogPushData::addMessage(), and really would be better in PeekResults or somewhere else.  Also unnecessary copying.
			StringRef msg = r->getMessage();
			wr << uint32_t( msg.size() + sizeof(uint32_t) ) << r->version().sub;
			wr.serializeBytes( msg );
			r->nextMessage();
		}

		tagAt = r->version().version;

		if(writtenBytes)
			uncommittedBytes->set(uncommittedBytes->get() + writtenBytes);

		while(uncommittedBytes->get() >= SERVER_KNOBS->LARGE_TLOG_COMMIT_BYTES) {
			Void _ = wait(uncommittedBytes->onChange());
		}
	}
	if(r) tagPopped = std::max(tagPopped, r->popped());

	auto tsm = logData->tag_data.find(tag);
	if (tsm == logData->tag_data.end()) {
		logData->tag_data.insert( mapPair(std::move(Tag(tag)), LogData::TagData(tagPopped, false, true, tag)) );
	}

	Void _ = wait(tLogPop( self, TLogPopRequest(tagPopped, tag), logData ));

	updatePersistentPopped( self, logData, tag, logData->tag_data.find(tag)->value );

	TraceEvent("LogRecoveringTagComplete", logData->logId).detail("Tag", tag).detail("recoverAt", endVersion);
	return Void();
}

ACTOR Future<Void> updateLogSystem(TLogData* self, Reference<LogData> logData, LogSystemConfig recoverFrom, Reference<AsyncVar<Reference<ILogSystem>>> logSystem) {
	loop {
		TraceEvent("TLogUpdate", self->dbgid).detail("logId", logData->logId).detail("recoverFrom", recoverFrom.toString()).detail("dbInfo", self->dbInfo->get().logSystemConfig.toString());
		if( self->dbInfo->get().logSystemConfig.isEqualIds(recoverFrom) ) {
			logSystem->set(ILogSystem::fromLogSystemConfig( logData->logId, self->dbInfo->get().myLocality, self->dbInfo->get().logSystemConfig ));
		} else if( self->dbInfo->get().logSystemConfig.isNextGenerationOf(recoverFrom) && std::count( self->dbInfo->get().logSystemConfig.tLogs.begin(), self->dbInfo->get().logSystemConfig.tLogs.end(), logData->logId ) ) {
			logSystem->set(ILogSystem::fromOldLogSystemConfig( logData->logId, self->dbInfo->get().myLocality, self->dbInfo->get().logSystemConfig ));
		} else {
			logSystem->set(Reference<ILogSystem>());
		}
		Void _ = wait( self->dbInfo->onChange() );
	}
}

ACTOR Future<Void> recoverFromLogSystem( TLogData* self, Reference<LogData> logData, LogSystemConfig recoverFrom, Version recoverAt, Version knownCommittedVersion, std::vector<Tag> recoverTags, Promise<Void> copyComplete ) {
	state Future<Void> committing = Void();
	state double lastCommitT = now();
	state Reference<AsyncVar<int>> uncommittedBytes = Reference<AsyncVar<int>>(new AsyncVar<int>());
	state std::vector<Future<Void>> recoverFutures;
	state Reference<AsyncVar<Reference<ILogSystem>>> logSystem = Reference<AsyncVar<Reference<ILogSystem>>>(new AsyncVar<Reference<ILogSystem>>());
	state Future<Void> updater = updateLogSystem(self, logData, recoverFrom, logSystem);

	for(auto tag : recoverTags )
		recoverFutures.push_back(recoverTagFromLogSystem(self, logData, knownCommittedVersion, recoverAt, tag, uncommittedBytes, logSystem, TaskTLogPeekReply));

	state Future<Void> copyDone = waitForAll(recoverFutures);
	state Future<Void> recoveryDone = Never();
	state Future<Void> commitTimeout = delay(SERVER_KNOBS->LONG_TLOG_COMMIT_TIME);

	try {
		loop {
			choose {
				when(Void _ = wait(copyDone)) {
					recoverFutures.clear();
					for(auto tag : recoverTags )
						recoverFutures.push_back(recoverTagFromLogSystem(self, logData, 0, knownCommittedVersion, tag, uncommittedBytes, logSystem, TaskBatchCopy));
					copyDone = Never();
					recoveryDone =  waitForAll(recoverFutures);

					Void __ = wait( committing );
					Void __ = wait( self->updatePersist );
					committing = self->persistentData->commit();
					commitTimeout = delay(SERVER_KNOBS->LONG_TLOG_COMMIT_TIME);
					uncommittedBytes->set(0);
					Void __ = wait( committing );
					TraceEvent("TLogCommitCopyData", logData->logId);

					if(!copyComplete.isSet())
						copyComplete.send(Void());
				}
				when(Void _ = wait(recoveryDone)) { break; }
				when(Void _ = wait(commitTimeout)) {
					TEST(true); // We need to commit occasionally if this process is long to avoid running out of memory.
					// We let one, but not more, commits pipeline with the network transfer
					Void __ = wait( committing );
					Void __ = wait( self->updatePersist );
					committing = self->persistentData->commit();
					commitTimeout = delay(SERVER_KNOBS->LONG_TLOG_COMMIT_TIME);
					uncommittedBytes->set(0);
					//TraceEvent("TLogCommitRecoveryData", self->dbgid).detail("MemoryUsage", DEBUG_DETERMINISM ? 0 : getMemoryUsage());
				}
				when(Void _ = wait(uncommittedBytes->onChange())) {
					if(uncommittedBytes->get() >= SERVER_KNOBS->LARGE_TLOG_COMMIT_BYTES)
						commitTimeout = Void();
				}
			}
		}

		Void _ = wait( committing );
		Void _ = wait( self->updatePersist );
		Void _ = wait( self->persistentData->commit() );

		TraceEvent("TLogRecoveryComplete", logData->logId).detail("Locality", self->dbInfo->get().myLocality.toString());
		TEST(true);  // tLog restore from old log system completed

		if(logData->recoverySuccessful.canBeSet()) {
			logData->recoverySuccessful.send(true);
		}
		return Void();
	} catch( Error &e ) {
		TraceEvent("TLogRecoveryError", logData->logId).error(e,true);
		if(!copyComplete.isSet())
			copyComplete.sendError(worker_removed());
		throw;
	}
}

ACTOR Future<Void> tLogStart( TLogData* self, InitializeTLogRequest req, LocalityData locality ) {
	state TLogInterface recruited;
	recruited.locality = locality;
	recruited.initEndpoints();

	DUMPTOKEN( recruited.peekMessages );
	DUMPTOKEN( recruited.popMessages );
	DUMPTOKEN( recruited.commit );
	DUMPTOKEN( recruited.lock );
	DUMPTOKEN( recruited.getQueuingMetrics );
	DUMPTOKEN( recruited.confirmRunning );

	for(auto it : self->id_data) {
		it.second->stopped = true;
		if(it.second->recoverySuccessful.canBeSet()) {
			it.second->recoverySuccessful.send(false);
		}
	}

	state Reference<LogData> logData = Reference<LogData>( new LogData(self, recruited) );
	self->id_data[recruited.id()] = logData;
	logData->recoveryCount = req.epoch;
	logData->removed = rejoinMasters(self, recruited, req.epoch, Future<Void>(Void()));
	self->queueOrder.push_back(recruited.id());

	TraceEvent("TLogStart", logData->logId);

	try {
		if( logData->removed.isReady() ) {
			throw logData->removed.getError();
		}

		if (req.recoverFrom.logSystemType == 1) {
			ASSERT(false);
		} else if (req.recoverFrom.logSystemType == 2) {
			logData->persistentDataVersion = req.recoverAt;
			logData->persistentDataDurableVersion = req.recoverAt; // durable is a white lie until initPersistentState() commits the store
			logData->queueCommittedVersion.set( req.recoverAt );
			logData->version.set( req.recoverAt );

			Void _ = wait( initPersistentState( self, logData ) || logData->removed );

			state Promise<Void> copyComplete;
			TraceEvent("TLogRecover", self->dbgid).detail("logId", logData->logId).detail("at", req.recoverAt).detail("known", req.knownCommittedVersion).detail("tags", describe(req.recoverTags));
			logData->recovery = respondToRecovered( recruited, logData->recoverySuccessful, recoverFromLogSystem( self, logData, req.recoverFrom, req.recoverAt, req.knownCommittedVersion, req.recoverTags, copyComplete ) );
			Void _ = wait(copyComplete.getFuture() || logData->removed );
		} else {
			// Brand new tlog, initialization has already been done by caller
			Void _ = wait( initPersistentState( self, logData ) || logData->removed );
		}
	} catch( Error &e ) {
		if(e.code() != error_code_actor_cancelled) {
			req.reply.sendError(e);
		}

		if( e.code() != error_code_worker_removed ) {
			throw;
		}

		Void _ = wait( delay(0.0) ); // if multiple recruitment requests were already in the promise stream make sure they are all started before any are removed

		removeLog(self, logData);
		return Void();
	}

	TraceEvent("TLogReady", logData->logId);

	req.reply.send( recruited );

	Void _ = wait( tLogCore( self, logData ) );
	return Void();
}

// New tLog (if !recoverFrom.size()) or restore from network
ACTOR Future<Void> tLog( IKeyValueStore* persistentData, IDiskQueue* persistentQueue, Reference<AsyncVar<ServerDBInfo>> db, LocalityData locality, PromiseStream<InitializeTLogRequest> tlogRequests, UID tlogId, bool restoreFromDisk, Promise<Void> oldLog, Promise<Void> recovered )
{
	state TLogData self( tlogId, persistentData, persistentQueue, db );
	state Future<Void> error = actorCollection( self.sharedActors.getFuture() );

	TraceEvent("SharedTlog", tlogId);

	try {
		if(restoreFromDisk) {
			Void _ = wait( restorePersistentState( &self, locality, oldLog, recovered, tlogRequests ) );
		} else {
			Void _ = wait( checkEmptyQueue(&self) && checkRecovered(&self) );
		}

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
				when ( Void _ = wait( error ) ) { throw internal_error(); }
			}
		}
	} catch (Error& e) {
		TraceEvent("TLogError", tlogId).error(e, true);
		if(recovered.canBeSet()) recovered.send(Void());

		while(!tlogRequests.isEmpty()) {
			tlogRequests.getFuture().pop().reply.sendError(recruitment_failed());
		}

		for( auto& it : self.id_data ) {
			if(it.second->recoverySuccessful.canBeSet()) {
				it.second->recoverySuccessful.send(false);
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

TEST_CASE( "fdbserver/tlogserver/VersionMessagesOverheadFactor" ) {

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
