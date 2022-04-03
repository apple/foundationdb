/*
 * KeyValueStoreMemory.actor.cpp
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

#include "fdbclient/Knobs.h"
#include "fdbclient/Notified.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/DeltaTree.h"
#include "fdbserver/IDiskQueue.h"
#include "fdbserver/IKeyValueContainer.h"
#include "fdbserver/IKeyValueStore.h"
#include "fdbserver/RadixTree.h"
#include "flow/ActorCollection.h"
#include "flow/actorcompiler.h" // This must be the last #include.

#define OP_DISK_OVERHEAD (sizeof(OpHeader) + 1)

template <typename Container>
class KeyValueStoreMemory final : public IKeyValueStore, NonCopyable {
public:
	KeyValueStoreMemory(IDiskQueue* log,
	                    UID id,
	                    int64_t memoryLimit,
	                    KeyValueStoreType storeType,
	                    bool disableSnapshot,
	                    bool replaceContent,
	                    bool exactRecovery);

	// IClosable
	Future<Void> getError() const override { return log->getError(); }
	Future<Void> onClosed() const override { return log->onClosed(); }
	void dispose() override {
		recovering.cancel();
		log->dispose();
		if (reserved_buffer != nullptr) {
			delete[] reserved_buffer;
			reserved_buffer = nullptr;
		}
		delete this;
	}
	void close() override {
		recovering.cancel();
		log->close();
		if (reserved_buffer != nullptr) {
			delete[] reserved_buffer;
			reserved_buffer = nullptr;
		}
		delete this;
	}

	// IKeyValueStore
	KeyValueStoreType getType() const override { return type; }

	std::tuple<size_t, size_t, size_t> getSize() const override { return data.size(); }

	int64_t getAvailableSize() const {
		int64_t residentSize = data.sumTo(data.end()) + queue.totalSize() + // doesn't account for overhead in queue
		                       transactionSize;

		return memoryLimit - residentSize;
	}

	StorageBytes getStorageBytes() const override {
		StorageBytes diskQueueBytes = log->getStorageBytes();

		// Try to bound how many in-memory bytes we might need to write to disk if we commit() now
		int64_t uncommittedBytes = queue.totalSize() + transactionSize;

		// Check that we have enough space in memory and on disk
		int64_t freeSize = std::min(getAvailableSize(), diskQueueBytes.free / 4 - uncommittedBytes);
		int64_t availableSize = std::min(getAvailableSize(), diskQueueBytes.available / 4 - uncommittedBytes);
		int64_t totalSize = std::min(memoryLimit, diskQueueBytes.total / 4 - uncommittedBytes);

		return StorageBytes(std::max((int64_t)0, freeSize),
		                    std::max((int64_t)0, totalSize),
		                    diskQueueBytes.used,
		                    std::max((int64_t)0, availableSize));
	}

	void semiCommit() {
		transactionSize += queue.totalSize();
		if (transactionSize > 0.5 * committedDataSize) {
			transactionIsLarge = true;
			TraceEvent("KVSMemSwitchingToLargeTransactionMode", id)
			    .detail("TransactionSize", transactionSize)
			    .detail("DataSize", committedDataSize);
			TEST(true); // KeyValueStoreMemory switching to large transaction mode
			TEST(committedDataSize >
			     1e3); // KeyValueStoreMemory switching to large transaction mode with committed data
		}

		int64_t bytesWritten = commit_queue(queue, true);
		committedWriteBytes += bytesWritten;
	}

	void set(KeyValueRef keyValue, const Arena* arena) override {
		// A commit that occurs with no available space returns Never, so we can throw out all modifications
		if (getAvailableSize() <= 0)
			return;

		if (transactionIsLarge) {
			data.insert(keyValue.key, keyValue.value);
		} else {
			queue.set(keyValue, arena);
			if (recovering.isReady() && !disableSnapshot) {
				semiCommit();
			}
		}
	}

	void clear(KeyRangeRef range, const Arena* arena) override {
		// A commit that occurs with no available space returns Never, so we can throw out all modifications
		if (getAvailableSize() <= 0)
			return;

		if (transactionIsLarge) {
			data.erase(data.lower_bound(range.begin), data.lower_bound(range.end));
		} else {
			queue.clear(range, arena);
			if (recovering.isReady() && !disableSnapshot) {
				semiCommit();
			}
		}
	}

	Future<Void> commit(bool sequential) override {
		if (getAvailableSize() <= 0) {
			TraceEvent(SevError, "KeyValueStoreMemory_OutOfSpace", id).log();
			return Never();
		}

		if (recovering.isError())
			throw recovering.getError();
		if (!recovering.isReady())
			return waitAndCommit(this, sequential);

		if (!disableSnapshot && replaceContent && !firstCommitWithSnapshot) {
			transactionSize += SERVER_KNOBS->REPLACE_CONTENTS_BYTES;
			committedWriteBytes += SERVER_KNOBS->REPLACE_CONTENTS_BYTES;
			semiCommit();
		}

		if (transactionIsLarge) {
			fullSnapshot(data);
			resetSnapshot = true;
			committedWriteBytes = notifiedCommittedWriteBytes.get();
			overheadWriteBytes = 0;

			if (disableSnapshot) {
				return Void();
			}
			log_op(OpCommit, StringRef(), StringRef());
		} else {
			int64_t bytesWritten = commit_queue(queue, !disableSnapshot, sequential);

			if (disableSnapshot) {
				return Void();
			}

			if (bytesWritten > 0 || committedWriteBytes > notifiedCommittedWriteBytes.get()) {
				committedWriteBytes += bytesWritten + overheadWriteBytes +
				                       OP_DISK_OVERHEAD; // OP_DISK_OVERHEAD is for the following log_op(OpCommit)
				notifiedCommittedWriteBytes.set(committedWriteBytes); // This set will cause snapshot items to be
				                                                      // written, so it must happen before the OpCommit
				log_op(OpCommit, StringRef(), StringRef());
				overheadWriteBytes = log->getCommitOverhead();
			}
		}

		auto c = log->commit();

		committedDataSize = data.sumTo(data.end());
		transactionSize = 0;
		transactionIsLarge = false;
		firstCommitWithSnapshot = false;

		addActor.send(commitAndUpdateVersions(this, c, previousSnapshotEnd));
		return c;
	}

	Future<Optional<Value>> readValue(KeyRef key, IKeyValueStore::ReadType, Optional<UID> debugID) override {
		if (recovering.isError())
			throw recovering.getError();
		if (!recovering.isReady())
			return waitAndReadValue(this, key);

		auto it = data.find(key);
		if (it == data.end())
			return Optional<Value>();
		return Optional<Value>(it.getValue());
	}

	Future<Optional<Value>> readValuePrefix(KeyRef key,
	                                        int maxLength,
	                                        IKeyValueStore::ReadType,
	                                        Optional<UID> debugID) override {
		if (recovering.isError())
			throw recovering.getError();
		if (!recovering.isReady())
			return waitAndReadValuePrefix(this, key, maxLength);

		auto it = data.find(key);
		if (it == data.end())
			return Optional<Value>();
		auto val = it.getValue();
		if (maxLength < val.size()) {
			return Optional<Value>(val.substr(0, maxLength));
		} else {
			return Optional<Value>(val);
		}
	}

	// If rowLimit>=0, reads first rows sorted ascending, otherwise reads last rows sorted descending
	// The total size of the returned value (less the last entry) will be less than byteLimit
	Future<RangeResult> readRange(KeyRangeRef keys, int rowLimit, int byteLimit, IKeyValueStore::ReadType) override {
		if (recovering.isError())
			throw recovering.getError();
		if (!recovering.isReady())
			return waitAndReadRange(this, keys, rowLimit, byteLimit);

		RangeResult result;
		if (rowLimit == 0) {
			return result;
		}

		if (rowLimit > 0) {
			auto it = data.lower_bound(keys.begin);
			while (it != data.end() && rowLimit && byteLimit > 0) {
				StringRef tempKey = it.getKey(reserved_buffer);
				if (tempKey >= keys.end)
					break;

				byteLimit -= sizeof(KeyValueRef) + tempKey.size() + it.getValue().size();
				result.push_back_deep(result.arena(), KeyValueRef(tempKey, it.getValue()));
				++it;
				--rowLimit;
			}
		} else {
			rowLimit = -rowLimit;
			auto it = data.previous(data.lower_bound(keys.end));
			while (it != data.end() && rowLimit && byteLimit > 0) {
				StringRef tempKey = it.getKey(reserved_buffer);
				if (tempKey < keys.begin)
					break;

				byteLimit -= sizeof(KeyValueRef) + tempKey.size() + it.getValue().size();
				result.push_back_deep(result.arena(), KeyValueRef(tempKey, it.getValue()));
				it = data.previous(it);
				--rowLimit;
			}
		}

		result.more = rowLimit == 0 || byteLimit <= 0;
		if (result.more) {
			ASSERT(result.size() > 0);
			result.readThrough = result[result.size() - 1].key;
		}
		return result;
	}

	void resyncLog() override {
		ASSERT(recovering.isReady());
		resetSnapshot = true;
		log_op(OpSnapshotAbort, StringRef(), StringRef());
	}

	void enableSnapshot() override { disableSnapshot = false; }

private:
	enum OpType {
		OpSet,
		OpClear,
		OpClearToEnd,
		OpSnapshotItem,
		OpSnapshotEnd,
		OpSnapshotAbort, // terminate an in progress snapshot in order to start a full snapshot
		OpCommit, // only in log, not in queue
		OpRollback, // only in log, not in queue
		OpSnapshotItemDelta
	};

	struct OpRef {
		OpType op;
		StringRef p1, p2;
		OpRef() {}
		OpRef(Arena& a, OpRef const& o) : op(o.op), p1(a, o.p1), p2(a, o.p2) {}
		size_t expectedSize() const { return p1.expectedSize() + p2.expectedSize(); }
	};
	struct OpHeader {
		int op;
		int len1, len2;
	};

	struct OpQueue {
		OpQueue() : numBytes(0) {}

		int totalSize() const { return numBytes; }

		void clear() {
			numBytes = 0;
			operations = Standalone<VectorRef<OpRef>>();
			arenas.clear();
		}

		void rollback() { clear(); }

		void set(KeyValueRef keyValue, const Arena* arena = nullptr) {
			queue_op(OpSet, keyValue.key, keyValue.value, arena);
		}

		void clear(KeyRangeRef range, const Arena* arena = nullptr) {
			queue_op(OpClear, range.begin, range.end, arena);
		}

		void clear_to_end(StringRef fromKey, const Arena* arena = nullptr) {
			queue_op(OpClearToEnd, fromKey, StringRef(), arena);
		}

		void queue_op(OpType op, StringRef p1, StringRef p2, const Arena* arena) {
			numBytes += p1.size() + p2.size() + sizeof(OpHeader) + sizeof(OpRef);

			OpRef r;
			r.op = op;
			r.p1 = p1;
			r.p2 = p2;
			if (arena == nullptr) {
				operations.push_back_deep(operations.arena(), r);
			} else {
				operations.push_back(operations.arena(), r);
				arenas.push_back(*arena);
			}
		}

		const OpRef* begin() { return operations.begin(); }

		const OpRef* end() { return operations.end(); }

	private:
		Standalone<VectorRef<OpRef>> operations;
		uint64_t numBytes;
		std::vector<Arena> arenas;
	};
	KeyValueStoreType type;
	UID id;

	Container data;
	// reserved buffer for snapshot/fullsnapshot
	uint8_t* reserved_buffer;

	OpQueue queue; // mutations not yet commit()ted
	IDiskQueue* log;
	Future<Void> recovering, snapshotting;
	int64_t committedWriteBytes;
	int64_t overheadWriteBytes;
	NotifiedVersion notifiedCommittedWriteBytes;
	Key recoveredSnapshotKey; // After recovery, the next key in the currently uncompleted snapshot
	IDiskQueue::location
	    currentSnapshotEnd; // The end of the most recently completed snapshot (this snapshot cannot be discarded)
	IDiskQueue::location previousSnapshotEnd; // The end of the second most recently completed snapshot (on commit, this
	                                          // snapshot can be discarded)
	PromiseStream<Future<Void>> addActor;
	Future<Void> commitActors;

	int64_t committedDataSize;
	int64_t transactionSize;
	bool transactionIsLarge;

	bool resetSnapshot; // Set to true after a fullSnapshot is performed.  This causes the regular snapshot mechanism to
	                    // restart
	bool disableSnapshot;
	bool replaceContent;
	bool firstCommitWithSnapshot;
	int snapshotCount;

	int64_t memoryLimit; // The upper limit on the memory used by the store (excluding, possibly, some clear operations)
	std::vector<std::pair<KeyValueMapPair, uint64_t>> dataSets;

	int64_t commit_queue(OpQueue& ops, bool log, bool sequential = false) {
		int64_t total = 0, count = 0;
		IDiskQueue::location log_location = 0;

		for (auto o = ops.begin(); o != ops.end(); ++o) {
			++count;
			total += o->p1.size() + o->p2.size() + OP_DISK_OVERHEAD;
			if (o->op == OpSet) {
				if (sequential) {
					KeyValueMapPair pair(o->p1, o->p2);
					dataSets.emplace_back(pair, pair.arena.getSize() + data.getElementBytes());
				} else {
					data.insert(o->p1, o->p2);
				}
			} else if (o->op == OpClear) {
				if (sequential) {
					data.insert(dataSets);
					dataSets.clear();
				}
				data.erase(data.lower_bound(o->p1), data.lower_bound(o->p2));
			} else if (o->op == OpClearToEnd) {
				if (sequential) {
					data.insert(dataSets);
					dataSets.clear();
				}
				data.erase(data.lower_bound(o->p1), data.end());
			} else
				ASSERT(false);
			if (log)
				log_location = log_op(o->op, o->p1, o->p2);
		}
		if (sequential) {
			data.insert(dataSets);
			dataSets.clear();
		}

		bool ok = count < 1e6;
		if (!ok) {
			TraceEvent(/*ok ? SevInfo : */ SevWarnAlways, "KVSMemCommitQueue", id)
			    .detail("Bytes", total)
			    .detail("Log", log)
			    .detail("Ops", count)
			    .detail("LastLoggedLocation", log_location)
			    .detail("Details", count);
		}

		ops.clear();
		return total;
	}

	IDiskQueue::location log_op(OpType op, StringRef v1, StringRef v2) {
		OpHeader h = { (int)op, v1.size(), v2.size() };
		log->push(StringRef((const uint8_t*)&h, sizeof(h)));
		log->push(v1);
		log->push(v2);
		return log->push(LiteralStringRef("\x01")); // Changes here should be reflected in OP_DISK_OVERHEAD
	}

	ACTOR static Future<Void> recover(KeyValueStoreMemory* self, bool exactRecovery) {
		loop {
			// 'uncommitted' variables track something that might be rolled back by an OpRollback, and are copied into
			// permanent variables (in self) in OpCommit.  OpRollback does the reverse (copying the permanent versions
			// over the uncommitted versions) the uncommitted and committed variables should be equal initially (to
			// whatever makes sense if there are no committed transactions recovered)
			state Key uncommittedNextKey = self->recoveredSnapshotKey;
			state IDiskQueue::location uncommittedPrevSnapshotEnd = self->previousSnapshotEnd =
			    self->log->getNextReadLocation(); // not really, but popping up to here does nothing
			state IDiskQueue::location uncommittedSnapshotEnd = self->currentSnapshotEnd = uncommittedPrevSnapshotEnd;

			state int zeroFillSize = 0;
			state int dbgSnapshotItemCount = 0;
			state int dbgSnapshotEndCount = 0;
			state int dbgMutationCount = 0;
			state int dbgCommitCount = 0;
			state double startt = now();
			state UID dbgid = self->id;

			state Future<Void> loggingDelay = delay(1.0);

			state OpQueue recoveryQueue;
			state OpHeader h;
			state Standalone<StringRef> lastSnapshotKey;

			TraceEvent("KVSMemRecoveryStarted", self->id).detail("SnapshotEndLocation", uncommittedSnapshotEnd);

			try {
				loop {
					{
						Standalone<StringRef> data = wait(self->log->readNext(sizeof(OpHeader)));
						if (data.size() != sizeof(OpHeader)) {
							if (data.size()) {
								TEST(true); // zero fill partial header in KeyValueStoreMemory
								memset(&h, 0, sizeof(OpHeader));
								memcpy(&h, data.begin(), data.size());
								zeroFillSize = sizeof(OpHeader) - data.size() + h.len1 + h.len2 + 1;
							}
							TraceEvent("KVSMemRecoveryComplete", self->id)
							    .detail("Reason", "Non-header sized data read")
							    .detail("DataSize", data.size())
							    .detail("ZeroFillSize", zeroFillSize)
							    .detail("SnapshotEndLocation", uncommittedSnapshotEnd)
							    .detail("NextReadLoc", self->log->getNextReadLocation());
							break;
						}
						h = *(OpHeader*)data.begin();
					}
					Standalone<StringRef> data = wait(self->log->readNext(h.len1 + h.len2 + 1));
					if (data.size() != h.len1 + h.len2 + 1) {
						zeroFillSize = h.len1 + h.len2 + 1 - data.size();
						TraceEvent("KVSMemRecoveryComplete", self->id)
						    .detail("Reason", "data specified by header does not exist")
						    .detail("DataSize", data.size())
						    .detail("ZeroFillSize", zeroFillSize)
						    .detail("SnapshotEndLocation", uncommittedSnapshotEnd)
						    .detail("OpCode", h.op)
						    .detail("NextReadLoc", self->log->getNextReadLocation());
						break;
					}

					if (data[data.size() - 1]) {
						StringRef p1 = data.substr(0, h.len1);
						StringRef p2 = data.substr(h.len1, h.len2);

						if (h.op == OpSnapshotItem || h.op == OpSnapshotItemDelta) { // snapshot data item
							/*if (p1 < uncommittedNextKey) {
							    TraceEvent(SevError, "RecSnapshotBack", self->id)
							        .detail("NextKey", uncommittedNextKey)
							        .detail("P1", p1)
							        .detail("Nextlocation", self->log->getNextReadLocation());
							}
							ASSERT( p1 >= uncommittedNextKey );*/
							if (h.op == OpSnapshotItemDelta) {
								ASSERT(p1.size() > 1);
								// Get number of bytes borrowed from previous item key
								int borrowed = *(uint8_t*)p1.begin();
								ASSERT(borrowed <= lastSnapshotKey.size());
								// Trim p1 to just the suffix
								StringRef suffix = p1.substr(1);
								// Allocate a new string in data arena to hold prefix + suffix
								Arena& dataArena = *(Arena*)&data.arena();
								p1 = makeString(borrowed + suffix.size(), dataArena);
								// Copy the prefix into the new reconstituted key
								memcpy(mutateString(p1), lastSnapshotKey.begin(), borrowed);
								// Copy the suffix into the new reconstituted key
								memcpy(mutateString(p1) + borrowed, suffix.begin(), suffix.size());
							}
							if (p1 >= uncommittedNextKey)
								recoveryQueue.clear(
								    KeyRangeRef(uncommittedNextKey, p1),
								    &uncommittedNextKey
								         .arena()); // FIXME: Not sure what this line is for, is it necessary?
							recoveryQueue.set(KeyValueRef(p1, p2), &data.arena());
							uncommittedNextKey = keyAfter(p1);
							++dbgSnapshotItemCount;
							lastSnapshotKey = Key(p1, data.arena());
						} else if (h.op == OpSnapshotEnd || h.op == OpSnapshotAbort) { // snapshot complete
							TraceEvent("RecSnapshotEnd", self->id)
							    .detail("NextKey", uncommittedNextKey)
							    .detail("Nextlocation", self->log->getNextReadLocation())
							    .detail("IsSnapshotEnd", h.op == OpSnapshotEnd);

							if (h.op == OpSnapshotEnd) {
								uncommittedPrevSnapshotEnd = uncommittedSnapshotEnd;
								uncommittedSnapshotEnd = self->log->getNextReadLocation();
								recoveryQueue.clear_to_end(uncommittedNextKey, &uncommittedNextKey.arena());
							}

							uncommittedNextKey = Key();
							lastSnapshotKey = Key();
							++dbgSnapshotEndCount;
						} else if (h.op == OpSet) { // set mutation
							recoveryQueue.set(KeyValueRef(p1, p2), &data.arena());
							++dbgMutationCount;
						} else if (h.op == OpClear) { // clear mutation
							recoveryQueue.clear(KeyRangeRef(p1, p2), &data.arena());
							++dbgMutationCount;
						} else if (h.op == OpClearToEnd) { // clear all data from begin key to end
							recoveryQueue.clear_to_end(p1, &data.arena());
						} else if (h.op == OpCommit) { // commit previous transaction
							self->commit_queue(recoveryQueue, false);
							++dbgCommitCount;
							self->recoveredSnapshotKey = uncommittedNextKey;
							self->previousSnapshotEnd = uncommittedPrevSnapshotEnd;
							self->currentSnapshotEnd = uncommittedSnapshotEnd;
						} else if (h.op == OpRollback) { // rollback previous transaction
							recoveryQueue.rollback();
							TraceEvent("KVSMemRecSnapshotRollback", self->id).detail("NextKey", uncommittedNextKey);
							uncommittedNextKey = self->recoveredSnapshotKey;
							uncommittedPrevSnapshotEnd = self->previousSnapshotEnd;
							uncommittedSnapshotEnd = self->currentSnapshotEnd;
						} else
							ASSERT(false);
					} else {
						TraceEvent("KVSMemRecoverySkippedZeroFill", self->id)
						    .detail("PayloadSize", data.size())
						    .detail("ExpectedSize", h.len1 + h.len2 + 1)
						    .detail("OpCode", h.op)
						    .detail("EndsAt", self->log->getNextReadLocation());
					}

					if (loggingDelay.isReady()) {
						TraceEvent("KVSMemRecoveryLogSnap", self->id)
						    .detail("SnapshotItems", dbgSnapshotItemCount)
						    .detail("SnapshotEnd", dbgSnapshotEndCount)
						    .detail("Mutations", dbgMutationCount)
						    .detail("Commits", dbgCommitCount)
						    .detail("EndsAt", self->log->getNextReadLocation());
						loggingDelay = delay(1.0);
					}

					wait(yield());
				}

				if (zeroFillSize) {
					if (exactRecovery) {
						TraceEvent(SevError, "KVSMemExpectedExact", self->id).log();
						ASSERT(false);
					}

					TEST(true); // Fixing a partial commit at the end of the KeyValueStoreMemory log
					for (int i = 0; i < zeroFillSize; i++)
						self->log->push(StringRef((const uint8_t*)"", 1));
				}
				// self->rollback(); not needed, since we are about to discard anything left in the recoveryQueue
				//TraceEvent("KVSMemRecRollback", self->id).detail("QueueEmpty", data.size() == 0);
				// make sure that before any new operations are added to the log that all uncommitted operations are
				// "rolled back"
				self->log_op(OpRollback, StringRef(), StringRef()); // rollback previous transaction

				self->committedDataSize = self->data.sumTo(self->data.end());

				TraceEvent("KVSMemRecovered", self->id)
				    .detail("SnapshotItems", dbgSnapshotItemCount)
				    .detail("SnapshotEnd", dbgSnapshotEndCount)
				    .detail("Mutations", dbgMutationCount)
				    .detail("Commits", dbgCommitCount)
				    .detail("TimeTaken", now() - startt);

				self->semiCommit();
				return Void();
			} catch (Error& e) {
				bool ok = e.code() == error_code_operation_cancelled || e.code() == error_code_file_not_found ||
				          e.code() == error_code_disk_adapter_reset;
				TraceEvent(ok ? SevInfo : SevError, "ErrorDuringRecovery", dbgid).errorUnsuppressed(e);
				if (e.code() != error_code_disk_adapter_reset) {
					throw e;
				}
				self->data.clear();
				self->dataSets.clear();
			}
		}
	}

	// Snapshots an entire data set
	void fullSnapshot(Container& snapshotData) {
		previousSnapshotEnd = log_op(OpSnapshotAbort, StringRef(), StringRef());
		replaceContent = false;

		// Clear everything since we are about to write the whole database
		log_op(OpClearToEnd, allKeys.begin, StringRef());

		int count = 0;
		int64_t snapshotSize = 0;
		for (auto kv = snapshotData.begin(); kv != snapshotData.end(); ++kv) {
			StringRef tempKey = kv.getKey(reserved_buffer);
			log_op(OpSnapshotItem, tempKey, kv.getValue());
			snapshotSize += tempKey.size() + kv.getValue().size() + OP_DISK_OVERHEAD;
			++count;
		}

		TraceEvent("FullSnapshotEnd", id)
		    .detail("PreviousSnapshotEndLoc", previousSnapshotEnd)
		    .detail("SnapshotSize", snapshotSize)
		    .detail("SnapshotElements", count);

		currentSnapshotEnd = log_op(OpSnapshotEnd, StringRef(), StringRef());
	}

	ACTOR static Future<Void> snapshot(KeyValueStoreMemory* self) {
		wait(self->recovering);

		state Key nextKey = self->recoveredSnapshotKey;
		state bool nextKeyAfter = false; // setting this to true is equilvent to setting nextKey = keyAfter(nextKey)
		state uint64_t snapshotTotalWrittenBytes = 0;
		state int lastDiff = 0;
		state int snapItems = 0;
		state uint64_t snapshotBytes = 0;

		// Snapshot keys will be alternately written to two preallocated buffers.
		// This allows consecutive snapshot keys to be compared for delta compression while only copying each key's
		// bytes once.
		state Key lastSnapshotKeyA = makeString(CLIENT_KNOBS->SYSTEM_KEY_SIZE_LIMIT);
		state Key lastSnapshotKeyB = makeString(CLIENT_KNOBS->SYSTEM_KEY_SIZE_LIMIT);
		state bool lastSnapshotKeyUsingA = true;

		TraceEvent("KVSMemStartingSnapshot", self->id).detail("StartKey", nextKey);

		loop {
			wait(self->notifiedCommittedWriteBytes.whenAtLeast(snapshotTotalWrittenBytes + 1));

			if (self->resetSnapshot) {
				nextKey = Key();
				nextKeyAfter = false;
				snapItems = 0;
				snapshotBytes = 0;
				self->resetSnapshot = false;
			}

			auto next = nextKeyAfter ? self->data.upper_bound(nextKey) : self->data.lower_bound(nextKey);
			int diff = self->notifiedCommittedWriteBytes.get() - snapshotTotalWrittenBytes;
			if (diff > lastDiff && diff > 5e7)
				TraceEvent(SevWarnAlways, "ManyWritesAtOnce", self->id)
				    .detail("CommittedWrites", self->notifiedCommittedWriteBytes.get())
				    .detail("SnapshotWrites", snapshotTotalWrittenBytes)
				    .detail("Diff", diff)
				    .detail("LastOperationWasASnapshot", nextKey == Key() && !nextKeyAfter);
			lastDiff = diff;

			// Since notifiedCommittedWriteBytes is only set() once per commit, before logging the commit operation,
			// when this line is reached it is certain that there are no snapshot items in this commit yet.  Since this
			// commit could be the first thing read during recovery, we can't write a delta yet.
			bool useDelta = false;

			// Write snapshot items until the wait above would block because we've used up all of the byte budget
			loop {

				if (next == self->data.end()) {
					// After a snapshot end is logged, recovery may not see the last snapshot item logged before it so
					// the next snapshot item logged cannot be a delta.
					useDelta = false;

					auto thisSnapshotEnd = self->log_op(OpSnapshotEnd, StringRef(), StringRef());
					//TraceEvent("SnapshotEnd", self->id)
					//	.detail("LastKey", lastKey.present() ? lastKey.get() : LiteralStringRef("<none>"))
					//	.detail("CurrentSnapshotEndLoc", self->currentSnapshotEnd)
					//	.detail("PreviousSnapshotEndLoc", self->previousSnapshotEnd)
					//	.detail("ThisSnapshotEnd", thisSnapshotEnd)
					//	.detail("Items", snapItems)
					//	.detail("CommittedWrites", self->notifiedCommittedWriteBytes.get())
					//	.detail("SnapshotSize", snapshotBytes);

					ASSERT(thisSnapshotEnd >= self->currentSnapshotEnd);
					self->previousSnapshotEnd = self->currentSnapshotEnd;
					self->currentSnapshotEnd = thisSnapshotEnd;

					if (++self->snapshotCount == 2) {
						self->replaceContent = false;
					}

					snapItems = 0;
					snapshotBytes = 0;
					snapshotTotalWrittenBytes += OP_DISK_OVERHEAD;

					// If we're not stopping now, reset next
					if (snapshotTotalWrittenBytes < self->notifiedCommittedWriteBytes.get()) {
						next = self->data.begin();
					} else {
						// Otherwise, save state for continuing after the next wait and stop
						nextKey = Key();
						nextKeyAfter = false;
						break;
					}

				} else {
					// destKey is whichever of the two last key buffers we should write to next.
					Key& destKey = lastSnapshotKeyUsingA ? lastSnapshotKeyA : lastSnapshotKeyB;

					// Get the key, using destKey as a temporary buffer if needed.
					KeyRef tempKey = next.getKey(mutateString(destKey));
					int opKeySize = tempKey.size();

					// If tempKey did not use the start of destKey, then copy tempKey into destKey.
					// It's technically possible for the source and dest to overlap but with the current container
					// implementations that will not happen.
					if (tempKey.begin() != destKey.begin()) {
						memcpy(mutateString(destKey), tempKey.begin(), tempKey.size());
					}

					// Now, tempKey's bytes definitely exist in memory at destKey.begin() so update destKey's contents
					// to be a proper KeyRef of the key. This intentionally leaves the Arena alone and doesn't copy
					// anything into it.
					destKey.contents() = KeyRef(destKey.begin(), tempKey.size());

					// Get the common prefix between this key and the previous one, or 0 if there was no previous one.
					int commonPrefix;
					if (useDelta && SERVER_KNOBS->PREFIX_COMPRESS_KVS_MEM_SNAPSHOTS) {
						commonPrefix = commonPrefixLength(lastSnapshotKeyA, lastSnapshotKeyB);
					} else {
						commonPrefix = 0;
						useDelta = true;
					}

					// If the common prefix is greater than 1, write a delta item.  It isn't worth doing for 0 or 1
					// bytes, it would merely add decode overhead (string copying).
					if (commonPrefix > 1) {
						// Cap the common prefix length to 255.  Sorry, ridiculously long keys!
						commonPrefix = std::min<int>(commonPrefix, std::numeric_limits<uint8_t>::max());

						// We're going to temporarily write a 1-byte integer just before the key suffix to create the
						// log op key and log it, then restore that byte.
						uint8_t& prefixLength = mutateString(destKey)[commonPrefix - 1];
						uint8_t backupByte = prefixLength;
						prefixLength = commonPrefix;

						opKeySize = opKeySize - commonPrefix + 1;
						KeyRef opKey(&prefixLength, opKeySize);
						self->log_op(OpSnapshotItemDelta, opKey, next.getValue());

						// Restore the overwritten byte
						prefixLength = backupByte;
					} else {
						self->log_op(OpSnapshotItem, tempKey, next.getValue());
					}

					snapItems++;
					uint64_t opBytes = opKeySize + next.getValue().size() + OP_DISK_OVERHEAD;
					snapshotBytes += opBytes;
					snapshotTotalWrittenBytes += opBytes;
					lastSnapshotKeyUsingA = !lastSnapshotKeyUsingA;

					// If we're not stopping now, increment next
					if (snapshotTotalWrittenBytes < self->notifiedCommittedWriteBytes.get()) {
						++next;
					} else {
						// Otherwise, save state for continuing after the next wait and stop
						nextKey = destKey;
						nextKeyAfter = true;
						break;
					}
				}
			}
		}
	}

	ACTOR static Future<Optional<Value>> waitAndReadValue(KeyValueStoreMemory* self, Key key) {
		wait(self->recovering);
		return static_cast<IKeyValueStore*>(self)->readValue(key).get();
	}
	ACTOR static Future<Optional<Value>> waitAndReadValuePrefix(KeyValueStoreMemory* self, Key key, int maxLength) {
		wait(self->recovering);
		return static_cast<IKeyValueStore*>(self)->readValuePrefix(key, maxLength).get();
	}
	ACTOR static Future<RangeResult> waitAndReadRange(KeyValueStoreMemory* self,
	                                                  KeyRange keys,
	                                                  int rowLimit,
	                                                  int byteLimit) {
		wait(self->recovering);
		return static_cast<IKeyValueStore*>(self)->readRange(keys, rowLimit, byteLimit).get();
	}
	ACTOR static Future<Void> waitAndCommit(KeyValueStoreMemory* self, bool sequential) {
		wait(self->recovering);
		wait(self->commit(sequential));
		return Void();
	}
	ACTOR static Future<Void> commitAndUpdateVersions(KeyValueStoreMemory* self,
	                                                  Future<Void> commit,
	                                                  IDiskQueue::location location) {
		wait(commit);
		self->log->pop(location);
		return Void();
	}
};

template <typename Container>
KeyValueStoreMemory<Container>::KeyValueStoreMemory(IDiskQueue* log,
                                                    UID id,
                                                    int64_t memoryLimit,
                                                    KeyValueStoreType storeType,
                                                    bool disableSnapshot,
                                                    bool replaceContent,
                                                    bool exactRecovery)
  : type(storeType), id(id), log(log), committedWriteBytes(0), overheadWriteBytes(0), currentSnapshotEnd(-1),
    previousSnapshotEnd(-1), committedDataSize(0), transactionSize(0), transactionIsLarge(false), resetSnapshot(false),
    disableSnapshot(disableSnapshot), replaceContent(replaceContent), firstCommitWithSnapshot(true), snapshotCount(0),
    memoryLimit(memoryLimit) {
	// create reserved buffer for radixtree store type
	this->reserved_buffer =
	    (storeType == KeyValueStoreType::MEMORY) ? nullptr : new uint8_t[CLIENT_KNOBS->SYSTEM_KEY_SIZE_LIMIT];
	if (this->reserved_buffer != nullptr)
		memset(this->reserved_buffer, 0, CLIENT_KNOBS->SYSTEM_KEY_SIZE_LIMIT);

	recovering = recover(this, exactRecovery);
	snapshotting = snapshot(this);
	commitActors = actorCollection(addActor.getFuture());
}

IKeyValueStore* keyValueStoreMemory(std::string const& basename,
                                    UID logID,
                                    int64_t memoryLimit,
                                    std::string ext,
                                    KeyValueStoreType storeType) {
	TraceEvent("KVSMemOpening", logID)
	    .detail("Basename", basename)
	    .detail("MemoryLimit", memoryLimit)
	    .detail("StoreType", storeType);

	// SOMEDAY: update to use DiskQueueVersion::V2 with xxhash3 checksum for FDB >= 7.2
	IDiskQueue* log = openDiskQueue(basename, ext, logID, DiskQueueVersion::V1);
	if (storeType == KeyValueStoreType::MEMORY_RADIXTREE) {
		return new KeyValueStoreMemory<radix_tree>(log, logID, memoryLimit, storeType, false, false, false);
	} else {
		return new KeyValueStoreMemory<IKeyValueContainer>(log, logID, memoryLimit, storeType, false, false, false);
	}
}

IKeyValueStore* keyValueStoreLogSystem(class IDiskQueue* queue,
                                       UID logID,
                                       int64_t memoryLimit,
                                       bool disableSnapshot,
                                       bool replaceContent,
                                       bool exactRecovery) {
	return new KeyValueStoreMemory<IKeyValueContainer>(
	    queue, logID, memoryLimit, KeyValueStoreType::MEMORY, disableSnapshot, replaceContent, exactRecovery);
}
