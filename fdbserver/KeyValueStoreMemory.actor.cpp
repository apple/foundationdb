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

#include "fdbclient/BlobCipher.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/Knobs.h"
#include "fdbclient/Notified.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/DeltaTree.h"
#include "fdbclient/GetEncryptCipherKeys.h"
#include "fdbserver/IDiskQueue.h"
#include "fdbserver/IKeyValueContainer.h"
#include "fdbserver/IKeyValueStore.h"
#include "fdbserver/RadixTree.h"
#include "flow/ActorCollection.h"
#include "flow/EncryptUtils.h"
#include "flow/Knobs.h"
#include "flow/actorcompiler.h" // This must be the last #include.

#define OP_DISK_OVERHEAD (sizeof(OpHeader) + 1)
#define ENCRYPTION_ENABLED_BIT 31
static_assert(sizeof(uint32_t) == 4);

template <typename Container>
class KeyValueStoreMemory final : public IKeyValueStore, NonCopyable {
public:
	KeyValueStoreMemory(IDiskQueue* log,
	                    Reference<AsyncVar<ServerDBInfo> const> db,
	                    UID id,
	                    int64_t memoryLimit,
	                    KeyValueStoreType storeType,
	                    bool disableSnapshot,
	                    bool replaceContent,
	                    bool exactRecovery,
	                    bool enableEncryption);

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
			CODE_PROBE(true, "KeyValueStoreMemory switching to large transaction mode");
			CODE_PROBE(committedDataSize > 1e3,
			           "KeyValueStoreMemory switching to large transaction mode with committed data");
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

	Future<Optional<Value>> readValue(KeyRef key, Optional<ReadOptions> options) override {
		if (recovering.isError())
			throw recovering.getError();
		if (!recovering.isReady())
			return waitAndReadValue(this, key, options);

		auto it = data.find(key);
		if (it == data.end())
			return Optional<Value>();
		return Optional<Value>(it.getValue());
	}

	Future<Optional<Value>> readValuePrefix(KeyRef key, int maxLength, Optional<ReadOptions> options) override {
		if (recovering.isError())
			throw recovering.getError();
		if (!recovering.isReady())
			return waitAndReadValuePrefix(this, key, maxLength, options);

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
	Future<RangeResult> readRange(KeyRangeRef keys,
	                              int rowLimit,
	                              int byteLimit,
	                              Optional<ReadOptions> options) override {
		if (recovering.isError())
			throw recovering.getError();
		if (!recovering.isReady())
			return waitAndReadRange(this, keys, rowLimit, byteLimit, options);

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

	int uncommittedBytes() { return queue.totalSize(); }

	// KeyValueStoreMemory does not support encryption-at-rest in general, despite it supports encryption
	// when being used as TxnStateStore backend.
	Future<EncryptionAtRestMode> encryptionMode() override {
		return EncryptionAtRestMode(EncryptionAtRestMode::DISABLED);
	}

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
		OpSnapshotItemDelta,
		OpEncrypted_Deprecated // deprecated since we now store the encryption status in the first bit of the opType
	};

	struct OpRef {
		OpType op;
		StringRef p1, p2;
		OpRef() {}
		OpRef(Arena& a, OpRef const& o) : op(o.op), p1(a, o.p1), p2(a, o.p2) {}
		size_t expectedSize() const { return p1.expectedSize() + p2.expectedSize(); }
	};
	struct OpHeader {
		uint32_t op;
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
	Reference<AsyncVar<ServerDBInfo> const> db;
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

	bool enableEncryption;
	TextAndHeaderCipherKeys cipherKeys;
	Future<Void> refreshCipherKeysActor;

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

	static bool isOpEncrypted(OpHeader* header) { return header->op >> ENCRYPTION_ENABLED_BIT == 1; }

	static void setEncryptFlag(OpHeader* header, bool set) {
		if (set) {
			header->op |= (1UL << ENCRYPTION_ENABLED_BIT);
		} else {
			header->op &= ~(1UL << ENCRYPTION_ENABLED_BIT);
		}
	}

	// NOTE: The first bit of opType indicates whether the entry is encrypted or not. This is fine for backwards
	// compatability since the first bit was never used previously
	//
	// Unencrypted data format:
	// +-------------+-------------+-------------+--------+--------+-----------+
	// | opType      | len1        | len2        | param2 | param2 |   \x01    |
	// | sizeof(int) | sizeof(int) | sizeof(int) | len1   | len2   |  1 byte   |
	// +-------------+-------------+-------------+--------+--------+-----------+
	//
	// Encrypted data format:
	// +-------------+-------+---------+------------+-----------------------------+--------+--------+------------+
	// |   opType    |len1   | len2    | headerSize | BlobCipherEncryptHeader     | param1 | param2 |    \x01    |
	// |  s(uint32)  | s(int)| s(int)  | s(uint16)  |  s(BlobCipherEncryptHeader) | len1   | len2   |   1 byte   |
	// +-------------+-------+---------+------------+-----------------------------+--------+--------+------------+
	// |                                plaintext                                 |    encrypted    |            |
	// +--------------------------------------------------------------------------+-----------------+------------+
	//
	IDiskQueue::location log_op(OpType op, StringRef v1, StringRef v2) {
		// Metadata op types to be excluded from encryption.
		static std::unordered_set<OpType> metaOps = { OpSnapshotEnd, OpSnapshotAbort, OpCommit, OpRollback };
		uint32_t opType = (uint32_t)op;
		// Make sure the first bit of the optype is empty
		ASSERT(opType >> ENCRYPTION_ENABLED_BIT == 0);
		if (!enableEncryption || metaOps.count(op) > 0) {
			OpHeader h = { opType, v1.size(), v2.size() };
			log->push(StringRef((const uint8_t*)&h, sizeof(h)));
			log->push(v1);
			log->push(v2);
		} else {
			OpHeader h = { opType, v1.size(), v2.size() };
			// Set the first bit of the header to 1 to indicate that the log entry is encrypted
			setEncryptFlag(&h, true);
			log->push(StringRef((const uint8_t*)&h, sizeof(h)));

			uint8_t* plaintext = new uint8_t[v1.size() + v2.size()];
			if (v1.size()) {
				memcpy(plaintext, v1.begin(), v1.size());
			}
			if (v2.size()) {
				memcpy(plaintext + v1.size(), v2.begin(), v2.size());
			}

			ASSERT(cipherKeys.cipherTextKey.isValid());
			ASSERT(cipherKeys.cipherHeaderKey.isValid());
			EncryptBlobCipherAes265Ctr cipher(
			    cipherKeys.cipherTextKey,
			    cipherKeys.cipherHeaderKey,
			    getEncryptAuthTokenMode(EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_SINGLE),
			    BlobCipherMetrics::KV_MEMORY);
			uint16_t encryptHeaderSize;
			// TODO: If possible we want to avoid memcpy to the disk log by using the same arena used by IDiskQueue
			Arena arena;
			if (CLIENT_KNOBS->ENABLE_CONFIGURABLE_ENCRYPTION) {
				BlobCipherEncryptHeaderRef headerRef;
				StringRef cipherText = cipher.encrypt(plaintext, v1.size() + v2.size(), &headerRef, arena);
				Standalone<StringRef> headerRefStr = BlobCipherEncryptHeaderRef::toStringRef(headerRef);
				encryptHeaderSize = headerRefStr.size();
				ASSERT(encryptHeaderSize > 0);
				log->push(StringRef((const uint8_t*)&encryptHeaderSize, sizeof(encryptHeaderSize)));
				log->push(headerRefStr);
				log->push(cipherText);
			} else {
				BlobCipherEncryptHeader cipherHeader;
				StringRef ciphertext =
				    cipher.encrypt(plaintext, v1.size() + v2.size(), &cipherHeader, arena)->toStringRef();
				encryptHeaderSize = BlobCipherEncryptHeader::headerSize;
				ASSERT(encryptHeaderSize > 0);
				log->push(StringRef((const uint8_t*)&encryptHeaderSize, sizeof(encryptHeaderSize)));
				log->push(StringRef((const uint8_t*)&cipherHeader, encryptHeaderSize));
				log->push(ciphertext);
			}
		}
		return log->push("\x01"_sr); // Changes here should be reflected in OP_DISK_OVERHEAD
	}

	// In case the op data is not encrypted, simply read the operands and the zero fill flag.
	// Otherwise, decrypt the op type and data.
	ACTOR static Future<Standalone<StringRef>> readOpData(KeyValueStoreMemory* self,
	                                                      OpHeader h,
	                                                      bool* isZeroFilled,
	                                                      int* zeroFillSize,
	                                                      bool encryptedOp) {
		ASSERT(!isOpEncrypted(&h));
		// Metadata op types to be excluded from encryption.
		static std::unordered_set<OpType> metaOps = { OpSnapshotEnd, OpSnapshotAbort, OpCommit, OpRollback };
		if (metaOps.count((OpType)h.op) == 0) {
			// It is not supported to open an encrypted store as unencrypted, or vice-versa.
			ASSERT_EQ(encryptedOp, self->enableEncryption);
		}
		// if encrypted op read the header size
		state uint16_t encryptHeaderSize = 0;
		if (encryptedOp) {
			state Standalone<StringRef> headerSizeStr = wait(self->log->readNext(sizeof(encryptHeaderSize)));
			ASSERT(headerSizeStr.size() <= sizeof(encryptHeaderSize));
			// Partial read on the header size
			memset(&encryptHeaderSize, 0, sizeof(encryptHeaderSize));
			memcpy(&encryptHeaderSize, headerSizeStr.begin(), headerSizeStr.size());
			if (headerSizeStr.size() < sizeof(encryptHeaderSize)) {
				CODE_PROBE(true, "zero fill partial encryption header size", probe::decoration::rare);
				*zeroFillSize =
				    (sizeof(encryptHeaderSize) - headerSizeStr.size()) + encryptHeaderSize + h.len1 + h.len2 + 1;
			}
			if (*zeroFillSize > 0) {
				return headerSizeStr;
			}
		}
		state int remainingBytes = h.len1 + h.len2 + 1;
		if (encryptedOp) {
			// encryption header, plus the real (encrypted) op type
			remainingBytes += encryptHeaderSize;
		}
		state Standalone<StringRef> data = wait(self->log->readNext(remainingBytes));
		ASSERT(data.size() <= remainingBytes);
		*zeroFillSize = remainingBytes - data.size();
		if (*zeroFillSize == 0) {
			*isZeroFilled = (data[data.size() - 1] == 0);
		}
		if (!encryptedOp || *zeroFillSize > 0 || *isZeroFilled) {
			return data;
		}
		state Arena arena;
		state StringRef plaintext;
		if (CLIENT_KNOBS->ENABLE_CONFIGURABLE_ENCRYPTION) {
			state BlobCipherEncryptHeaderRef cipherHeaderRef =
			    BlobCipherEncryptHeaderRef::fromStringRef(StringRef(data.begin(), encryptHeaderSize));
			TextAndHeaderCipherKeys cipherKeys = wait(GetEncryptCipherKeys<ServerDBInfo>::getEncryptCipherKeys(
			    self->db, cipherHeaderRef, BlobCipherMetrics::KV_MEMORY));
			DecryptBlobCipherAes256Ctr cipher(cipherKeys.cipherTextKey,
			                                  cipherKeys.cipherHeaderKey,
			                                  cipherHeaderRef.getIV(),
			                                  BlobCipherMetrics::KV_MEMORY);
			plaintext = cipher.decrypt(data.begin() + encryptHeaderSize, h.len1 + h.len2, cipherHeaderRef, arena);
		} else {
			state BlobCipherEncryptHeader cipherHeader = *(BlobCipherEncryptHeader*)data.begin();
			TextAndHeaderCipherKeys cipherKeys = wait(GetEncryptCipherKeys<ServerDBInfo>::getEncryptCipherKeys(
			    self->db, cipherHeader, BlobCipherMetrics::KV_MEMORY));
			DecryptBlobCipherAes256Ctr cipher(
			    cipherKeys.cipherTextKey, cipherKeys.cipherHeaderKey, cipherHeader.iv, BlobCipherMetrics::KV_MEMORY);
			plaintext =
			    cipher.decrypt(data.begin() + encryptHeaderSize, h.len1 + h.len2, cipherHeader, arena)->toStringRef();
		}
		return Standalone<StringRef>(plaintext, arena);
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
			state bool isZeroFilled;

			TraceEvent("KVSMemRecoveryStarted", self->id).detail("SnapshotEndLocation", uncommittedSnapshotEnd);

			try {
				loop {
					state bool encryptedOp = false;
					{
						Standalone<StringRef> data = wait(self->log->readNext(sizeof(OpHeader)));
						if (data.size() != sizeof(OpHeader)) {
							if (data.size()) {
								CODE_PROBE(
								    true, "zero fill partial header in KeyValueStoreMemory", probe::decoration::rare);
								memset(&h, 0, sizeof(OpHeader));
								memcpy(&h, data.begin(), data.size());
								zeroFillSize = sizeof(OpHeader) - data.size() + h.len1 + h.len2 + 1;
								if (isOpEncrypted(&h)) {
									// encrypt header size + encryption header
									// If it's a partial header we assume the header size is 0 (this is fine since we
									// don't read the header in this case)
									zeroFillSize += 0 + sizeof(uint16_t);
								}
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
						encryptedOp = isOpEncrypted(&h);
						// Reset the first bit to 0 so the op can be read properly
						setEncryptFlag(&h, false);
						ASSERT(h.op != OpEncrypted_Deprecated);
					}
					state Standalone<StringRef> data =
					    wait(readOpData(self, h, &isZeroFilled, &zeroFillSize, encryptedOp));
					if (zeroFillSize > 0) {
						TraceEvent("KVSMemRecoveryComplete", self->id)
						    .detail("Reason", "data specified by header does not exist")
						    .detail("DataSize", data.size())
						    .detail("ZeroFillSize", zeroFillSize)
						    .detail("SnapshotEndLocation", uncommittedSnapshotEnd)
						    .detail("OpCode", h.op)
						    .detail("NextReadLoc", self->log->getNextReadLocation());
						break;
					}

					if (!isZeroFilled) {
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

					CODE_PROBE(true, "Fixing a partial commit at the end of the KeyValueStoreMemory log");
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

				// Make sure cipher keys are ready before recovery finishes. The semiCommit below also require cipher
				// keys.
				if (self->enableEncryption) {
					wait(updateCipherKeys(self));
				}

				CODE_PROBE(self->enableEncryption && self->uncommittedBytes() > 0,
				           "KeyValueStoreMemory recovered partial transaction while encryption-at-rest is enabled",
				           probe::decoration::rare);
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
					DisabledTraceEvent("SnapshotEnd", self->id)
					    .detail("CurrentSnapshotEndLoc", self->currentSnapshotEnd)
					    .detail("PreviousSnapshotEndLoc", self->previousSnapshotEnd)
					    .detail("ThisSnapshotEnd", thisSnapshotEnd)
					    .detail("Items", snapItems)
					    .detail("CommittedWrites", self->notifiedCommittedWriteBytes.get())
					    .detail("SnapshotSize", snapshotBytes);

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

	ACTOR static Future<Optional<Value>> waitAndReadValue(KeyValueStoreMemory* self,
	                                                      Key key,
	                                                      Optional<ReadOptions> options) {
		wait(self->recovering);
		return static_cast<IKeyValueStore*>(self)->readValue(key, options).get();
	}
	ACTOR static Future<Optional<Value>> waitAndReadValuePrefix(KeyValueStoreMemory* self,
	                                                            Key key,
	                                                            int maxLength,
	                                                            Optional<ReadOptions> options) {
		wait(self->recovering);
		return static_cast<IKeyValueStore*>(self)->readValuePrefix(key, maxLength, options).get();
	}
	ACTOR static Future<RangeResult> waitAndReadRange(KeyValueStoreMemory* self,
	                                                  KeyRange keys,
	                                                  int rowLimit,
	                                                  int byteLimit,
	                                                  Optional<ReadOptions> options) {
		wait(self->recovering);
		return static_cast<IKeyValueStore*>(self)->readRange(keys, rowLimit, byteLimit, options).get();
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

	ACTOR static Future<Void> updateCipherKeys(KeyValueStoreMemory* self) {
		TextAndHeaderCipherKeys cipherKeys = wait(GetEncryptCipherKeys<ServerDBInfo>::getLatestSystemEncryptCipherKeys(
		    self->db, BlobCipherMetrics::KV_MEMORY));
		self->cipherKeys = cipherKeys;
		return Void();
	}

	// TODO(yiwu): Implement background refresh mechanism for BlobCipher and use that mechanism to refresh cipher key.
	ACTOR static Future<Void> refreshCipherKeys(KeyValueStoreMemory* self) {
		loop {
			wait(updateCipherKeys(self));
			wait(delay(FLOW_KNOBS->ENCRYPT_KEY_REFRESH_INTERVAL));
		}
	}
};

template <typename Container>
KeyValueStoreMemory<Container>::KeyValueStoreMemory(IDiskQueue* log,
                                                    Reference<AsyncVar<ServerDBInfo> const> db,
                                                    UID id,
                                                    int64_t memoryLimit,
                                                    KeyValueStoreType storeType,
                                                    bool disableSnapshot,
                                                    bool replaceContent,
                                                    bool exactRecovery,
                                                    bool enableEncryption)
  : type(storeType), id(id), log(log), db(db), committedWriteBytes(0), overheadWriteBytes(0), currentSnapshotEnd(-1),
    previousSnapshotEnd(-1), committedDataSize(0), transactionSize(0), transactionIsLarge(false), resetSnapshot(false),
    disableSnapshot(disableSnapshot), replaceContent(replaceContent), firstCommitWithSnapshot(true), snapshotCount(0),
    memoryLimit(memoryLimit), enableEncryption(enableEncryption) {
	// create reserved buffer for radixtree store type
	this->reserved_buffer =
	    (storeType == KeyValueStoreType::MEMORY) ? nullptr : new uint8_t[CLIENT_KNOBS->SYSTEM_KEY_SIZE_LIMIT];
	if (this->reserved_buffer != nullptr)
		memset(this->reserved_buffer, 0, CLIENT_KNOBS->SYSTEM_KEY_SIZE_LIMIT);

	recovering = recover(this, exactRecovery);
	snapshotting = snapshot(this);
	commitActors = actorCollection(addActor.getFuture());
	if (enableEncryption) {
		refreshCipherKeysActor = refreshCipherKeys(this);
	}
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
		return new KeyValueStoreMemory<radix_tree>(
		    log, Reference<AsyncVar<ServerDBInfo> const>(), logID, memoryLimit, storeType, false, false, false, false);
	} else {
		return new KeyValueStoreMemory<IKeyValueContainer>(
		    log, Reference<AsyncVar<ServerDBInfo> const>(), logID, memoryLimit, storeType, false, false, false, false);
	}
}

IKeyValueStore* keyValueStoreLogSystem(class IDiskQueue* queue,
                                       Reference<AsyncVar<ServerDBInfo> const> db,
                                       UID logID,
                                       int64_t memoryLimit,
                                       bool disableSnapshot,
                                       bool replaceContent,
                                       bool exactRecovery,
                                       bool enableEncryption) {
	// ServerDBInfo is required if encryption is to be enabled, or the KV store instance have been encrypted.
	ASSERT(!enableEncryption || db.isValid());
	return new KeyValueStoreMemory<IKeyValueContainer>(queue,
	                                                   db,
	                                                   logID,
	                                                   memoryLimit,
	                                                   KeyValueStoreType::MEMORY,
	                                                   disableSnapshot,
	                                                   replaceContent,
	                                                   exactRecovery,
	                                                   enableEncryption);
}
