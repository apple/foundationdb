/*
 * CommitProxyServer.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#include <algorithm>
#include <string_view>
#include <tuple>
#include <variant>

#include "fdbclient/AccumulativeChecksum.h"
#include "fdbclient/Atomic.h"
#include "fdbclient/BackupAgent.actor.h"
#include "fdbclient/BlobCipher.h"
#include "fdbclient/BuildIdempotencyIdMutations.h"
#include "fdbclient/CommitTransaction.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/IdempotencyId.actor.h"
#include "fdbclient/Knobs.h"
#include "fdbclient/CommitProxyInterface.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/Tenant.h"
#include "fdbclient/TenantManagement.actor.h"
#include "fdbclient/Tracing.h"
#include "fdbclient/TransactionLineage.h"
#include "fdbrpc/TenantInfo.h"
#include "fdbrpc/sim_validation.h"
#include "fdbserver/AccumulativeChecksumUtil.h"
#include "fdbserver/ApplyMetadataMutation.h"
#include "fdbserver/ConflictSet.h"
#include "fdbserver/DataDistributorInterface.h"
#include "fdbserver/FDBExecHelper.actor.h"
#include "fdbclient/GetEncryptCipherKeys.h"
#include "fdbserver/IKeyValueStore.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/LogSystem.h"
#include "fdbserver/LogSystemDiskQueueAdapter.h"
#include "fdbserver/MasterInterface.h"
#include "fdbserver/MutationTracking.h"
#include "fdbserver/ProxyCommitData.actor.h"
#include "fdbserver/RatekeeperInterface.h"
#include "fdbserver/RecoveryState.h"
#include "fdbserver/RestoreUtil.h"
#include "fdbserver/ServerDBInfo.actor.h"
#include "fdbserver/WaitFailure.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "flow/ActorCollection.h"
#include "flow/CodeProbe.h"
#include "flow/EncryptUtils.h"
#include "flow/Error.h"
#include "flow/IRandom.h"
#include "flow/Knobs.h"
#include "flow/Trace.h"
#include "flow/network.h"

#include "flow/actorcompiler.h" // This must be the last #include.

using WriteMutationRefVar = std::variant<MutationRef, VectorRef<MutationRef>>;

ACTOR Future<Void> broadcastTxnRequest(TxnStateRequest req, int sendAmount, bool sendReply) {
	state ReplyPromise<Void> reply = req.reply;
	resetReply(req);
	std::vector<Future<Void>> replies;
	int currentStream = 0;
	std::vector<Endpoint> broadcastEndpoints = req.broadcastInfo;
	for (int i = 0; i < sendAmount && currentStream < broadcastEndpoints.size(); i++) {
		std::vector<Endpoint> endpoints;
		RequestStream<TxnStateRequest> cur(broadcastEndpoints[currentStream++]);
		while (currentStream < broadcastEndpoints.size() * (i + 1) / sendAmount) {
			endpoints.push_back(broadcastEndpoints[currentStream++]);
		}
		req.broadcastInfo = endpoints;
		replies.push_back(brokenPromiseToNever(cur.getReply(req)));
		resetReply(req);
	}
	wait(waitForAll(replies));
	if (sendReply) {
		reply.send(Void());
	}
	return Void();
}

ACTOR void discardCommit(UID id, Future<LogSystemDiskQueueAdapter::CommitMessage> fcm, Future<Void> dummyCommitState) {
	ASSERT(!dummyCommitState.isReady());
	LogSystemDiskQueueAdapter::CommitMessage cm = wait(fcm);
	TraceEvent("Discarding", id).detail("Count", cm.messages.size());
	cm.acknowledge.send(Void());
	ASSERT(dummyCommitState.isReady());
}

struct ResolutionRequestBuilder {
	const ProxyCommitData* self;

	// One request per resolver.
	std::vector<ResolveTransactionBatchRequest> requests;

	// Txn i to resolvers that have i'th data sent
	std::vector<std::vector<int>> transactionResolverMap;
	std::vector<CommitTransactionRef*> outTr;

	// Used to report conflicting keys, the format is
	// [CommitTransactionRef_Index][Resolver_Index][Read_Conflict_Range_Index_on_Resolver]
	// -> read_conflict_range's original index in the commitTransactionRef
	std::vector<std::vector<std::vector<int>>> txReadConflictRangeIndexMap;

	ResolutionRequestBuilder(ProxyCommitData* self,
	                         Version version,
	                         Version prevVersion,
	                         Version lastReceivedVersion,
	                         Span& parentSpan)
	  : self(self), requests(self->resolvers.size()) {
		for (auto& req : requests) {
			req.spanContext = parentSpan.context;
			req.prevVersion = prevVersion;
			req.version = version;
			req.lastReceivedVersion = lastReceivedVersion;
		}
	}

	CommitTransactionRef& getOutTransaction(int resolver, Version read_snapshot) {
		CommitTransactionRef*& out = outTr[resolver];
		if (!out) {
			ResolveTransactionBatchRequest& request = requests[resolver];
			request.transactions.resize(request.arena, request.transactions.size() + 1);
			out = &request.transactions.back();
			out->read_snapshot = read_snapshot;
		}
		return *out;
	}

	// Returns a read conflict index map: [resolver_index][read_conflict_range_index_on_the_resolver]
	// -> read_conflict_range's original index
	std::vector<std::vector<int>> addReadConflictRanges(CommitTransactionRef& trIn) {
		std::vector<std::vector<int>> rCRIndexMap(requests.size());
		for (int idx = 0; idx < trIn.read_conflict_ranges.size(); ++idx) {
			const auto& r = trIn.read_conflict_ranges[idx];
			auto ranges = self->keyResolvers.intersectingRanges(r);
			std::set<int> resolvers;
			for (auto& ir : ranges) {
				auto& version_resolver = ir.value();
				for (int i = version_resolver.size() - 1; i >= 0; i--) {
					resolvers.insert(version_resolver[i].second);
					if (version_resolver[i].first < trIn.read_snapshot)
						break;
				}
			}
			if (SERVER_KNOBS->PROXY_USE_RESOLVER_PRIVATE_MUTATIONS && systemKeys.intersects(r)) {
				for (int k = 0; k < self->resolvers.size(); k++) {
					resolvers.insert(k);
				}
			}
			ASSERT(resolvers.size());
			for (int resolver : resolvers) {
				getOutTransaction(resolver, trIn.read_snapshot)
				    .read_conflict_ranges.push_back(requests[resolver].arena, r);
				rCRIndexMap[resolver].push_back(idx);
			}
		}
		return rCRIndexMap;
	}

	void addWriteConflictRanges(CommitTransactionRef& trIn) {
		for (auto& r : trIn.write_conflict_ranges) {
			auto ranges = self->keyResolvers.intersectingRanges(r);
			std::set<int> resolvers;
			for (auto& ir : ranges) {
				auto& version_resolver = ir.value();
				if (!version_resolver.empty()) {
					resolvers.insert(version_resolver.back().second);
				}
			}
			if (SERVER_KNOBS->PROXY_USE_RESOLVER_PRIVATE_MUTATIONS && systemKeys.intersects(r)) {
				for (int k = 0; k < self->resolvers.size(); k++) {
					resolvers.insert(k);
				}
			}
			ASSERT(resolvers.size());
			for (int resolver : resolvers)
				getOutTransaction(resolver, trIn.read_snapshot)
				    .write_conflict_ranges.push_back(requests[resolver].arena, r);
		}
	}

	void addTransaction(CommitTransactionRequest& trRequest, Version ver, int transactionNumberInBatch) {
		auto& trIn = trRequest.transaction;
		// SOMEDAY: There are a couple of unnecessary O( # resolvers ) steps here
		outTr.assign(requests.size(), nullptr);
		ASSERT(transactionNumberInBatch >= 0 && transactionNumberInBatch < 32768);

		bool isTXNStateTransaction = false;
		DisabledTraceEvent("AddTransaction", self->dbgid).detail("TenantMode", (int)self->getTenantMode());
		bool needParseTenantId = !trRequest.tenantInfo.hasTenant() && self->getTenantMode() == TenantMode::REQUIRED;
		VectorRef<int64_t> tenantIds;
		for (auto& m : trIn.mutations) {
			DEBUG_MUTATION("AddTr", ver, m, self->dbgid).detail("Idx", transactionNumberInBatch);
			if (m.type == MutationRef::SetVersionstampedKey) {
				transformVersionstampMutation(m, &MutationRef::param1, requests[0].version, transactionNumberInBatch);
				trIn.write_conflict_ranges.push_back(requests[0].arena, singleKeyRange(m.param1, requests[0].arena));
			} else if (m.type == MutationRef::SetVersionstampedValue) {
				transformVersionstampMutation(m, &MutationRef::param2, requests[0].version, transactionNumberInBatch);
			}
			if (isMetadataMutation(m)) {
				isTXNStateTransaction = true;
				auto& tr = getOutTransaction(0, trIn.read_snapshot);
				tr.mutations.push_back(requests[0].arena, m);
				tr.lock_aware = trRequest.isLockAware();
			} else if (needParseTenantId && !isSystemKey(m.param1) && isSingleKeyMutation((MutationRef::Type)m.type)) {
				tenantIds.push_back(requests[0].arena, TenantAPI::extractTenantIdFromMutation(m));
			}
		}
		if (isTXNStateTransaction && !trRequest.isLockAware()) {
			// This mitigates https://github.com/apple/foundationdb/issues/3647. Since this transaction is not lock
			// aware, if this transaction got a read version then \xff/dbLocked must not have been set at this
			// transaction's read snapshot. If that changes by commit time, then it won't commit on any proxy because of
			// a conflict. A client could set a read version manually so this isn't totally bulletproof.
			trIn.read_conflict_ranges.push_back(trRequest.arena, KeyRangeRef(databaseLockedKey, databaseLockedKeyEnd));
		}

		std::vector<std::vector<int>> rCRIndexMap = addReadConflictRanges(trIn);
		txReadConflictRangeIndexMap.push_back(std::move(rCRIndexMap));

		addWriteConflictRanges(trIn);

		if (isTXNStateTransaction) {
			for (int r = 0; r < requests.size(); r++) {
				int transactionNumberInRequest =
				    &getOutTransaction(r, trIn.read_snapshot) - requests[r].transactions.begin();
				requests[r].txnStateTransactions.push_back(requests[r].arena, transactionNumberInRequest);
			}
			// Note only Resolver 0 got the correct spanContext, which means
			// the reply from Resolver 0 has the right one back.
			auto& tr = getOutTransaction(0, trIn.read_snapshot);
			tr.spanContext = trRequest.spanContext;
			if (self->getTenantMode() == TenantMode::REQUIRED) {
				tr.tenantIds = tenantIds;
			}
		}

		std::vector<int> resolversUsed;
		for (int r = 0; r < outTr.size(); r++)
			if (outTr[r]) {
				resolversUsed.push_back(r);
				outTr[r]->report_conflicting_keys = trIn.report_conflicting_keys;
			}
		transactionResolverMap.emplace_back(std::move(resolversUsed));
	}
};

bool checkTenantNoWait(ProxyCommitData* commitData, int64_t tenant, const char* context, bool logOnFailure) {
	if (tenant != TenantInfo::INVALID_TENANT) {
		auto itr = commitData->tenantMap.find(tenant);
		if (itr == commitData->tenantMap.end()) {
			if (logOnFailure) {
				TraceEvent(SevWarn, "CommitProxyTenantNotFound", commitData->dbgid)
				    .detail("Tenant", tenant)
				    .detail("Context", context);
			}
			CODE_PROBE(true, "Commit proxy tenant not found");
			return false;
		}

		return true;
	}

	return true;
}

ACTOR Future<bool> checkTenant(ProxyCommitData* commitData, int64_t tenant, Version minVersion, const char* context) {
	loop {
		state Version currentVersion = commitData->version.get();
		if (checkTenantNoWait(commitData, tenant, context, currentVersion >= minVersion)) {
			return true;
		} else if (currentVersion >= minVersion) {
			return false;
		} else {
			CODE_PROBE(true, "Commit proxy tenant not found waiting for min version");
			wait(commitData->version.whenAtLeast(currentVersion + 1));
		}
	}
}

bool verifyTenantPrefix(ProxyCommitData* const commitData, const CommitTransactionRequest& req) {
	if (req.tenantInfo.hasTenant()) {
		KeyRef tenantPrefix = req.tenantInfo.prefix.get();
		for (auto& m : req.transaction.mutations) {
			if (m.param1 != metadataVersionKey) {
				if (!m.param1.startsWith(tenantPrefix)) {
					TraceEvent(SevWarnAlways, "TenantPrefixMismatch")
					    .detail("Tenant", req.tenantInfo.tenantId)
					    .detail("Prefix", tenantPrefix)
					    .detail("Key", m.param1);
					CODE_PROBE(true, "Committed mutation tenant prefix mismatch", probe::decoration::rare);
					return false;
				}

				if (m.type == MutationRef::ClearRange && !m.param2.startsWith(tenantPrefix)) {
					TraceEvent(SevWarnAlways, "TenantClearRangePrefixMismatch")
					    .suppressFor(60)
					    .detail("Tenant", req.tenantInfo.tenantId)
					    .detail("Prefix", tenantPrefix)
					    .detail("Key", m.param2);
					CODE_PROBE(true, "Committed mutation clear range prefix mismatch", probe::decoration::rare);
					return false;
				} else if (m.type == MutationRef::SetVersionstampedKey) {
					ASSERT(m.param1.size() >= 4);
					uint8_t* key = const_cast<uint8_t*>(m.param1.begin());
					int* offset = reinterpret_cast<int*>(&key[m.param1.size() - 4]);
					if (*offset < tenantPrefix.size()) {
						TraceEvent(SevWarnAlways, "TenantVersionstampInvalidOffset")
						    .suppressFor(60)
						    .detail("Tenant", req.tenantInfo.tenantId)
						    .detail("Prefix", tenantPrefix)
						    .detail("Key", m.param1)
						    .detail("Offset", *offset);
						CODE_PROBE(true,
						           "Committed mutation versionstamp offset inside tenant prefix",
						           probe::decoration::rare);
						return false;
					}
				}
			} else {
				CODE_PROBE(true, "Modifying metadata version key in tenant");
			}
		}

		for (auto& rc : req.transaction.read_conflict_ranges) {
			if (rc.begin != metadataVersionKey &&
			    (!rc.begin.startsWith(tenantPrefix) || !rc.end.startsWith(tenantPrefix))) {
				TraceEvent(SevWarnAlways, "TenantReadConflictPrefixMismatch")
				    .suppressFor(60)
				    .detail("Tenant", req.tenantInfo.tenantId)
				    .detail("Prefix", tenantPrefix)
				    .detail("BeginKey", rc.begin)
				    .detail("EndKey", rc.end);
				CODE_PROBE(true, "Committed mutation read conflict prefix mismatch", probe::decoration::rare);
				return false;
			}
		}

		for (auto& wc : req.transaction.write_conflict_ranges) {
			if (wc.begin != metadataVersionKey &&
			    (!wc.begin.startsWith(tenantPrefix) || !wc.end.startsWith(tenantPrefix))) {
				TraceEvent(SevWarnAlways, "TenantWriteConflictPrefixMismatch")
				    .suppressFor(60)
				    .detail("Tenant", req.tenantInfo.tenantId)
				    .detail("Prefix", tenantPrefix)
				    .detail("BeginKey", wc.begin)
				    .detail("EndKey", wc.end);
				CODE_PROBE(true, "Committed mutation write conflict prefix mismatch", probe::decoration::rare);
				return false;
			}
		}
	}

	return true;
}

ACTOR Future<Void> commitBatcher(ProxyCommitData* commitData,
                                 PromiseStream<std::pair<std::vector<CommitTransactionRequest>, int>> out,
                                 FutureStream<CommitTransactionRequest> in,
                                 int desiredBytes,
                                 int64_t memBytesLimit) {
	wait(delayJittered(commitData->commitBatchInterval, TaskPriority::ProxyCommitBatcher));

	state double lastBatch = 0;

	loop {
		state Future<Void> timeout;
		state std::vector<CommitTransactionRequest> batch;
		state int batchBytes = 0;
		// TODO: Enable this assertion (currently failing with gcc)
		// static_assert(std::is_nothrow_move_constructible_v<CommitTransactionRequest>);

		if (SERVER_KNOBS->MAX_COMMIT_BATCH_INTERVAL <= 0) {
			timeout = Never();
		} else {
			timeout = delayJittered(SERVER_KNOBS->MAX_COMMIT_BATCH_INTERVAL, TaskPriority::ProxyCommitBatcher);
		}

		while (!timeout.isReady() &&
		       !(batch.size() == SERVER_KNOBS->COMMIT_TRANSACTION_BATCH_COUNT_MAX || batchBytes >= desiredBytes)) {
			choose {
				when(CommitTransactionRequest req = waitNext(in)) {
					// WARNING: this code is run at a high priority, so it needs to do as little work as possible
					int bytes = getBytes(req);

					// Drop requests if memory is under severe pressure
					if (commitData->commitBatchesMemBytesCount + bytes > memBytesLimit) {
						++commitData->stats.txnCommitErrors;
						req.reply.sendError(commit_proxy_memory_limit_exceeded());
						TraceEvent(SevWarnAlways, "ProxyCommitBatchMemoryThresholdExceeded")
						    .suppressFor(60)
						    .detail("MemBytesCount", commitData->commitBatchesMemBytesCount)
						    .detail("MemLimit", memBytesLimit);
						continue;
					}

					if (bytes > FLOW_KNOBS->PACKET_WARNING) {
						TraceEvent(SevWarn, "LargeTransaction")
						    .suppressFor(1.0)
						    .detail("Size", bytes)
						    .detail("Client", req.reply.getEndpoint().getPrimaryAddress());
					}

					if (!verifyTenantPrefix(commitData, req)) {
						++commitData->stats.txnCommitErrors;
						req.reply.sendError(illegal_tenant_access());
						continue;
					}

					if (SERVER_KNOBS->STORAGE_QUOTA_ENABLED && !req.bypassStorageQuota() &&
					    req.tenantInfo.hasTenant() &&
					    commitData->tenantsOverStorageQuota.contains(req.tenantInfo.tenantId)) {
						req.reply.sendError(storage_quota_exceeded());
						continue;
					}

					++commitData->stats.txnCommitIn;
					commitData->stats.uniqueClients.insert(req.reply.getEndpoint().getPrimaryAddress());

					if (req.debugID.present()) {
						g_traceBatch.addEvent("CommitDebug", req.debugID.get().first(), "CommitProxyServer.batcher");
					}

					if (!batch.size()) {
						if (now() - lastBatch > commitData->commitBatchInterval) {
							timeout = delayJittered(SERVER_KNOBS->COMMIT_TRANSACTION_BATCH_INTERVAL_FROM_IDLE,
							                        TaskPriority::ProxyCommitBatcher);
						} else {
							timeout = delayJittered(commitData->commitBatchInterval - (now() - lastBatch),
							                        TaskPriority::ProxyCommitBatcher);
						}
					}

					if ((batchBytes + bytes > CLIENT_KNOBS->TRANSACTION_SIZE_LIMIT || req.firstInBatch()) &&
					    batch.size()) {
						commitData->triggerCommit.set(false);
						out.send({ std::move(batch), batchBytes });
						lastBatch = now();
						timeout = delayJittered(commitData->commitBatchInterval, TaskPriority::ProxyCommitBatcher);
						batch.clear();
						batchBytes = 0;
					}

					batch.push_back(req);
					batchBytes += bytes;
					commitData->commitBatchesMemBytesCount += bytes;
				}
				when(wait(timeout)) {}
				when(wait(commitData->triggerCommit.onChange())) {
					ASSERT(commitData->triggerCommit.get());
					double commitTime = lastBatch + SERVER_KNOBS->COMMIT_TRIGGER_DELAY;
					if (now() > commitTime) {
						break;
					}

					timeout = timeout || delayJittered(commitTime - now(), TaskPriority::ProxyCommitBatcher);
				}
			}
		}
		commitData->triggerCommit.set(false);
		out.send({ std::move(batch), batchBytes });
		lastBatch = now();
	}
}

void createWhitelistBinPathVec(const std::string& binPath, std::vector<Standalone<StringRef>>& binPathVec) {
	TraceEvent(SevDebug, "BinPathConverter").detail("Input", binPath);
	StringRef input(binPath);
	while (input != StringRef()) {
		StringRef token = input.eat(","_sr);
		if (token != StringRef()) {
			const uint8_t* ptr = token.begin();
			while (ptr != token.end() && *ptr == ' ') {
				ptr++;
			}
			if (ptr != token.end()) {
				Standalone<StringRef> newElement(token.substr(ptr - token.begin()));
				TraceEvent(SevDebug, "BinPathItem").detail("Element", newElement);
				binPathVec.push_back(newElement);
			}
		}
	}
	return;
}

bool isWhitelisted(const std::vector<Standalone<StringRef>>& binPathVec, StringRef binPath) {
	TraceEvent("BinPath").detail("Value", binPath);
	for (const auto& item : binPathVec) {
		TraceEvent("Element").detail("Value", item);
	}
	return std::find(binPathVec.begin(), binPathVec.end(), binPath) != binPathVec.end();
}

ACTOR Future<Void> addBackupMutations(ProxyCommitData* self,
                                      const std::map<Key, MutationListRef>* logRangeMutations,
                                      LogPushData* toCommit,
                                      Version commitVersion,
                                      double* computeDuration,
                                      double* computeStart) {
	state std::map<Key, MutationListRef>::const_iterator logRangeMutation = logRangeMutations->cbegin();
	state int32_t version = commitVersion / CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE;
	state int yieldBytes = 0;
	state BinaryWriter valueWriter(Unversioned());

	toCommit->addTransactionInfo(SpanContext());

	// Serialize the log range mutations within the map
	for (; logRangeMutation != logRangeMutations->cend(); ++logRangeMutation) {
		// FIXME: this is re-implementing the serialize function of MutationListRef in order to have a yield
		valueWriter = BinaryWriter(IncludeVersion(ProtocolVersion::withBackupMutations()));
		valueWriter << logRangeMutation->second.totalSize();

		state MutationListRef::Blob* blobIter = logRangeMutation->second.blob_begin;
		while (blobIter) {
			if (yieldBytes > SERVER_KNOBS->DESIRED_TOTAL_BYTES) {
				yieldBytes = 0;
				if (g_network->check_yield(TaskPriority::ProxyCommitYield1)) {
					*computeDuration += g_network->timer_monotonic() - *computeStart;
					wait(delay(0, TaskPriority::ProxyCommitYield1));
					*computeStart = g_network->timer_monotonic();
				}
			}
			valueWriter.serializeBytes(blobIter->data);
			yieldBytes += blobIter->data.size();
			blobIter = blobIter->next;
		}

		Key val = valueWriter.toValue();

		BinaryWriter wr(Unversioned());

		// Serialize the log destination
		wr.serializeBytes(logRangeMutation->first);

		// Write the log keys and version information
		wr << (uint8_t)hashlittle(&version, sizeof(version), 0);
		wr << bigEndian64(commitVersion);

		uint32_t* partBuffer = nullptr;

		for (int part = 0; part * CLIENT_KNOBS->MUTATION_BLOCK_SIZE < val.size(); part++) {

			MutationRef backupMutation;
			backupMutation.type = MutationRef::SetValue;
			// Assign the second parameter as the part
			backupMutation.param2 = val.substr(
			    part * CLIENT_KNOBS->MUTATION_BLOCK_SIZE,
			    std::min(val.size() - part * CLIENT_KNOBS->MUTATION_BLOCK_SIZE, CLIENT_KNOBS->MUTATION_BLOCK_SIZE));

			// Write the last part of the mutation to the serialization, if the buffer is not defined
			if (!partBuffer) {
				// Serialize the part to the writer
				wr << bigEndian32(part);

				// Define the last buffer part
				partBuffer = (uint32_t*)((char*)wr.getData() + wr.getLength() - sizeof(uint32_t));
			} else {
				*partBuffer = bigEndian32(part);
			}

			// Define the mutation type and and location
			backupMutation.param1 = wr.toValue();
			ASSERT(backupMutation.param1.startsWith(
			    logRangeMutation->first)); // We are writing into the configured destination

			auto& tags = self->tagsForKey(backupMutation.param1);
			toCommit->addTags(tags);

			if (self->acsBuilder != nullptr) {
				updateMutationWithAcsAndAddMutationToAcsBuilder(
				    self->acsBuilder,
				    backupMutation,
				    tags,
				    getCommitProxyAccumulativeChecksumIndex(self->commitProxyIndex),
				    self->epoch,
				    commitVersion,
				    self->dbgid);
			}

			toCommit->writeTypedMessage(backupMutation);

			//			if (DEBUG_MUTATION("BackupProxyCommit", commitVersion, backupMutation)) {
			//				TraceEvent("BackupProxyCommitTo", self->dbgid).detail("To",
			// describe(tags)).detail("BackupMutation", backupMutation.toString())
			// .detail("BackupMutationSize", val.size()).detail("Version", commitVersion).detail("DestPath",
			// logRangeMutation.first) 					.detail("PartIndex", part).detail("PartIndexEndian",
			// bigEndian32(part)).detail("PartData", backupMutation.param1);
			//			}
		}
	}
	return Void();
}

ACTOR Future<Void> releaseResolvingAfter(ProxyCommitData* self, Future<Void> releaseDelay, int64_t localBatchNumber) {
	wait(releaseDelay);
	ASSERT(self->latestLocalCommitBatchResolving.get() == localBatchNumber - 1);
	self->latestLocalCommitBatchResolving.set(localBatchNumber);
	return Void();
}

ACTOR static Future<ResolveTransactionBatchReply> trackResolutionMetrics(Reference<Histogram> dist,
                                                                         Future<ResolveTransactionBatchReply> in) {
	state double startTime = g_network->timer_monotonic();
	ResolveTransactionBatchReply reply = wait(in);
	dist->sampleSeconds(g_network->timer_monotonic() - startTime);
	return reply;
}

namespace CommitBatch {

constexpr const std::string_view UNSET = std::string_view();
constexpr const std::string_view INITIALIZE = "initialize"sv;
constexpr const std::string_view PRE_RESOLUTION = "preResolution"sv;
constexpr const std::string_view RESOLUTION = "resolution"sv;
constexpr const std::string_view POST_RESOLUTION = "postResolution"sv;
constexpr const std::string_view TRANSACTION_LOGGING = "transactionLogging"sv;
constexpr const std::string_view REPLY = "reply"sv;
constexpr const std::string_view COMPLETE = "complete"sv;

struct CommitBatchContext {
	using StoreCommit_t = std::vector<std::pair<Future<LogSystemDiskQueueAdapter::CommitMessage>, Future<Void>>>;

	ProxyCommitData* const pProxyCommitData;
	std::vector<CommitTransactionRequest> trs;
	const int currentBatchMemBytesCount;

	double startTime;

	// The current stage of batch commit
	std::string_view stage = UNSET;

	// If encryption is enabled this value represents the total time (in nanoseconds) that was spent on encryption in
	// the commit proxy for a given Commit Batch
	Optional<double> encryptionTime;

	Optional<UID> debugID;

	bool forceRecovery = false;
	bool rejected = false; // If rejected due to long queue length

	int64_t localBatchNumber;
	LogPushData toCommit;

	int batchOperations = 0;

	Span span;

	int64_t batchBytes = 0;

	int latencyBucket = 0;

	Version commitVersion;
	Version prevVersion;

	int64_t maxTransactionBytes;
	std::vector<std::vector<int>> transactionResolverMap;
	std::vector<std::vector<std::vector<int>>> txReadConflictRangeIndexMap;

	Future<Void> releaseDelay;
	Future<Void> releaseFuture;

	std::vector<ResolveTransactionBatchReply> resolution;

	double computeStart;
	double computeDuration = 0;

	Arena arena;

	/// true if the batch is the 1st batch for this proxy, additional metadata
	/// processing is involved for this batch.
	bool isMyFirstBatch;
	bool firstStateMutations;

	Optional<Value> previousCoordinators;

	StoreCommit_t storeCommits;

	std::vector<uint8_t> committed;

	Optional<Key> lockedKey;
	bool locked;

	int commitCount = 0;

	std::vector<int> nextTr;

	bool lockedAfter;

	Optional<Value> metadataVersionAfter;

	int mutationCount = 0;
	int mutationBytes = 0;

	std::map<Key, MutationListRef> logRangeMutations;
	Arena logRangeMutationsArena;

	int transactionNum = 0;
	int yieldBytes = 0;

	LogSystemDiskQueueAdapter::CommitMessage msg;

	Future<Version> loggingComplete;

	double commitStartTime;

	std::unordered_map<uint16_t, Version> tpcvMap; // obtained from resolver
	std::set<Tag> writtenTags; // final set tags written to in the batch
	std::set<Tag> writtenTagsPreResolution; // tags written to in the batch not including any changes from the resolver.

	// Cipher keys to be used to encrypt mutations
	std::unordered_map<EncryptCipherDomainId, Reference<BlobCipherKey>> cipherKeys;

	IdempotencyIdKVBuilder idempotencyKVBuilder;

	CommitBatchContext(ProxyCommitData*, const std::vector<CommitTransactionRequest>*, const int);

	void setupTraceBatch();

	std::set<Tag> getWrittenTagsPreResolution();

	void checkHotShards();

	bool rangeLockEnabled();

private:
	void evaluateBatchSize();
};

bool CommitBatchContext::rangeLockEnabled() {
	return pProxyCommitData->rangeLockEnabled();
}

void CommitBatchContext::checkHotShards() {
	// removed expired hot shards
	for (auto it = pProxyCommitData->hotShards.begin(); it != pProxyCommitData->hotShards.end();) {
		if (now() > it->second) {
			it = pProxyCommitData->hotShards.erase(it);
		} else {
			++it;
		}
	}

	if (pProxyCommitData->hotShards.empty()) {
		return;
	}

	auto trsBegin = trs.begin();

	std::vector<size_t> transactionsToRemove;
	for (int transactionNum = 0; transactionNum < trs.size(); transactionNum++) {
		VectorRef<MutationRef>* pMutations = &trs[transactionNum].transaction.mutations;
		bool abortTransaction = false;
		for (int mutationNum = 0; mutationNum < pMutations->size(); mutationNum++) {
			auto& m = (*pMutations)[mutationNum];
			if (isSingleKeyMutation((MutationRef::Type)m.type)) {
				for (const auto& shard : pProxyCommitData->hotShards) {
					if (shard.first.contains(KeyRef(m.param1))) {
						abortTransaction = true;
						break;
					}
				}
			} else if (m.type == MutationRef::ClearRange) {
				for (const auto& shard : pProxyCommitData->hotShards) {
					if (shard.first.intersects(KeyRangeRef(m.param1, m.param2))) {
						abortTransaction = true;
						break;
					}
				}
			} else {
				UNREACHABLE();
			}
		}
		if (abortTransaction) {
			trs[transactionNum].reply.sendError(transaction_throttled_hot_shard());
			transactionsToRemove.push_back(transactionNum);
		}
	}
	// Remove transactions marked for removal in reverse order to avoid shifting indices
	for (auto it = transactionsToRemove.rbegin(); it != transactionsToRemove.rend(); ++it) {
		trs.erase(trsBegin + *it);
	}
	committed.resize(trs.size());
	return;
}

// Check whether the mutation intersects any legal backup ranges
// If so, it will be clamped to the intersecting range(s) later
inline bool shouldBackup(MutationRef const& m) {
	if (normalKeys.contains(m.param1) || m.param1 == metadataVersionKey) {
		return true;
	} else if (m.type != MutationRef::Type::ClearRange) {
		return systemBackupMutationMask().rangeContaining(m.param1).value();
	} else {
		for (auto& r : systemBackupMutationMask().intersectingRanges(KeyRangeRef(m.param1, m.param2))) {
			if (r->value()) {
				return true;
			}
		}
	}
	return false;
}

std::set<Tag> CommitBatchContext::getWrittenTagsPreResolution() {
	std::set<Tag> transactionTags;
	std::vector<Tag> cacheVector = { cacheTag };
	if (pProxyCommitData->txnStateStore->getReplaceContent()) {
		// return empty set if txnStateStore will snapshot.
		// empty sets are sent to all logs.
		return transactionTags;
	}
	for (int transactionNum = 0; transactionNum < trs.size(); transactionNum++) {
		int mutationNum = 0;
		VectorRef<MutationRef>* pMutations = &trs[transactionNum].transaction.mutations;
		for (; mutationNum < pMutations->size(); mutationNum++) {
			auto& m = (*pMutations)[mutationNum];
			// disable version vector's effect if any mutation in the batch is backed up.
			// TODO: make backup work with version vector.
			if (pProxyCommitData->vecBackupKeys.size() > 1 && shouldBackup(m)) {
				return std::set<Tag>();
			}
			if (isSingleKeyMutation((MutationRef::Type)m.type)) {
				auto& tags = pProxyCommitData->tagsForKey(m.param1);
				transactionTags.insert(tags.begin(), tags.end());
				if (pProxyCommitData->cacheInfo[m.param1]) {
					transactionTags.insert(cacheTag);
				}
			} else if (m.type == MutationRef::ClearRange) {
				KeyRangeRef clearRange(KeyRangeRef(m.param1, m.param2));
				auto ranges = pProxyCommitData->keyInfo.intersectingRanges(clearRange);
				auto firstRange = ranges.begin();
				++firstRange;
				if (firstRange == ranges.end()) {
					std::set<Tag> filteredTags;
					ranges.begin().value().populateTags();
					filteredTags.insert(ranges.begin().value().tags.begin(), ranges.begin().value().tags.end());
					transactionTags.insert(ranges.begin().value().tags.begin(), ranges.begin().value().tags.end());
				} else {
					std::set<Tag> allSources;
					for (auto r : ranges) {
						r.value().populateTags();
						allSources.insert(r.value().tags.begin(), r.value().tags.end());
						transactionTags.insert(r.value().tags.begin(), r.value().tags.end());
					}
				}
				if (pProxyCommitData->needsCacheTag(clearRange)) {
					transactionTags.insert(cacheTag);
				}
			} else {
				UNREACHABLE();
			}
		}
	}

	if (toCommit.getLogRouterTags()) {
		toCommit.storeRandomRouterTag();
		transactionTags.insert(toCommit.savedRandomRouterTag.get());
	}

	return transactionTags;
}

CommitBatchContext::CommitBatchContext(ProxyCommitData* const pProxyCommitData_,
                                       const std::vector<CommitTransactionRequest>* trs_,
                                       const int currentBatchMemBytesCount)
  : pProxyCommitData(pProxyCommitData_), trs(std::move(*const_cast<std::vector<CommitTransactionRequest>*>(trs_))),
    currentBatchMemBytesCount(currentBatchMemBytesCount), startTime(g_network->now()),
    localBatchNumber(++pProxyCommitData->localCommitBatchesStarted),
    toCommit(pProxyCommitData->logSystem, pProxyCommitData->localTLogCount), span("MP:commitBatch"_loc),
    committed(trs.size()) {

	evaluateBatchSize();

	if (batchOperations != 0) {
		latencyBucket =
		    std::min<int>(SERVER_KNOBS->PROXY_COMPUTE_BUCKETS - 1,
		                  SERVER_KNOBS->PROXY_COMPUTE_BUCKETS * batchBytes /
		                      (batchOperations * (CLIENT_KNOBS->VALUE_SIZE_LIMIT + CLIENT_KNOBS->KEY_SIZE_LIMIT)));
	}

	// since we are using just the former to limit the number of versions actually in flight!
	ASSERT(SERVER_KNOBS->MAX_READ_TRANSACTION_LIFE_VERSIONS <= SERVER_KNOBS->MAX_VERSIONS_IN_FLIGHT);
}

void CommitBatchContext::setupTraceBatch() {
	for (const auto& tr : trs) {
		if (tr.debugID.present()) {
			if (!debugID.present()) {
				debugID = nondeterministicRandom()->randomUniqueID();
			}

			g_traceBatch.addAttach("CommitAttachID", tr.debugID.get().first(), debugID.get().first());
		}
		span.addLink(tr.spanContext);
	}

	if (debugID.present()) {
		g_traceBatch.addEvent("CommitDebug", debugID.get().first(), "CommitProxyServer.commitBatch.Before");
	}
}

void CommitBatchContext::evaluateBatchSize() {
	for (const auto& tr : trs) {
		const auto& mutations = tr.transaction.mutations;
		batchOperations += mutations.size();
		batchBytes += mutations.expectedSize();
	}
}

// Try to identify recovery transaction and backup's apply mutations (blind writes).
// Both cannot be rejected and are approximated by looking at first mutation
// starting with 0xff.
bool canReject(const std::vector<CommitTransactionRequest>& trs) {
	for (const auto& tr : trs) {
		if (tr.transaction.mutations.empty())
			continue;
		if (!tr.tenantInfo.hasTenant() &&
		    (tr.transaction.mutations[0].param1.startsWith("\xff"_sr) || tr.transaction.read_conflict_ranges.empty())) {
			return false;
		}
	}
	return true;
}

double computeReleaseDelay(CommitBatchContext* self, double latencyBucket) {
	return std::min(SERVER_KNOBS->MAX_PROXY_COMPUTE,
	                self->batchOperations * self->pProxyCommitData->commitComputePerOperation[latencyBucket]);
}

ACTOR Future<Void> preresolutionProcessing(CommitBatchContext* self) {

	state ProxyCommitData* const pProxyCommitData = self->pProxyCommitData;
	state std::vector<CommitTransactionRequest>& trs = self->trs;
	state const int64_t localBatchNumber = self->localBatchNumber;
	state const int latencyBucket = self->latencyBucket;
	state const Optional<UID>& debugID = self->debugID;
	state Span span("MP:preresolutionProcessing"_loc, self->span.context);
	state double startTime = g_network->timer_monotonic();

	if (self->localBatchNumber - self->pProxyCommitData->latestLocalCommitBatchResolving.get() >
	        SERVER_KNOBS->RESET_MASTER_BATCHES &&
	    now() - self->pProxyCommitData->lastMasterReset > SERVER_KNOBS->RESET_MASTER_DELAY) {
		TraceEvent(SevWarnAlways, "ResetMasterNetwork", self->pProxyCommitData->dbgid)
		    .detail("CurrentBatch", self->localBatchNumber)
		    .detail("InProcessBatch", self->pProxyCommitData->latestLocalCommitBatchResolving.get());
		FlowTransport::transport().resetConnection(self->pProxyCommitData->master.address());
		self->pProxyCommitData->lastMasterReset = now();
	}

	// Pre-resolution the commits
	CODE_PROBE(pProxyCommitData->latestLocalCommitBatchResolving.get() < localBatchNumber - 1, "Wait for local batch");
	wait(pProxyCommitData->latestLocalCommitBatchResolving.whenAtLeast(localBatchNumber - 1));
	double queuingDelay = g_network->timer_monotonic() - startTime;
	pProxyCommitData->stats.computeLatency.addMeasurement(queuingDelay);
	pProxyCommitData->stats.commitBatchQueuingDist->sampleSeconds(queuingDelay);
	if ((queuingDelay > (double)SERVER_KNOBS->MAX_READ_TRANSACTION_LIFE_VERSIONS / SERVER_KNOBS->VERSIONS_PER_SECOND ||
	     (g_network->isSimulated() && BUGGIFY_WITH_PROB(0.01))) &&
	    SERVER_KNOBS->PROXY_REJECT_BATCH_QUEUED_TOO_LONG && canReject(trs)) {
		// Disabled for the recovery transaction. otherwise, recovery can't finish and keeps doing more recoveries.
		CODE_PROBE(true, "Reject transactions in the batch");
		TraceEvent(g_network->isSimulated() ? SevInfo : SevWarnAlways, "ProxyReject", pProxyCommitData->dbgid)
		    .suppressFor(0.1)
		    .detail("QDelay", queuingDelay)
		    .detail("Transactions", trs.size())
		    .detail("BatchNumber", localBatchNumber);
		ASSERT(pProxyCommitData->latestLocalCommitBatchResolving.get() == localBatchNumber - 1);
		pProxyCommitData->latestLocalCommitBatchResolving.set(localBatchNumber);

		wait(pProxyCommitData->latestLocalCommitBatchLogging.whenAtLeast(localBatchNumber - 1));
		ASSERT(pProxyCommitData->latestLocalCommitBatchLogging.get() == localBatchNumber - 1);
		pProxyCommitData->latestLocalCommitBatchLogging.set(localBatchNumber);
		for (const auto& tr : trs) {
			tr.reply.sendError(transaction_too_old());
		}
		++pProxyCommitData->stats.commitBatchOut;
		pProxyCommitData->stats.txnCommitOut += trs.size();
		pProxyCommitData->stats.txnRejectedForQueuedTooLong += trs.size();
		self->rejected = true;
		return Void();
	}

	self->releaseDelay = delay(computeReleaseDelay(self, latencyBucket), TaskPriority::ProxyMasterVersionReply);

	if (debugID.present()) {
		g_traceBatch.addEvent(
		    "CommitDebug", debugID.get().first(), "CommitProxyServer.commitBatch.GettingCommitVersion");
	}

	if (SERVER_KNOBS->ENABLE_VERSION_VECTOR_TLOG_UNICAST) {
		self->writtenTagsPreResolution = self->getWrittenTagsPreResolution();
	}

	if (SERVER_KNOBS->HOT_SHARD_THROTTLING_ENABLED && !pProxyCommitData->hotShards.empty()) {
		self->checkHotShards();
	}

	GetCommitVersionRequest req(span.context,
	                            pProxyCommitData->commitVersionRequestNumber++,
	                            pProxyCommitData->mostRecentProcessedRequestNumber,
	                            pProxyCommitData->dbgid);
	state double beforeGettingCommitVersion = g_network->timer_monotonic();
	GetCommitVersionReply versionReply = wait(brokenPromiseToNever(
	    pProxyCommitData->master.getCommitVersion.getReply(req, TaskPriority::ProxyMasterVersionReply)));

	pProxyCommitData->mostRecentProcessedRequestNumber = versionReply.requestNum;

	pProxyCommitData->stats.txnCommitVersionAssigned += trs.size();
	pProxyCommitData->stats.lastCommitVersionAssigned = versionReply.version;
	pProxyCommitData->stats.getCommitVersionDist->sampleSeconds(g_network->timer_monotonic() -
	                                                            beforeGettingCommitVersion);

	self->commitVersion = versionReply.version;
	self->prevVersion = versionReply.prevVersion;

	//TraceEvent("CPGetVersion", pProxyCommitData->dbgid).detail("Master", pProxyCommitData->master.id().toString()).detail("CommitVersion", self->commitVersion).detail("PrvVersion", self->prevVersion);

	for (auto it : versionReply.resolverChanges) {
		auto rs = pProxyCommitData->keyResolvers.modify(it.range);
		for (auto r = rs.begin(); r != rs.end(); ++r)
			r->value().emplace_back(versionReply.resolverChangesVersion, it.dest);
	}

	//TraceEvent("ProxyGotVer", pProxyContext->dbgid).detail("Commit", commitVersion).detail("Prev", prevVersion);

	if (debugID.present()) {
		g_traceBatch.addEvent("CommitDebug", debugID.get().first(), "CommitProxyServer.commitBatch.GotCommitVersion");
	}

	return Void();
}

namespace {
EncryptCipherDomainId getEncryptDetailsFromMutationRef(ProxyCommitData* commitData, MutationRef m) {
	EncryptCipherDomainId domainId = INVALID_ENCRYPT_DOMAIN_ID;

	// Possible scenarios:
	// 1. Encryption domain (Tenant details) weren't explicitly provided, extract Tenant details using
	// TenantPrefix (first 8 bytes of FDBKey)
	// 2. Encryption domain isn't available, leverage 'default encryption domain'

	if (isSystemKey(m.param1)) {
		// Encryption domain == FDB SystemKeyspace encryption domain
		domainId = SYSTEM_KEYSPACE_ENCRYPT_DOMAIN_ID;
	} else if (commitData->tenantMap.empty() || commitData->encryptMode.mode == EncryptionAtRestMode::CLUSTER_AWARE) {
		// Cluster serves no-tenants; use 'default encryption domain'
	} else if (isSingleKeyMutation((MutationRef::Type)m.type)) {
		ASSERT_NE((MutationRef::Type)m.type, MutationRef::Type::ClearRange);

		if (m.param1.size() >= TenantAPI::PREFIX_SIZE) {
			// Parse mutation key to determine mutation encryption domain
			StringRef prefix = m.param1.substr(0, TenantAPI::PREFIX_SIZE);
			int64_t tenantId = TenantAPI::prefixToId(prefix, EnforceValidTenantId::False);
			if (commitData->tenantMap.contains(tenantId)) {
				domainId = tenantId;
			} else {
				// Leverage 'default encryption domain'
			}
		}
	} else {
		// ClearRange is the 'only' MultiKey transaction allowed
		ASSERT_EQ((MutationRef::Type)m.type, MutationRef::Type::ClearRange);

		// FIXME: Handle Clear-range transaction, actions needed:
		// 1. Transaction range can spawn multiple encryption domains (tenants)
		// 2. Transaction can be a multi-key transaction spawning multiple tenants
		// For now fallback to 'default encryption domain'

		CODE_PROBE(true, "ClearRange mutation encryption", probe::decoration::rare);
	}

	// Unknown tenant, fallback to fdb default encryption domain
	if (domainId == INVALID_ENCRYPT_DOMAIN_ID) {
		domainId = FDB_DEFAULT_ENCRYPT_DOMAIN_ID;

		CODE_PROBE(true, "Default domain mutation encryption", probe::decoration::rare);
	}

	return domainId;
}

} // namespace

ACTOR Future<Void> getResolution(CommitBatchContext* self) {
	state double resolutionStart = g_network->timer_monotonic();
	// Sending these requests is the fuzzy border between phase 1 and phase 2; it could conceivably overlap with
	// resolution processing but is still using CPU
	state ProxyCommitData* pProxyCommitData = self->pProxyCommitData;
	std::vector<CommitTransactionRequest>& trs = self->trs;
	state Span span("MP:getResolution"_loc, self->span.context);

	ResolutionRequestBuilder requests(
	    pProxyCommitData, self->commitVersion, self->prevVersion, pProxyCommitData->version.get(), span);
	int conflictRangeCount = 0;
	self->maxTransactionBytes = 0;
	for (int t = 0; t < trs.size(); t++) {
		requests.addTransaction(trs[t], self->commitVersion, t);
		conflictRangeCount +=
		    trs[t].transaction.read_conflict_ranges.size() + trs[t].transaction.write_conflict_ranges.size();
		//TraceEvent("MPTransactionDump", self->dbgid).detail("Snapshot", trs[t].transaction.read_snapshot);
		// for(auto& m : trs[t].transaction.mutations)
		self->maxTransactionBytes = std::max<int64_t>(self->maxTransactionBytes, trs[t].transaction.expectedSize());
		//	TraceEvent("MPTransactionsDump", self->dbgid).detail("Mutation", m.toString());
	}
	pProxyCommitData->stats.conflictRanges += conflictRangeCount;

	for (int r = 1; r < pProxyCommitData->resolvers.size(); r++)
		ASSERT(requests.requests[r].txnStateTransactions.size() == requests.requests[0].txnStateTransactions.size());

	pProxyCommitData->stats.txnCommitResolving += trs.size();
	std::vector<Future<ResolveTransactionBatchReply>> replies;
	for (int r = 0; r < pProxyCommitData->resolvers.size(); r++) {
		requests.requests[r].debugID = self->debugID;
		requests.requests[r].writtenTags = self->writtenTagsPreResolution;
		replies.push_back(trackResolutionMetrics(pProxyCommitData->stats.resolverDist[r],
		                                         brokenPromiseToNever(pProxyCommitData->resolvers[r].resolve.getReply(
		                                             requests.requests[r], TaskPriority::ProxyResolverReply))));
	}

	self->transactionResolverMap.swap(requests.transactionResolverMap);
	// Used to report conflicting keys
	self->txReadConflictRangeIndexMap.swap(requests.txReadConflictRangeIndexMap);

	// Fetch cipher keys if needed.
	state Future<std::unordered_map<EncryptCipherDomainId, Reference<BlobCipherKey>>> getCipherKeys;
	if (pProxyCommitData->encryptMode.isEncryptionEnabled()) {
		std::unordered_set<EncryptCipherDomainId> encryptDomainIds = { SYSTEM_KEYSPACE_ENCRYPT_DOMAIN_ID,
			                                                           FDB_DEFAULT_ENCRYPT_DOMAIN_ID };
		if (FLOW_KNOBS->ENCRYPT_HEADER_AUTH_TOKEN_ENABLED) {
			encryptDomainIds.insert(ENCRYPT_HEADER_DOMAIN_ID);
		}
		// For cluster aware encryption only the default domain id is needed
		if (pProxyCommitData->encryptMode.mode == EncryptionAtRestMode::DOMAIN_AWARE) {
			for (int t = 0; t < trs.size(); t++) {
				TenantInfo const& tenantInfo = trs[t].tenantInfo;
				int64_t tenantId = tenantInfo.tenantId;
				if (tenantId != TenantInfo::INVALID_TENANT) {
					encryptDomainIds.emplace(tenantId);
				}
			}
		}
		getCipherKeys = GetEncryptCipherKeys<ServerDBInfo>::getLatestEncryptCipherKeys(
		    pProxyCommitData->db, encryptDomainIds, BlobCipherMetrics::TLOG, pProxyCommitData->encryptionMonitor);
	}

	self->releaseFuture = releaseResolvingAfter(pProxyCommitData, self->releaseDelay, self->localBatchNumber);

	if (self->localBatchNumber - self->pProxyCommitData->latestLocalCommitBatchLogging.get() >
	        SERVER_KNOBS->RESET_RESOLVER_BATCHES &&
	    now() - self->pProxyCommitData->lastResolverReset > SERVER_KNOBS->RESET_RESOLVER_DELAY) {
		for (int r = 0; r < self->pProxyCommitData->resolvers.size(); r++) {
			TraceEvent(SevWarnAlways, "ResetResolverNetwork", self->pProxyCommitData->dbgid)
			    .detail("PeerAddr", self->pProxyCommitData->resolvers[r].address())
			    .detail("PeerAddress", self->pProxyCommitData->resolvers[r].address())
			    .detail("CurrentBatch", self->localBatchNumber)
			    .detail("InProcessBatch", self->pProxyCommitData->latestLocalCommitBatchLogging.get());
			FlowTransport::transport().resetConnection(self->pProxyCommitData->resolvers[r].address());
		}
		self->pProxyCommitData->lastResolverReset = now();
	}

	// Wait for the final resolution
	std::vector<ResolveTransactionBatchReply> resolutionResp = wait(getAll(replies));
	self->resolution.swap(*const_cast<std::vector<ResolveTransactionBatchReply>*>(&resolutionResp));

	self->pProxyCommitData->stats.resolutionDist->sampleSeconds(g_network->timer_monotonic() - resolutionStart);
	if (self->debugID.present()) {
		g_traceBatch.addEvent(
		    "CommitDebug", self->debugID.get().first(), "CommitProxyServer.commitBatch.AfterResolution");
	}
	if (pProxyCommitData->encryptMode.isEncryptionEnabled()) {
		std::unordered_map<EncryptCipherDomainId, Reference<BlobCipherKey>> cipherKeys = wait(getCipherKeys);
		self->cipherKeys = cipherKeys;
	}

	return Void();
}

void assertResolutionStateMutationsSizeConsistent(const std::vector<ResolveTransactionBatchReply>& resolution) {
	for (int r = 1; r < resolution.size(); r++) {
		ASSERT(resolution[r].stateMutations.size() == resolution[0].stateMutations.size());
		for (int s = 0; s < resolution[r].stateMutations.size(); s++) {
			ASSERT(resolution[r].stateMutations[s].size() == resolution[0].stateMutations[s].size());
		}
	}
}

// Return true if a single-key mutation is associated with a valid tenant id or a system key
bool validTenantAccess(MutationRef m, std::map<int64_t, TenantName> const& tenantMap, Optional<int64_t>& tenantId) {
	if (isSingleKeyMutation((MutationRef::Type)m.type)) {
		tenantId = TenantAPI::extractTenantIdFromMutation(m);
		bool isLegalTenant = tenantMap.contains(tenantId.get());
		CODE_PROBE(!isLegalTenant, "Commit proxy access invalid tenant");
		return isLegalTenant;
	}
	return true;
}

// return an iterator to the first tenantId whose idToPrefix(id) >= prefix[0..8] in lexicographic order. If no such id,
// return tenantMap.end()
inline auto lowerBoundTenantId(const StringRef& prefix, const std::map<int64_t, TenantName>& tenantMap) {
	Optional<int64_t> id = TenantIdCodec::lowerBound(prefix.substr(0, std::min(prefix.size(), TenantAPI::PREFIX_SIZE)));
	return id.present() ? tenantMap.lower_bound(id.get()) : tenantMap.end();
}

TEST_CASE("/CommitProxy/SplitRange/LowerBoundTenantId") {
	int mapSize = 1000;
	std::map<int64_t, TenantName> tenantMap;
	for (int i = 0; i < mapSize; ++i) {
		tenantMap[i * 2] = ""_sr;
	}

	int64_t tid = lowerBoundTenantId(""_sr, tenantMap)->first;
	ASSERT_EQ(tid, 0);

	auto it = lowerBoundTenantId("\xff"_sr, tenantMap);
	ASSERT(it == tenantMap.end());

	it = lowerBoundTenantId("\xff\x01\x02\x03\x04\x05\x06\x07\x08"_sr, tenantMap);
	ASSERT(it == tenantMap.end());

	it = lowerBoundTenantId("\x99\x01\x02\x03\x04\x05\x06\x07\x08"_sr, tenantMap);
	ASSERT(it == tenantMap.end());

	int64_t targetId = deterministicRandom()->randomInt64(0, mapSize) * 2;
	Key prefix = TenantAPI::idToPrefix(targetId);
	tid = lowerBoundTenantId(prefix, tenantMap)->first;
	ASSERT_EQ(tid, targetId);

	tid = lowerBoundTenantId(prefix.withSuffix("any"_sr), tenantMap)->first;
	ASSERT_EQ(tid, targetId);

	targetId = deterministicRandom()->randomInt64(1, mapSize) * 2;
	prefix = TenantAPI::idToPrefix(targetId - 1);
	tid = lowerBoundTenantId(prefix, tenantMap)->first;
	ASSERT_EQ(tid, targetId);

	targetId = deterministicRandom()->randomInt64(mapSize * 2, mapSize * 3);
	prefix = TenantAPI::idToPrefix(targetId);
	it = lowerBoundTenantId(prefix, tenantMap);
	ASSERT(it == tenantMap.end());

	targetId = deterministicRandom()->randomInt64((int64_t)1 << 32, std::numeric_limits<int64_t>::max());
	tenantMap[targetId] = ""_sr;
	prefix = TenantAPI::idToPrefix(targetId);
	int shift = deterministicRandom()->randomInt(0, TenantAPI::PREFIX_SIZE / 2);
	prefix = prefix.substr(0, TenantAPI::PREFIX_SIZE - shift);
	tid = lowerBoundTenantId(prefix, tenantMap)->first;
	ASSERT_EQ(tid, targetId);

	return Void();
}

// Given a clear range [a, b), make a vector of clear range mutations split by tenant boundary [a, t0_end), [t1_begin,
// t1_end), ... [tn_begin, b); The references are allocated on arena;
std::vector<MutationRef> splitClearRangeByTenant(Arena& arena,
                                                 const MutationRef& mutation,
                                                 const std::map<int64_t, TenantName>& tenantMap,
                                                 std::vector<int64_t>* tenantIds = nullptr) {
	std::vector<MutationRef> results;
	auto it = lowerBoundTenantId(mutation.param1, tenantMap);
	while (it != tenantMap.end()) {
		if (tenantIds != nullptr) {
			tenantIds->push_back(it->first);
		}
		KeyRef tPrefix = TenantAPI::idToPrefix(arena, it->first);
		if (tPrefix >= mutation.param2) {
			break;
		}

		// max(tenant_begin, range begin)
		KeyRef param1 = tPrefix >= mutation.param1 ? tPrefix : mutation.param1;

		// min(tenant end, range end)
		KeyRef param2 = strinc(tPrefix, arena);
		if (param2 >= mutation.param2) {
			param2 = mutation.param2;
			results.emplace_back(MutationRef::ClearRange, param1, param2);
			break;
		}
		results.emplace_back(MutationRef::ClearRange, param1, param2);
		++it;
	}

	if (KeyRangeRef(mutation.param1, mutation.param2).intersects(systemKeys)) {
		results.emplace_back(MutationRef::ClearRange,
		                     std::max(mutation.param1, systemKeys.begin),
		                     std::min(mutation.param2, systemKeys.end));
	}

	return results;
}

TEST_CASE("/CommitProxy/SplitRange/SplitClearRangeByTenant") {
	int mapSize = 1000;
	std::map<int64_t, TenantName> tenantMap;
	for (int i = 0; i < mapSize; ++i) {
		tenantMap[i * 2] = ""_sr;
	}

	// single tenant
	Arena arena(15 << 10);
	int64_t tenantId = deterministicRandom()->randomInt64(0, mapSize) * 2;
	KeyRef prefix = TenantAPI::idToPrefix(arena, tenantId);
	KeyRef param1 = prefix.withSuffix("a"_sr, arena);
	KeyRef param2 = prefix.withSuffix("b"_sr, arena);
	MutationRef mutation(MutationRef::ClearRange, param1, param2);
	std::vector<MutationRef> result = splitClearRangeByTenant(arena, mutation, tenantMap);
	ASSERT_EQ(result.size(), 1);
	ASSERT(result.front().param1 == param1);
	ASSERT(result.front().param2 == param2);

	// multiple tenant
	int64_t tid1 = deterministicRandom()->randomInt64(0, mapSize - 2);
	int64_t tid2 = deterministicRandom()->randomInt64(tid1 + 2, mapSize) * 2;
	tid1 *= 2;
	KeyRef prefix1 = TenantAPI::idToPrefix(arena, tid1);
	param1 = deterministicRandom()->coinflip() ? prefix1 : prefix1.withSuffix("a"_sr, arena); // align or not
	KeyRef prefix2 = TenantAPI::idToPrefix(arena, tid2);
	bool tailAligned = deterministicRandom()->coinflip();
	param2 = tailAligned ? prefix2 : prefix2.withSuffix("b"_sr, arena);
	int targetSize = (tid2 - tid1) / 2 + (!tailAligned);
	mutation.param1 = param1;
	mutation.param2 = param2;
	result = splitClearRangeByTenant(arena, mutation, tenantMap);
	ASSERT_EQ(result.size(), targetSize);
	ASSERT(result.front().param1 == param1);
	if (tailAligned) {
		KeyRange r = prefixRange(TenantAPI::idToPrefix(tid2 - 2));
		ASSERT(r == KeyRangeRef(result.back().param1, result.back().param2));
	} else {
		ASSERT(result.back().param1 == prefix2);
		ASSERT(result.back().param2 == param2);
	}

	// with system keys
	targetSize = mapSize - tid1 / 2 + 1;
	Key randomSysKey = systemKeys.begin.withSuffix("sf"_sr, arena);
	mutation.param2 = randomSysKey;
	result = splitClearRangeByTenant(arena, mutation, tenantMap);
	ASSERT_EQ(result.size(), targetSize);
	ASSERT(result.back().param1 == systemKeys.begin);
	ASSERT(result.back().param2 == randomSysKey);

	// within system keys
	Key sysKey1 = systemKeys.begin.withSuffix("a"_sr, arena);
	mutation.param1 = sysKey1;
	result = splitClearRangeByTenant(arena, mutation, tenantMap);
	ASSERT_EQ(result.size(), 1);
	ASSERT(result.front().param1 == sysKey1);
	ASSERT(result.front().param2 == randomSysKey);

	// empty tenant map
	tenantMap.clear();
	mutation.param1 = prefix.withSuffix("a"_sr, arena);
	mutation.param2 = prefix.withSuffix("b"_sr, arena);
	result = splitClearRangeByTenant(arena, mutation, tenantMap);
	ASSERT(result.empty());
	return Void();
}

// If the splitMutations is not empty, which means some clear range in mutations are split into multiple clear range
// ops. Modify mutations by replace the old clear range with the split clear ranges
void replaceRawClearRanges(Arena& arena,
                           VectorRef<MutationRef>& mutations,
                           std::vector<std::pair<int, std::vector<MutationRef>>>& splitMutations,
                           size_t totalSize,
                           Optional<UID> debugId = Optional<UID>()) {
	if (splitMutations.empty())
		return;

	int i = mutations.size() - 1;
	mutations.resize(arena, totalSize);
	// place from back
	int curr = totalSize - 1;
	for (; i >= 0; --i) {
		if (splitMutations.empty()) {
			ASSERT_EQ(curr, i);
			break;
		}

		if (splitMutations.back().first == i) {
			ASSERT_EQ(mutations[i].type, MutationRef::ClearRange);
			// replace with tenant aligned mutations
			auto& currMutations = splitMutations.back().second;
			while (!currMutations.empty()) {
				mutations[curr] = currMutations.back();
				currMutations.pop_back();
				curr--;
			}
			splitMutations.pop_back();
		} else {
			ASSERT_GT(curr, i);
			mutations[curr] = mutations[i];
			curr--;
		}
	}

	ASSERT_EQ(splitMutations.size(), 0);
}

// split clear range mutation according to tenantMap. If the original mutation is split to multiple mutations, push the
// mutation offset and the split ones into idxSplitMutations
size_t processClearRangeMutation(Arena& arena,
                                 const std::map<int64_t, TenantName>& tenantMap,
                                 MutationRef& mutation,
                                 int mutationIdx,
                                 int& newMutationSize,
                                 std::vector<std::pair<int, std::vector<MutationRef>>>& idxSplitMutations,
                                 std::vector<int64_t>* tenantIds = nullptr) {
	std::vector<MutationRef> newClears = splitClearRangeByTenant(arena, mutation, tenantMap, tenantIds);
	if (newClears.size() == 1) {
		mutation = newClears[0];
	} else if (newClears.size() > 1) {
		CODE_PROBE(true, "Clear Range raw access or cross multiple tenants");
		idxSplitMutations.emplace_back(mutationIdx, newClears);
		newMutationSize += newClears.size() - 1;
	} else {
		mutation.type = MutationRef::NoOp;
	}
	return newClears.size();
}

TEST_CASE("/CommitProxy/SplitRange/replaceRawClearRanges") {
	int mapSize = 1000;
	std::map<int64_t, TenantName> tenantMap;
	for (int i = 0; i < mapSize; ++i) {
		tenantMap[i * 2] = ""_sr;
	}

	Arena arena(15 << 10);
	VectorRef<MutationRef> mutations;
	VectorRef<MutationRef> targetMutations;
	mutations.emplace_back_deep(arena, MutationRef::SetValue, "0"_sr, ""_sr);
	targetMutations.emplace_back_deep(arena, MutationRef::SetValue, "0"_sr, ""_sr);
	// single tenant
	int64_t tenantId = deterministicRandom()->randomInt64(0, mapSize) * 2;
	KeyRef prefix = TenantAPI::idToPrefix(arena, tenantId);
	KeyRef param1 = prefix.withSuffix("a"_sr, arena);
	KeyRef param2 = prefix.withSuffix("b"_sr, arena);
	mutations.emplace_back(arena, MutationRef::ClearRange, param1, param2);
	targetMutations.emplace_back_deep(arena, MutationRef::ClearRange, param1, param2);

	// other op
	mutations.emplace_back_deep(arena, MutationRef::SetValue, "1"_sr, ""_sr);
	targetMutations.emplace_back_deep(arena, MutationRef::SetValue, "1"_sr, ""_sr);

	// multiple tenants
	int64_t tid1 = deterministicRandom()->randomInt64(0, mapSize - 1) * 2;
	KeyRef prefix1 = TenantAPI::idToPrefix(arena, tid1);
	param1 = deterministicRandom()->coinflip() ? prefix1 : prefix1.withSuffix("a"_sr, arena); // align or not
	// with system keys
	int targetSize = mapSize - tid1 / 2 + 1;
	Key randomSysKey = systemKeys.begin.withSuffix("sf"_sr, arena);
	mutations.emplace_back(arena, MutationRef::ClearRange, param1, randomSysKey);
	auto sMutations = splitClearRangeByTenant(arena, mutations.back(), tenantMap);
	ASSERT_EQ(targetSize, sMutations.size());
	targetMutations.append(arena, sMutations.begin(), sMutations.size());

	// other op
	mutations.emplace_back_deep(arena, MutationRef::SetValue, "3"_sr, ""_sr);
	targetMutations.emplace_back_deep(arena, MutationRef::SetValue, "3"_sr, ""_sr);

	// [s, 0], [cr, t0a, t0b], [s, 1], [c, t1a, randomSys], [s, 3]
	std::vector<std::pair<int, std::vector<MutationRef>>> idxSplitMutations;
	int newMutationSize = mutations.size();
	for (int i = 0; i < mutations.size(); ++i) {
		if (mutations[i].type == MutationRef::ClearRange) {
			processClearRangeMutation(arena, tenantMap, mutations[i], i, newMutationSize, idxSplitMutations);
		}
	}

	replaceRawClearRanges(arena, mutations, idxSplitMutations, newMutationSize);
	// verify
	ASSERT_EQ(mutations.size(), targetMutations.size());
	for (int i = 0; i < mutations.size(); ++i) {
		ASSERT_EQ(targetMutations[i].type, mutations[i].type);
		ASSERT(targetMutations[i].param1 == mutations[i].param1);
		ASSERT(targetMutations[i].param2 == mutations[i].param2);
	}
	return Void();
}

// Return success and properly split clear range mutations if all tenant check pass. Otherwise, return corresponding
// error
Error validateAndProcessTenantAccess(Arena& arena,
                                     VectorRef<MutationRef>& mutations,
                                     ProxyCommitData* const pProxyCommitData,
                                     std::unordered_set<int64_t>& rawAccessTenantIds,
                                     Optional<UID> debugId = Optional<UID>(),
                                     const char* context = "") {
	bool changeTenant = false;
	bool writeNormalKey = false;
	std::vector<int64_t> tids; // tenant ids accessed by the raw access transaction

	std::vector<std::pair<int, std::vector<MutationRef>>> idxSplitMutations;
	int newMutationSize = mutations.size();
	KeyRangeRef tenantMapRange = TenantMetadata::tenantMap().subspace;
	for (int i = 0; i < mutations.size(); ++i) {
		auto& mutation = mutations[i];
		Optional<int64_t> tenantId;
		bool validAccess = true;
		changeTenant = changeTenant || TenantAPI::tenantMapChanging(mutation, tenantMapRange);

		if (mutation.type == MutationRef::ClearRange) {
			int newClearSize = processClearRangeMutation(
			    arena, pProxyCommitData->tenantMap, mutation, i, newMutationSize, idxSplitMutations, &tids);

			if (debugId.present()) {
				DisabledTraceEvent(SevDebug, "SplitTenantClearRange", pProxyCommitData->dbgid)
				    .detail("TxnId", debugId)
				    .detail("Idx", i)
				    .detail("TenantMap", pProxyCommitData->tenantMap.size())
				    .detail("NewMutationSize", newMutationSize)
				    .detail("OldMutationSize", mutations.size())
				    .detail("NewClears", newClearSize);
			}
		} else if (!isSystemKey(mutation.param1)) {
			validAccess = validTenantAccess(mutation, pProxyCommitData->tenantMap, tenantId);
			writeNormalKey = true;
		}

		if (debugId.present()) {
			DisabledTraceEvent(SevDebug, "ValidateAndProcessTenantAccess", pProxyCommitData->dbgid)
			    .detail("Context", context)
			    .detail("TxnId", debugId)
			    .detail("Version", pProxyCommitData->version.get())
			    .detail("ChangeTenant", changeTenant)
			    .detail("WriteNormalKey", writeNormalKey)
			    .detail("TenantId", tenantId)
			    .detail("ValidAccess", validAccess)
			    .detail("MutationType", getTypeString(mutation.type))
			    .detail("Mutation1", mutation.param1)
			    .detail("Mutation2", mutation.param2);
		}

		if (!validAccess) {
			TraceEvent(SevWarn, "IllegalTenantAccess", pProxyCommitData->dbgid)
			    .suppressFor(10.0)
			    .detail("Reason", "Raw write to unknown tenant");
			return illegal_tenant_access();
		}

		if (writeNormalKey && changeTenant) {
			TraceEvent(SevWarn, "IllegalTenantAccess", pProxyCommitData->dbgid)
			    .suppressFor(10.0)
			    .detail("Reason", "Tenant change and normal key write in same transaction");
			CODE_PROBE(true, "Writing normal keys while changing the tenant map");
			return illegal_tenant_access();
		}
		if (tenantId.present()) {
			ASSERT(tenantId.get() != TenantInfo::INVALID_TENANT);
			tids.push_back(tenantId.get());
		}
	}
	rawAccessTenantIds.insert(tids.begin(), tids.end());

	replaceRawClearRanges(arena, mutations, idxSplitMutations, newMutationSize);
	return success();
}

// If the validation success, return the list of tenant Ids referred by the transaction via tenantIds.
Error validateAndProcessTenantAccess(CommitTransactionRequest& tr,
                                     ProxyCommitData* const pProxyCommitData,
                                     std::unordered_set<int64_t>& rawAccessTenantIds) {
	bool isValid = checkTenantNoWait(pProxyCommitData, tr.tenantInfo.tenantId, "Commit", true);
	if (!isValid) {
		return tenant_not_found();
	}
	if (!tr.isLockAware() && pProxyCommitData->lockedTenants.contains(tr.tenantInfo.tenantId)) {
		CODE_PROBE(true, "Attempt access to locked tenant without lock awareness");
		return tenant_locked();
	}

	// only do the mutation check when the transaction use raw_access option and the tenant mode is required
	if (pProxyCommitData->getTenantMode() != TenantMode::REQUIRED || tr.tenantInfo.hasTenant()) {
		if (tr.tenantInfo.hasTenant()) {
			rawAccessTenantIds.insert(tr.tenantInfo.tenantId);
		}
		return success();
	}

	return validateAndProcessTenantAccess(tr.arena,
	                                      tr.transaction.mutations,
	                                      pProxyCommitData,
	                                      rawAccessTenantIds,
	                                      tr.debugID,
	                                      "validateAndProcessTenantAccess");
}

// Compute and apply "metadata" effects of each other proxy's most recent batch
void applyMetadataEffect(CommitBatchContext* self) {
	bool initialState = self->isMyFirstBatch;
	self->firstStateMutations = self->isMyFirstBatch;
	KeyRangeRef tenantMapRange = TenantMetadata::tenantMap().subspace;
	for (int versionIndex = 0; versionIndex < self->resolution[0].stateMutations.size(); versionIndex++) {
		// pProxyCommitData->logAdapter->setNextVersion( ??? );  << Ideally we would be telling the log adapter that the
		// pushes in this commit will be in the version at which these state mutations were committed by another proxy,
		// but at present we don't have that information here.  So the disk queue may be unnecessarily conservative
		// about popping.

		for (int transactionIndex = 0;
		     transactionIndex < self->resolution[0].stateMutations[versionIndex].size() && !self->forceRecovery;
		     transactionIndex++) {
			bool committed = true;
			for (int resolver = 0; resolver < self->resolution.size(); resolver++) {
				committed =
				    committed && self->resolution[resolver].stateMutations[versionIndex][transactionIndex].committed;
			}

			if (committed && self->pProxyCommitData->getTenantMode() == TenantMode::REQUIRED) {
				auto& tenantIds = self->resolution[0].stateMutations[versionIndex][transactionIndex].tenantIds;
				ASSERT(tenantIds.present());
				// fail transaction if it contain both of tenant changes and normal key writing
				auto& mutations = self->resolution[0].stateMutations[versionIndex][transactionIndex].mutations;
				committed =
				    tenantIds.get().empty() || std::none_of(mutations.begin(), mutations.end(), [&](MutationRef m) {
					    return TenantAPI::tenantMapChanging(m, tenantMapRange);
				    });

				// check if all tenant ids are valid if committed == true
				committed = committed &&
				            std::all_of(tenantIds.get().begin(), tenantIds.get().end(), [self](const int64_t& tid) {
					            return self->pProxyCommitData->tenantMap.contains(tid);
				            });

				if (self->debugID.present()) {
					TraceEvent(SevDebug, "TenantAccessCheck_ApplyMetadataEffect", self->debugID.get())
					    .detail("TenantIds", tenantIds)
					    .detail("Mutations", mutations);
				}
			}

			if (committed) {
				// Note: since we are not to commit, we don't need to pass cipherKeys for encryption.
				applyMetadataMutations(SpanContext(),
				                       *self->pProxyCommitData,
				                       self->arena,
				                       self->pProxyCommitData->logSystem,
				                       self->resolution[0].stateMutations[versionIndex][transactionIndex].mutations,
				                       /* pToCommit= */ nullptr,
				                       /* pCipherKeys= */ nullptr,
				                       EncryptionAtRestMode::DISABLED,
				                       self->forceRecovery,
				                       /* version= */ self->commitVersion,
				                       /* popVersion= */ 0,
				                       /* initialCommit */ false,
				                       /* provisionalCommitProxy */ self->pProxyCommitData->provisional);
			}

			if (self->resolution[0].stateMutations[versionIndex][transactionIndex].mutations.size() &&
			    self->firstStateMutations) {
				ASSERT(committed);
				self->firstStateMutations = false;
				self->forceRecovery = false;
			}
		}

		// These changes to txnStateStore will be committed by the other proxy, so we simply discard the commit message
		auto fcm = self->pProxyCommitData->logAdapter->getCommitMessage();
		self->storeCommits.emplace_back(fcm, self->pProxyCommitData->txnStateStore->commit());

		if (initialState) {
			initialState = false;
			self->forceRecovery = false;
			self->pProxyCommitData->txnStateStore->resyncLog();

			for (auto& p : self->storeCommits) {
				ASSERT(!p.second.isReady());
				p.first.get().acknowledge.send(Void());
				ASSERT(p.second.isReady());
			}
			self->storeCommits.clear();
		}
	}
}

/// Determine which transactions actually committed (conservatively) by combining results from the resolvers
void determineCommittedTransactions(CommitBatchContext* self) {
	auto pProxyCommitData = self->pProxyCommitData;
	const auto& trs = self->trs;

	ASSERT(self->transactionResolverMap.size() == self->committed.size());
	// For each commitTransactionRef, it is only sent to resolvers specified in transactionResolverMap
	// Thus, we use this nextTr to track the correct transaction index on each resolver.
	self->nextTr.resize(self->resolution.size());
	for (int t = 0; t < trs.size(); t++) {
		uint8_t commit = ConflictBatch::TransactionCommitted;
		for (int r : self->transactionResolverMap[t]) {
			commit = std::min(self->resolution[r].committed[self->nextTr[r]++], commit);
		}
		self->committed[t] = commit;
	}
	for (int r = 0; r < self->resolution.size(); r++)
		ASSERT(self->nextTr[r] == self->resolution[r].committed.size());

	pProxyCommitData->logAdapter->setNextVersion(self->commitVersion);

	self->lockedKey = pProxyCommitData->txnStateStore->readValue(databaseLockedKey).get();
	self->locked = self->lockedKey.present() && self->lockedKey.get().size();

	const Optional<Value> mustContainSystemKey =
	    pProxyCommitData->txnStateStore->readValue(mustContainSystemMutationsKey).get();
	if (mustContainSystemKey.present() && mustContainSystemKey.get().size()) {
		for (int t = 0; t < trs.size(); t++) {
			if (self->committed[t] == ConflictBatch::TransactionCommitted) {
				bool foundSystem = false;
				for (auto& m : trs[t].transaction.mutations) {
					if ((m.type == MutationRef::ClearRange ? m.param2 : m.param1) >= nonMetadataSystemKeys.end) {
						foundSystem = true;
						break;
					}
				}
				if (!foundSystem) {
					self->committed[t] = ConflictBatch::TransactionConflict;
				}
			}
		}
	}
}

// This first pass through committed transactions deals with "metadata" effects (modifications of txnStateStore, changes
// to storage servers' responsibilities)
ACTOR Future<Void> applyMetadataToCommittedTransactions(CommitBatchContext* self) {
	state ProxyCommitData* const pProxyCommitData = self->pProxyCommitData;
	state std::unordered_set<int64_t> rawAccessTenantIds;
	auto& trs = self->trs;

	int t;
	for (t = 0; t < trs.size() && !self->forceRecovery; t++) {
		Error e = validateAndProcessTenantAccess(trs[t], pProxyCommitData, rawAccessTenantIds);
		if (e.code() != error_code_success) {
			trs[t].reply.sendError(e);
			self->committed[t] = ConflictBatch::TransactionTenantFailure;
			CODE_PROBE(true, "Commit proxy transaction tenant failure");
		} else if (self->committed[t] == ConflictBatch::TransactionCommitted &&
		           (!self->locked || trs[t].isLockAware())) {
			self->commitCount++;
			applyMetadataMutations(trs[t].spanContext,
			                       *pProxyCommitData,
			                       self->arena,
			                       pProxyCommitData->logSystem,
			                       trs[t].transaction.mutations,
			                       SERVER_KNOBS->PROXY_USE_RESOLVER_PRIVATE_MUTATIONS ? nullptr : &self->toCommit,
			                       &self->cipherKeys,
			                       pProxyCommitData->encryptMode,
			                       self->forceRecovery,
			                       self->commitVersion,
			                       self->commitVersion + 1,
			                       /* initialCommit= */ false,
			                       /* provisionalCommitProxy */ self->pProxyCommitData->provisional);
		}

		if (self->firstStateMutations) {
			ASSERT(self->committed[t] == ConflictBatch::TransactionCommitted);
			self->firstStateMutations = false;
			self->forceRecovery = false;
		}
	}

	if (self->forceRecovery) {
		for (; t < trs.size(); t++)
			self->committed[t] = ConflictBatch::TransactionConflict;
		TraceEvent(SevWarn, "RestartingTxnSubsystem", pProxyCommitData->dbgid).detail("Stage", "AwaitCommit");
	}
	if (SERVER_KNOBS->PROXY_USE_RESOLVER_PRIVATE_MUTATIONS) {
		// Resolver also calculates forceRecovery and only applies metadata mutations
		// in the same set of transactions as this proxy.
		ResolveTransactionBatchReply& reply = self->resolution[0];
		self->toCommit.setMutations(reply.privateMutationCount, reply.privateMutations);
		if (SERVER_KNOBS->ENABLE_VERSION_VECTOR_TLOG_UNICAST) {
			// TraceEvent("ResolverReturn").detail("ReturnTags",reply.writtenTags).detail("TPCVsize",reply.tpcvMap.size()).detail("ReqTags",self->writtenTagsPreResolution);
			self->tpcvMap = reply.tpcvMap;
		}
		self->toCommit.addWrittenTags(reply.writtenTags);
	}

	self->lockedKey = pProxyCommitData->txnStateStore->readValue(databaseLockedKey).get();
	self->lockedAfter = self->lockedKey.present() && self->lockedKey.get().size();

	self->metadataVersionAfter = pProxyCommitData->txnStateStore->readValue(metadataVersionKey).get();

	auto fcm = pProxyCommitData->logAdapter->getCommitMessage();
	self->storeCommits.emplace_back(fcm, pProxyCommitData->txnStateStore->commit());
	pProxyCommitData->version.set(self->commitVersion);
	if (!pProxyCommitData->validState.isSet())
		pProxyCommitData->validState.send(Void());
	ASSERT(self->commitVersion);

	if (!self->isMyFirstBatch &&
	    pProxyCommitData->txnStateStore->readValue(coordinatorsKey).get().get() != self->previousCoordinators.get()) {
		wait(brokenPromiseToNever(pProxyCommitData->db->get().clusterInterface.changeCoordinators.getReply(
		    ChangeCoordinatorsRequest(pProxyCommitData->txnStateStore->readValue(coordinatorsKey).get().get(),
		                              self->pProxyCommitData->master.id()))));
		ASSERT(false); // ChangeCoordinatorsRequest should always throw
	}

	// If there are raw access requests or cross-tenant boundary clear ranges in the batch, tenant ids for those
	// requests are available only after resolution. We need to fetch additional cipher keys for these requests.
	if (pProxyCommitData->encryptMode == EncryptionAtRestMode::DOMAIN_AWARE && !rawAccessTenantIds.empty()) {
		std::unordered_set<EncryptCipherDomainId> extraDomainIds;
		for (auto tenantId : rawAccessTenantIds) {
			if (!self->cipherKeys.contains(tenantId)) {
				extraDomainIds.insert(tenantId);
			}
		}
		if (!extraDomainIds.empty()) {
			std::unordered_map<EncryptCipherDomainId, Reference<BlobCipherKey>> extraCipherKeys =
			    wait(GetEncryptCipherKeys<ServerDBInfo>::getLatestEncryptCipherKeys(
			        pProxyCommitData->db, extraDomainIds, BlobCipherMetrics::TLOG_POST_RESOLUTION));
			self->cipherKeys.insert(extraCipherKeys.begin(), extraCipherKeys.end());
		}
	}

	return Void();
}

ACTOR Future<WriteMutationRefVar> writeMutationEncryptedMutation(CommitBatchContext* self,
                                                                 int64_t tenantId,
                                                                 const MutationRef* mutation,
                                                                 Optional<MutationRef>* encryptedMutationOpt,
                                                                 Arena* arena) {
	state MutationRef encryptedMutation = encryptedMutationOpt->get();
	state BlobCipherEncryptHeaderRef headerRef;
	state MutationRef decryptedMutation;

	static_assert(TenantInfo::INVALID_TENANT == INVALID_ENCRYPT_DOMAIN_ID);
	ASSERT(self->pProxyCommitData->encryptMode.isEncryptionEnabled());
	ASSERT(g_network && g_network->isSimulated());

	ASSERT(encryptedMutation.isEncrypted());
	Reference<AsyncVar<ServerDBInfo> const> dbInfo = self->pProxyCommitData->db;
	headerRef = encryptedMutation.configurableEncryptionHeader();
	TextAndHeaderCipherKeys cipherKeys = wait(GetEncryptCipherKeys<ServerDBInfo>::getEncryptCipherKeys(
	    dbInfo, headerRef, BlobCipherMetrics::TLOG, self->pProxyCommitData->encryptionMonitor));
	decryptedMutation = encryptedMutation.decrypt(cipherKeys, *arena, BlobCipherMetrics::TLOG);

	ASSERT(decryptedMutation.type == mutation->type);
	ASSERT(decryptedMutation.param1 == mutation->param1);
	ASSERT(decryptedMutation.param2 == mutation->param2);

	CODE_PROBE(true, "encrypting non-metadata mutations", probe::decoration::rare);
	self->toCommit.writeTypedMessage(encryptedMutation);
	return encryptedMutation;
}

Future<WriteMutationRefVar> writeMutation(CommitBatchContext* self,
                                          int64_t domainId,
                                          const MutationRef* mutation,
                                          Optional<MutationRef>* encryptedMutationOpt,
                                          Arena* arena,
                                          double* encryptTime = nullptr) {
	static_assert(TenantInfo::INVALID_TENANT == INVALID_ENCRYPT_DOMAIN_ID);

	// WriteMutation routine is responsible for appending mutations to be persisted in TLog, the operation
	// isn't a 'blocking' operation, except for few cases when Encryption is supported by the cluster such
	// as:
	// 1. Fetch encryption keys to encrypt the mutation.
	// 2. Split ClearRange mutation to respect Encryption domain boundaries.
	// 3. Ensure sanity of already encrypted mutation - simulation limited check.
	//
	// Approach optimizes "fast" path by avoiding alloc/dealloc overhead due to be ACTOR framework support,
	// the penalty happens iff any of above conditions are met. Otherwise, corresponding handle routine (ACTOR
	// compliant) gets invoked ("slow path").

	if (self->pProxyCommitData->encryptMode.isEncryptionEnabled()) {
		if (self->pProxyCommitData->encryptMode.mode == EncryptionAtRestMode::CLUSTER_AWARE) {
			ASSERT(domainId == FDB_DEFAULT_ENCRYPT_DOMAIN_ID || domainId == SYSTEM_KEYSPACE_ENCRYPT_DOMAIN_ID);
		}
		MutationRef encryptedMutation;
		CODE_PROBE(self->pProxyCommitData->getTenantMode() == TenantMode::DISABLED, "using disabled tenant mode");
		CODE_PROBE(self->pProxyCommitData->getTenantMode() == TenantMode::OPTIONAL_TENANT,
		           "using optional tenant mode");
		CODE_PROBE(self->pProxyCommitData->getTenantMode() == TenantMode::REQUIRED, "using required tenant mode");

		if (encryptedMutationOpt && encryptedMutationOpt->present()) {
			CODE_PROBE(true, "using already encrypted mutation", probe::decoration::rare);
			encryptedMutation = encryptedMutationOpt->get();
			ASSERT(encryptedMutation.isEncrypted());
			// During simulation check whether the encrypted mutation matches the decrpyted mutation
			if (g_network && g_network->isSimulated()) {
				return writeMutationEncryptedMutation(self, domainId, mutation, encryptedMutationOpt, arena);
			}
		} else {
			if (domainId == INVALID_ENCRYPT_DOMAIN_ID) {
				domainId = getEncryptDetailsFromMutationRef(self->pProxyCommitData, *mutation);
				CODE_PROBE(true, "Raw access mutation encryption", probe::decoration::rare);
			}
			ASSERT_NE(domainId, INVALID_ENCRYPT_DOMAIN_ID);
			ASSERT(self->cipherKeys.contains(domainId));
			encryptedMutation =
			    mutation->encrypt(self->cipherKeys, domainId, *arena, BlobCipherMetrics::TLOG, encryptTime);
		}
		ASSERT(encryptedMutation.isEncrypted());
		CODE_PROBE(true, "encrypting non-metadata mutations", probe::decoration::rare);
		self->toCommit.writeTypedMessage(encryptedMutation);
		return std::variant<MutationRef, VectorRef<MutationRef>>{ encryptedMutation };
	} else {
		self->toCommit.writeTypedMessage(*mutation);
		return std::variant<MutationRef, VectorRef<MutationRef>>{ *mutation };
	}
}

double pushToBackupMutations(CommitBatchContext* self,
                             ProxyCommitData* const pProxyCommitData,
                             Arena& arena,
                             MutationRef const& m,
                             MutationRef const& writtenMutation,
                             Optional<MutationRef> const& encryptedMutation) {
	// In required tenant mode, the clear ranges are already split by tenant
	double encryptionTime = 0;
	if (m.type != MutationRef::Type::ClearRange ||
	    (pProxyCommitData->getTenantMode() == TenantMode::REQUIRED && !systemKeys.contains(m.param1))) {
		if (EXPENSIVE_VALIDATION && m.type == MutationRef::ClearRange) {
			DisabledTraceEvent("DebugSingleTenant", pProxyCommitData->dbgid)
			    .detail("M1", m.param1)
			    .detail("M2", m.param2)
			    .detail("TenantMap", pProxyCommitData->tenantMap.size());
			ASSERT(TenantAPI::withinSingleTenant(KeyRangeRef(m.param1, m.param2)));
		}
		ASSERT(!pProxyCommitData->encryptMode.isEncryptionEnabled() || writtenMutation.isEncrypted());

		// Add the mutation to the relevant backup tag
		for (auto backupName : pProxyCommitData->vecBackupKeys[m.param1]) {
			// If encryption is enabled make sure the mutation we are writing is also encrypted
			CODE_PROBE(writtenMutation.isEncrypted(), "using encrypted backup mutation", probe::decoration::rare);
			self->logRangeMutations[backupName].push_back_deep(self->logRangeMutationsArena, writtenMutation);
		}

	} else {
		KeyRangeRef mutationRange(m.param1, m.param2);
		KeyRangeRef intersectionRange;

		// Identify and add the intersecting ranges of the mutation to the array of mutations to serialize
		for (auto backupRange : pProxyCommitData->vecBackupKeys.intersectingRanges(mutationRange)) {
			// Get the backup sub range
			const auto& backupSubrange = backupRange.range();

			// Determine the intersecting range
			intersectionRange = mutationRange & backupSubrange;

			// Create the custom mutation for the specific backup tag
			MutationRef backupMutation(MutationRef::Type::ClearRange, intersectionRange.begin, intersectionRange.end);

			if (pProxyCommitData->encryptMode.isEncryptionEnabled()) {
				CODE_PROBE(true, "encrypting clear range backup mutation", probe::decoration::rare);
				if (backupMutation.param1 == m.param1 && backupMutation.param2 == m.param2 &&
				    encryptedMutation.present()) {
					backupMutation = encryptedMutation.get();
				} else {
					EncryptCipherDomainId domainId = getEncryptDetailsFromMutationRef(pProxyCommitData, backupMutation);
					double encryptionTimeV = 0;
					backupMutation = backupMutation.encrypt(
					    self->cipherKeys, domainId, arena, BlobCipherMetrics::BACKUP, &encryptionTimeV);
					encryptionTime += encryptionTimeV;
				}
			}
			ASSERT(!pProxyCommitData->encryptMode.isEncryptionEnabled() || backupMutation.isEncrypted());

			// Add the mutation to the relevant backup tag
			for (auto backupName : backupRange.value()) {
				self->logRangeMutations[backupName].push_back_deep(self->logRangeMutationsArena, backupMutation);
			}
		}
	}
	return encryptionTime;
}

void addAccumulativeChecksumMutations(CommitBatchContext* self) {
	ASSERT(self->pProxyCommitData->acsBuilder != nullptr);
	const uint16_t acsIndex = getCommitProxyAccumulativeChecksumIndex(self->pProxyCommitData->commitProxyIndex);
	for (const auto& [tag, acsState] : self->pProxyCommitData->acsBuilder->getAcsTable()) {
		ASSERT(tagSupportAccumulativeChecksum(tag));
		ASSERT(acsState.version <= self->commitVersion);
		if (acsState.version < self->commitVersion) {
			// Have not updated in the current commit batch
			// So, need not send acs mutation for this tag
			continue;
		}
		ASSERT(acsState.epoch == self->pProxyCommitData->epoch);
		MutationRef acsMutation;
		acsMutation.type = MutationRef::SetValue;
		acsMutation.param1 = accumulativeChecksumKey; // private mutation
		AccumulativeChecksumState acsToSend(acsIndex, acsState.acs, self->commitVersion, self->pProxyCommitData->epoch);
		Value acsValue = accumulativeChecksumValue(acsToSend);
		acsMutation.param2 = acsValue;
		acsMutation.setAccumulativeChecksumIndex(acsIndex);
		if (CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM_LOGGING) {
			TraceEvent(SevInfo, "AcsBuilderIssueAccumulativeChecksumMutation", self->pProxyCommitData->dbgid)
			    .detail("AcsTag", tag)
			    .detail("AcsIndex", acsIndex)
			    .detail("AcsToSend", acsToSend.toString())
			    .detail("Mutation", acsMutation)
			    .detail("CommitProxyIndex", self->pProxyCommitData->commitProxyIndex);
		}
		self->toCommit.addTag(tag);
		self->toCommit.writeTypedMessage(acsMutation);
	}
}
// RangeLock takes effect only when the feature flag is on and database is unlocked and the mutation is not encrypted
void rejectMutationsForReadLockOnRange(CommitBatchContext* self) {
	ASSERT(self->rangeLockEnabled());
	ProxyCommitData* const pProxyCommitData = self->pProxyCommitData;
	ASSERT(pProxyCommitData->rangeLock != nullptr);
	std::vector<CommitTransactionRequest>& trs = self->trs;
	for (int i = self->transactionNum; i < trs.size(); i++) {
		if (self->committed[i] != ConflictBatch::TransactionCommitted) {
			continue;
		} else if (trs[i].isLockAware()) {
			continue; // rangeLock is transparent to lock-aware transactions
		}
		VectorRef<MutationRef>* pMutations = &trs[i].transaction.mutations;
		bool transactionRejected = false;
		for (int j = 0; j < pMutations->size(); j++) {
			MutationRef m = (*pMutations)[j];
			ASSERT_WE_THINK(!m.isEncrypted());
			if (m.isEncrypted()) {
				continue;
			}
			KeyRange rangeToCheck;
			if (isSingleKeyMutation((MutationRef::Type)m.type)) {
				rangeToCheck = singleKeyRange(m.param1);
			} else if (m.type == MutationRef::ClearRange) {
				rangeToCheck = KeyRangeRef(m.param1, m.param2);
			}
			bool shouldReject = pProxyCommitData->rangeLock->isLocked(rangeToCheck);
			if (shouldReject) {
				self->committed[i] = ConflictBatch::TransactionLockReject;
				trs[i].reply.sendError(transaction_rejected_range_locked());
				transactionRejected = true;
			}
			if (transactionRejected) {
				break;
			}
		}
	}
	return;
}

/// This second pass through committed transactions assigns the actual mutations to the appropriate storage servers'
/// tags
ACTOR Future<Void> assignMutationsToStorageServers(CommitBatchContext* self) {
	state ProxyCommitData* const pProxyCommitData = self->pProxyCommitData;
	state std::vector<CommitTransactionRequest>& trs = self->trs;
	state double curEncryptionTime = 0;
	state double totalEncryptionTime = 0;

	for (; self->transactionNum < trs.size(); self->transactionNum++) {
		if (!(self->committed[self->transactionNum] == ConflictBatch::TransactionCommitted &&
		      (!self->locked || trs[self->transactionNum].isLockAware()))) {
			continue;
		}

		state bool checkSample = trs[self->transactionNum].commitCostEstimation.present();
		state Optional<ClientTrCommitCostEstimation>* trCost = &trs[self->transactionNum].commitCostEstimation;
		state int mutationNum = 0;
		state VectorRef<MutationRef>* pMutations = &trs[self->transactionNum].transaction.mutations;
		state VectorRef<Optional<MutationRef>>* encryptedMutations =
		    &trs[self->transactionNum].transaction.encryptedMutations;

		if (!encryptedMutations->empty()) {
			ASSERT_EQ(encryptedMutations->size(), pMutations->size());
		}

		state int64_t encryptDomain = trs[self->transactionNum].tenantInfo.tenantId;
		if (self->pProxyCommitData->encryptMode.mode == EncryptionAtRestMode::CLUSTER_AWARE &&
		    encryptDomain != SYSTEM_KEYSPACE_ENCRYPT_DOMAIN_ID) {
			encryptDomain = FDB_DEFAULT_ENCRYPT_DOMAIN_ID;
		}

		self->toCommit.addTransactionInfo(trs[self->transactionNum].spanContext);

		for (; mutationNum < pMutations->size(); mutationNum++) {
			if (self->yieldBytes > SERVER_KNOBS->DESIRED_TOTAL_BYTES) {
				self->yieldBytes = 0;
				if (g_network->check_yield(TaskPriority::ProxyCommitYield1)) {
					self->computeDuration += g_network->timer_monotonic() - self->computeStart;
					wait(delay(0, TaskPriority::ProxyCommitYield1));
					self->computeStart = g_network->timer_monotonic();
				}
			}

			state MutationRef m = (*pMutations)[mutationNum];
			state Optional<MutationRef> encryptedMutation =
			    encryptedMutations->size() > 0 ? (*encryptedMutations)[mutationNum] : Optional<MutationRef>();
			state Arena arena;
			state MutationRef writtenMutation;
			self->mutationCount++;
			self->mutationBytes += m.expectedSize();
			self->yieldBytes += m.expectedSize();
			ASSERT(!m.isEncrypted());
			// Determine the set of tags (responsible storage servers) for the mutation, splitting it
			// if necessary.  Serialize (splits of) the mutation into the message buffer and add the tags.
			if (isSingleKeyMutation((MutationRef::Type)m.type)) {
				auto& tags = pProxyCommitData->tagsForKey(m.param1);

				// sample single key mutation based on cost
				// the expectation of sampling is every COMMIT_SAMPLE_COST sample once
				if (checkSample) {
					double totalCosts = trCost->get().writeCosts;
					double cost = getWriteOperationCost(m.expectedSize());
					double mul = std::max(1.0, totalCosts / std::max(1.0, (double)CLIENT_KNOBS->COMMIT_SAMPLE_COST));
					ASSERT(totalCosts > 0);
					double prob = mul * cost / totalCosts;

					if (deterministicRandom()->random01() < prob) {
						const auto& storageServers = pProxyCommitData->keyInfo[m.param1].src_info;
						for (const auto& ssInfo : storageServers) {
							auto id = ssInfo->interf.id();
							// scale cost
							cost = cost < CLIENT_KNOBS->COMMIT_SAMPLE_COST ? CLIENT_KNOBS->COMMIT_SAMPLE_COST : cost;
							pProxyCommitData->updateSSTagCost(
							    id, trs[self->transactionNum].tagSet.get(), m, cost / storageServers.size());
						}
					}
				}

				if (pProxyCommitData->singleKeyMutationEvent->enabled) {
					KeyRangeRef shard = pProxyCommitData->keyInfo.rangeContaining(m.param1).range();
					pProxyCommitData->singleKeyMutationEvent->tag1 = (int64_t)tags[0].id;
					pProxyCommitData->singleKeyMutationEvent->tag2 = (int64_t)tags[1].id;
					pProxyCommitData->singleKeyMutationEvent->tag3 = (int64_t)tags[2].id;
					pProxyCommitData->singleKeyMutationEvent->shardBegin = shard.begin;
					pProxyCommitData->singleKeyMutationEvent->shardEnd = shard.end;
					pProxyCommitData->singleKeyMutationEvent->log();
				}

				DEBUG_MUTATION("ProxyCommit", self->commitVersion, m, pProxyCommitData->dbgid).detail("To", tags);
				self->toCommit.addTags(tags);
				if (pProxyCommitData->cacheInfo[m.param1]) {
					self->toCommit.addTag(cacheTag);
				}
				if (encryptedMutation.present()) {
					ASSERT(encryptedMutation.get().isEncrypted());
				}

				if (pProxyCommitData->acsBuilder != nullptr) {
					updateMutationWithAcsAndAddMutationToAcsBuilder(
					    pProxyCommitData->acsBuilder,
					    m,
					    tags,
					    getCommitProxyAccumulativeChecksumIndex(pProxyCommitData->commitProxyIndex),
					    pProxyCommitData->epoch,
					    self->commitVersion,
					    pProxyCommitData->dbgid);
				}

				WriteMutationRefVar var =
				    wait(writeMutation(self, encryptDomain, &m, &encryptedMutation, &arena, &curEncryptionTime));
				totalEncryptionTime += curEncryptionTime;
				// FIXME: Remove assert once ClearRange RAW_ACCESS usecase handling is done
				ASSERT(std::holds_alternative<MutationRef>(var));
				writtenMutation = std::get<MutationRef>(var);
			} else if (m.type == MutationRef::ClearRange) {
				KeyRangeRef clearRange(KeyRangeRef(m.param1, m.param2));
				auto ranges = pProxyCommitData->keyInfo.intersectingRanges(clearRange);
				auto firstRange = ranges.begin();
				++firstRange;
				if (firstRange == ranges.end()) {
					// Fast path
					DEBUG_MUTATION("ProxyCommit", self->commitVersion, m, pProxyCommitData->dbgid)
					    .detail("To", ranges.begin().value().tags);
					ranges.begin().value().populateTags();
					self->toCommit.addTags(ranges.begin().value().tags);

					if (pProxyCommitData->acsBuilder != nullptr) {
						updateMutationWithAcsAndAddMutationToAcsBuilder(
						    pProxyCommitData->acsBuilder,
						    m,
						    ranges.begin().value().tags,
						    getCommitProxyAccumulativeChecksumIndex(pProxyCommitData->commitProxyIndex),
						    pProxyCommitData->epoch,
						    self->commitVersion,
						    pProxyCommitData->dbgid);
					}

					// check whether clear is sampled
					if (checkSample && !trCost->get().clearIdxCosts.empty() &&
					    trCost->get().clearIdxCosts[0].first == mutationNum) {
						auto const& ssInfos = ranges.begin().value().src_info;
						for (auto const& ssInfo : ssInfos) {
							auto id = ssInfo->interf.id();
							pProxyCommitData->updateSSTagCost(id,
							                                  trs[self->transactionNum].tagSet.get(),
							                                  m,
							                                  trCost->get().clearIdxCosts[0].second / ssInfos.size());
						}
						trCost->get().clearIdxCosts.pop_front();
					}
				} else {
					CODE_PROBE(true, "A clear range extends past a shard boundary");
					std::set<Tag> allSources;
					for (auto r : ranges) {
						r.value().populateTags();
						allSources.insert(r.value().tags.begin(), r.value().tags.end());

						// check whether clear is sampled
						if (checkSample && !trCost->get().clearIdxCosts.empty() &&
						    trCost->get().clearIdxCosts[0].first == mutationNum) {
							auto const& ssInfos = r.value().src_info;
							for (auto const& ssInfo : ssInfos) {
								auto id = ssInfo->interf.id();
								pProxyCommitData->updateSSTagCost(id,
								                                  trs[self->transactionNum].tagSet.get(),
								                                  m,
								                                  trCost->get().clearIdxCosts[0].second /
								                                      ssInfos.size());
							}
							trCost->get().clearIdxCosts.pop_front();
						}
					}

					DEBUG_MUTATION("ProxyCommit", self->commitVersion, m)
					    .detail("Dbgid", pProxyCommitData->dbgid)
					    .detail("To", allSources);
					self->toCommit.addTags(allSources);

					if (self->pProxyCommitData->acsBuilder != nullptr) {
						updateMutationWithAcsAndAddMutationToAcsBuilder(
						    pProxyCommitData->acsBuilder,
						    m,
						    allSources,
						    getCommitProxyAccumulativeChecksumIndex(pProxyCommitData->commitProxyIndex),
						    pProxyCommitData->epoch,
						    self->commitVersion,
						    pProxyCommitData->dbgid);
					}
				}

				if (pProxyCommitData->needsCacheTag(clearRange)) {
					self->toCommit.addTag(cacheTag);
				}
				WriteMutationRefVar var =
				    wait(writeMutation(self, encryptDomain, &m, &encryptedMutation, &arena, &curEncryptionTime));
				totalEncryptionTime += curEncryptionTime;
				// FIXME: Remove assert once ClearRange RAW_ACCESS usecase handling is done
				ASSERT(std::holds_alternative<MutationRef>(var));
				writtenMutation = std::get<MutationRef>(var);
			} else if (m.type == MutationRef::NoOp) {
				ASSERT_EQ(pProxyCommitData->getTenantMode(), TenantMode::REQUIRED);
				continue;
			} else {
				UNREACHABLE();
			}

			DisabledTraceEvent(SevDebug, "BeforeBackup", pProxyCommitData->dbgid)
			    .detail("M1", m.param1)
			    .detail("M2", m.param2)
			    .detail("MT", getTypeString(m.type))
			    .detail("VecBackupKeys", pProxyCommitData->vecBackupKeys.size())
			    .detail("ShouldBackup", shouldBackup(m))
			    .detail("TenantMapSize", pProxyCommitData->tenantMap.size())
			    .detail("TenantMode", (int)pProxyCommitData->getTenantMode());

			if (pProxyCommitData->vecBackupKeys.size() <= 1 || !shouldBackup(m)) {
				continue;
			}

			totalEncryptionTime +=
			    pushToBackupMutations(self, pProxyCommitData, arena, m, writtenMutation, encryptedMutation);
		}

		if (checkSample) {
			self->pProxyCommitData->stats.txnExpensiveClearCostEstCount +=
			    trs[self->transactionNum].commitCostEstimation.get().expensiveCostEstCount;
		}
	}

	ASSERT(CLIENT_KNOBS->ENABLE_ENCRYPTION_CPU_TIME_LOGGING || self->encryptionTime == 0);
	if (self->pProxyCommitData->encryptMode.isEncryptionEnabled()) {
		self->encryptionTime = totalEncryptionTime;
	}

	return Void();
}

ACTOR Future<Void> postResolution(CommitBatchContext* self) {
	state double postResolutionStart = g_network->timer_monotonic();
	state ProxyCommitData* const pProxyCommitData = self->pProxyCommitData;
	state std::vector<CommitTransactionRequest>& trs = self->trs;
	state const int64_t localBatchNumber = self->localBatchNumber;
	state const Optional<UID>& debugID = self->debugID;
	state Span span("MP:postResolution"_loc, self->span.context);

	bool queuedCommits = pProxyCommitData->latestLocalCommitBatchLogging.get() < localBatchNumber - 1;
	CODE_PROBE(queuedCommits, "Queuing post-resolution commit processing");
	wait(pProxyCommitData->latestLocalCommitBatchLogging.whenAtLeast(localBatchNumber - 1));
	state double postResolutionQueuing = g_network->timer_monotonic();
	pProxyCommitData->stats.postResolutionDist->sampleSeconds(postResolutionQueuing - postResolutionStart);
	wait(yield(TaskPriority::ProxyCommitYield1));

	self->computeStart = g_network->timer_monotonic();

	pProxyCommitData->stats.txnCommitResolved += trs.size();

	if (debugID.present()) {
		g_traceBatch.addEvent(
		    "CommitDebug", debugID.get().first(), "CommitProxyServer.commitBatch.ProcessingMutations");
	}

	self->isMyFirstBatch = !pProxyCommitData->version.get();
	self->previousCoordinators = pProxyCommitData->txnStateStore->readValue(coordinatorsKey).get();

	assertResolutionStateMutationsSizeConsistent(self->resolution);

	applyMetadataEffect(self);

	if (debugID.present()) {
		g_traceBatch.addEvent(
		    "CommitDebug", debugID.get().first(), "CommitProxyServer.commitBatch.ApplyMetadataEffect");
	}

	determineCommittedTransactions(self);

	if (debugID.present()) {
		g_traceBatch.addEvent(
		    "CommitDebug", debugID.get().first(), "CommitProxyServer.commitBatch.DetermineCommittedTransactions");
	}

	if (self->forceRecovery) {
		wait(Future<Void>(Never()));
	}

	// First pass
	wait(applyMetadataToCommittedTransactions(self));

	if (debugID.present()) {
		g_traceBatch.addEvent(
		    "CommitDebug", debugID.get().first(), "CommitProxyServer.commitBatch.ApplyMetadataToCommittedTxn");
	}

	// After applyed metadata change, this commit proxy has the latest view of locked ranges.
	// If a transaction has any mutation accessing to the locked range, reject the transaction with
	// error_code_transaction_rejected_range_locked
	if (self->rangeLockEnabled()) {
		rejectMutationsForReadLockOnRange(self);
	}

	// Second pass
	wait(assignMutationsToStorageServers(self));

	if (debugID.present()) {
		g_traceBatch.addEvent("CommitDebug", debugID.get().first(), "CommitProxyServer.commitBatch.AssignMutationToSS");
	}

	// Serialize and backup the mutations as a single mutation
	if ((pProxyCommitData->vecBackupKeys.size() > 1) && self->logRangeMutations.size()) {
		wait(addBackupMutations(pProxyCommitData,
		                        &self->logRangeMutations,
		                        &self->toCommit,
		                        self->commitVersion,
		                        &self->computeDuration,
		                        &self->computeStart));
	}

	buildIdempotencyIdMutations(
	    self->trs,
	    self->idempotencyKVBuilder,
	    self->commitVersion,
	    self->committed,
	    ConflictBatch::TransactionCommitted,
	    self->locked,
	    [&](const KeyValue& kv) {
		    MutationRef idempotencyIdSet;
		    idempotencyIdSet.type = MutationRef::Type::SetValue;
		    idempotencyIdSet.param1 = kv.key;
		    idempotencyIdSet.param2 = kv.value;
		    auto& tags = pProxyCommitData->tagsForKey(kv.key);
		    self->toCommit.addTags(tags);
		    if (self->pProxyCommitData->encryptMode.isEncryptionEnabled()) {
			    CODE_PROBE(true, "encrypting idempotency mutation", probe::decoration::rare);
			    EncryptCipherDomainId domainId =
			        getEncryptDetailsFromMutationRef(self->pProxyCommitData, idempotencyIdSet);
			    MutationRef encryptedMutation =
			        idempotencyIdSet.encrypt(self->cipherKeys, domainId, self->arena, BlobCipherMetrics::TLOG);
			    ASSERT(encryptedMutation.isEncrypted());
			    self->toCommit.writeTypedMessage(encryptedMutation);
		    } else {
			    if (pProxyCommitData->acsBuilder != nullptr) {
				    updateMutationWithAcsAndAddMutationToAcsBuilder(
				        pProxyCommitData->acsBuilder,
				        idempotencyIdSet,
				        tags,
				        getCommitProxyAccumulativeChecksumIndex(pProxyCommitData->commitProxyIndex),
				        pProxyCommitData->epoch,
				        self->commitVersion,
				        pProxyCommitData->dbgid);
			    }
			    self->toCommit.writeTypedMessage(idempotencyIdSet);
		    }
	    });
	state int i = 0;
	for (i = 0; i < pProxyCommitData->idempotencyClears.size(); i++) {
		auto& tags = pProxyCommitData->tagsForKey(pProxyCommitData->idempotencyClears[i].param1);
		self->toCommit.addTags(tags);
		// We already have an arena with an appropriate lifetime handy
		Arena& arena = pProxyCommitData->idempotencyClears.arena();
		if (pProxyCommitData->acsBuilder != nullptr) {
			updateMutationWithAcsAndAddMutationToAcsBuilder(
			    pProxyCommitData->acsBuilder,
			    pProxyCommitData->idempotencyClears[i],
			    tags,
			    getCommitProxyAccumulativeChecksumIndex(pProxyCommitData->commitProxyIndex),
			    pProxyCommitData->epoch,
			    self->commitVersion,
			    pProxyCommitData->dbgid);
		}
		WriteMutationRefVar var = wait(writeMutation(
		    self, SYSTEM_KEYSPACE_ENCRYPT_DOMAIN_ID, &pProxyCommitData->idempotencyClears[i], nullptr, &arena));
		ASSERT(std::holds_alternative<MutationRef>(var));
	}
	pProxyCommitData->idempotencyClears = Standalone<VectorRef<MutationRef>>();

	self->toCommit.saveTags(self->writtenTags);

	pProxyCommitData->stats.mutations += self->mutationCount;
	pProxyCommitData->stats.mutationBytes += self->mutationBytes;

	// Storage servers mustn't make durable versions which are not fully committed (because then they are impossible
	// to roll back) We prevent this by limiting the number of versions which are semi-committed but not fully
	// committed to be less than the MVCC window
	if (pProxyCommitData->committedVersion.get() <
	    self->commitVersion - SERVER_KNOBS->MAX_READ_TRANSACTION_LIFE_VERSIONS) {
		self->computeDuration += g_network->timer_monotonic() - self->computeStart;
		state Span waitVersionSpan;
		while (pProxyCommitData->committedVersion.get() <
		       self->commitVersion - SERVER_KNOBS->MAX_READ_TRANSACTION_LIFE_VERSIONS) {
			// This should be *extremely* rare in the real world, but knob buggification should make it happen in
			// simulation
			CODE_PROBE(true, "Semi-committed pipeline limited by MVCC window");
			//TraceEvent("ProxyWaitingForCommitted", pProxyCommitData->dbgid).detail("CommittedVersion", pProxyCommitData->committedVersion.get()).detail("NeedToCommit", commitVersion);
			waitVersionSpan = Span("MP:overMaxReadTransactionLifeVersions"_loc, span.context);
			choose {
				when(wait(pProxyCommitData->committedVersion.whenAtLeast(
				    self->commitVersion - SERVER_KNOBS->MAX_READ_TRANSACTION_LIFE_VERSIONS))) {
					wait(yield());
					break;
				}
				when(wait(pProxyCommitData->cx->onProxiesChanged())) {}
				// @todo probably there is no need to get the (entire) version vector from the sequencer
				// in this case, and if so, consider adding a flag to the request to tell the sequencer
				// to not send the version vector information.
				when(GetRawCommittedVersionReply v = wait(pProxyCommitData->master.getLiveCommittedVersion.getReply(
				         GetRawCommittedVersionRequest(waitVersionSpan.context, debugID, invalidVersion),
				         TaskPriority::GetLiveCommittedVersionReply))) {
					if (v.version > pProxyCommitData->committedVersion.get()) {
						pProxyCommitData->locked = v.locked;
						pProxyCommitData->metadataVersion = v.metadataVersion;
						pProxyCommitData->committedVersion.set(v.version);
					}

					if (pProxyCommitData->committedVersion.get() <
					    self->commitVersion - SERVER_KNOBS->MAX_READ_TRANSACTION_LIFE_VERSIONS)
						wait(delay(SERVER_KNOBS->PROXY_SPIN_DELAY));
				}
			}
		}
		waitVersionSpan = Span{};
		self->computeStart = g_network->timer_monotonic();
	}

	self->msg = self->storeCommits.back().first.get();

	if (self->debugID.present())
		g_traceBatch.addEvent(
		    "CommitDebug", self->debugID.get().first(), "CommitProxyServer.commitBatch.AfterStoreCommits");

	// txnState (transaction subsystem state) tag: message extracted from log adapter
	bool firstMessage = true;
	for (auto m : self->msg.messages) {
		if (firstMessage) {
			ASSERT(!SERVER_KNOBS->ENABLE_VERSION_VECTOR ||
			       pProxyCommitData->db->get().logSystemConfig.numLogs() == self->tpcvMap.size());
			self->toCommit.addTxsTag();
		}
		self->toCommit.writeMessage(StringRef(m.begin(), m.size()), !firstMessage);
		firstMessage = false;
	}

	if (self->prevVersion && self->commitVersion - self->prevVersion < SERVER_KNOBS->MAX_VERSIONS_IN_FLIGHT / 2)
		debug_advanceMaxCommittedVersion(UID(), self->commitVersion); //< Is this valid?

	// TraceEvent("ProxyPush", pProxyCommitData->dbgid)
	//     .detail("PrevVersion", self->prevVersion)
	//     .detail("Version", self->commitVersion)
	//     .detail("TransactionsSubmitted", trs.size())
	//     .detail("TransactionsCommitted", self->commitCount)
	//     .detail("TxsPopTo", self->msg.popTo);

	if (self->prevVersion && self->commitVersion - self->prevVersion < SERVER_KNOBS->MAX_VERSIONS_IN_FLIGHT / 2)
		debug_advanceMaxCommittedVersion(UID(), self->commitVersion);

	self->commitStartTime = now();
	pProxyCommitData->lastStartCommit = self->commitStartTime;
	Optional<std::unordered_map<uint16_t, Version>> tpcvMap = Optional<std::unordered_map<uint16_t, Version>>();
	if (SERVER_KNOBS->ENABLE_VERSION_VECTOR) {
		tpcvMap = self->tpcvMap;
	}
	if (self->pProxyCommitData->acsBuilder != nullptr) {
		// Issue acs mutation at the end of this commit batch
		addAccumulativeChecksumMutations(self);
	}
	const auto versionSet = ILogSystem::PushVersionSet{ self->prevVersion,
		                                                self->commitVersion,
		                                                pProxyCommitData->committedVersion.get(),
		                                                pProxyCommitData->minKnownCommittedVersion };
	self->loggingComplete =
	    pProxyCommitData->logSystem->push(versionSet, self->toCommit, span.context, self->debugID, tpcvMap);

	float ratio = self->toCommit.getEmptyMessageRatio();
	pProxyCommitData->stats.commitBatchingEmptyMessageRatio.addMeasurement(ratio);

	if (!self->forceRecovery) {
		ASSERT(pProxyCommitData->latestLocalCommitBatchLogging.get() == self->localBatchNumber - 1);
		pProxyCommitData->latestLocalCommitBatchLogging.set(self->localBatchNumber);
	}

	self->computeDuration += g_network->timer_monotonic() - self->computeStart;
	if (self->batchOperations > 0) {
		double estimatedDelay = computeReleaseDelay(self, self->latencyBucket);
		double computePerOperation =
		    std::min(SERVER_KNOBS->MAX_COMPUTE_PER_OPERATION, self->computeDuration / self->batchOperations);

		if (computePerOperation <= pProxyCommitData->commitComputePerOperation[self->latencyBucket]) {
			pProxyCommitData->commitComputePerOperation[self->latencyBucket] = computePerOperation;
		} else {
			pProxyCommitData->commitComputePerOperation[self->latencyBucket] =
			    SERVER_KNOBS->PROXY_COMPUTE_GROWTH_RATE * computePerOperation +
			    ((1.0 - SERVER_KNOBS->PROXY_COMPUTE_GROWTH_RATE) *
			     pProxyCommitData->commitComputePerOperation[self->latencyBucket]);
		}
		pProxyCommitData->stats.maxComputeNS =
		    std::max<int64_t>(pProxyCommitData->stats.maxComputeNS,
		                      1e9 * pProxyCommitData->commitComputePerOperation[self->latencyBucket]);
		pProxyCommitData->stats.minComputeNS =
		    std::min<int64_t>(pProxyCommitData->stats.minComputeNS,
		                      1e9 * pProxyCommitData->commitComputePerOperation[self->latencyBucket]);

		if (estimatedDelay >= SERVER_KNOBS->MAX_COMPUTE_DURATION_LOG_CUTOFF ||
		    self->computeDuration >= SERVER_KNOBS->MAX_COMPUTE_DURATION_LOG_CUTOFF) {
			TraceEvent(SevInfo, "LongComputeDuration", pProxyCommitData->dbgid)
			    .suppressFor(10.0)
			    .detail("EstimatedComputeDuration", estimatedDelay)
			    .detail("ComputeDuration", self->computeDuration)
			    .detail("ComputePerOperation", computePerOperation)
			    .detail("LatencyBucket", self->latencyBucket)
			    .detail("UpdatedComputePerOperationEstimate",
			            pProxyCommitData->commitComputePerOperation[self->latencyBucket])
			    .detail("BatchBytes", self->batchBytes)
			    .detail("BatchOperations", self->batchOperations);
		}
	}

	pProxyCommitData->stats.processingMutationDist->sampleSeconds(g_network->timer_monotonic() - postResolutionQueuing);
	return Void();
}

ACTOR Future<Void> transactionLogging(CommitBatchContext* self) {
	state double tLoggingStart = g_network->timer_monotonic();
	state ProxyCommitData* const pProxyCommitData = self->pProxyCommitData;
	state Span span("MP:transactionLogging"_loc, self->span.context);

	try {
		choose {
			when(Version ver = wait(self->loggingComplete)) {
				if (!SERVER_KNOBS->ENABLE_VERSION_VECTOR_TLOG_UNICAST) {
					pProxyCommitData->minKnownCommittedVersion =
					    std::max(pProxyCommitData->minKnownCommittedVersion, ver);
				}
			}
			when(wait(pProxyCommitData->committedVersion.whenAtLeast(self->commitVersion + 1))) {}
		}
	} catch (Error& e) {
		if (e.code() == error_code_broken_promise) {
			throw tlog_failed();
		}
		throw;
	}

	pProxyCommitData->lastCommitLatency = now() - self->commitStartTime;
	pProxyCommitData->lastCommitTime = std::max(pProxyCommitData->lastCommitTime.get(), self->commitStartTime);

	wait(yield(TaskPriority::ProxyCommitYield2));

	if (pProxyCommitData->popRemoteTxs &&
	    self->msg.popTo > (pProxyCommitData->txsPopVersions.size() ? pProxyCommitData->txsPopVersions.back().second
	                                                               : pProxyCommitData->lastTxsPop)) {
		if (pProxyCommitData->txsPopVersions.size() >= SERVER_KNOBS->MAX_TXS_POP_VERSION_HISTORY) {
			TraceEvent(SevWarnAlways, "DiscardingTxsPopHistory").suppressFor(1.0);
			pProxyCommitData->txsPopVersions.pop_front();
		}

		pProxyCommitData->txsPopVersions.emplace_back(self->commitVersion, self->msg.popTo);
	}
	pProxyCommitData->logSystem->popTxs(self->msg.popTo);
	pProxyCommitData->stats.tlogLoggingDist->sampleSeconds(g_network->timer_monotonic() - tLoggingStart);
	return Void();
}

ACTOR Future<Void> reply(CommitBatchContext* self) {
	state double replyStart = g_network->timer_monotonic();
	state ProxyCommitData* const pProxyCommitData = self->pProxyCommitData;
	state Span span("MP:reply"_loc, self->span.context);

	state const Optional<UID>& debugID = self->debugID;

	if (!SERVER_KNOBS->ENABLE_VERSION_VECTOR_TLOG_UNICAST) {
		// Do not advance min committed version at this point when version vector is enabled, as we can treat the
		// current transaction as committed only after receiving a reply from the sequencer (at which point we can
		// guarantee that all the versions prior to the current version have also been made durable).
		if (self->prevVersion && self->commitVersion - self->prevVersion < SERVER_KNOBS->MAX_VERSIONS_IN_FLIGHT / 2) {
			//TraceEvent("CPAdvanceMinVersion", self->pProxyCommitData->dbgid).detail("PrvVersion", self->prevVersion).detail("CommitVersion", self->commitVersion).detail("Master", self->pProxyCommitData->master.id().toString()).detail("TxSize", self->trs.size());
			debug_advanceMinCommittedVersion(UID(), self->commitVersion);
		}
	}

	// TraceEvent("ProxyPushed", pProxyCommitData->dbgid)
	//     .detail("PrevVersion", self->prevVersion)
	//     .detail("Version", self->commitVersion);
	if (debugID.present())
		g_traceBatch.addEvent("CommitDebug", debugID.get().first(), "CommitProxyServer.commitBatch.AfterLogPush");

	for (auto& p : self->storeCommits) {
		ASSERT(!p.second.isReady());
		p.first.get().acknowledge.send(Void());
		ASSERT(p.second.isReady());
	}

	// After logging finishes, we report the commit version to master so that every other proxy can get the most
	// up-to-date live committed version. We also maintain the invariant that master's committed version >=
	// self->committedVersion by reporting commit version first before updating self->committedVersion. Otherwise, a
	// client may get a commit version that the master is not aware of, and next GRV request may get a version less
	// than self->committedVersion.

	CODE_PROBE(pProxyCommitData->committedVersion.get() > self->commitVersion,
	           "later version was reported committed first");

	if (self->commitVersion >= pProxyCommitData->committedVersion.get()) {
		state Optional<std::set<Tag>> writtenTags;
		if (SERVER_KNOBS->ENABLE_VERSION_VECTOR) {
			writtenTags = self->writtenTags;
		}
		wait(pProxyCommitData->master.reportLiveCommittedVersion.getReply(
		    ReportRawCommittedVersionRequest(self->commitVersion,
		                                     self->lockedAfter,
		                                     self->metadataVersionAfter,
		                                     pProxyCommitData->minKnownCommittedVersion,
		                                     self->prevVersion,
		                                     writtenTags),
		    TaskPriority::ProxyMasterVersionReply));
	}

	if (debugID.present()) {
		g_traceBatch.addEvent(
		    "CommitDebug", debugID.get().first(), "CommitProxyServer.commitBatch.AfterReportRawCommittedVersion");
	}

	if (SERVER_KNOBS->ENABLE_VERSION_VECTOR_TLOG_UNICAST) {
		// We have received a reply from the sequencer, so all versions prior to the current version have been
		// made durable and we can consider the current transaction to be committed - advance min commit version now.
		if (self->prevVersion && self->commitVersion - self->prevVersion < SERVER_KNOBS->MAX_VERSIONS_IN_FLIGHT / 2) {
			//TraceEvent("CPAdvanceMinVersion", self->pProxyCommitData->dbgid).detail("PrvVersion", self->prevVersion).detail("CommitVersion", self->commitVersion).detail("Master", self->pProxyCommitData->master.id().toString()).detail("TxSize", self->trs.size());
			debug_advanceMinCommittedVersion(UID(), self->commitVersion);
		}
	}

	if (self->commitVersion > pProxyCommitData->committedVersion.get()) {
		pProxyCommitData->locked = self->lockedAfter;
		pProxyCommitData->metadataVersion = self->metadataVersionAfter;
		pProxyCommitData->committedVersion.set(self->commitVersion);
		if (SERVER_KNOBS->ENABLE_VERSION_VECTOR_TLOG_UNICAST) {
			ASSERT(self->loggingComplete.isReady());
			pProxyCommitData->minKnownCommittedVersion =
			    std::max(pProxyCommitData->minKnownCommittedVersion, self->loggingComplete.get());
		}
	}

	if (self->forceRecovery) {
		TraceEvent(SevWarn, "RestartingTxnSubsystem", pProxyCommitData->dbgid).detail("Stage", "ProxyShutdown");
		throw worker_removed();
	}

	// Send replies to clients
	// TODO: should be timer_monotonic(), but gets compared to request time, which uses g_network->timer().
	double endTime = g_network->timer();
	// Reset all to zero, used to track the correct index of each commitTransacitonRef on each resolver

	std::fill(self->nextTr.begin(), self->nextTr.end(), 0);
	std::unordered_map<uint8_t, int16_t> idCountsForKey;
	for (int t = 0; t < self->trs.size(); t++) {
		auto& tr = self->trs[t];
		if (self->committed[t] == ConflictBatch::TransactionCommitted && (!self->locked || tr.isLockAware())) {
			ASSERT_WE_THINK(self->commitVersion != invalidVersion);
			if (self->trs[t].idempotencyId.valid()) {
				idCountsForKey[uint8_t(t >> 8)] += 1;
			}
			tr.reply.send(CommitID(self->commitVersion, t, self->metadataVersionAfter));
		} else if (self->committed[t] == ConflictBatch::TransactionTooOld) {
			tr.reply.sendError(transaction_too_old());
		} else if (self->committed[t] == ConflictBatch::TransactionTenantFailure ||
		           self->committed[t] == ConflictBatch::TransactionLockReject) {
			// We already sent the error
			ASSERT(tr.reply.isSet());
		} else {
			// If enable the option to report conflicting keys from resolvers, we send back all keyranges' indices
			// through CommitID
			if (tr.transaction.report_conflicting_keys) {
				Standalone<VectorRef<int>> conflictingKRIndices;
				for (int resolverInd : self->transactionResolverMap[t]) {
					auto const& cKRs =
					    self->resolution[resolverInd]
					        .conflictingKeyRangeMap[self->nextTr[resolverInd]]; // nextTr[resolverInd] -> index of
					                                                            // this trs[t] on the resolver
					for (auto const& rCRIndex : cKRs)
						// read_conflict_range can change when sent to resolvers, mapping the index from
						// resolver-side to original index in commitTransactionRef
						conflictingKRIndices.push_back(conflictingKRIndices.arena(),
						                               self->txReadConflictRangeIndexMap[t][resolverInd][rCRIndex]);
				}
				// At least one keyRange index should be returned
				ASSERT(conflictingKRIndices.size());
				tr.reply.send(CommitID(
				    invalidVersion, t, Optional<Value>(), Optional<Standalone<VectorRef<int>>>(conflictingKRIndices)));
			} else {
				tr.reply.sendError(not_committed());
			}
		}

		// Update corresponding transaction indices on each resolver
		for (int resolverInd : self->transactionResolverMap[t])
			self->nextTr[resolverInd]++;

		// TODO: filter if pipelined with large commit
		const double duration = endTime - tr.requestTime();
		pProxyCommitData->stats.commitLatencySample.addMeasurement(duration);
		if (pProxyCommitData->latencyBandConfig.present()) {
			bool filter = self->maxTransactionBytes >
			              pProxyCommitData->latencyBandConfig.get().commitConfig.maxCommitBytes.orDefault(
			                  std::numeric_limits<int>::max());
			pProxyCommitData->stats.commitLatencyBands.addMeasurement(duration, 1, Filtered(filter));
		}
	}

	for (auto [highOrderBatchIndex, count] : idCountsForKey) {
		pProxyCommitData->expectedIdempotencyIdCountForKey.send(
		    ExpectedIdempotencyIdCountForKey{ self->commitVersion, count, highOrderBatchIndex });
	}

	if (self->pProxyCommitData->encryptMode.isEncryptionEnabled() && self->encryptionTime.present()) {
		pProxyCommitData->stats.encryptionLatencySample.addMeasurement(self->encryptionTime.get());
	}
	++pProxyCommitData->stats.commitBatchOut;
	pProxyCommitData->stats.txnCommitOut += self->trs.size();
	pProxyCommitData->stats.txnConflicts += self->trs.size() - self->commitCount;
	pProxyCommitData->stats.txnCommitOutSuccess += self->commitCount;

	if (now() - pProxyCommitData->lastCoalesceTime > SERVER_KNOBS->RESOLVER_COALESCE_TIME) {
		pProxyCommitData->lastCoalesceTime = now();
		int lastSize = pProxyCommitData->keyResolvers.size();
		auto rs = pProxyCommitData->keyResolvers.ranges();
		Version oldestVersion = self->prevVersion - SERVER_KNOBS->MAX_WRITE_TRANSACTION_LIFE_VERSIONS;
		for (auto r = rs.begin(); r != rs.end(); ++r) {
			while (r->value().size() > 1 && r->value()[1].first < oldestVersion)
				r->value().pop_front();
			if (r->value().size() && r->value().front().first < oldestVersion)
				r->value().front().first = 0;
		}
		if (SERVER_KNOBS->PROXY_USE_RESOLVER_PRIVATE_MUTATIONS) {
			// Only normal key space, because \xff key space is processed by all resolvers.
			pProxyCommitData->keyResolvers.coalesce(normalKeys);
			auto& versions = pProxyCommitData->systemKeyVersions;
			while (versions.size() > 1 && versions[1] < oldestVersion) {
				versions.pop_front();
			}
			if (!versions.empty() && versions[0] < oldestVersion) {
				versions[0] = 0;
			}
		} else {
			pProxyCommitData->keyResolvers.coalesce(allKeys);
		}
		if (pProxyCommitData->keyResolvers.size() != lastSize)
			TraceEvent("KeyResolverSize", pProxyCommitData->dbgid)
			    .detail("Size", pProxyCommitData->keyResolvers.size());
	}

	// Dynamic batching for commits
	double target_latency =
	    (now() - self->startTime) * SERVER_KNOBS->COMMIT_TRANSACTION_BATCH_INTERVAL_LATENCY_FRACTION;
	pProxyCommitData->commitBatchInterval =
	    std::max(SERVER_KNOBS->COMMIT_TRANSACTION_BATCH_INTERVAL_MIN,
	             std::min(SERVER_KNOBS->COMMIT_TRANSACTION_BATCH_INTERVAL_MAX,
	                      target_latency * SERVER_KNOBS->COMMIT_TRANSACTION_BATCH_INTERVAL_SMOOTHER_ALPHA +
	                          pProxyCommitData->commitBatchInterval *
	                              (1 - SERVER_KNOBS->COMMIT_TRANSACTION_BATCH_INTERVAL_SMOOTHER_ALPHA)));
	pProxyCommitData->stats.commitBatchingWindowSize.addMeasurement(pProxyCommitData->commitBatchInterval);
	pProxyCommitData->commitBatchesMemBytesCount -= self->currentBatchMemBytesCount;
	ASSERT_ABORT(pProxyCommitData->commitBatchesMemBytesCount >= 0);
	wait(self->releaseFuture);
	pProxyCommitData->stats.replyCommitDist->sampleSeconds(g_network->timer_monotonic() - replyStart);
	return Void();
}

// Commit one batch of transactions trs
ACTOR Future<Void> commitBatchImpl(CommitBatchContext* pContext) {
	// WARNING: this code is run at a high priority (until the first delay(0)), so it needs to do as little work as
	// possible

	pContext->stage = INITIALIZE;
	getCurrentLineage()->modify(&TransactionLineage::operation) = TransactionLineage::Operation::Commit;

	// Active load balancing runs at a very high priority (to obtain accurate estimate of memory used by commit batches)
	// so we need to downgrade here
	wait(delay(0, TaskPriority::ProxyCommit));

	pContext->pProxyCommitData->lastVersionTime = pContext->startTime;
	++pContext->pProxyCommitData->stats.commitBatchIn;
	pContext->setupTraceBatch();

	/////// Phase 1: Pre-resolution processing (CPU bound except waiting for a version # which is separately pipelined
	/// and *should* be available by now (unless empty commit); ordered; currently atomic but could yield)
	pContext->stage = PRE_RESOLUTION;
	wait(CommitBatch::preresolutionProcessing(pContext));
	if (pContext->rejected) {
		pContext->pProxyCommitData->commitBatchesMemBytesCount -= pContext->currentBatchMemBytesCount;
		return Void();
	}

	/////// Phase 2: Resolution (waiting on the network; pipelined)
	pContext->stage = RESOLUTION;
	wait(CommitBatch::getResolution(pContext));

	////// Phase 3: Post-resolution processing (CPU bound except for very rare situations; ordered; currently atomic but
	/// doesn't need to be)
	pContext->stage = POST_RESOLUTION;
	wait(CommitBatch::postResolution(pContext));

	/////// Phase 4: Logging (network bound; pipelined up to MAX_READ_TRANSACTION_LIFE_VERSIONS (limited by loop above))
	pContext->stage = TRANSACTION_LOGGING;
	wait(CommitBatch::transactionLogging(pContext));

	/////// Phase 5: Replies (CPU bound; no particular order required, though ordered execution would be best for
	/// latency)
	pContext->stage = REPLY;
	wait(CommitBatch::reply(pContext));

	pContext->stage = COMPLETE;
	return Void();
}

} // namespace CommitBatch

ACTOR Future<Void> commitBatch(ProxyCommitData* pCommitData,
                               std::vector<CommitTransactionRequest>* trs,
                               int currentBatchMemBytesCount) {

	state CommitBatch::CommitBatchContext context(pCommitData, trs, currentBatchMemBytesCount);

	Future<Void> commit = CommitBatch::commitBatchImpl(&context);

	// When encryption is enabled, cipher key fetching issue (e.g KMS outage) is detected by the
	// encryption monitor. In that case, commit timeout is expected and timeout error is suppressed. But
	// we still want to trigger recovery occasionally (with the COMMIT_PROXY_MAX_LIVENESS_TIMEOUT), in
	// the hope that the cipher key fetching issue could be resolve by recovery (e.g, if one CP have
	// networking issue connecting to EKP, and recovery may exclude the CP).
	Future<Void> livenessTimeout = timeoutErrorIfCleared(
	    commit, pCommitData->encryptionMonitor->degraded(), SERVER_KNOBS->COMMIT_PROXY_LIVENESS_TIMEOUT);

	Future<Void> maxLivenessTimeout = timeoutError(livenessTimeout, SERVER_KNOBS->COMMIT_PROXY_MAX_LIVENESS_TIMEOUT);
	try {
		wait(maxLivenessTimeout);
	} catch (Error& err) {
		TraceEvent(SevInfo, "CommitBatchFailed").detail("Stage", context.stage).detail("ErrorCode", err.code());
		throw failed_to_progress();
	}

	return Void();
}

// Add tss mapping data to the reply, if any of the included storage servers have a TSS pair
void maybeAddTssMapping(GetKeyServerLocationsReply& reply,
                        ProxyCommitData* commitData,
                        std::unordered_set<UID>& included,
                        UID ssId) {
	if (!included.contains(ssId)) {
		auto mappingItr = commitData->tssMapping.find(ssId);
		if (mappingItr != commitData->tssMapping.end()) {
			reply.resultsTssMapping.push_back(*mappingItr);
		}
		included.insert(ssId);
	}
}

void addTagMapping(GetKeyServerLocationsReply& reply, ProxyCommitData* commitData) {
	for (const auto& [_, shard] : reply.results) {
		for (auto& ssi : shard) {
			auto iter = commitData->storageCache.find(ssi.id());
			ASSERT_WE_THINK(iter != commitData->storageCache.end());
			reply.resultsTagMapping.emplace_back(ssi.id(), iter->second->tag);
		}
	}
}

ACTOR static Future<Void> doTenantIdRequest(GetTenantIdRequest req, ProxyCommitData* commitData) {
	// We can't respond to these requests until we have valid txnStateStore
	wait(commitData->validState.getFuture());
	wait(delay(0, TaskPriority::DefaultEndpoint));

	CODE_PROBE(
	    req.minTenantVersion != latestVersion, "Tenant ID request with specific version", probe::decoration::rare);
	CODE_PROBE(req.minTenantVersion == latestVersion, "Tenant ID request at latest version");

	state ErrorOr<int64_t> tenantId;
	state Version minTenantVersion =
	    req.minTenantVersion == latestVersion ? commitData->stats.lastCommitVersionAssigned + 1 : req.minTenantVersion;

	// If a large minTenantVersion is specified, we limit how long we wait for it to be available
	state Future<Void> futureVersionDelay = minTenantVersion > commitData->stats.lastCommitVersionAssigned + 1
	                                            ? delay(SERVER_KNOBS->FUTURE_VERSION_DELAY)
	                                            : Never();

	if (minTenantVersion > commitData->version.get()) {
		CODE_PROBE(true, "Tenant ID request trigger commit");
		commitData->triggerCommit.set(true);
	}

	choose {
		// Wait until we are sure that we've received metadata updates through minTenantVersion
		// If latestVersion is specified, this will wait until we have definitely received
		// updates through the version at the time we received the request
		when(wait(commitData->version.whenAtLeast(minTenantVersion))) {
			CODE_PROBE(true, "Tenant ID request wait for min version");
		}
		when(wait(futureVersionDelay)) {
			CODE_PROBE(true, "Tenant ID request future version", probe::decoration::rare);
			req.reply.sendError(future_version());
			++commitData->stats.tenantIdRequestOut;
			++commitData->stats.tenantIdRequestErrors;
			return Void();
		}
	}

	auto itr = commitData->tenantNameIndex.find(req.tenantName);
	if (itr != commitData->tenantNameIndex.end()) {
		req.reply.send(GetTenantIdReply(itr->second));
	} else {
		TraceEvent(SevWarn, "CommitProxyTenantNotFound", commitData->dbgid).detail("TenantName", req.tenantName);
		++commitData->stats.tenantIdRequestErrors;
		req.reply.sendError(tenant_not_found());
	}

	++commitData->stats.tenantIdRequestOut;
	return Void();
}

ACTOR static Future<Void> tenantIdServer(CommitProxyInterface proxy,
                                         PromiseStream<Future<Void>> addActor,
                                         ProxyCommitData* commitData) {
	loop {
		GetTenantIdRequest req = waitNext(proxy.getTenantId.getFuture());
		// WARNING: this code is run at a high priority, so it needs to do as little work as possible
		if (commitData->stats.tenantIdRequestIn.getValue() - commitData->stats.tenantIdRequestOut.getValue() >
		        SERVER_KNOBS->TENANT_ID_REQUEST_MAX_QUEUE_SIZE ||
		    (g_network->isSimulated() && !g_simulator->speedUpSimulation && BUGGIFY_WITH_PROB(0.0001))) {
			++commitData->stats.tenantIdRequestErrors;
			req.reply.sendError(commit_proxy_memory_limit_exceeded());
			TraceEvent(SevWarnAlways, "ProxyGetTenantRequestThresholdExceeded").suppressFor(60);
		} else {
			++commitData->stats.tenantIdRequestIn;
			addActor.send(doTenantIdRequest(req, commitData));
		}
	}
}

ACTOR static Future<Void> doKeyServerLocationRequest(GetKeyServerLocationsRequest req, ProxyCommitData* commitData) {
	// We can't respond to these requests until we have valid txnStateStore
	getCurrentLineage()->modify(&TransactionLineage::operation) = TransactionLineage::Operation::GetKeyServersLocations;
	getCurrentLineage()->modify(&TransactionLineage::txID) = req.spanContext.traceID;

	CODE_PROBE(req.minTenantVersion != latestVersion, "Key server location request with specific version");
	CODE_PROBE(req.minTenantVersion == latestVersion, "Key server location request at latest version");

	wait(commitData->validState.getFuture());

	state Version minVersion =
	    req.minTenantVersion == latestVersion ? commitData->stats.lastCommitVersionAssigned + 1 : req.minTenantVersion;

	wait(delay(0, TaskPriority::DefaultEndpoint));

	bool validTenant = wait(checkTenant(commitData, req.tenant.tenantId, minVersion, "GetKeyServerLocation"));

	if (!validTenant) {
		CODE_PROBE(true, "Key server location request with invalid tenant");
		++commitData->stats.keyServerLocationOut;
		req.reply.sendError(tenant_not_found());
		return Void();
	}

	std::unordered_set<UID> tssMappingsIncluded;
	GetKeyServerLocationsReply rep;

	if (req.tenant.hasTenant()) {
		req.begin = req.begin.withPrefix(req.tenant.prefix.get(), req.arena);
		if (req.end.present()) {
			req.end = req.end.get().withPrefix(req.tenant.prefix.get(), req.arena);
		}
	}

	if (!req.end.present()) {
		auto r = req.reverse ? commitData->keyInfo.rangeContainingKeyBefore(req.begin)
		                     : commitData->keyInfo.rangeContaining(req.begin);
		std::vector<StorageServerInterface> ssis;
		ssis.reserve(r.value().src_info.size());
		for (auto& it : r.value().src_info) {
			ssis.push_back(it->interf);
			maybeAddTssMapping(rep, commitData, tssMappingsIncluded, it->interf.id());
		}
		rep.results.emplace_back(TenantAPI::clampRangeToTenant(r.range(), req.tenant, req.arena), ssis);
	} else if (!req.reverse) {
		int count = 0;
		for (auto r = commitData->keyInfo.rangeContaining(req.begin);
		     r != commitData->keyInfo.ranges().end() && count < req.limit && r.begin() < req.end.get();
		     ++r) {
			std::vector<StorageServerInterface> ssis;
			ssis.reserve(r.value().src_info.size());
			for (auto& it : r.value().src_info) {
				ssis.push_back(it->interf);
				maybeAddTssMapping(rep, commitData, tssMappingsIncluded, it->interf.id());
			}
			rep.results.emplace_back(TenantAPI::clampRangeToTenant(r.range(), req.tenant, req.arena), ssis);
			count++;
		}
	} else {
		int count = 0;
		auto r = commitData->keyInfo.rangeContainingKeyBefore(req.end.get());
		while (count < req.limit && req.begin < r.end()) {
			std::vector<StorageServerInterface> ssis;
			ssis.reserve(r.value().src_info.size());
			for (auto& it : r.value().src_info) {
				ssis.push_back(it->interf);
				maybeAddTssMapping(rep, commitData, tssMappingsIncluded, it->interf.id());
			}
			rep.results.emplace_back(TenantAPI::clampRangeToTenant(r.range(), req.tenant, req.arena), ssis);
			if (r == commitData->keyInfo.ranges().begin()) {
				break;
			}
			count++;
			--r;
		}
	}
	addTagMapping(rep, commitData);
	req.reply.send(rep);
	++commitData->stats.keyServerLocationOut;
	return Void();
}

ACTOR static Future<Void> readRequestServer(CommitProxyInterface proxy,
                                            PromiseStream<Future<Void>> addActor,
                                            ProxyCommitData* commitData) {
	loop {
		GetKeyServerLocationsRequest req = waitNext(proxy.getKeyServersLocations.getFuture());
		// WARNING: this code is run at a high priority, so it needs to do as little work as possible
		if (req.limit != CLIENT_KNOBS->STORAGE_METRICS_SHARD_LIMIT && // Always do data distribution requests
		    (commitData->stats.keyServerLocationIn.getValue() - commitData->stats.keyServerLocationOut.getValue() >
		         SERVER_KNOBS->KEY_LOCATION_MAX_QUEUE_SIZE ||
		     (g_network->isSimulated() && BUGGIFY_WITH_PROB(0.001)))) {
			++commitData->stats.keyServerLocationErrors;
			req.reply.sendError(commit_proxy_memory_limit_exceeded());
			TraceEvent(SevWarnAlways, "ProxyLocationRequestThresholdExceeded").suppressFor(60);
		} else {
			++commitData->stats.keyServerLocationIn;
			addActor.send(doKeyServerLocationRequest(req, commitData));
		}
	}
}

// Right now this just proxies a call to read the system keyspace for tenant+authorization purposes, but this will
// eventually be extended to have this mapping in the transaction state store
ACTOR static Future<Void> doBlobGranuleLocationRequest(GetBlobGranuleLocationsRequest req,
                                                       ProxyCommitData* commitData) {
	if (req.reverse) {
		// FIXME: support! currently unused
		++commitData->stats.blobGranuleLocationOut;
		req.reply.sendError(unsupported_operation());
		return Void();
	}
	if (req.end.present() && req.end.get() <= req.begin) {
		++commitData->stats.blobGranuleLocationOut;
		req.reply.sendError(unsupported_operation());
		return Void();
	}
	// We can't respond to these requests until we have valid txnStateStore
	wait(commitData->validState.getFuture());
	state int i = 0;

	state Version minVersion =
	    req.minTenantVersion == latestVersion ? commitData->stats.lastCommitVersionAssigned + 1 : req.minTenantVersion;

	wait(delay(0, TaskPriority::DefaultEndpoint));

	bool validTenant = wait(checkTenant(commitData, req.tenant.tenantId, minVersion, "GetBlobGranuleLocation"));

	if (!validTenant) {
		++commitData->stats.blobGranuleLocationOut;
		req.reply.sendError(tenant_not_found());
		return Void();
	}

	state GetBlobGranuleLocationsReply rep;
	state KeyRange keyRange;
	if (req.end.present()) {
		keyRange = KeyRangeRef(req.begin, req.end.get());
	} else {
		keyRange = KeyRangeRef(req.begin, keyAfter(req.begin, req.arena));
	}
	if (req.tenant.hasTenant()) {
		keyRange = keyRange.withPrefix(req.tenant.prefix.get(), req.arena);
	}

	req.limit = std::min(req.limit, CLIENT_KNOBS->BG_TOO_MANY_GRANULES);

	ASSERT(!keyRange.empty());

	state Transaction tr(commitData->cx);
	try {
		// TODO: could skip GRV, and keeps consistent with tenant mapping? Appears to be other issues though
		// tr.setVersion(minVersion);
		// FIXME: could use streaming range read for large mappings?
		state RangeResult blobGranuleMapping;
		// add +1 to convert from range limit to boundary limit, 3 to allow for sequence of:
		// end of previous range - query begin key - start of granule - query end key - end of granule
		// to pick up the intersecting granule
		req.limit = std::max(3, req.limit + 1);
		if (req.justGranules) {
			// use krm unaligned for granules
			wait(store(
			    blobGranuleMapping,
			    krmGetRangesUnaligned(
			        &tr, blobGranuleMappingKeys.begin, keyRange, req.limit, GetRangeLimits::BYTE_LIMIT_UNLIMITED)));
		} else {
			// use aligned mapping for reads
			wait(store(
			    blobGranuleMapping,
			    krmGetRanges(
			        &tr, blobGranuleMappingKeys.begin, keyRange, req.limit, GetRangeLimits::BYTE_LIMIT_UNLIMITED)));
		}
		rep.arena.dependsOn(blobGranuleMapping.arena());
		ASSERT(blobGranuleMapping.empty() || blobGranuleMapping.size() >= 2);

		// load and decode worker interfaces if not cached
		// FIXME: potentially remove duplicate racing requests for same BW interface? Should only happen at CP startup
		// or when BW joins cluster
		std::unordered_set<UID> bwiLookedUp;
		state std::vector<Future<Optional<Value>>> bwiLookupFutures;
		for (i = 0; i < blobGranuleMapping.size() - 1; i++) {
			if (!blobGranuleMapping[i].value.size()) {
				if (req.justGranules) {
					// skip non-blobbified ranges if justGranules
					continue;
				}
				throw blob_granule_transaction_too_old();
			}

			UID workerId = decodeBlobGranuleMappingValue(blobGranuleMapping[i].value);
			if (workerId == UID() && !req.justGranules) {
				// range with no worker but set for blobbification counts for granule boundaries but not readability
				throw blob_granule_transaction_too_old();
			}

			if (!req.justGranules && !commitData->blobWorkerInterfCache.contains(workerId) &&
			    !bwiLookedUp.contains(workerId)) {
				bwiLookedUp.insert(workerId);
				bwiLookupFutures.push_back(tr.get(blobWorkerListKeyFor(workerId)));
			}
		}

		wait(waitForAll(bwiLookupFutures));

		for (auto& f : bwiLookupFutures) {
			Optional<Value> workerInterface = f.get();
			// from the time the mapping was read from the db, the associated blob worker
			// could have died and so its interface wouldn't be present as part of the blobWorkerList
			// we persist in the db. So throw blob_granule_request_failed to have client retry to get the new mapping
			if (!workerInterface.present()) {
				throw blob_granule_request_failed();
			}

			BlobWorkerInterface bwInterf = decodeBlobWorkerListValue(workerInterface.get());
			commitData->blobWorkerInterfCache[bwInterf.id()] = bwInterf;
		}

		// Mapping is valid, all worker interfaces are cached, we can populate response
		std::unordered_set<UID> interfsIncluded;
		for (i = 0; i < blobGranuleMapping.size() - 1; i++) {
			KeyRangeRef granule(blobGranuleMapping[i].key, blobGranuleMapping[i + 1].key);
			if (req.justGranules) {
				if (!blobGranuleMapping[i].value.size()) {
					continue;
				}
				rep.results.push_back({ granule, UID() });
			} else {
				// FIXME: avoid duplicate decode?
				UID workerId = decodeBlobGranuleMappingValue(blobGranuleMapping[i].value);
				rep.results.push_back({ granule, workerId });
				if (interfsIncluded.insert(workerId).second) {
					rep.bwInterfs.push_back(commitData->blobWorkerInterfCache[workerId]);
				}
			}
		}

		rep.more = blobGranuleMapping.more;
	} catch (Error& e) {
		++commitData->stats.blobGranuleLocationOut;
		if (e.code() == error_code_operation_cancelled) {
			throw;
		}
		// TODO: only specific error types?
		req.reply.sendError(e);
		return Void();
	}

	req.reply.send(rep);
	++commitData->stats.blobGranuleLocationOut;
	return Void();
}

ACTOR static Future<Void> bgReadRequestServer(CommitProxyInterface proxy,
                                              PromiseStream<Future<Void>> addActor,
                                              ProxyCommitData* commitData) {
	loop {
		GetBlobGranuleLocationsRequest req = waitNext(proxy.getBlobGranuleLocations.getFuture());
		// WARNING: this code is run at a high priority, so it needs to do as little work as possible
		if ((commitData->stats.blobGranuleLocationIn.getValue() - commitData->stats.blobGranuleLocationOut.getValue() >
		     SERVER_KNOBS->BLOB_GRANULE_LOCATION_MAX_QUEUE_SIZE) ||
		    (g_network->isSimulated() && !g_simulator->speedUpSimulation && BUGGIFY_WITH_PROB(0.0001))) {
			++commitData->stats.blobGranuleLocationErrors;
			req.reply.sendError(commit_proxy_memory_limit_exceeded());
			TraceEvent(SevWarnAlways, "ProxyBGLocationRequestThresholdExceeded", commitData->dbgid).suppressFor(60);
		} else {
			++commitData->stats.blobGranuleLocationIn;
			addActor.send(doBlobGranuleLocationRequest(req, commitData));
		}
	}
}

ACTOR static Future<Void> rejoinServer(CommitProxyInterface proxy, ProxyCommitData* commitData) {
	// We can't respond to these requests until we have valid txnStateStore
	wait(commitData->validState.getFuture());

	TraceEvent("ProxyReadyForReads", proxy.id()).log();

	loop {
		GetStorageServerRejoinInfoRequest req = waitNext(proxy.getStorageServerRejoinInfo.getFuture());
		if (commitData->txnStateStore->readValue(serverListKeyFor(req.id)).get().present()) {
			GetStorageServerRejoinInfoReply rep;
			rep.version = commitData->version.get();
			rep.tag = decodeServerTagValue(commitData->txnStateStore->readValue(serverTagKeyFor(req.id)).get().get());
			RangeResult history = commitData->txnStateStore->readRange(serverTagHistoryRangeFor(req.id)).get();
			for (int i = history.size() - 1; i >= 0; i--) {
				rep.history.push_back(
				    std::make_pair(decodeServerTagHistoryKey(history[i].key), decodeServerTagValue(history[i].value)));
			}
			auto localityKey = commitData->txnStateStore->readValue(tagLocalityListKeyFor(req.dcId)).get();
			rep.newLocality = false;
			if (localityKey.present()) {
				int8_t locality = decodeTagLocalityListValue(localityKey.get());
				if (locality != rep.tag.locality) {
					TraceEvent(SevWarnAlways, "SSRejoinedWithChangedLocality")
					    .detail("Tag", rep.tag.toString())
					    .detail("DcId", req.dcId)
					    .detail("NewLocality", locality);
				} else if (locality != rep.tag.locality) {
					uint16_t tagId = 0;
					std::vector<uint16_t> usedTags;
					auto tagKeys = commitData->txnStateStore->readRange(serverTagKeys).get();
					for (auto& kv : tagKeys) {
						Tag t = decodeServerTagValue(kv.value);
						if (t.locality == locality) {
							usedTags.push_back(t.id);
						}
					}
					auto historyKeys = commitData->txnStateStore->readRange(serverTagHistoryKeys).get();
					for (auto& kv : historyKeys) {
						Tag t = decodeServerTagValue(kv.value);
						if (t.locality == locality) {
							usedTags.push_back(t.id);
						}
					}
					std::sort(usedTags.begin(), usedTags.end());

					int usedIdx = 0;
					for (; usedTags.size() > 0 && tagId <= usedTags.end()[-1]; tagId++) {
						if (tagId < usedTags[usedIdx]) {
							break;
						} else {
							usedIdx++;
						}
					}
					rep.newTag = Tag(locality, tagId);
				}
			} else {
				ASSERT_WE_THINK(rep.tag.locality != tagLocalityUpgraded);
				TraceEvent(SevWarnAlways, "SSRejoinedWithUnknownLocality")
				    .detail("Tag", rep.tag.toString())
				    .detail("DcId", req.dcId);
			}
			rep.encryptMode = commitData->encryptMode;
			req.reply.send(rep);
		} else {
			req.reply.sendError(worker_removed());
		}
	}
}

ACTOR Future<Void> ddMetricsRequestServer(CommitProxyInterface proxy, Reference<AsyncVar<ServerDBInfo> const> db) {
	loop {
		choose {
			when(state GetDDMetricsRequest req = waitNext(proxy.getDDMetrics.getFuture())) {
				if (!db->get().distributor.present()) {
					req.reply.sendError(dd_not_found());
					continue;
				}
				ErrorOr<GetDataDistributorMetricsReply> reply =
				    wait(errorOr(db->get().distributor.get().dataDistributorMetrics.getReply(
				        GetDataDistributorMetricsRequest(req.keys, req.shardLimit))));
				if (reply.isError()) {
					req.reply.sendError(reply.getError());
				} else {
					GetDDMetricsReply newReply;
					newReply.storageMetricsList = reply.get().storageMetricsList;
					req.reply.send(newReply);
				}
			}
		}
	}
}

ACTOR Future<Void> monitorRemoteCommitted(ProxyCommitData* self) {
	loop {
		wait(delay(0)); // allow this actor to be cancelled if we are removed after db changes.
		state Optional<std::vector<OptionalInterface<TLogInterface>>> remoteLogs;
		if (self->db->get().recoveryState >= RecoveryState::ALL_LOGS_RECRUITED) {
			for (auto& logSet : self->db->get().logSystemConfig.tLogs) {
				if (!logSet.isLocal) {
					remoteLogs = logSet.tLogs;
					for (auto& tLog : logSet.tLogs) {
						if (!tLog.present()) {
							remoteLogs = Optional<std::vector<OptionalInterface<TLogInterface>>>();
							break;
						}
					}
					break;
				}
			}
		}

		if (!remoteLogs.present()) {
			wait(self->db->onChange());
			continue;
		}
		self->popRemoteTxs = true;

		state Future<Void> onChange = self->db->onChange();
		loop {
			state std::vector<Future<TLogQueuingMetricsReply>> replies;
			for (auto& it : remoteLogs.get()) {
				replies.push_back(
				    brokenPromiseToNever(it.interf().getQueuingMetrics.getReply(TLogQueuingMetricsRequest())));
			}
			wait(waitForAll(replies) || onChange);

			if (onChange.isReady()) {
				break;
			}

			// FIXME: use the configuration to calculate a more precise minimum recovery version.
			Version minVersion = std::numeric_limits<Version>::max();
			for (auto& it : replies) {
				minVersion = std::min(minVersion, it.get().v);
			}

			while (self->txsPopVersions.size() && self->txsPopVersions.front().first <= minVersion) {
				self->lastTxsPop = self->txsPopVersions.front().second;
				self->logSystem->popTxs(self->txsPopVersions.front().second, tagLocalityRemoteLog);
				self->txsPopVersions.pop_front();
			}

			wait(delay(SERVER_KNOBS->UPDATE_REMOTE_LOG_VERSION_INTERVAL) || onChange);
			if (onChange.isReady()) {
				break;
			}
		}
	}
}

ACTOR Future<Void> proxySnapCreate(ProxySnapRequest snapReq, ProxyCommitData* commitData) {
	TraceEvent("SnapCommitProxy_SnapReqEnter")
	    .detail("SnapPayload", snapReq.snapPayload)
	    .detail("SnapUID", snapReq.snapUID);
	try {
		// whitelist check
		ExecCmdValueString execArg(snapReq.snapPayload);
		StringRef binPath = execArg.getBinaryPath();
		if (!isWhitelisted(commitData->whitelistedBinPathVec, binPath)) {
			TraceEvent("SnapCommitProxy_WhiteListCheckFailed")
			    .detail("SnapPayload", snapReq.snapPayload)
			    .detail("SnapUID", snapReq.snapUID);
			throw snap_path_not_whitelisted();
		}
		// db fully recovered check
		if (commitData->db->get().recoveryState != RecoveryState::FULLY_RECOVERED) {
			// Cluster is not fully recovered and needs TLogs
			// from previous generation for full recovery.
			// Currently, snapshot of old tlog generation is not
			// supported and hence failing the snapshot request until
			// cluster is fully_recovered.
			TraceEvent("SnapCommitProxy_ClusterNotFullyRecovered")
			    .detail("SnapPayload", snapReq.snapPayload)
			    .detail("SnapUID", snapReq.snapUID);
			throw snap_not_fully_recovered_unsupported();
		}

		auto result = commitData->txnStateStore->readValue("log_anti_quorum"_sr.withPrefix(configKeysPrefix)).get();
		int logAntiQuorum = 0;
		if (result.present()) {
			logAntiQuorum = atoi(result.get().toString().c_str());
		}
		// FIXME: logAntiQuorum not supported, remove it later,
		// In version2, we probably don't need this limitation, but this needs to be tested.
		if (logAntiQuorum > 0) {
			TraceEvent("SnapCommitProxy_LogAntiQuorumNotSupported")
			    .detail("SnapPayload", snapReq.snapPayload)
			    .detail("SnapUID", snapReq.snapUID);
			throw snap_log_anti_quorum_unsupported();
		}

		state int snapReqRetry = 0;
		state double snapRetryBackoff = FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY;
		loop {
			// send a snap request to DD
			if (!commitData->db->get().distributor.present()) {
				TraceEvent(SevWarnAlways, "DataDistributorNotPresent").detail("Operation", "SnapRequest");
				throw dd_not_found();
			}
			try {
				Future<ErrorOr<Void>> ddSnapReq =
				    commitData->db->get().distributor.get().distributorSnapReq.tryGetReply(
				        DistributorSnapRequest(snapReq.snapPayload, snapReq.snapUID));
				wait(throwErrorOr(ddSnapReq));
				break;
			} catch (Error& e) {
				TraceEvent("SnapCommitProxy_DDSnapResponseError")
				    .errorUnsuppressed(e)
				    .detail("SnapPayload", snapReq.snapPayload)
				    .detail("SnapUID", snapReq.snapUID)
				    .detail("Retry", snapReqRetry);
				// Retry if we have network issues
				if (e.code() != error_code_request_maybe_delivered ||
				    ++snapReqRetry > SERVER_KNOBS->SNAP_NETWORK_FAILURE_RETRY_LIMIT)
					throw e;
				wait(delay(snapRetryBackoff));
				snapRetryBackoff = snapRetryBackoff * 2; // exponential backoff
			}
		}
		snapReq.reply.send(Void());
	} catch (Error& e) {
		TraceEvent("SnapCommitProxy_SnapReqError")
		    .errorUnsuppressed(e)
		    .detail("SnapPayload", snapReq.snapPayload)
		    .detail("SnapUID", snapReq.snapUID);
		if (e.code() != error_code_operation_cancelled) {
			snapReq.reply.sendError(e);
		} else {
			throw e;
		}
	}
	TraceEvent("SnapCommitProxy_SnapReqExit")
	    .detail("SnapPayload", snapReq.snapPayload)
	    .detail("SnapUID", snapReq.snapUID);
	return Void();
}

ACTOR Future<Void> proxyCheckSafeExclusion(Reference<AsyncVar<ServerDBInfo> const> db,
                                           ExclusionSafetyCheckRequest req) {
	TraceEvent("SafetyCheckCommitProxyBegin").log();
	state ExclusionSafetyCheckReply reply(false);
	if (!db->get().distributor.present()) {
		TraceEvent(SevWarnAlways, "DataDistributorNotPresent").detail("Operation", "ExclusionSafetyCheck");
		req.reply.send(reply);
		return Void();
	}
	try {
		state Future<ErrorOr<DistributorExclusionSafetyCheckReply>> ddSafeFuture =
		    db->get().distributor.get().distributorExclCheckReq.tryGetReply(
		        DistributorExclusionSafetyCheckRequest(req.exclusions));
		DistributorExclusionSafetyCheckReply _reply = wait(throwErrorOr(ddSafeFuture));
		reply.safe = _reply.safe;
		if (db->get().blobManager.present()) {
			TraceEvent("SafetyCheckCommitProxyBM").detail("BMID", db->get().blobManager.get().id());
			state Future<ErrorOr<BlobManagerExclusionSafetyCheckReply>> bmSafeFuture =
			    db->get().blobManager.get().blobManagerExclCheckReq.tryGetReply(
			        BlobManagerExclusionSafetyCheckRequest(req.exclusions));
			BlobManagerExclusionSafetyCheckReply _reply = wait(throwErrorOr(bmSafeFuture));
			reply.safe &= _reply.safe;
		} else {
			TraceEvent("SafetyCheckCommitProxyNoBM");
		}
	} catch (Error& e) {
		TraceEvent("SafetyCheckCommitProxyResponseError").error(e);
		if (e.code() != error_code_operation_cancelled) {
			req.reply.sendError(e);
			return Void();
		} else {
			throw e;
		}
	}
	TraceEvent("SafetyCheckCommitProxyFinish").log();
	req.reply.send(reply);
	return Void();
}

ACTOR Future<Void> reportTxnTagCommitCost(UID myID,
                                          Reference<AsyncVar<ServerDBInfo> const> db,
                                          UIDTransactionTagMap<TransactionCommitCostEstimation>* ssTrTagCommitCost) {
	state Future<Void> nextRequestTimer = Never();
	state Future<Void> nextReply = Never();
	if (db->get().ratekeeper.present())
		nextRequestTimer = Void();
	loop choose {
		when(wait(db->onChange())) {
			if (db->get().ratekeeper.present()) {
				TraceEvent("ProxyRatekeeperChanged", myID).detail("RKID", db->get().ratekeeper.get().id());
				nextRequestTimer = Void();
			} else {
				TraceEvent("ProxyRatekeeperDied", myID).log();
				nextRequestTimer = Never();
			}
		}
		when(wait(nextRequestTimer)) {
			nextRequestTimer = Never();
			if (db->get().ratekeeper.present()) {
				nextReply = brokenPromiseToNever(db->get().ratekeeper.get().reportCommitCostEstimation.getReply(
				    ReportCommitCostEstimationRequest(std::move(*ssTrTagCommitCost))));
				ssTrTagCommitCost->clear();
			} else {
				nextReply = Never();
			}
		}
		when(wait(nextReply)) {
			nextReply = Never();
			nextRequestTimer = delay(SERVER_KNOBS->REPORT_TRANSACTION_COST_ESTIMATION_DELAY);
		}
	}
}

// Get the list of tenants that are over the storage quota from the data distributor for quota enforcement.
ACTOR Future<Void> monitorTenantsOverStorageQuota(UID myID,
                                                  Reference<AsyncVar<ServerDBInfo> const> db,
                                                  ProxyCommitData* commitData) {
	state Future<Void> nextRequestTimer = Never();
	state Future<TenantsOverStorageQuotaReply> nextReply = Never();
	if (db->get().distributor.present())
		nextRequestTimer = Void();
	loop choose {
		when(wait(db->onChange())) {
			if (db->get().distributor.present()) {
				CODE_PROBE(true, "ServerDBInfo changed during monitorTenantsOverStorageQuota");
				TraceEvent("ServerDBInfoChanged", myID).detail("DDID", db->get().distributor.get().id());
				nextRequestTimer = Void();
			} else {
				TraceEvent("DataDistributorDied", myID).log();
				nextRequestTimer = Never();
			}
		}
		when(wait(nextRequestTimer)) {
			nextRequestTimer = Never();
			if (db->get().distributor.present()) {
				nextReply = brokenPromiseToNever(
				    db->get().distributor.get().tenantsOverStorageQuota.getReply(TenantsOverStorageQuotaRequest()));
			} else {
				nextReply = Never();
			}
		}
		when(TenantsOverStorageQuotaReply reply = wait(nextReply)) {
			nextReply = Never();
			commitData->tenantsOverStorageQuota = reply.tenants;
			TraceEvent(SevDebug, "MonitorTenantsOverStorageQuota")
			    .detail("NumTenants", commitData->tenantsOverStorageQuota.size());
			nextRequestTimer = delay(SERVER_KNOBS->CP_FETCH_TENANTS_OVER_STORAGE_QUOTA_INTERVAL);
		}
	}
}

namespace {
struct ExpireServerEntry {
	int64_t timeReceived;
	int expectedCount = 0;
	int receivedCount = 0;
	bool initialized = false;
};

struct IdempotencyKey {
	Version version;
	uint8_t highOrderBatchIndex;
	bool operator==(const IdempotencyKey& other) const {
		return version == other.version && highOrderBatchIndex == other.highOrderBatchIndex;
	}
};

} // namespace

namespace std {
template <>
struct hash<IdempotencyKey> {
	std::size_t operator()(const IdempotencyKey& key) const {
		std::size_t seed = 0;
		boost::hash_combine(seed, std::hash<Version>{}(key.version));
		boost::hash_combine(seed, std::hash<uint8_t>{}(key.highOrderBatchIndex));
		return seed;
	}
};

} // namespace std

ACTOR static Future<Void> idempotencyIdsExpireServer(
    Database db,
    PublicRequestStream<ExpireIdempotencyIdRequest> expireIdempotencyId,
    PromiseStream<ExpectedIdempotencyIdCountForKey> expectedIdempotencyIdCountForKey,
    Standalone<VectorRef<MutationRef>>* idempotencyClears) {
	state std::unordered_map<IdempotencyKey, ExpireServerEntry> idStatus;
	state std::unordered_map<IdempotencyKey, ExpireServerEntry>::iterator iter;
	state int64_t purgeBefore;
	state IdempotencyKey key;
	state ExpireServerEntry* status = nullptr;
	state Future<Void> purgeOld = Void();
	loop {
		choose {
			when(ExpireIdempotencyIdRequest req = waitNext(expireIdempotencyId.getFuture())) {
				key = IdempotencyKey{ req.commitVersion, req.batchIndexHighByte };
				status = &idStatus[key];
				status->receivedCount += 1;
				CODE_PROBE(status->expectedCount == 0, "ExpireIdempotencyIdRequest received before count is known");
				if (status->expectedCount > 0) {
					ASSERT_LE(status->receivedCount, status->expectedCount);
				}
			}
			when(ExpectedIdempotencyIdCountForKey req = waitNext(expectedIdempotencyIdCountForKey.getFuture())) {
				key = IdempotencyKey{ req.commitVersion, req.batchIndexHighByte };
				status = &idStatus[key];
				ASSERT_EQ(status->expectedCount, 0);
				status->expectedCount = req.idempotencyIdCount;
			}
			when(wait(purgeOld)) {
				purgeOld = delay(SERVER_KNOBS->IDEMPOTENCY_ID_IN_MEMORY_LIFETIME);
				purgeBefore = now() - SERVER_KNOBS->IDEMPOTENCY_ID_IN_MEMORY_LIFETIME;
				for (iter = idStatus.begin(); iter != idStatus.end();) {
					// We have exclusive access to idStatus in this when block, so iter will still be valid after the
					// wait
					wait(yield());
					if (iter->second.timeReceived < purgeBefore) {
						iter = idStatus.erase(iter);
					} else {
						++iter;
					}
				}
				continue;
			}
		}
		if (status->initialized) {
			if (status->receivedCount == status->expectedCount) {
				auto keyRange =
				    makeIdempotencySingleKeyRange(idempotencyClears->arena(), key.version, key.highOrderBatchIndex);
				idempotencyClears->push_back(idempotencyClears->arena(),
				                             MutationRef(MutationRef::ClearRange, keyRange.begin, keyRange.end));
				idStatus.erase(key);
			}
		} else {
			status->timeReceived = now();
			status->initialized = true;
		}
	}
}

namespace {

struct TransactionStateResolveContext {
	// Maximum sequence for txnStateRequest, this is defined when the request last flag is set.
	Sequence maxSequence = std::numeric_limits<Sequence>::max();

	// Flags marks received transaction state requests, we only process the transaction request when *all* requests are
	// received.
	std::unordered_set<Sequence> receivedSequences;

	ProxyCommitData* pCommitData = nullptr;

	// Pointer to transaction state store, shortcut for commitData.txnStateStore
	IKeyValueStore* pTxnStateStore = nullptr;

	Future<Void> txnRecovery;

	// Actor streams
	PromiseStream<Future<Void>>* pActors = nullptr;

	// Flag reports if the transaction state request is complete. This request should only happen during recover, i.e.
	// once per commit proxy.
	bool processed = false;

	TransactionStateResolveContext() = default;

	TransactionStateResolveContext(ProxyCommitData* pCommitData_, PromiseStream<Future<Void>>* pActors_)
	  : pCommitData(pCommitData_), pTxnStateStore(pCommitData_->txnStateStore), pActors(pActors_) {
		ASSERT(pTxnStateStore != nullptr);
	}
};

ACTOR Future<Void> processCompleteTransactionStateRequest(TransactionStateResolveContext* pContext) {
	state KeyRange txnKeys = allKeys;
	state std::map<Tag, UID> tag_uid;

	RangeResult UIDtoTagMap = pContext->pTxnStateStore->readRange(serverTagKeys).get();
	for (const KeyValueRef& kv : UIDtoTagMap) {
		tag_uid[decodeServerTagValue(kv.value)] = decodeServerTagKey(kv.key);
	}

	state std::unordered_map<EncryptCipherDomainId, Reference<BlobCipherKey>> systemCipherKeys;
	if (pContext->pCommitData->encryptMode.isEncryptionEnabled()) {
		std::unordered_map<EncryptCipherDomainId, Reference<BlobCipherKey>> cks = wait(
		    GetEncryptCipherKeys<ServerDBInfo>::getLatestEncryptCipherKeys(pContext->pCommitData->db,
		                                                                   ENCRYPT_CIPHER_SYSTEM_DOMAINS,
		                                                                   BlobCipherMetrics::TLOG,
		                                                                   pContext->pCommitData->encryptionMonitor));
		systemCipherKeys = cks;
	}

	loop {
		wait(yield());

		RangeResult data =
		    pContext->pTxnStateStore
		        ->readRange(txnKeys, SERVER_KNOBS->BUGGIFIED_ROW_LIMIT, SERVER_KNOBS->APPLY_MUTATION_BYTES)
		        .get();
		if (!data.size())
			break;

		((KeyRangeRef&)txnKeys) = KeyRangeRef(keyAfter(data.back().key, txnKeys.arena()), txnKeys.end);

		MutationsVec mutations;
		std::vector<std::pair<MapPair<Key, ServerCacheInfo>, int>> keyInfoData;
		std::vector<UID> src, dest;
		ServerCacheInfo info;
		// NOTE: An ACTOR will be compiled into several classes, the this pointer is from one of them.
		auto updateTagInfo = [pContext = pContext](const std::vector<UID>& uids,
		                                           std::vector<Tag>& tags,
		                                           std::vector<Reference<StorageInfo>>& storageInfoItems) {
			for (const auto& id : uids) {
				auto storageInfo = getStorageInfo(id, &pContext->pCommitData->storageCache, pContext->pTxnStateStore);
				ASSERT(storageInfo->tag != invalidTag);
				tags.push_back(storageInfo->tag);
				storageInfoItems.push_back(storageInfo);
			}
		};
		for (auto& kv : data) {
			if (kv.key.startsWith(keyServersPrefix)) {
				KeyRef k = kv.key.removePrefix(keyServersPrefix);
				if (k == allKeys.end) {
					continue;
				}
				decodeKeyServersValue(tag_uid, kv.value, src, dest);

				info.tags.clear();

				info.src_info.clear();
				updateTagInfo(src, info.tags, info.src_info);

				info.dest_info.clear();
				updateTagInfo(dest, info.tags, info.dest_info);

				uniquify(info.tags);
				keyInfoData.emplace_back(MapPair<Key, ServerCacheInfo>(k, info), 1);
			} else if (kv.key.startsWith(rangeLockPrefix)) {
				if (pContext->pCommitData->rangeLockEnabled()) {
					ASSERT(pContext->pCommitData->rangeLock != nullptr);
					Key keyInsert = kv.key.removePrefix(rangeLockPrefix);
					pContext->pCommitData->rangeLock->initKeyPoint(keyInsert, kv.value);
				}
			} else {
				mutations.emplace_back(mutations.arena(), MutationRef::SetValue, kv.key, kv.value);
				continue;
			}
		}

		// insert keyTag data separately from metadata mutations so that we can do one bulk insert which
		// avoids a lot of map lookups.
		pContext->pCommitData->keyInfo.rawInsert(keyInfoData);

		Arena arena;
		bool confChanges;
		CODE_PROBE(
		    pContext->pCommitData->encryptMode.isEncryptionEnabled(),
		    "Commit proxy apply metadata mutations from txnStateStore on recovery, with encryption-at-rest enabled");
		applyMetadataMutations(SpanContext(),
		                       *pContext->pCommitData,
		                       arena,
		                       Reference<ILogSystem>(),
		                       mutations,
		                       /* pToCommit= */ nullptr,
		                       &systemCipherKeys,
		                       pContext->pCommitData->encryptMode,
		                       confChanges,
		                       /* version= */ 0,
		                       /* popVersion= */ 0,
		                       /* initialCommit= */ true,
		                       /* provisionalCommitProxy */ pContext->pCommitData->provisional);
	} // loop

	auto lockedKey = pContext->pTxnStateStore->readValue(databaseLockedKey).get();
	pContext->pCommitData->locked = lockedKey.present() && lockedKey.get().size();
	pContext->pCommitData->metadataVersion = pContext->pTxnStateStore->readValue(metadataVersionKey).get();

	pContext->pTxnStateStore->enableSnapshot();

	return Void();
}

ACTOR Future<Void> processTransactionStateRequestPart(TransactionStateResolveContext* pContext,
                                                      TxnStateRequest request) {
	ASSERT(pContext->pCommitData != nullptr);
	ASSERT(pContext->pActors != nullptr);

	if (pContext->receivedSequences.contains(request.sequence)) {
		if (pContext->receivedSequences.size() == pContext->maxSequence) {
			wait(pContext->txnRecovery);
		}
		// This part is already received. Still we will re-broadcast it to other CommitProxies
		pContext->pActors->send(broadcastTxnRequest(request, SERVER_KNOBS->TXN_STATE_SEND_AMOUNT, true));
		wait(yield());
		return Void();
	}

	if (request.last) {
		// This is the last piece of subsequence, yet other pieces might still on the way.
		pContext->maxSequence = request.sequence + 1;
	}
	pContext->receivedSequences.insert(request.sequence);

	// Although we may receive the CommitTransactionRequest for the recovery transaction before all of the
	// TxnStateRequest, we will not get a resolution result from any resolver until the master has submitted its initial
	// (sequence 0) resolution request, which it doesn't do until we have acknowledged all TxnStateRequests
	ASSERT(!pContext->pCommitData->validState.isSet());

	for (auto& kv : request.data) {
		pContext->pTxnStateStore->set(kv, &request.arena);
	}
	pContext->pTxnStateStore->commit(true);

	if (pContext->receivedSequences.size() == pContext->maxSequence) {
		// Received all components of the txnStateRequest
		ASSERT(!pContext->processed);
		pContext->txnRecovery = processCompleteTransactionStateRequest(pContext);
		wait(pContext->txnRecovery);
		pContext->processed = true;
	}

	pContext->pActors->send(broadcastTxnRequest(request, SERVER_KNOBS->TXN_STATE_SEND_AMOUNT, true));
	wait(yield());
	return Void();
}

} // anonymous namespace

//
// Metrics related to the commit proxy are logged on a five second interval in
// the `ProxyMetrics` trace. However, it can be hard to determine workload
// burstiness when looking at such a large time range. This function adds much
// more frequent logging for certain metrics to provide fine-grained insight
// into workload patterns. The metrics logged by this function break down into
// two categories:
//
//   * existing counters reported by `ProxyMetrics`
//   * new counters that are only reported by this function
//
// Neither is implemented optimally, but the data collected should be helpful
// in identifying workload patterns on the server.
//
// Metrics reporting by this function can be disabled by setting the
// `BURSTINESS_METRICS_ENABLED` knob to false. The reporting interval can be
// adjusted by modifying the knob `BURSTINESS_METRICS_LOG_INTERVAL`.
//
ACTOR Future<Void> logDetailedMetrics(ProxyCommitData* commitData) {
	state double startTime = 0;
	state int64_t commitBatchInBaseline = 0;
	state int64_t txnCommitInBaseline = 0;
	state int64_t mutationsBaseline = 0;
	state int64_t mutationBytesBaseline = 0;

	loop {
		if (!SERVER_KNOBS->BURSTINESS_METRICS_ENABLED) {
			return Void();
		}

		startTime = now();
		commitBatchInBaseline = commitData->stats.commitBatchIn.getValue();
		txnCommitInBaseline = commitData->stats.txnCommitIn.getValue();
		mutationsBaseline = commitData->stats.mutations.getValue();
		mutationBytesBaseline = commitData->stats.mutationBytes.getValue();

		wait(delay(SERVER_KNOBS->BURSTINESS_METRICS_LOG_INTERVAL));

		int64_t commitBatchInReal = commitData->stats.commitBatchIn.getValue();
		int64_t txnCommitInReal = commitData->stats.txnCommitIn.getValue();
		int64_t mutationsReal = commitData->stats.mutations.getValue();
		int64_t mutationBytesReal = commitData->stats.mutationBytes.getValue();

		// Don't log anything if any of the counters got reset during the wait
		// interval. Assume that typically all the counters get reset at once.
		if (commitBatchInReal < commitBatchInBaseline || txnCommitInReal < txnCommitInBaseline ||
		    mutationsReal < mutationsBaseline || mutationBytesReal < mutationBytesBaseline) {
			continue;
		}

		TraceEvent("ProxyDetailedMetrics")
		    .detail("Elapsed", now() - startTime)
		    .detail("CommitBatchIn", commitBatchInReal - commitBatchInBaseline)
		    .detail("TxnCommitIn", txnCommitInReal - txnCommitInBaseline)
		    .detail("Mutations", mutationsReal - mutationsBaseline)
		    .detail("MutationBytes", mutationBytesReal - mutationBytesBaseline)
		    .detail("UniqueClients", commitData->stats.getSizeAndResetUniqueClients());
	}
}

ACTOR Future<Void> commitProxyServerCore(CommitProxyInterface proxy,
                                         MasterInterface master,
                                         LifetimeToken masterLifetime,
                                         Reference<AsyncVar<ServerDBInfo> const> db,
                                         LogEpoch epoch,
                                         Version recoveryTransactionVersion,
                                         bool firstProxy,
                                         std::string whitelistBinPaths,
                                         EncryptionAtRestMode encryptMode,
                                         bool provisional,
                                         uint16_t commitProxyIndex) {
	state ProxyCommitData commitData(proxy.id(),
	                                 master,
	                                 proxy.getConsistentReadVersion,
	                                 recoveryTransactionVersion,
	                                 proxy.commit,
	                                 db,
	                                 firstProxy,
	                                 encryptMode,
	                                 provisional,
	                                 commitProxyIndex,
	                                 epoch);

	state Future<Sequence> sequenceFuture = (Sequence)0;
	state PromiseStream<std::pair<std::vector<CommitTransactionRequest>, int>> batchedCommits;
	state Future<Void> commitBatcherActor;
	state Future<Void> lastCommitComplete = Void();

	state PromiseStream<Future<Void>> addActor;
	state Future<Void> onError = transformError(actorCollection(addActor.getFuture()), broken_promise(), tlog_failed());

	TraceEvent("CPEncryptionAtRestMode", proxy.id()).detail("Mode", commitData.encryptMode);

	addActor.send(waitFailureServer(proxy.waitFailure.getFuture()));
	addActor.send(traceRole(Role::COMMIT_PROXY, proxy.id()));

	//TraceEvent("CommitProxyInit1", proxy.id());

	// Wait until we can load the "real" logsystem, since we don't support switching them currently
	while (
	    !(masterLifetime.isEqual(commitData.db->get().masterLifetime) &&
	      commitData.db->get().recoveryState >= RecoveryState::RECOVERY_TRANSACTION &&
	      (!commitData.encryptMode.isEncryptionEnabled() || commitData.db->get().client.encryptKeyProxy.present()))) {
		//TraceEvent("ProxyInit2", proxy.id()).detail("LSEpoch", db->get().logSystemConfig.epoch).detail("Need", epoch);
		wait(commitData.db->onChange());
	}
	state Future<Void> dbInfoChange = commitData.db->onChange();
	//TraceEvent("ProxyInit3", proxy.id());

	commitData.resolvers = commitData.db->get().resolvers;
	commitData.localTLogCount = commitData.db->get().logSystemConfig.numLogs();
	ASSERT(commitData.resolvers.size() != 0);
	for (int i = 0; i < commitData.resolvers.size(); ++i) {
		commitData.stats.resolverDist.push_back(Histogram::getHistogram(
		    "CommitProxy"_sr, "ToResolver_" + commitData.resolvers[i].id().toString(), Histogram::Unit::milliseconds));
	}

	// Initialize keyResolvers map
	auto rs = commitData.keyResolvers.modify(SERVER_KNOBS->PROXY_USE_RESOLVER_PRIVATE_MUTATIONS ? normalKeys : allKeys);
	for (auto r = rs.begin(); r != rs.end(); ++r)
		r->value().emplace_back(0, 0);
	commitData.systemKeyVersions.push_back(0);

	commitData.logSystem = ILogSystem::fromServerDBInfo(proxy.id(), commitData.db->get(), false, addActor);
	commitData.logAdapter =
	    new LogSystemDiskQueueAdapter(commitData.logSystem, Reference<AsyncVar<PeekTxsInfo>>(), 1, false);
	// TODO: Pass the encrypt mode once supported in IKeyValueStore
	commitData.txnStateStore = keyValueStoreLogSystem(
	    commitData.logAdapter, commitData.db, proxy.id(), 2e9, true, true, true, encryptMode.isEncryptionEnabled());
	createWhitelistBinPathVec(whitelistBinPaths, commitData.whitelistedBinPathVec);

	commitData.updateLatencyBandConfig(commitData.db->get().latencyBandConfig);

	// ((SERVER_MEM_LIMIT * COMMIT_BATCHES_MEM_FRACTION_OF_TOTAL) / COMMIT_BATCHES_MEM_TO_TOTAL_MEM_SCALE_FACTOR) is
	// only a approximate formula for limiting the memory used. COMMIT_BATCHES_MEM_TO_TOTAL_MEM_SCALE_FACTOR is an
	// estimate based on experiments and not an accurate one.
	state int64_t commitBatchesMemoryLimit = SERVER_KNOBS->COMMIT_BATCHES_MEM_BYTES_HARD_LIMIT;
	if (SERVER_KNOBS->SERVER_MEM_LIMIT > 0) {
		commitBatchesMemoryLimit = std::min(
		    commitBatchesMemoryLimit,
		    static_cast<int64_t>((SERVER_KNOBS->SERVER_MEM_LIMIT * SERVER_KNOBS->COMMIT_BATCHES_MEM_FRACTION_OF_TOTAL) /
		                         SERVER_KNOBS->COMMIT_BATCHES_MEM_TO_TOTAL_MEM_SCALE_FACTOR));
	}
	TraceEvent(SevInfo, "CommitBatchesMemoryLimit").detail("BytesLimit", commitBatchesMemoryLimit);

	// Initialize RangeLock
	if (commitData.rangeLockEnabled()) {
		commitData.rangeLock = std::make_shared<RangeLock>(&commitData);
		TraceEvent(SevInfo, "CommitProxyRangeLockEnabled", commitData.dbgid);
	}

	addActor.send(monitorRemoteCommitted(&commitData));
	addActor.send(tenantIdServer(proxy, addActor, &commitData));
	addActor.send(readRequestServer(proxy, addActor, &commitData));
	addActor.send(bgReadRequestServer(proxy, addActor, &commitData));
	addActor.send(rejoinServer(proxy, &commitData));
	addActor.send(ddMetricsRequestServer(proxy, db));
	addActor.send(reportTxnTagCommitCost(proxy.id(), db, &commitData.ssTrTagCommitCost));
	addActor.send(logDetailedMetrics(&commitData));

	auto openDb = openDBOnServer(db);

	if (firstProxy) {
		addActor.send(recurringAsync(
		    [openDb = openDb]() { return cleanIdempotencyIds(openDb, SERVER_KNOBS->IDEMPOTENCY_IDS_MIN_AGE_SECONDS); },
		    SERVER_KNOBS->IDEMPOTENCY_IDS_CLEANER_POLLING_INTERVAL,
		    true,
		    SERVER_KNOBS->IDEMPOTENCY_IDS_CLEANER_POLLING_INTERVAL));
	}
	addActor.send(idempotencyIdsExpireServer(
	    openDb, proxy.expireIdempotencyId, commitData.expectedIdempotencyIdCountForKey, &commitData.idempotencyClears));

	if (SERVER_KNOBS->STORAGE_QUOTA_ENABLED) {
		addActor.send(monitorTenantsOverStorageQuota(proxy.id(), db, &commitData));
	}

	// wait for txnStateStore recovery
	wait(success(commitData.txnStateStore->readValue(StringRef())));

	int commitBatchByteLimit =
	    (int)std::min<double>(SERVER_KNOBS->COMMIT_TRANSACTION_BATCH_BYTES_MAX,
	                          std::max<double>(SERVER_KNOBS->COMMIT_TRANSACTION_BATCH_BYTES_MIN,
	                                           SERVER_KNOBS->COMMIT_TRANSACTION_BATCH_BYTES_SCALE_BASE *
	                                               pow(commitData.db->get().client.commitProxies.size(),
	                                                   SERVER_KNOBS->COMMIT_TRANSACTION_BATCH_BYTES_SCALE_POWER)));

	commitBatcherActor = commitBatcher(
	    &commitData, batchedCommits, proxy.commit.getFuture(), commitBatchByteLimit, commitBatchesMemoryLimit);

	// This has to be declared after the commitData.txnStateStore get initialized
	state TransactionStateResolveContext transactionStateResolveContext(&commitData, &addActor);

	loop choose {
		when(wait(dbInfoChange)) {
			dbInfoChange = commitData.db->onChange();
			if (masterLifetime.isEqual(commitData.db->get().masterLifetime) &&
			    commitData.db->get().recoveryState >= RecoveryState::RECOVERY_TRANSACTION) {
				commitData.logSystem = ILogSystem::fromServerDBInfo(proxy.id(), commitData.db->get(), false, addActor);
				for (auto it : commitData.tag_popped) {
					commitData.logSystem->pop(it.second, it.first);
				}
				commitData.logSystem->popTxs(commitData.lastTxsPop, tagLocalityRemoteLog);
			}

			commitData.updateLatencyBandConfig(commitData.db->get().latencyBandConfig);
		}
		when(wait(onError)) {}
		when(std::pair<std::vector<CommitTransactionRequest>, int> batchedRequests =
		         waitNext(batchedCommits.getFuture())) {
			// WARNING: this code is run at a high priority, so it needs to do as little work as possible
			/*
			TraceEvent("CommitProxyCTR", proxy.id())
			    .detail("CommitTransactions", trs.size())
			    .detail("TransactionRate", transactionRate)
			    .detail("TransactionQueue", transactionQueue.size())
			    .detail("ReleasedTransactionCount", transactionCount);
			TraceEvent("CommitProxyCore", commitData.dbgid)
			    .detail("TxSize", trs.size())
			    .detail("MasterLifetime", masterLifetime.toString())
			    .detail("DbMasterLifetime", commitData.db->get().masterLifetime.toString())
			    .detail("RecoveryState", commitData.db->get().recoveryState)
			    .detail("CCInf", commitData.db->get().clusterInterface.id().toString());
			*/
			const std::vector<CommitTransactionRequest>& trs = batchedRequests.first;
			const int batchBytes = batchedRequests.second;
			if (trs.size() ||
			    (commitData.db->get().recoveryState >= RecoveryState::ACCEPTING_COMMITS &&
			     masterLifetime.isEqual(commitData.db->get().masterLifetime) && lastCommitComplete.isReady())) {

				lastCommitComplete =
				    commitBatch(&commitData,
				                const_cast<std::vector<CommitTransactionRequest>*>(&batchedRequests.first),
				                batchBytes);

				addActor.send(lastCommitComplete);
			}
		}
		when(ProxySnapRequest snapReq = waitNext(proxy.proxySnapReq.getFuture())) {
			TraceEvent(SevDebug, "SnapMasterEnqueue").log();
			addActor.send(proxySnapCreate(snapReq, &commitData));
		}
		when(ExclusionSafetyCheckRequest exclCheckReq = waitNext(proxy.exclusionSafetyCheckReq.getFuture())) {
			addActor.send(proxyCheckSafeExclusion(db, exclCheckReq));
		}
		when(TxnStateRequest request = waitNext(proxy.txnState.getFuture())) {
			addActor.send(processTransactionStateRequestPart(&transactionStateResolveContext, request));
		}
		when(SetThrottledShardRequest request = waitNext(proxy.setThrottledShard.getFuture())) {
			for (auto& shard : request.throttledShards) {
				auto it = commitData.hotShards.begin();
				for (; it != commitData.hotShards.end(); ++it) {
					if (it->first == shard) {
						it->second = request.expirationTime;
						break;
					}
				}
				if (it == commitData.hotShards.end()) {
					commitData.hotShards.emplace_back(std::make_pair(shard, request.expirationTime));
				}
			}
			// TraceEvent(SevDebug, "ReceivedSetThrottledShards").detail("NumHotShards", commitData.hotShards.size());
		}
	}
}

// only update the local Db info if the CP is not removed
ACTOR Future<Void> updateLocalDbInfo(Reference<AsyncVar<ServerDBInfo> const> in,
                                     Reference<AsyncVar<ServerDBInfo>> out,
                                     uint64_t recoveryCount,
                                     CommitProxyInterface myInterface) {
	// whether this CP already receive the db info including itself
	state bool firstValidDbInfo = false;

	loop {
		bool isIncluded =
		    std::count(in->get().client.commitProxies.begin(), in->get().client.commitProxies.end(), myInterface);
		if (in->get().recoveryCount >= recoveryCount && !isIncluded) {
			throw worker_removed();
		}

		if (isIncluded) {
			firstValidDbInfo = true;
		}

		// only update the db info if this is the current CP, or before we received first one including current CP.
		// Several db infos at the beginning just contain the provisional CP
		if (isIncluded || !firstValidDbInfo) {
			DisabledTraceEvent("UpdateLocalDbInfo", myInterface.id())
			    .detail("Provisional", myInterface.provisional)
			    .detail("Included", isIncluded)
			    .detail("FirstValid", firstValidDbInfo)
			    .detail("ReceivedRC", in->get().recoveryCount)
			    .detail("RecoveryCount", recoveryCount)
			    .detail("TenantMode", (int)in->get().client.tenantMode);
			if (in->get().recoveryCount >= out->get().recoveryCount) {
				out->set(in->get());
			}
		}

		wait(in->onChange());
	}
}

ACTOR Future<Void> commitProxyServer(CommitProxyInterface proxy,
                                     InitializeCommitProxyRequest req,
                                     Reference<AsyncVar<ServerDBInfo> const> db,
                                     std::string whitelistBinPaths) {
	try {
		state Reference<AsyncVar<ServerDBInfo>> localDb = makeReference<AsyncVar<ServerDBInfo>>();
		state Future<Void> core = commitProxyServerCore(proxy,
		                                                req.master,
		                                                req.masterLifetime,
		                                                localDb,
		                                                req.recoveryCount,
		                                                req.recoveryTransactionVersion,
		                                                req.firstProxy,
		                                                whitelistBinPaths,
		                                                req.encryptMode,
		                                                proxy.provisional,
		                                                req.commitProxyIndex);
		wait(core || updateLocalDbInfo(db, localDb, req.recoveryCount, proxy));
	} catch (Error& e) {
		Severity sev = e.code() == error_code_failed_to_progress ? SevWarnAlways : SevInfo;
		TraceEvent(sev, "CommitProxyTerminated", proxy.id()).errorUnsuppressed(e);

		if (e.code() != error_code_worker_removed && e.code() != error_code_tlog_stopped &&
		    e.code() != error_code_tlog_failed && e.code() != error_code_coordinators_changed &&
		    e.code() != error_code_coordinated_state_conflict && e.code() != error_code_new_coordinators_timed_out &&
		    e.code() != error_code_failed_to_progress) {
			throw;
		}
		CODE_PROBE(e.code() == error_code_failed_to_progress, "Commit proxy failed to progress");
	}
	return Void();
}

void forceLinkCommitProxyTests() {}
