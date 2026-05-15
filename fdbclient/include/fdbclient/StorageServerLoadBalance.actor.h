/*
 * StorageServerLoadBalance.actor.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

#pragma once

// This header is included at the end of StorageServerInterface.h, after the storage-server request and reply types are
// defined. Keeping these hooks here lets fdbrpc's generic load balancer stay unaware of storage-specific comparisons.
#if defined(NO_INTELLISENSE) && !defined(FDBCLIENT_STORAGESERVERLOADBALANCE_ACTOR_G_H)
#define FDBCLIENT_STORAGESERVERLOADBALANCE_ACTOR_G_H
#include "fdbclient/StorageServerLoadBalance.actor.g.h"
#elif !defined(FDBCLIENT_STORAGESERVERLOADBALANCE_ACTOR_H)
#define FDBCLIENT_STORAGESERVERLOADBALANCE_ACTOR_H

#include "fdbrpc/simulator.h"
#include "flow/actorcompiler.h" // This must be the last #include.

enum ComparisonType { TSS_COMPARISON, REPLICA_COMPARISON };

// FIXME: use a less obscure name than `P` here
ACTOR template <class Req, class Resp, bool P>
Future<Void> tssComparison(Req req,
                           Future<ErrorOr<Resp>> fSource,
                           Future<ErrorOr<Resp>> fTss,
                           TSSEndpointData tssData,
                           uint64_t srcEndpointId,
                           Reference<MultiInterface<ReferencedInterface<StorageServerInterface>>> ssTeam,
                           RequestStream<Req, P> StorageServerInterface::* channel) {
	state double startTime = now();
	state Future<Optional<ErrorOr<Resp>>> fTssWithTimeout = timeout(fTss, FLOW_KNOBS->LOAD_BALANCE_TSS_TIMEOUT);
	state int finished = 0;
	state double srcEndTime;
	state double tssEndTime;
	// we want to record ss/tss errors to metrics
	state int srcErrorCode = error_code_success;
	state int tssErrorCode = error_code_success;
	state ErrorOr<Resp> src;
	state Optional<ErrorOr<Resp>> tss;

	loop {
		choose {
			when(wait(store(src, fSource))) {
				srcEndTime = now();
				fSource = Never();
				finished++;
				if (finished == 2) {
					break;
				}
			}
			when(wait(store(tss, fTssWithTimeout))) {
				tssEndTime = now();
				fTssWithTimeout = Never();
				finished++;
				if (finished == 2) {
					break;
				}
			}
		}
	}
	++tssData.metrics->requests;

	if (src.isError()) {
		srcErrorCode = src.getError().code();
		tssData.metrics->ssError(srcErrorCode);
	}
	if (!tss.present()) {
		++tssData.metrics->tssTimeouts;
	} else if (tss.get().isError()) {
		tssErrorCode = tss.get().getError().code();
		tssData.metrics->tssError(tssErrorCode);
	}
	if (!src.isError() && tss.present() && !tss.get().isError()) {
		Optional<LoadBalancedReply> srcLB = getLoadBalancedReply(&src.get());
		Optional<LoadBalancedReply> tssLB = getLoadBalancedReply(&tss.get().get());
		ASSERT(srcLB.present() ==
		       tssLB.present()); // getLoadBalancedReply returned different responses for same templated type

		// if Resp is a LoadBalancedReply, only compare if both replies are non-error
		if (!srcLB.present() || (!srcLB.get().error.present() && !tssLB.get().error.present())) {
			// only record latency difference if both requests actually succeeded, so that we're comparing apples to
			// apples
			tssData.metrics->recordLatency(req, srcEndTime - startTime, tssEndTime - startTime);

			if (!TSS_doCompare(src.get(), tss.get().get())) {
				CODE_PROBE(true, "TSS Mismatch");
				state TraceEvent mismatchEvent(
				    (simulationPolicyHasCapability(ISimulationPolicy::Capability::WarnOnStorageMismatch))
				        ? SevWarnAlways
				        : SevError,
				    LB_mismatchTraceName(req, TSS_COMPARISON));
				mismatchEvent.setMaxEventLength(FLOW_KNOBS->TSS_LARGE_TRACE_SIZE);
				mismatchEvent.detail("TSSID", tssData.tssId);

				if (FLOW_KNOBS->LOAD_BALANCE_TSS_MISMATCH_VERIFY_SS && ssTeam->size() > 1) {
					CODE_PROBE(true, "checking TSS mismatch against rest of storage team");

					// if there is more than 1 SS in the team, attempt to verify that the other SS servers have the same
					// data
					state std::vector<Future<ErrorOr<Resp>>> restOfTeamFutures;
					restOfTeamFutures.reserve(ssTeam->size() - 1);
					for (int i = 0; i < ssTeam->size(); i++) {
						RequestStream<Req, P> const* si = &ssTeam->get(i, channel);
						if (si->getEndpoint().token.first() !=
						    srcEndpointId) { // don't re-request to SS we already have a response from
							resetReply(req);
							restOfTeamFutures.push_back(si->tryGetReply(req));
						}
					}

					wait(waitForAllReady(restOfTeamFutures));

					int numError = 0;
					int numMatchSS = 0;
					int numMatchTSS = 0;
					int numMatchNeither = 0;
					for (Future<ErrorOr<Resp>> f : restOfTeamFutures) {
						if (!f.canGet() || f.get().isError()) {
							numError++;
						} else {
							Optional<LoadBalancedReply> fLB = getLoadBalancedReply(&f.get().get());
							if (fLB.present() && fLB.get().error.present()) {
								numError++;
							} else if (TSS_doCompare(src.get(), f.get().get())) {
								numMatchSS++;
							} else if (TSS_doCompare(tss.get().get(), f.get().get())) {
								numMatchTSS++;
							} else {
								numMatchNeither++;
							}
						}
					}
					mismatchEvent.detail("TeamCheckErrors", numError)
					    .detail("TeamCheckMatchSS", numMatchSS)
					    .detail("TeamCheckMatchTSS", numMatchTSS)
					    .detail("TeamCheckMatchNeither", numMatchNeither);
				}
				if (tssData.metrics->shouldRecordDetailedMismatch()) {
					TSS_traceMismatch(mismatchEvent, req, src.get(), tss.get().get(), TSS_COMPARISON);

					CODE_PROBE(FLOW_KNOBS->LOAD_BALANCE_TSS_MISMATCH_TRACE_FULL, "Tracing Full TSS Mismatch");
					CODE_PROBE(!FLOW_KNOBS->LOAD_BALANCE_TSS_MISMATCH_TRACE_FULL,
					           "Tracing Partial TSS Mismatch and storing the rest in FDB");

					if (!FLOW_KNOBS->LOAD_BALANCE_TSS_MISMATCH_TRACE_FULL) {
						mismatchEvent.disable();
						UID mismatchUID = deterministicRandom()->randomUniqueID();
						tssData.metrics->recordDetailedMismatchData(mismatchUID, mismatchEvent.getFields().toString());

						// record a summarized trace event instead
						TraceEvent summaryEvent(
						    (g_network->isSimulated() &&
						     simulationPolicyHasCapability(ISimulationPolicy::Capability::WarnOnStorageMismatch))
						        ? SevWarnAlways
						        : SevError,
						    LB_mismatchTraceName(req, TSS_COMPARISON));
						summaryEvent.detail("TSSID", tssData.tssId).detail("MismatchId", mismatchUID);
					}
				} else {
					// don't record trace event
					mismatchEvent.disable();
				}
			}
		} else if (tssLB.present() && tssLB.get().error.present()) {
			tssErrorCode = tssLB.get().error.get().code();
			tssData.metrics->tssError(tssErrorCode);
		} else if (srcLB.present() && srcLB.get().error.present()) {
			srcErrorCode = srcLB.get().error.get().code();
			tssData.metrics->ssError(srcErrorCode);
		}
	}

	if (srcErrorCode != error_code_success && tssErrorCode != error_code_success && srcErrorCode != tssErrorCode) {
		// if ss and tss both got different errors, record them
		TraceEvent("TSSErrorMismatch")
		    .suppressFor(1.0)
		    .detail("TSSID", tssData.tssId)
		    .detail("SSError", srcErrorCode)
		    .detail("TSSError", tssErrorCode);
	}

	return Void();
}

ACTOR template <class Req, class Resp, bool P>
Future<Void> replicaComparison(Req req,
                               Future<ErrorOr<Resp>> fSource,
                               uint64_t srcEndpointId,
                               Reference<MultiInterface<ReferencedInterface<StorageServerInterface>>> ssTeam,
                               RequestStream<Req, P> StorageServerInterface::* channel,
                               int requiredReplicas) {
	state ErrorOr<Resp> src;

	if (ssTeam->size() <= 1 || requiredReplicas == 0) {
		return Void();
	}

	wait(store(src, fSource));

	if (src.isError()) {
		ASSERT_WE_THINK(false); // TODO: Change this into an ASSERT after getting enough test coverage.
		if (requiredReplicas == ALL_REPLICAS) {
			throw src.getError();
		}
	} else {
		state Optional<LoadBalancedReply> srcLB = getLoadBalancedReply(&src.get());

		if (srcLB.present() && srcLB.get().error.present()) {
			ASSERT_WE_THINK(false); // TODO: Change this into an ASSERT after getting enough test coverage.
			if (requiredReplicas == ALL_REPLICAS) {
				throw srcLB.get().error.get();
			}
		} else if (!srcLB.present() || !srcLB.get().error.present()) {
			// Verify that the other SS servers in the team have the same data.
			std::vector<uint64_t> candidates;
			// candidates includes all healthy SS endpoints in the team except the one we already
			// have a response from
			for (int i = 0; i < ssTeam->size(); i++) {
				RequestStream<Req, P> const* si = &ssTeam->get(i, channel);
				if (si->getEndpoint().token.first() == srcEndpointId) {
					// Don't re-request to SS we already have a response from
					continue;
				}
				if (!IFailureMonitor::failureMonitor().getState(si->getEndpoint()).failed) {
					candidates.push_back(si->getEndpoint().token.first());
				} else if (requiredReplicas == ALL_REPLICAS) {
					TraceEvent(SevWarnAlways, "UnreachableStorageServer").detail("SSID", ssTeam->getInterface(i).id());
					throw unreachable_storage_replica();
				}
			}
			int numReplicaToRead = candidates.size();
			if (requiredReplicas != BEST_EFFORT && requiredReplicas != ALL_REPLICAS) {
				ASSERT(requiredReplicas > 0);
				numReplicaToRead = std::min((int)candidates.size(), requiredReplicas);
				if (FLOW_KNOBS->ENABLE_WARNING_READ_CONSISTENCY_CHECK_NOT_ENOUGH_REPLICA &&
				    candidates.size() < requiredReplicas) {
					TraceEvent(SevWarn, "ReplicaConsistencyCheckNotEnoughReplica")
					    .suppressFor(5.0)
					    .detail("RequiredReplicas", requiredReplicas)
					    .detail("AvailableReplicas", candidates.size());
				}
			}
			state std::vector<Future<Optional<ErrorOr<Resp>>>> restOfTeamFutures;
			restOfTeamFutures.reserve(numReplicaToRead);
			// Randomly select numReplicaToRead SSes to read from
			deterministicRandom()->randomShuffle(candidates);
			candidates.erase(candidates.begin() + numReplicaToRead, candidates.end());
			std::unordered_set<uint64_t> ssToRead(candidates.begin(), candidates.end());

			for (int i = 0; i < ssTeam->size(); i++) {
				RequestStream<Req, P> const* si = &ssTeam->get(i, channel);
				if (!ssToRead.contains(si->getEndpoint().token.first())) {
					// Only send requests to the SSes that we randomly selected
					continue;
				}
				resetReply(req);
				restOfTeamFutures.push_back(
				    (requiredReplicas == BEST_EFFORT
				         ? timeout(si->tryGetReply(req), FLOW_KNOBS->LOAD_BALANCE_FETCH_REPLICA_TIMEOUT)
				         : timeout(errorOr(si->getReply(req)), FLOW_KNOBS->LOAD_BALANCE_FETCH_REPLICA_TIMEOUT)));
			}

			wait(waitForAllReady(restOfTeamFutures));

			int numError = 0;
			int numMismatch = 0;
			int numFetchReplicaTimeout = 0;
			int replicaErrorCode = error_code_success;
			int successfulReplies = 0;
			for (Future<Optional<ErrorOr<Resp>>> f : restOfTeamFutures) {
				if (!f.isReady()) {
					ASSERT(requiredReplicas > 0);
					continue;
				}
				if (f.isError()) {
					numError++;
					replicaErrorCode = f.getError().code();
				} else if (!f.get().present()) {
					numFetchReplicaTimeout++;
					replicaErrorCode = error_code_timed_out;
				} else if (f.get().get().isError()) {
					numError++;
					replicaErrorCode = f.get().get().getError().code();
				} else {
					Optional<LoadBalancedReply> fLB = getLoadBalancedReply(&f.get().get().get());

					ASSERT(srcLB.present() ==
					       fLB.present()); // getLoadBalancedReply returned different responses for same templated type

					if (fLB.present()) {
						if (fLB.get().error.present()) {
							numError++;
							replicaErrorCode = fLB.get().error.get().code();
						} else {
							if (!TSS_doCompare(
							        src.get(),
							        f.get().get().get())) { // re-use TSS compare logic to compare the replicas
								numMismatch++;
								TraceEvent mismatchEvent(
								    (requiredReplicas == BEST_EFFORT || requiredReplicas == ALL_REPLICAS)
								        ? SevError
								        : SevWarnAlways,
								    LB_mismatchTraceName(req, REPLICA_COMPARISON));
								mismatchEvent.detail("ReplicaFetchErrors", numError)
								    .detail("ReplicaFetchTimeouts", numFetchReplicaTimeout);
								// Re-use TSS trace mechanism to log replica mismatch information.
								TSS_traceMismatch(
								    mismatchEvent, req, src.get(), f.get().get().get(), REPLICA_COMPARISON);
							}
							if (++successfulReplies == requiredReplicas) {
								break;
							}
						}
					}
				}

				// We must always propagate wrong_shard_server to the caller because it is signal to
				// perform critical operations like invalidating the shard mapping cache.
				if (replicaErrorCode == error_code_wrong_shard_server) {
					TraceEvent(SevWarnAlways, "ReplicaComparisonReadError")
					    .suppressFor(1.0)
					    .detail("TeamSize", restOfTeamFutures.size() + 1)
					    .detail("RequiredReplies", requiredReplicas)
					    .detail("SSError", error_code_wrong_shard_server);
					throw wrong_shard_server();
				}
			}

			if (numMismatch) {
				throw storage_replica_comparison_error();
			} else if (((numError || numFetchReplicaTimeout) && (requiredReplicas == ALL_REPLICAS)) ||
			           (successfulReplies != requiredReplicas && requiredReplicas > 0 &&
			            restOfTeamFutures.size() >= requiredReplicas)) {
				const char* type = numError ? "ReplicaComparisonReadError" : "ReplicaComparisonTimeoutError";
				TraceEvent(SevWarnAlways, type)
				    .detail("TeamSize", restOfTeamFutures.size() + 1)
				    .detail("RequiredReplies", requiredReplicas)
				    .detail("SuccessfulReplies", successfulReplies)
				    .detail("SSError", replicaErrorCode);

				throw Error((requiredReplicas == ALL_REPLICAS) ? replicaErrorCode
				                                               : error_code_unreachable_storage_replica);
			}
		}
	}
	return Void();
}

template <class Request, bool P>
struct LoadBalanceRequestHooks<Request,
                               StorageServerInterface,
                               ReferencedInterface<StorageServerInterface>,
                               StorageServerQueueModel,
                               P> {
	static void maybeDuplicate(RequestStream<Request, P> const* stream,
	                           Request& request,
	                           StorageServerQueueModel* model,
	                           Future<ErrorOr<REPLY_TYPE(Request)>> ssResponse,
	                           Reference<MultiInterface<ReferencedInterface<StorageServerInterface>>> alternatives,
	                           RequestStream<Request, P> StorageServerInterface::* channel) {
		if (model) {
			// Send parallel request to TSS pair, if it exists
			Optional<TSSEndpointData> tssData = model->getTssData(stream->getEndpoint().token.first());

			if (tssData.present()) {
				CODE_PROBE(true, "duplicating request to TSS");
				resetReply(request);
				// FIXME: optimize to avoid creating new netNotifiedQueue for each message
				RequestStream<Request, P> tssRequestStream(tssData.get().endpoint);
				Future<ErrorOr<REPLY_TYPE(Request)>> fTssResult = tssRequestStream.tryGetReply(request);
				model->addActor.send(tssComparison(request,
				                                   ssResponse,
				                                   fTssResult,
				                                   tssData.get(),
				                                   stream->getEndpoint().token.first(),
				                                   alternatives,
				                                   channel));
			}
		}
	}

	static Future<Void> maybeCompare(
	    Request& request,
	    StorageServerQueueModel* model,
	    RequestStream<Request, P> const* requestStream,
	    Future<ErrorOr<REPLY_TYPE(Request)>> response,
	    Reference<MultiInterface<ReferencedInterface<StorageServerInterface>>> alternatives,
	    RequestStream<Request, P> StorageServerInterface::* channel,
	    bool compareReplicas,
	    int requiredReplicas) {
		if (model && (compareReplicas || FLOW_KNOBS->ENABLE_REPLICA_CONSISTENCY_CHECK_ON_READS)) {
			if (compareReplicas) {
				// In case compareReplicas == true, we may read extra requiredReplicas replica.
				// The value of compareReplicas is decided by the caller and the knobs.
				// If the caller is fetchKeys, when ENABLE_REPLICA_CONSISTENCY_CHECK_ON_DATA_MOVEMENT is on,
				// the value is DATAMOVE_CONSISTENCY_CHECK_REQUIRED_REPLICAS.
				// If the caller is backup agents, when ENABLE_REPLICA_CONSISTENCY_CHECK_ON_BACKUP_READS is on,
				// the value is BACKUP_CONSISTENCY_CHECK_REQUIRED_REPLICAS.
				// Otherwise, the value is 0.
				return replicaComparison(request,
				                         response,
				                         requestStream->getEndpoint().token.first(),
				                         alternatives,
				                         channel,
				                         requiredReplicas);
			}
			// In case ENABLE_REPLICA_CONSISTENCY_CHECK_ON_READS is on, we read extra
			// READ_CONSISTENCY_CHECK_REQUIRED_REPLICAS replica and conduct consistency
			// check among replica for any read request.
			return replicaComparison(request,
			                         response,
			                         requestStream->getEndpoint().token.first(),
			                         alternatives,
			                         channel,
			                         FLOW_KNOBS->READ_CONSISTENCY_CHECK_REQUIRED_REPLICAS);
		}
		return Void();
	}
};

#include "flow/unactorcompiler.h"
#endif
