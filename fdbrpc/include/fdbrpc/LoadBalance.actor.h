/*
 * LoadBalance.actor.h
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

#pragma once

// When actually compiled (NO_INTELLISENSE), include the generated version of this file.  In intellisense use the source
// version.
#if defined(NO_INTELLISENSE) && !defined(FLOW_LOADBALANCE_ACTOR_G_H)
#define FLOW_LOADBALANCE_ACTOR_G_H
#include "fdbrpc/LoadBalance.actor.g.h"
#elif !defined(FLOW_LOADBALANCE_ACTOR_H)
#define FLOW_LOADBALANCE_ACTOR_H

#include "flow/BooleanParam.h"
#include "flow/flow.h"
#include "flow/Error.h"
#include "flow/Knobs.h"

#include "fdbrpc/FailureMonitor.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbrpc/Locality.h"
#include "fdbrpc/QueueModel.h"
#include "fdbrpc/MultiInterface.h"
#include "fdbrpc/simulator.h" // for checking tss simulation mode
#include "fdbrpc/TSSComparison.h"
#include "flow/actorcompiler.h" // This must be the last #include.

ACTOR Future<Void> allAlternativesFailedDelay(Future<Void> okFuture);

enum ComparisonType { TSS_COMPARISON, REPLICA_COMPARISON };

enum RequiredReplicas { BEST_EFFORT = -2, ALL_REPLICAS = -1 };

struct ModelHolder : NonCopyable, public ReferenceCounted<ModelHolder> {
	QueueModel* model;
	bool released;
	double startTime;
	double delta;
	uint64_t token;

	ModelHolder(QueueModel* model, uint64_t token) : model(model), released(false), startTime(now()), token(token) {
		if (model) {
			delta = model->addRequest(token);
		}
	}

	void release(bool clean, bool futureVersion, double penalty, bool measureLatency = true) {
		if (model && !released) {
			released = true;
			double latency = (clean || measureLatency) ? now() - startTime : 0.0;
			model->endRequest(token, latency, penalty, delta, clean, futureVersion);
		}
	}

	~ModelHolder() { release(false, false, -1.0, false); }
};

// Subclasses must initialize all members in their default constructors
// Subclasses must serialize all members
struct LoadBalancedReply {
	double penalty;
	Optional<Error> error;
	LoadBalancedReply() : penalty(1.0) {}
};

Optional<LoadBalancedReply> getLoadBalancedReply(const LoadBalancedReply* reply);
Optional<LoadBalancedReply> getLoadBalancedReply(const void*);

ACTOR template <class Req, class Resp, class Interface, class Multi, bool P>
Future<Void> tssComparison(Req req,
                           Future<ErrorOr<Resp>> fSource,
                           Future<ErrorOr<Resp>> fTss,
                           TSSEndpointData tssData,
                           uint64_t srcEndpointId,
                           Reference<MultiInterface<Multi>> ssTeam,
                           RequestStream<Req, P> Interface::* channel) {
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
				    (g_network->isSimulated() && g_simulator->tssMode == ISimulator::TSSMode::EnabledDropMutations)
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
						TraceEvent summaryEvent((g_network->isSimulated() &&
						                         g_simulator->tssMode == ISimulator::TSSMode::EnabledDropMutations)
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

ACTOR template <class Resp>
Future<Void> waitForQuorumReplies(std::vector<Future<Optional<ErrorOr<Resp>>>>* replies, int required) {
	state int outstandingReplies = (int)replies->size();
	state int requiredReplies = std::min(required, (int)replies->size());
	state std::vector<Future<Optional<ErrorOr<Resp>>>> ongoingReplies;
	loop {
		ongoingReplies.clear();
		for (auto& reply : (*replies)) {
			if (!reply.isReady()) {
				ongoingReplies.push_back(reply);
			}
		}
		ASSERT(ongoingReplies.size() == outstandingReplies);

		if (requiredReplies == 0 || outstandingReplies == 0) {
			break;
		}

		wait(quorum(ongoingReplies, std::min(requiredReplies, outstandingReplies)));

		for (auto& reply : ongoingReplies) {
			if (reply.isReady()) {
				outstandingReplies--;
				if (!reply.isError() && reply.get().present() && !reply.get().get().isError()) {
					Optional<LoadBalancedReply> lbReply = getLoadBalancedReply(&reply.get().get().get());
					if (lbReply.present() && !lbReply.get().error.present()) {
						requiredReplies--;
					}
				}
			}
		}
	}

	return Void();
}

ACTOR template <class Req, class Resp, class Interface, class Multi, bool P>
Future<Void> replicaComparison(Req req,
                               Future<ErrorOr<Resp>> fSource,
                               uint64_t srcEndpointId,
                               Reference<MultiInterface<Multi>> ssTeam,
                               RequestStream<Req, P> Interface::* channel,
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
			state std::vector<Future<Optional<ErrorOr<Resp>>>> restOfTeamFutures;
			restOfTeamFutures.reserve(ssTeam->size() - 1);
			for (int i = 0; i < ssTeam->size(); i++) {
				RequestStream<Req, P> const* si = &ssTeam->get(i, channel);
				if (si->getEndpoint().token.first() !=
				    srcEndpointId) { // don't re-request to SS we already have a response from
					if (!IFailureMonitor::failureMonitor().getState(si->getEndpoint()).failed) {
						resetReply(req);
						restOfTeamFutures.push_back((
						    requiredReplicas == BEST_EFFORT
						        ? timeout(si->tryGetReply(req), FLOW_KNOBS->LOAD_BALANCE_FETCH_REPLICA_TIMEOUT)
						        : timeout(errorOr(si->getReply(req)), FLOW_KNOBS->LOAD_BALANCE_FETCH_REPLICA_TIMEOUT)));
					} else if (requiredReplicas == ALL_REPLICAS) {
						TraceEvent(SevWarnAlways, "UnreachableStorageServer")
						    .detail("SSID", ssTeam->getInterface(i).id());
						throw unreachable_storage_replica();
					}
				}
			}

			if (requiredReplicas == BEST_EFFORT || requiredReplicas == ALL_REPLICAS) {
				wait(waitForAllReady(restOfTeamFutures));
			} else {
				wait(waitForQuorumReplies(&restOfTeamFutures, requiredReplicas));
			}

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

FDB_BOOLEAN_PARAM(AtMostOnce);
FDB_BOOLEAN_PARAM(TriedAllOptions);

// Stores state for a request made by the load balancer
template <class Request, class Interface, class Multi, bool P>
struct RequestData : NonCopyable {
	typedef ErrorOr<REPLY_TYPE(Request)> Reply;

	Future<Reply> response;
	Reference<ModelHolder> modelHolder;
	TriedAllOptions triedAllOptions{ false };
	RequestStream<Request, P> const* requestStream = nullptr;

	bool requestStarted = false; // true once the request has been sent to an alternative
	bool requestProcessed = false; // true once a response has been received and handled by checkAndProcessResult

	bool compareReplicas = false;
	Future<Void> comparisonResult;

	RequestData(bool compareReplicas = false) : compareReplicas(compareReplicas) {}

	// Whether or not the response future is valid
	// This is true once setupRequest is called, even though at that point the response is Never().
	bool isValid() { return response.isValid(); }

	static void maybeDuplicateTSSRequest(RequestStream<Request, P> const* stream,
	                                     Request& request,
	                                     QueueModel* model,
	                                     Future<Reply> ssResponse,
	                                     Reference<MultiInterface<Multi>> alternatives,
	                                     RequestStream<Request, P> Interface::* channel) {
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

	Future<Void> maybeDoReplicaComparison(Request& request,
	                                      QueueModel* model,
	                                      Reference<MultiInterface<Multi>> alternatives,
	                                      RequestStream<Request, P> Interface::* channel,
	                                      int requiredReplicas) {
		if (model && (compareReplicas || FLOW_KNOBS->ENABLE_REPLICA_CONSISTENCY_CHECK_ON_READS)) {
			ASSERT(requestStream != nullptr);
			int requiredReplicaCount =
			    compareReplicas ? requiredReplicas : FLOW_KNOBS->CONSISTENCY_CHECK_REQUIRED_REPLICAS;
			return replicaComparison(request,
			                         response,
			                         requestStream->getEndpoint().token.first(),
			                         alternatives,
			                         channel,
			                         requiredReplicaCount);
		}

		return Void();
	}

	// Initializes the request state and starts it, possibly after a backoff delay
	void startRequest(
	    double backoff,
	    TriedAllOptions triedAllOptions,
	    RequestStream<Request, P> const* stream,
	    Request& request,
	    QueueModel* model,
	    Reference<MultiInterface<Multi>> alternatives, // alternatives and channel passed through for TSS check
	    RequestStream<Request, P> Interface::* channel) {
		modelHolder = Reference<ModelHolder>();
		requestStream = stream;
		requestStarted = false;

		if (backoff > 0) {
			response = mapAsync(delay(backoff), [this, stream, &request, model, alternatives, channel](Void _) {
				requestStarted = true;
				modelHolder = Reference<ModelHolder>(new ModelHolder(model, stream->getEndpoint().token.first()));
				Future<Reply> resp = stream->tryGetReply(request);
				maybeDuplicateTSSRequest(stream, request, model, resp, alternatives, channel);
				return resp;
			});
		} else {
			requestStarted = true;
			modelHolder = Reference<ModelHolder>(new ModelHolder(model, stream->getEndpoint().token.first()));
			response = stream->tryGetReply(request);
			maybeDuplicateTSSRequest(stream, request, model, response, alternatives, channel);
		}

		requestProcessed = false;
		this->triedAllOptions = triedAllOptions;
	}

	// Implementation of the logic to handle a response.
	// Checks the state of the response, updates the queue model, and returns one of the following outcomes:
	// A return value of true means that the request completed successfully
	// A return value of false means that the request failed but should be retried
	// A return value with an error means that the error should be thrown back to original caller
	static ErrorOr<bool> checkAndProcessResultImpl(Reply const& result,
	                                               Reference<ModelHolder> modelHolder,
	                                               AtMostOnce atMostOnce,
	                                               TriedAllOptions triedAllOptions) {
		ASSERT(modelHolder);

		Optional<LoadBalancedReply> loadBalancedReply;
		if (!result.isError()) {
			loadBalancedReply = getLoadBalancedReply(&result.get());
		}

		int errCode;
		if (loadBalancedReply.present()) {
			errCode = loadBalancedReply.get().error.present() ? loadBalancedReply.get().error.get().code()
			                                                  : error_code_success;
		} else {
			errCode = result.isError() ? result.getError().code() : error_code_success;
		}

		bool maybeDelivered = errCode == error_code_broken_promise || errCode == error_code_request_maybe_delivered;
		bool receivedResponse =
		    loadBalancedReply.present() ? !loadBalancedReply.get().error.present() : result.present();
		receivedResponse = receivedResponse || (!maybeDelivered && errCode != error_code_process_behind);
		bool futureVersion = errCode == error_code_future_version || errCode == error_code_process_behind;

		modelHolder->release(
		    receivedResponse, futureVersion, loadBalancedReply.present() ? loadBalancedReply.get().penalty : -1.0);

		if (errCode == error_code_server_overloaded) {
			return false;
		}

		if (loadBalancedReply.present() && !loadBalancedReply.get().error.present()) {
			return true;
		}

		if (!loadBalancedReply.present() && result.present()) {
			return true;
		}

		if (receivedResponse) {
			return loadBalancedReply.present() ? loadBalancedReply.get().error.get() : result.getError();
		}

		if (atMostOnce && maybeDelivered) {
			return request_maybe_delivered();
		}

		if (triedAllOptions && errCode == error_code_process_behind) {
			return process_behind();
		}

		return false;
	}

	// Checks the state of the response, updates the queue model, and returns one of the following outcomes:
	// A return value of true means that the request completed successfully
	// A return value of false means that the request failed but should be retried
	// In the event of a non-retryable failure, an error is thrown indicating the failure
	bool checkAndProcessResult(AtMostOnce atMostOnce) {
		ASSERT(response.isReady());
		requestProcessed = true;

		ErrorOr<bool> outcome =
		    checkAndProcessResultImpl(response.get(), std::move(modelHolder), atMostOnce, triedAllOptions);

		if (outcome.isError()) {
			throw outcome.getError();
		} else if (!outcome.get()) {
			response = Future<Reply>();
		}

		return outcome.get();
	}

	// Convert this request to a lagging request. Such a request is no longer being waited on, but it still needs to be
	// processed so we can update the queue model.
	void makeLaggingRequest() {
		ASSERT(response.isValid());
		ASSERT(modelHolder);
		ASSERT(modelHolder->model);

		QueueModel* model = modelHolder->model;
		if (model->laggingRequestCount > FLOW_KNOBS->MAX_LAGGING_REQUESTS_OUTSTANDING ||
		    model->laggingRequests.isReady()) {
			model->laggingRequests.cancel();
			model->laggingRequestCount = 0;
			model->addActor = PromiseStream<Future<Void>>();
			model->laggingRequests = actorCollection(model->addActor.getFuture(), &model->laggingRequestCount);
		}

		// We need to process the lagging request in order to update the queue model
		Reference<ModelHolder> holderCapture = std::move(modelHolder);
		auto triedAllOptionsCapture = triedAllOptions;
		Future<Void> updateModel = map(response, [holderCapture, triedAllOptionsCapture](Reply result) {
			checkAndProcessResultImpl(result, holderCapture, AtMostOnce::False, triedAllOptionsCapture);
			return Void();
		});
		model->addActor.send(updateModel);
	}

	~RequestData() {
		// If the request has been started but hasn't completed, mark it as a lagging request
		if (requestStarted && !requestProcessed && modelHolder && modelHolder->model) {
			makeLaggingRequest();
		}
	}
};

// Try to get a reply from one of the alternatives until success, cancellation, or certain errors.
// Load balancing has a budget to race requests to a second alternative if the first request is slow.
// Tries to take into account failMon's information for load balancing and avoiding failed servers.
// If ALL the servers are failed and the list of servers is not fresh, throws an exception to let the caller refresh the
// list of servers.
// When model is set, load balance among alternatives in the same DC aims to balance request queue length on these
// interfaces. If too many interfaces in the same DC are bad, try remote interfaces.
// If compareReplicas is set, does a consistency check by fetching and comparing results from storage
// replicas (as many as specified by "requiredReplicas") and throws an exception if an inconsistency is found.
ACTOR template <class Interface, class Request, class Multi, bool P>
Future<REPLY_TYPE(Request)> loadBalance(
    Reference<MultiInterface<Multi>> alternatives,
    RequestStream<Request, P> Interface::* channel,
    Request request = Request(),
    TaskPriority taskID = TaskPriority::DefaultPromiseEndpoint,
    AtMostOnce atMostOnce =
        AtMostOnce::False, // if true, throws request_maybe_delivered() instead of retrying automatically
    QueueModel* model = nullptr,
    bool compareReplicas = false,
    int requiredReplicas = 0) {

	state RequestData<Request, Interface, Multi, P> firstRequestData(compareReplicas);
	state RequestData<Request, Interface, Multi, P> secondRequestData(compareReplicas);

	state Optional<uint64_t> firstRequestEndpoint;
	state Future<Void> secondDelay = Never();

	state Promise<Void> requestFinished;
	state double startTime = now();

	state TriedAllOptions triedAllOptions = TriedAllOptions::False;

	setReplyPriority(request, taskID);
	if (!alternatives)
		return Never();

	ASSERT(alternatives->size());

	state int bestAlt = deterministicRandom()->randomInt(0, alternatives->countBest());
	state int nextAlt = deterministicRandom()->randomInt(0, std::max(alternatives->size() - 1, 1));
	if (nextAlt >= bestAlt)
		nextAlt++;

	if (model) {
		double bestMetric = 1e9; // Storage server with the least outstanding requests.
		double nextMetric = 1e9;
		double bestTime = 1e9; // The latency to the server with the least outstanding requests.
		double nextTime = 1e9;
		int badServers = 0;

		for (int i = 0; i < alternatives->size(); i++) {
			// countBest(): the number of alternatives in the same locality (i.e., DC by default) as alternatives[0].
			// if the if-statement is correct, it won't try to send requests to the remote ones.
			if (badServers < std::min(i, FLOW_KNOBS->LOAD_BALANCE_MAX_BAD_OPTIONS + 1) &&
			    i == alternatives->countBest()) {
				// When we have at least one healthy local server, and the bad
				// server count is within "LOAD_BALANCE_MAX_BAD_OPTIONS". We
				// do not need to consider any remote servers.
				break;
			} else if (badServers == alternatives->countBest() && i == badServers) {
				TraceEvent("AllLocalAlternativesFailed")
				    .suppressFor(1.0)
				    .detail("Alternatives", alternatives->description())
				    .detail("Total", alternatives->size())
				    .detail("Best", alternatives->countBest());
			}

			RequestStream<Request, P> const* thisStream = &alternatives->get(i, channel);
			if (!IFailureMonitor::failureMonitor().getState(thisStream->getEndpoint()).failed) {
				auto const& qd = model->getMeasurement(thisStream->getEndpoint().token.first());
				if (now() > qd.failedUntil) {
					double thisMetric = qd.smoothOutstanding.smoothTotal();
					double thisTime = qd.latency;
					if (FLOW_KNOBS->LOAD_BALANCE_PENALTY_IS_BAD && qd.penalty > 1.001) {
						// When a server wants to penalize itself (the default
						// penalty value is 1.0), consider this server as bad.
						// penalty is sent from server.
						++badServers;
					}

					if (thisMetric < bestMetric) {
						if (i != bestAlt) {
							nextAlt = bestAlt;
							nextMetric = bestMetric;
							nextTime = bestTime;
						}
						bestAlt = i;
						bestMetric = thisMetric;
						bestTime = thisTime;
					} else if (thisMetric < nextMetric) {
						nextAlt = i;
						nextMetric = thisMetric;
						nextTime = thisTime;
					}
				} else {
					++badServers;
				}
			} else {
				++badServers;
			}
		}
		if (nextMetric > 1e8) {
			// If we still don't have a second best choice to issue request to,
			// go through all the remote servers again, since we may have
			// skipped it.
			for (int i = alternatives->countBest(); i < alternatives->size(); i++) {
				RequestStream<Request, P> const* thisStream = &alternatives->get(i, channel);
				if (!IFailureMonitor::failureMonitor().getState(thisStream->getEndpoint()).failed) {
					auto const& qd = model->getMeasurement(thisStream->getEndpoint().token.first());
					if (now() > qd.failedUntil) {
						double thisMetric = qd.smoothOutstanding.smoothTotal();
						double thisTime = qd.latency;

						if (thisMetric < nextMetric) {
							nextAlt = i;
							nextMetric = thisMetric;
							nextTime = thisTime;
						}
					}
				}
			}
		}

		if (nextTime < 1e9) {
			// Decide when to send the request to the second best choice.
			if (bestTime > FLOW_KNOBS->INSTANT_SECOND_REQUEST_MULTIPLIER *
			                   (model->secondMultiplier * (nextTime) + FLOW_KNOBS->BASE_SECOND_REQUEST_TIME)) {
				secondDelay = Void();
			} else {
				secondDelay = delay(model->secondMultiplier * nextTime + FLOW_KNOBS->BASE_SECOND_REQUEST_TIME);
			}
		} else {
			secondDelay = Never();
		}
	}

	state int startAlt = nextAlt;
	state int startDistance = (bestAlt + alternatives->size() - startAlt) % alternatives->size();

	state int numAttempts = 0;
	state double backoff = 0;
	// Issue requests to selected servers.
	loop {
		if (now() - startTime > (g_network->isSimulated() ? 30.0 : 600.0)) {
			TraceEvent ev(g_network->isSimulated() ? SevWarn : SevWarnAlways, "LoadBalanceTooLong");
			ev.suppressFor(1.0);
			ev.detail("Duration", now() - startTime);
			ev.detail("NumAttempts", numAttempts);
			ev.detail("Backoff", backoff);
			ev.detail("TriedAllOptions", triedAllOptions);
			if (ev.isEnabled()) {
				ev.log();
				for (int alternativeNum = 0; alternativeNum < alternatives->size(); alternativeNum++) {
					RequestStream<Request, P> const* thisStream = &alternatives->get(alternativeNum, channel);
					TraceEvent(SevWarn, "LoadBalanceTooLongEndpoint")
					    .detail("Addr", thisStream->getEndpoint().getPrimaryAddress())
					    .detail("Token", thisStream->getEndpoint().token)
					    .detail("Failed", IFailureMonitor::failureMonitor().getState(thisStream->getEndpoint()).failed);
				}
			}
		}

		// Find an alternative, if any, that is not failed, starting with
		// nextAlt. This logic matters only if model == nullptr. Otherwise, the
		// bestAlt and nextAlt have been decided.
		state RequestStream<Request, P> const* stream = nullptr;
		state LBDistance::Type distance;
		for (int alternativeNum = 0; alternativeNum < alternatives->size(); alternativeNum++) {
			int useAlt = nextAlt;
			if (nextAlt == startAlt)
				useAlt = bestAlt;
			else if ((nextAlt + alternatives->size() - startAlt) % alternatives->size() <= startDistance)
				useAlt = (nextAlt + alternatives->size() - 1) % alternatives->size();

			stream = &alternatives->get(useAlt, channel);
			distance = alternatives->getDistance(useAlt);
			if (!IFailureMonitor::failureMonitor().getState(stream->getEndpoint()).failed &&
			    (!firstRequestEndpoint.present() || stream->getEndpoint().token.first() != firstRequestEndpoint.get()))
				break;
			nextAlt = (nextAlt + 1) % alternatives->size();
			if (nextAlt == startAlt)
				triedAllOptions = TriedAllOptions::True;
			stream = nullptr;
			distance = LBDistance::DISTANT;
		}

		if (!stream && !firstRequestData.isValid()) {
			// Everything is down!  Wait for someone to be up.

			std::vector<Future<Void>> ok(alternatives->size());
			for (int i = 0; i < ok.size(); i++) {
				ok[i] = IFailureMonitor::failureMonitor().onStateEqual(alternatives->get(i, channel).getEndpoint(),
				                                                       FailureStatus(false));
			}

			Future<Void> okFuture = quorum(ok, 1);

			if (!alternatives->alwaysFresh()) {
				// Making this SevWarn means a lot of clutter
				if (now() - g_network->networkInfo.newestAlternativesFailure > 1 ||
				    deterministicRandom()->random01() < 0.01) {
					TraceEvent("AllAlternativesFailed").detail("Alternatives", alternatives->description());
				}
				wait(allAlternativesFailedDelay(okFuture));
			} else {
				wait(okFuture);
			}

			numAttempts = 0; // now that we've got a server back, reset the backoff
		} else if (!stream) {
			// Only the first location is available.
			wait(success(firstRequestData.response));
			if (firstRequestData.checkAndProcessResult(atMostOnce)) {
				// Do consistency check, if requested.
				wait(
				    firstRequestData.maybeDoReplicaComparison(request, model, alternatives, channel, requiredReplicas));

				ASSERT(firstRequestData.response.isReady());
				return firstRequestData.response.get().get();
			}

			firstRequestEndpoint = Optional<uint64_t>();
		} else if (firstRequestData.isValid()) {
			// Issue a second request, the first one is taking a long time.
			if (distance == LBDistance::DISTANT) {
				TraceEvent("LBDistant2nd")
				    .suppressFor(0.1)
				    .detail("Distance", (int)distance)
				    .detail("BackOff", backoff)
				    .detail("TriedAllOptions", triedAllOptions)
				    .detail("Alternatives", alternatives->description())
				    .detail("Token", stream->getEndpoint().token)
				    .detail("Total", alternatives->size())
				    .detail("Best", alternatives->countBest())
				    .detail("Attempts", numAttempts);
			}
			secondRequestData.startRequest(backoff, triedAllOptions, stream, request, model, alternatives, channel);

			state bool firstRequestSuccessful = false;
			state bool secondRequestSuccessful = false;

			loop choose {
				when(wait(success(firstRequestData.response.isValid() ? firstRequestData.response : Never()))) {
					if (firstRequestData.checkAndProcessResult(atMostOnce)) {
						firstRequestSuccessful = true;
						break;
					}

					firstRequestEndpoint = Optional<uint64_t>();
				}
				when(wait(success(secondRequestData.response))) {
					if (secondRequestData.checkAndProcessResult(atMostOnce)) {
						secondRequestSuccessful = true;
					}

					break;
				}
			}

			if (firstRequestSuccessful || secondRequestSuccessful) {
				// Do consistency check, by comparing results from storage replicas, if requested.
				state RequestData<Request, Interface, Multi, P>* requestData =
				    firstRequestSuccessful ? &firstRequestData : &secondRequestData;
				wait(requestData->maybeDoReplicaComparison(request, model, alternatives, channel, requiredReplicas));

				ASSERT(requestData->response.isReady());
				return requestData->response.get().get();
			}

			if (++numAttempts >= alternatives->size()) {
				backoff = std::min(
				    FLOW_KNOBS->LOAD_BALANCE_MAX_BACKOFF,
				    std::max(FLOW_KNOBS->LOAD_BALANCE_START_BACKOFF, backoff * FLOW_KNOBS->LOAD_BALANCE_BACKOFF_RATE));
			}
		} else {
			// Issue a request, if it takes too long to get a reply, go around the loop
			if (distance == LBDistance::DISTANT) {
				TraceEvent("LBDistant")
				    .suppressFor(0.1)
				    .detail("Distance", (int)distance)
				    .detail("BackOff", backoff)
				    .detail("TriedAllOptions", triedAllOptions)
				    .detail("Alternatives", alternatives->description())
				    .detail("Token", stream->getEndpoint().token)
				    .detail("Total", alternatives->size())
				    .detail("Best", alternatives->countBest())
				    .detail("Attempts", numAttempts);
			}
			firstRequestData.startRequest(backoff, triedAllOptions, stream, request, model, alternatives, channel);
			firstRequestEndpoint = stream->getEndpoint().token.first();

			loop {
				choose {
					when(wait(success(firstRequestData.response))) {
						if (model) {
							model->secondMultiplier =
							    std::max(model->secondMultiplier - FLOW_KNOBS->SECOND_REQUEST_MULTIPLIER_DECAY, 1.0);
							model->secondBudget =
							    std::min(model->secondBudget + FLOW_KNOBS->SECOND_REQUEST_BUDGET_GROWTH,
							             FLOW_KNOBS->SECOND_REQUEST_MAX_BUDGET);
						}

						if (firstRequestData.checkAndProcessResult(atMostOnce)) {
							// Do consistency check, by comparing results from storage replicas, if requested.
							wait(firstRequestData.maybeDoReplicaComparison(
							    request, model, alternatives, channel, requiredReplicas));

							ASSERT(firstRequestData.response.isReady());
							return firstRequestData.response.get().get();
						}

						firstRequestEndpoint = Optional<uint64_t>();
						break;
					}
					when(wait(secondDelay)) {
						secondDelay = Never();
						if (model && model->secondBudget >= 1.0) {
							model->secondMultiplier += FLOW_KNOBS->SECOND_REQUEST_MULTIPLIER_GROWTH;
							model->secondBudget -= 1.0;
							break;
						}
					}
				}
			}

			if (++numAttempts >= alternatives->size()) {
				backoff = std::min(
				    FLOW_KNOBS->LOAD_BALANCE_MAX_BACKOFF,
				    std::max(FLOW_KNOBS->LOAD_BALANCE_START_BACKOFF, backoff * FLOW_KNOBS->LOAD_BALANCE_BACKOFF_RATE));
			}
		}

		nextAlt = (nextAlt + 1) % alternatives->size();
		if (nextAlt == startAlt)
			triedAllOptions = TriedAllOptions::True;
		resetReply(request, taskID);
		secondDelay = Never();
	}
}

// Subclasses must initialize all members in their default constructors
// Subclasses must serialize all members
struct BasicLoadBalancedReply {
	int processBusyTime;
	BasicLoadBalancedReply() : processBusyTime(0) {}
};

Optional<BasicLoadBalancedReply> getBasicLoadBalancedReply(const BasicLoadBalancedReply* reply);
Optional<BasicLoadBalancedReply> getBasicLoadBalancedReply(const void*);

// A simpler version of LoadBalance that does not send second requests where the list of servers are always fresh
//
// If |alternativeChosen| is not null, then atMostOnce must be True, and if the returned future completes successfully
// then *alternativeChosen will be the alternative to which the message was sent. *alternativeChosen must outlive the
// returned future.
ACTOR template <class Interface, class Request, class Multi, bool P>
Future<REPLY_TYPE(Request)> basicLoadBalance(Reference<ModelInterface<Multi>> alternatives,
                                             RequestStream<Request, P> Interface::* channel,
                                             Request request = Request(),
                                             TaskPriority taskID = TaskPriority::DefaultPromiseEndpoint,
                                             AtMostOnce atMostOnce = AtMostOnce::False,
                                             int* alternativeChosen = nullptr) {
	ASSERT(alternativeChosen == nullptr || atMostOnce == AtMostOnce::True);
	setReplyPriority(request, taskID);
	if (!alternatives)
		return Never();

	ASSERT(alternatives->size() && alternatives->alwaysFresh());

	state int bestAlt = alternatives->getBest();
	state int nextAlt = deterministicRandom()->randomInt(0, std::max(alternatives->size() - 1, 1));
	if (nextAlt >= bestAlt)
		nextAlt++;

	state int startAlt = nextAlt;
	state int startDistance = (bestAlt + alternatives->size() - startAlt) % alternatives->size();

	state int numAttempts = 0;
	state double backoff = 0;
	state int useAlt;
	loop {
		// Find an alternative, if any, that is not failed, starting with nextAlt
		state RequestStream<Request, P> const* stream = nullptr;
		for (int alternativeNum = 0; alternativeNum < alternatives->size(); alternativeNum++) {
			useAlt = nextAlt;
			if (nextAlt == startAlt)
				useAlt = bestAlt;
			else if ((nextAlt + alternatives->size() - startAlt) % alternatives->size() <= startDistance)
				useAlt = (nextAlt + alternatives->size() - 1) % alternatives->size();

			stream = &alternatives->get(useAlt, channel);
			if (alternativeChosen != nullptr) {
				*alternativeChosen = useAlt;
			}
			if (!IFailureMonitor::failureMonitor().getState(stream->getEndpoint()).failed)
				break;
			nextAlt = (nextAlt + 1) % alternatives->size();
			stream = nullptr;
		}

		if (!stream) {
			// Everything is down!  Wait for someone to be up.

			std::vector<Future<Void>> ok(alternatives->size());
			for (int i = 0; i < ok.size(); i++) {
				ok[i] = IFailureMonitor::failureMonitor().onStateEqual(alternatives->get(i, channel).getEndpoint(),
				                                                       FailureStatus(false));
			}
			wait(quorum(ok, 1));

			numAttempts = 0; // now that we've got a server back, reset the backoff
		} else {
			if (backoff > 0.0) {
				wait(delay(backoff));
			}

			ErrorOr<REPLY_TYPE(Request)> result = wait(stream->tryGetReply(request));

			if (result.present()) {
				Optional<BasicLoadBalancedReply> loadBalancedReply = getBasicLoadBalancedReply(&result.get());
				if (loadBalancedReply.present()) {
					alternatives->updateRecent(useAlt, loadBalancedReply.get().processBusyTime);
				}

				return result.get();
			}

			if (result.getError().code() != error_code_broken_promise &&
			    result.getError().code() != error_code_request_maybe_delivered) {
				throw result.getError();
			}

			if (atMostOnce) {
				throw request_maybe_delivered();
			}

			if (++numAttempts >= alternatives->size()) {
				backoff = std::min(
				    FLOW_KNOBS->LOAD_BALANCE_MAX_BACKOFF,
				    std::max(FLOW_KNOBS->LOAD_BALANCE_START_BACKOFF, backoff * FLOW_KNOBS->LOAD_BALANCE_BACKOFF_RATE));
			}
		}

		nextAlt = (nextAlt + 1) % alternatives->size();
		resetReply(request, taskID);
	}
}

#include "flow/unactorcompiler.h"

#endif
