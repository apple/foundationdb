/*
 * LoadBalance.actor.h
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

#pragma once

// When actually compiled (NO_INTELLISENSE), include the generated version of this file.  In intellisense use the source version.
#if defined(NO_INTELLISENSE) && !defined(FLOW_LOADBALANCE_ACTOR_G_H)
	#define FLOW_LOADBALANCE_ACTOR_G_H
	#include "fdbrpc/LoadBalance.actor.g.h"
#elif !defined(FLOW_LOADBALANCE_ACTOR_H)
	#define FLOW_LOADBALANCE_ACTOR_H

#include "flow/flow.h"
#include "flow/Knobs.h"

#include "fdbrpc/FailureMonitor.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbrpc/Locality.h"
#include "fdbrpc/QueueModel.h"
#include "fdbrpc/MultiInterface.h"
#include "flow/actorcompiler.h"  // This must be the last #include.

using std::vector;

struct ModelHolder : NonCopyable, public ReferenceCounted<ModelHolder> {
	QueueModel* model;
	bool released;
	double startTime;
	double delta;
	uint64_t token;

	ModelHolder( QueueModel* model, uint64_t token ) : model(model), token(token), released(false), startTime(now()) {
		if(model) {
			delta = model->addRequest(token);
		}
	}

	void release(bool clean, bool futureVersion, double penalty, bool measureLatency = true) {
		if(model && !released) {
			released = true;
			double latency = (clean || measureLatency) ? now() - startTime : 0.0;
			model->endRequest(token, latency, penalty, delta, clean, futureVersion);
		}
	}

	~ModelHolder() { 
		release(false, false, -1.0, false);
	}
};

// Subclasses must initialize all members in their default constructors
// Subclasses must serialize all members
struct LoadBalancedReply {
	double penalty;
	Optional<Error> error;
	LoadBalancedReply() : penalty(1.0) {}
};

Optional<LoadBalancedReply> getLoadBalancedReply(const LoadBalancedReply *reply);
Optional<LoadBalancedReply> getLoadBalancedReply(const void*);

// Returns true if we got a value for our request
// Throws an error if the request returned an error that should bubble out
// Returns false if we got an error that should result in reissuing the request
template <class T>
bool checkAndProcessResult(ErrorOr<T> result, Reference<ModelHolder> holder, bool atMostOnce, bool triedAllOptions) {
	Optional<LoadBalancedReply> loadBalancedReply;
	if(!result.isError()) {
		loadBalancedReply = getLoadBalancedReply(&result.get());
	}

	int errCode;
	if (loadBalancedReply.present()) {
		errCode = loadBalancedReply.get().error.present() ? loadBalancedReply.get().error.get().code() : error_code_success;
	}
	else {
		errCode = result.isError() ? result.getError().code() : error_code_success;
	}

	bool maybeDelivered = errCode == error_code_broken_promise || errCode == error_code_request_maybe_delivered;
	bool receivedResponse = loadBalancedReply.present() ? !loadBalancedReply.get().error.present() : result.present();
	receivedResponse = receivedResponse || (!maybeDelivered && errCode != error_code_process_behind);
	bool futureVersion = errCode == error_code_future_version || errCode == error_code_process_behind;

	holder->release(receivedResponse, futureVersion, loadBalancedReply.present() ? loadBalancedReply.get().penalty : -1.0);

	if (errCode == error_code_server_overloaded)
	{
		return false;
	}

	if (loadBalancedReply.present() && !loadBalancedReply.get().error.present()) {
		return true;
	}

	if (!loadBalancedReply.present() && result.present()) {
		return true;
	}

	if(receivedResponse) {
		throw loadBalancedReply.present() ? loadBalancedReply.get().error.get() : result.getError();
	}

	if(atMostOnce && maybeDelivered) {
		throw request_maybe_delivered();
	}

	if(triedAllOptions && errCode == error_code_process_behind) {
		throw process_behind(); 
	}

	return false;
}

ACTOR template <class Request>
Future<Optional<REPLY_TYPE(Request)>> makeRequest(RequestStream<Request> const* stream, Request request, double backoff, Future<Void> requestUnneeded, QueueModel *model, bool isFirstRequest, bool atMostOnce, bool triedAllOptions) {
	if(backoff > 0.0) {
		wait(delay(backoff) || requestUnneeded);
	}

	if(requestUnneeded.isReady()) {
		return Optional<REPLY_TYPE(Request)>();
	}

	state Reference<ModelHolder> holder(new ModelHolder(model, stream->getEndpoint().token.first()));

	ErrorOr<REPLY_TYPE(Request)> result = wait(stream->tryGetReply(request));
	if(checkAndProcessResult(result, holder, atMostOnce, triedAllOptions)) {
		return result.get();	
	}
	else {
		return Optional<REPLY_TYPE(Request)>();
	}
}

template <class Reply>
void addLaggingRequest(Future<Optional<Reply>> reply, Promise<Void> requestFinished, QueueModel *model) {
	requestFinished.send(Void());
	if(!reply.isReady()) {
		if(model) {
			if(model->laggingRequestCount > FLOW_KNOBS->MAX_LAGGING_REQUESTS_OUTSTANDING || model->laggingRequests.isReady()) {
				model->laggingRequests.cancel();
				model->laggingRequestCount = 0;
				model->addActor = PromiseStream<Future<Void>>();
				model->laggingRequests = actorCollection( model->addActor.getFuture(), &model->laggingRequestCount );
			}

			model->addActor.send(success(errorOr(reply)));
		}
	}
}

// Keep trying to get a reply from any of servers until success or cancellation; tries to take into account
//   failMon's information for load balancing and avoiding failed servers
// If ALL the servers are failed and the list of servers is not fresh, throws an exception to let the caller refresh the list of servers
ACTOR template <class Interface, class Request, class Multi>
Future< REPLY_TYPE(Request) > loadBalance(
	Reference<MultiInterface<Multi>> alternatives,
	RequestStream<Request> Interface::* channel,
	Request request = Request(),
	TaskPriority taskID = TaskPriority::DefaultPromiseEndpoint,
	bool atMostOnce = false, // if true, throws request_maybe_delivered() instead of retrying automatically
	QueueModel* model = nullptr) 
{
	state Future<Optional<REPLY_TYPE(Request)>> firstRequest;
	state Optional<uint64_t> firstRequestEndpoint;
	state Future<Optional<REPLY_TYPE(Request)>> secondRequest;
	state Future<Void> secondDelay = Never();

	state Promise<Void> requestFinished;
	state double startTime = now();
	
	setReplyPriority(request, taskID);
	if (!alternatives)
		return Never();

	ASSERT( alternatives->size() );

	state int bestAlt = deterministicRandom()->randomInt(0, alternatives->countBest());
	state int nextAlt = deterministicRandom()->randomInt(0, std::max(alternatives->size() - 1,1));
	if( nextAlt >= bestAlt )
		nextAlt++;

	if(model) {
		double bestMetric = 1e9;
		double nextMetric = 1e9;
		double bestTime = 1e9;
		double nextTime = 1e9;
		int badServers = 0;

		for(int i=0; i<alternatives->size(); i++) {
			if(badServers < std::min(i, FLOW_KNOBS->LOAD_BALANCE_MAX_BAD_OPTIONS + 1) && i == alternatives->countBest()) {
				break;
			}
			
			RequestStream<Request> const* thisStream = &alternatives->get( i, channel );
			if (!IFailureMonitor::failureMonitor().getState( thisStream->getEndpoint() ).failed) {
				auto& qd = model->getMeasurement(thisStream->getEndpoint().token.first());
				if(now() > qd.failedUntil) {
					double thisMetric = qd.smoothOutstanding.smoothTotal();
					double thisTime = qd.latency;
					if(FLOW_KNOBS->LOAD_BALANCE_PENALTY_IS_BAD && qd.penalty > 1.001) {
						++badServers;
					}
				
					if(thisMetric < bestMetric) {
						if(i != bestAlt) {
							nextAlt = bestAlt;
							nextMetric = bestMetric;
							nextTime = bestTime;
						}
						bestAlt = i;
						bestMetric = thisMetric;
						bestTime = thisTime;
					} else if( thisMetric < nextMetric ) {
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
		if( nextMetric > 1e8 ) {
			for(int i=alternatives->countBest(); i<alternatives->size(); i++) {
				RequestStream<Request> const* thisStream = &alternatives->get( i, channel );
				if (!IFailureMonitor::failureMonitor().getState( thisStream->getEndpoint() ).failed) {
					auto& qd = model->getMeasurement(thisStream->getEndpoint().token.first());
					if(now() > qd.failedUntil) {
						double thisMetric = qd.smoothOutstanding.smoothTotal();
						double thisTime = qd.latency;
				
						if( thisMetric < nextMetric ) {
							nextAlt = i;
							nextMetric = thisMetric;
							nextTime = thisTime;
						}
					}
				}
			}
		}

		if(nextTime < 1e9) {
			if(bestTime > FLOW_KNOBS->INSTANT_SECOND_REQUEST_MULTIPLIER*(model->secondMultiplier*(nextTime) + FLOW_KNOBS->BASE_SECOND_REQUEST_TIME)) {
				secondDelay = Void();
			} else {
				secondDelay = delay( model->secondMultiplier*nextTime + FLOW_KNOBS->BASE_SECOND_REQUEST_TIME );
			}
		}
		else {
			secondDelay = Never();
		}
	}

	state int startAlt = nextAlt;
	state int startDistance = (bestAlt+alternatives->size()-startAlt) % alternatives->size();

	state int numAttempts = 0;
	state double backoff = 0;
	state bool triedAllOptions = false;
	loop {
		if(now() - startTime > (g_network->isSimulated() ? 30.0 : 600.0)) {
			TraceEvent ev(g_network->isSimulated() ? SevWarn : SevWarnAlways, "LoadBalanceTooLong");
			ev.suppressFor(1.0);
			ev.detail("Duration", now() - startTime);
			ev.detail("NumAttempts", numAttempts);
			ev.detail("Backoff", backoff);
			ev.detail("TriedAllOptions", triedAllOptions);
			if(ev.isEnabled()) {
				ev.log();
				for(int alternativeNum=0; alternativeNum<alternatives->size(); alternativeNum++) {
					RequestStream<Request> const* thisStream = &alternatives->get( alternativeNum, channel );
					TraceEvent(SevWarn, "LoadBalanceTooLongEndpoint").detail("Addr", thisStream->getEndpoint().getPrimaryAddress()).detail("Token", thisStream->getEndpoint().token).detail("Failed", IFailureMonitor::failureMonitor().getState( thisStream->getEndpoint() ).failed);
				}
			}
		}

		// Find an alternative, if any, that is not failed, starting with nextAlt
		state RequestStream<Request> const* stream = nullptr;
		for(int alternativeNum=0; alternativeNum<alternatives->size(); alternativeNum++) {
			int useAlt = nextAlt;
			if( nextAlt == startAlt )
				useAlt = bestAlt;
			else if( (nextAlt+alternatives->size()-startAlt) % alternatives->size() <= startDistance )
				useAlt = (nextAlt+alternatives->size()-1) % alternatives->size();
			
			stream = &alternatives->get( useAlt, channel );
			if (!IFailureMonitor::failureMonitor().getState( stream->getEndpoint() ).failed && (!firstRequestEndpoint.present() || stream->getEndpoint().token.first() != firstRequestEndpoint.get()))
				break;
			nextAlt = (nextAlt+1) % alternatives->size();
			if(nextAlt == startAlt) triedAllOptions = true;
			stream=nullptr;
		}

		if(!stream && !firstRequest.isValid() ) {
			// Everything is down!  Wait for someone to be up.

			vector<Future<Void>> ok( alternatives->size() );
			for(int i=0; i<ok.size(); i++) {
				ok[i] = IFailureMonitor::failureMonitor().onStateEqual( alternatives->get(i, channel).getEndpoint(), FailureStatus(false) );
			}

			if(!alternatives->alwaysFresh()) {
				if(now() - g_network->networkInfo.newestAlternativesFailure > FLOW_KNOBS->ALTERNATIVES_FAILURE_RESET_TIME) {
					g_network->networkInfo.oldestAlternativesFailure = now();
				}
				
				double delay = FLOW_KNOBS->ALTERNATIVES_FAILURE_MIN_DELAY;
				if(now() - g_network->networkInfo.lastAlternativesFailureSkipDelay > FLOW_KNOBS->ALTERNATIVES_FAILURE_SKIP_DELAY) {
					g_network->networkInfo.lastAlternativesFailureSkipDelay = now();
				} else {
					double elapsed = now()-g_network->networkInfo.oldestAlternativesFailure;
					delay = std::max(delay, std::min(elapsed*FLOW_KNOBS->ALTERNATIVES_FAILURE_DELAY_RATIO, FLOW_KNOBS->ALTERNATIVES_FAILURE_MAX_DELAY));
					delay = std::max(delay, std::min(elapsed*FLOW_KNOBS->ALTERNATIVES_FAILURE_SLOW_DELAY_RATIO, FLOW_KNOBS->ALTERNATIVES_FAILURE_SLOW_MAX_DELAY));
				}

				// Making this SevWarn means a lot of clutter
				if(now() - g_network->networkInfo.newestAlternativesFailure > 1 || deterministicRandom()->random01() < 0.01) {
					TraceEvent("AllAlternativesFailed")
						.detail("Interval", FLOW_KNOBS->CACHE_REFRESH_INTERVAL_WHEN_ALL_ALTERNATIVES_FAILED)
						.detail("Alternatives", alternatives->description())
						.detail("Delay", delay);
				}

				g_network->networkInfo.newestAlternativesFailure = now();

				choose {
					when ( wait( quorum( ok, 1 ) ) ) {}
					when ( wait( ::delayJittered( delay ) ) ) {
						throw all_alternatives_failed();
					}
				}
			} else {
				wait( quorum( ok, 1 ) );
			}

			numAttempts = 0; // now that we've got a server back, reset the backoff
		} else if(!stream) {
			//Only the first location is available. 
			Optional<REPLY_TYPE(Request)> result = wait( firstRequest );
			if(result.present()) {
				return result.get();
			}

			firstRequest = Future<Optional<REPLY_TYPE(Request)>>();
			firstRequestEndpoint = Optional<uint64_t>();
		} else if( firstRequest.isValid() ) {
			//Issue a second request, the first one is taking a long time.
			secondRequest = makeRequest(stream, request, backoff, requestFinished.getFuture(), model, false, atMostOnce, triedAllOptions);
			state bool firstFinished = false;

			loop {
				choose {
					when(ErrorOr<Optional<REPLY_TYPE(Request)>> result = wait( firstRequest.isValid() ? errorOr(firstRequest) : Never() )) {
						if(result.isError() || result.get().present()) {
							addLaggingRequest(secondRequest, requestFinished, model);
							if(result.isError()) {
								throw result.getError();
							}
							else {
								return result.get().get();
							}
						}

						firstRequest = Future<Optional<REPLY_TYPE(Request)>>();
						firstRequestEndpoint = Optional<uint64_t>();
						firstFinished = true;
					}
					when(ErrorOr<Optional<REPLY_TYPE(Request)>> result = wait( errorOr(secondRequest) )) {
						if(result.isError() || result.get().present()) {
							if(!firstFinished) {
								addLaggingRequest(firstRequest, requestFinished, model);
							}
							if(result.isError()) {
								throw result.getError();
							}
							else {
								return result.get().get();
							}
						}

						break;
					}
				}
			}

			if(++numAttempts >= alternatives->size()) {
				backoff = std::min(FLOW_KNOBS->LOAD_BALANCE_MAX_BACKOFF, std::max(FLOW_KNOBS->LOAD_BALANCE_START_BACKOFF, backoff * FLOW_KNOBS->LOAD_BALANCE_BACKOFF_RATE));
			}
		} else {
			//Issue a request, if it takes too long to get a reply, go around the loop
			firstRequest = makeRequest(stream, request, backoff, requestFinished.getFuture(), model, true, atMostOnce, triedAllOptions);
			firstRequestEndpoint = stream->getEndpoint().token.first();

			loop {
				choose {
					when(ErrorOr<Optional<REPLY_TYPE(Request)>> result = wait( errorOr(firstRequest) )) {
						if(model) {
							model->secondMultiplier = std::max(model->secondMultiplier-FLOW_KNOBS->SECOND_REQUEST_MULTIPLIER_DECAY, 1.0);
							model->secondBudget = std::min(model->secondBudget+FLOW_KNOBS->SECOND_REQUEST_BUDGET_GROWTH, FLOW_KNOBS->SECOND_REQUEST_MAX_BUDGET);
						}

						if(result.isError()) {
							throw result.getError();
						}

						if(result.get().present()) {
							return result.get().get();
						}

						firstRequest = Future<Optional<REPLY_TYPE(Request)>>();
						firstRequestEndpoint = Optional<uint64_t>();
						break;
					}
					when(wait(secondDelay)) {
						secondDelay = Never();
						if(model && model->secondBudget >= 1.0) {
							model->secondMultiplier += FLOW_KNOBS->SECOND_REQUEST_MULTIPLIER_GROWTH;
							model->secondBudget -= 1.0;
							break;
						}
					}
				}
			}

			if(++numAttempts >= alternatives->size()) {
				backoff = std::min(FLOW_KNOBS->LOAD_BALANCE_MAX_BACKOFF, std::max(FLOW_KNOBS->LOAD_BALANCE_START_BACKOFF, backoff * FLOW_KNOBS->LOAD_BALANCE_BACKOFF_RATE));
			}
		}

		nextAlt = (nextAlt+1) % alternatives->size();
		if(nextAlt == startAlt) triedAllOptions = true;
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

Optional<BasicLoadBalancedReply> getBasicLoadBalancedReply(const BasicLoadBalancedReply *reply);
Optional<BasicLoadBalancedReply> getBasicLoadBalancedReply(const void*);

// A simpler version of LoadBalance that does not send second requests where the list of servers are always fresh
ACTOR template <class Interface, class Request, class Multi>
Future< REPLY_TYPE(Request) > basicLoadBalance(
	Reference<ModelInterface<Multi>> alternatives,
	RequestStream<Request> Interface::* channel,
	Request request = Request(),
	TaskPriority taskID = TaskPriority::DefaultPromiseEndpoint,
	bool atMostOnce = false) 
{
	setReplyPriority(request, taskID);
	if (!alternatives)
		return Never();

	ASSERT( alternatives->size() && alternatives->alwaysFresh() );

	state int bestAlt = alternatives->getBest();
	state int nextAlt = deterministicRandom()->randomInt(0, std::max(alternatives->size() - 1,1));
	if( nextAlt >= bestAlt )
		nextAlt++;

	state int startAlt = nextAlt;
	state int startDistance = (bestAlt+alternatives->size()-startAlt) % alternatives->size();

	state int numAttempts = 0;
	state double backoff = 0;
	state int useAlt;
	loop {
		// Find an alternative, if any, that is not failed, starting with nextAlt
		state RequestStream<Request> const* stream = nullptr;
		for(int alternativeNum=0; alternativeNum<alternatives->size(); alternativeNum++) {
			useAlt = nextAlt;
			if( nextAlt == startAlt )
				useAlt = bestAlt;
			else if( (nextAlt+alternatives->size()-startAlt) % alternatives->size() <= startDistance )
				useAlt = (nextAlt+alternatives->size()-1) % alternatives->size();
			
			stream = &alternatives->get( useAlt, channel );
			if (!IFailureMonitor::failureMonitor().getState( stream->getEndpoint() ).failed)
				break;
			nextAlt = (nextAlt+1) % alternatives->size();
			stream=nullptr;
		}

		if(!stream) {
			// Everything is down!  Wait for someone to be up.

			vector<Future<Void>> ok( alternatives->size() );
			for(int i=0; i<ok.size(); i++) {
				ok[i] = IFailureMonitor::failureMonitor().onStateEqual( alternatives->get(i, channel).getEndpoint(), FailureStatus(false) );
			}
			wait( quorum( ok, 1 ) );

			numAttempts = 0; // now that we've got a server back, reset the backoff
		} else {
			if(backoff > 0.0) {
				wait(delay(backoff));
			}

			ErrorOr<REPLY_TYPE(Request)> result = wait(stream->tryGetReply(request));

			if(result.present()) {
				Optional<BasicLoadBalancedReply> loadBalancedReply = getBasicLoadBalancedReply(&result.get());
				if(loadBalancedReply.present()) {
					alternatives->updateRecent( useAlt, loadBalancedReply.get().processBusyTime );
				}

				return result.get();
			}

			if(result.getError().code() != error_code_broken_promise && result.getError().code() != error_code_request_maybe_delivered) {
				throw result.getError();
			}

			if(atMostOnce) {
				throw request_maybe_delivered();
			}

			if(++numAttempts >= alternatives->size()) {
				backoff = std::min(FLOW_KNOBS->LOAD_BALANCE_MAX_BACKOFF, std::max(FLOW_KNOBS->LOAD_BALANCE_START_BACKOFF, backoff * FLOW_KNOBS->LOAD_BALANCE_BACKOFF_RATE));
			}
		}

		nextAlt = (nextAlt+1) % alternatives->size();
		resetReply(request, taskID);
	}
}

#include "flow/unactorcompiler.h"

#endif
