/*
 * ActorCollection.actor.cpp
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

#include "flow/ActorCollection.h"
#include "flow/IndexedSet.h"
#include "flow/UnitTest.h"
#include <boost/intrusive/list.hpp>
#include "flow/actorcompiler.h" // This must be the last #include.

struct Runner : public boost::intrusive::list_base_hook<>, FastAllocated<Runner>, NonCopyable {
	Future<Void> handler;
};

// An intrusive list of Runners, which are FastAllocated.  Each runner holds a handler future
typedef boost::intrusive::list<Runner, boost::intrusive::constant_time_size<false>> RunnerList;

// The runners list in the ActorCollection must be destroyed when the actor is destructed rather
// than before returning or throwing
struct RunnerListDestroyer : NonCopyable {
	RunnerListDestroyer(RunnerList* list) : list(list) {}

	~RunnerListDestroyer() {
		list->clear_and_dispose([](Runner* r) { delete r; });
	}

	RunnerList* list;
};

ACTOR Future<Void> runnerHandler(PromiseStream<RunnerList::iterator> output,
                                 PromiseStream<Error> errors,
                                 Future<Void> task,
                                 RunnerList::iterator runner) {
	try {
		wait(task);
		output.send(runner);
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled)
			throw;
		errors.send(e);
	}
	return Void();
}

ACTOR Future<Void> actorCollection(FutureStream<Future<Void>> addActor,
                                   int* pCount,
                                   double* lastChangeTime,
                                   double* idleTime,
                                   double* allTime,
                                   bool returnWhenEmptied) {
	state RunnerList runners;
	state RunnerListDestroyer runnersDestroyer(&runners);
	state PromiseStream<RunnerList::iterator> complete;
	state PromiseStream<Error> errors;
	state int count = 0;
	if (!pCount)
		pCount = &count;

	loop choose {
		when(Future<Void> f = waitNext(addActor)) {
			// Insert new Runner at the end of the instrusive list and get an iterator to it
			auto i = runners.insert(runners.end(), *new Runner());

			// Start the handler for completions or errors from f, sending runner to complete stream
			Future<Void> handler = runnerHandler(complete, errors, f, i);
			i->handler = handler;

			++*pCount;
			if (*pCount == 1 && lastChangeTime && idleTime && allTime) {
				double currentTime = now();
				*idleTime += currentTime - *lastChangeTime;
				*allTime += currentTime - *lastChangeTime;
				*lastChangeTime = currentTime;
			}
		}
		when(RunnerList::iterator i = waitNext(complete.getFuture())) {
			if (!--*pCount) {
				if (lastChangeTime && idleTime && allTime) {
					double currentTime = now();
					*allTime += currentTime - *lastChangeTime;
					*lastChangeTime = currentTime;
				}
				if (returnWhenEmptied)
					return Void();
			}
			// If we didn't return then the entire list wasn't destroyed so erase/destroy i
			runners.erase_and_dispose(i, [](Runner* r) { delete r; });
		}
		when(Error e = waitNext(errors.getFuture())) {
			throw e;
		}
	}
}

template <class T, class U>
struct Traceable<std::pair<T, U>> {
	static constexpr bool value = Traceable<T>::value && Traceable<U>::value;
	static std::string toString(const std::pair<T, U>& p) {
		auto tStr = Traceable<T>::toString(p.first);
		auto uStr = Traceable<U>::toString(p.second);
		std::string result(tStr.size() + uStr.size() + 3, 'x');
		std::copy(tStr.begin(), tStr.end(), result.begin());
		auto iter = result.begin() + tStr.size();
		*(iter++) = ' ';
		*(iter++) = '-';
		*(iter++) = ' ';
		std::copy(uStr.begin(), uStr.end(), iter);
		return result;
	}
};

void forceLinkActorCollectionTests() {}

// The above implementation relies on the behavior that fulfilling a promise
// that another when clause in the same choose block is waiting on is not fired synchronously.
TEST_CASE("/flow/actorCollection/chooseWhen") {
	state Promise<Void> promise;
	choose {
		when(wait(delay(0))) {
			promise.send(Void());
		}
		when(wait(promise.getFuture())) {
			// Should be cancelled, since another when clause in this choose block has executed
			ASSERT(false);
		}
	}
	return Void();
}

ACTOR Future<Void> failIfNotCancelled() {
	wait(delay(0));
	ASSERT(false);
	return Void();
}

// test contract that actors are cancelled when the actor collection is cleared
TEST_CASE("/flow/actorCollection/testCancel") {
	state ActorCollection actorCollection(false);
	int actors = deterministicRandom()->randomInt(1, 1000);
	for (int i = 0; i < actors; i++) {
		actorCollection.add(failIfNotCancelled());
	}
	actorCollection.clear(false);
	wait(delay(0));
	return Void();
}

ACTOR Future<Void> failedActor() {
	throw operation_failed();
}

// test contract that even if the actor collection has stopped and new actors are added to the promise stream, they are
// all cancelled when resetting actor
TEST_CASE("/flow/actorCollection/testCancelPromiseStream") {
	state ActorCollection actorCollection(false);
	int actors = deterministicRandom()->randomInt(1, 500);
	for (int i = 0; i < actors; i++) {
		actorCollection.add(failIfNotCancelled());
	}
	// this actor should cause the actorCollection actor to exit, meaning the new futures just build up in the promise
	// stream
	actorCollection.add(failedActor());
	for (int i = 0; i < actors; i++) {
		actorCollection.add(failIfNotCancelled());
	}
	// Instead of doing actorCollection.clear(false) we reinitialize to also clear the promise stream. Otherwise on
	// resetting the actor collection actor, the new actors will be pulled from the promise stream into the new instance
	// Note that this test fails on the assert in failIfNotCancelled() when this is replaced with
	// actorCollection.clear(false).
	actorCollection = ActorCollection(false);
	wait(delay(0));
	return Void();
}
