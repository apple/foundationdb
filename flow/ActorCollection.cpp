/*
 * ActorCollection.cpp
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

#include "flow/ActorCollection.h"
#include "flow/CoroUtils.h"
#include "flow/IndexedSet.h"
#include "flow/UnitTest.h"
#include <boost/intrusive/list.hpp>
#include <string>
#include <vector>

#ifdef ENABLE_SAMPLING
static LineageReference actorCollectionLineage(LineageReference const& parent) {
	LineageReference lineage = parent;
	lineage.setActorName("actorCollection");
	return lineage;
}
#endif

class ActorCollectionRuntime;

// Owns a child callback and preserves its intrusive-list membership until completion or cancellation.
class Runner final : public boost::intrusive::list_base_hook<>,
                     public Callback<Void>,
                     public FastAllocated<Runner>,
                     NonCopyable {
public:
	explicit Runner(ActorCollectionRuntime* owner) : owner(owner) {}

	~Runner() { detach(); }

	void start(Future<Void> task);
	void fire(Void const&) override;
	void error(Error e) override;

private:
	void detach() {
		if (registered) {
			registered = false;
			Callback<Void>::remove();
		}
	}

	ActorCollectionRuntime* owner;
	Runner* nextCompleted = nullptr;
	bool registered = false;
#ifdef ENABLE_SAMPLING
	LineageReference lineage = actorCollectionLineage(*currentLineage);
#endif

	friend class ActorCollectionRuntime;
};

// An intrusive list of Runners, which are FastAllocated.
using RunnerList = boost::intrusive::list<Runner, boost::intrusive::constant_time_size<false>>;

// Disposes remaining runners in insertion order before their intrusive list is destroyed.
class RunnerListDestroyer final : NonCopyable {
public:
	explicit RunnerListDestroyer(RunnerList* list) : list(list) {}

	~RunnerListDestroyer() {
		list->clear_and_dispose([](Runner* r) { delete r; });
	}

private:
	RunnerList* list;
};

// Coordinates queued additions, child completion, and reentrant-safe collection teardown.
class ActorCollectionRuntime final : NonCopyable {
	// Keeps the add stream's single callback registered for the runtime's active lifetime.
	class AddActorCallback final : public SingleCallback<Future<Void>> {
	public:
		explicit AddActorCallback(ActorCollectionRuntime* owner) : owner(owner) {}

		void fire(Future<Void> const& actor) override;
		void fire(Future<Void>&& actor) override;
		void error(Error e) override;

	private:
		ActorCollectionRuntime* owner;
	};

public:
	ActorCollectionRuntime(FutureStream<Future<Void>> addActor,
	                       int* pCount,
	                       double* lastChangeTime,
	                       double* idleTime,
	                       double* allTime,
	                       bool returnWhenEmptied)
	  : addActor(std::move(addActor)), pCount(pCount), lastChangeTime(lastChangeTime), idleTime(idleTime),
	    allTime(allTime), returnWhenEmptied(returnWhenEmptied), runnersDestroyer(&runners), addActorCallback(this) {
		if (!this->pCount) {
			this->pCount = &count;
		}
	}

	~ActorCollectionRuntime() {
		finished = true;
		if (addCallbackRegistered) {
			addActorCallback.remove();
			addCallbackRegistered = false;
		}
		runners.clear_and_dispose([](Runner* runner) { delete runner; });
	}

	Future<Void> getResult() { return done.getFuture(); }
	void start() {
#ifdef ENABLE_SAMPLING
		LineageReference callbackLineage = actorCollectionLineage(lineage);
		LineageScope scope(&callbackLineage);
#endif
		drain();
	}

private:
	void onAdded(Future<Void> actor) {
		if (finished) {
			return;
		}
		if (draining) {
			pendingAdds.emplace_back(std::move(actor));
			return;
		}

		draining = true;
		handleAdded(std::move(actor));
		draining = false;
		drain();
	}

	void onAddError(Error e) {
		if (!finished) {
			requestError(e);
			drain();
		}
	}

	void onCompleted(Runner* runner) {
		if (finished) {
			return;
		}
		if (completedTail) {
			completedTail->nextCompleted = runner;
		} else {
			completedHead = runner;
		}
		completedTail = runner;
		drain();
	}

	void onError(Error e) {
		if (finished) {
			return;
		}
		if (!failure.present()) {
			failure = e;
		}
		drain();
	}

	void incrementCount() {
		++*pCount;
		if (*pCount == 1 && lastChangeTime && idleTime && allTime) {
			double currentTime = now();
			*idleTime += currentTime - *lastChangeTime;
			*allTime += currentTime - *lastChangeTime;
			*lastChangeTime = currentTime;
		}
	}

	void decrementCount() {
		if (!--*pCount && lastChangeTime && idleTime && allTime) {
			double currentTime = now();
			*allTime += currentTime - *lastChangeTime;
			*lastChangeTime = currentTime;
		}
	}

	void addRunner(Future<Void> actor) {
		auto runner = runners.insert(runners.end(), *new Runner(this));
		runner->start(std::move(actor));
		incrementCount();
	}

	void handleAdded(Future<Void> actor) {
		// Completing inline must not outrun an earlier queued addition.
		if (!runners.empty() || !actor.isReady() || addActor.isReady() || !pendingAdds.empty()) {
			addRunner(std::move(actor));
			return;
		}

		if (actor.isError()) {
			Error e = actor.getError();
			if (e.code() == error_code_actor_cancelled) {
				addRunner(std::move(actor));
				return;
			}
			incrementCount();
			requestError(e);
			return;
		}

		incrementCount();
		decrementCount();
		if (!*pCount && returnWhenEmptied) {
			terminalRequested = true;
		}
	}

	void handleCompleted(Runner* runner) {
		decrementCount();
		if (!*pCount && returnWhenEmptied) {
			terminalRequested = true;
			return;
		}
		runners.erase_and_dispose(runners.iterator_to(*runner), [](Runner* runner) { delete runner; });
	}

	Runner* popCompleted() {
		Runner* runner = completedHead;
		completedHead = runner->nextCompleted;
		if (!completedHead) {
			completedTail = nullptr;
		}
		runner->nextCompleted = nullptr;
		return runner;
	}

	void armAddCallback() {
		if (!addCallbackRegistered) {
			addCallbackRegistered = true;
			auto stream = addActor;
			stream.addCallbackAndClear(&addActorCallback);
		}
	}

	void requestError(Error e) {
		if (!terminalError.present()) {
			terminalError = e;
		}
		terminalRequested = true;
	}

	void deliverTerminal() {
		// Sending can synchronously destroy this runtime, so retain the promise before notifying waiters.
		Promise<Void> terminal = done;
		Optional<Error> error = terminalError;
		finished = true;
		if (addCallbackRegistered) {
			addActorCallback.remove();
			addCallbackRegistered = false;
		}
		if (error.present()) {
			terminal.sendError(error.get());
		} else {
			terminal.send(Void());
		}
	}

	void drain() {
		if (draining || finished) {
			return;
		}
		draining = true;

		while (!terminalRequested) {
			if (!pendingAdds.empty()) {
				Future<Void> actor = std::move(pendingAdds[nextPendingAdd++]);
				if (nextPendingAdd == pendingAdds.size()) {
					pendingAdds.clear();
					nextPendingAdd = 0;
				}
				handleAdded(std::move(actor));
				continue;
			}
			if (addActor.isReady()) {
				if (addActor.isError()) {
					requestError(addActor.getError());
				} else {
					handleAdded(addActor.pop());
				}
				continue;
			}
			if (completedHead) {
				handleCompleted(popCompleted());
				continue;
			}
			if (failure.present()) {
				requestError(failure.get());
				continue;
			}
			armAddCallback();
			draining = false;
			return;
		}

		deliverTerminal();
	}

	FutureStream<Future<Void>> addActor;
	int* pCount;
	double* lastChangeTime;
	double* idleTime;
	double* allTime;
	bool returnWhenEmptied;
	int count = 0;
	RunnerList runners;
	RunnerListDestroyer runnersDestroyer;
	Promise<Void> done;
	AddActorCallback addActorCallback;
	Runner* completedHead = nullptr;
	Runner* completedTail = nullptr;
	std::vector<Future<Void>> pendingAdds;
	size_t nextPendingAdd = 0;
	Optional<Error> failure;
	Optional<Error> terminalError;
	bool addCallbackRegistered = false;
	bool draining = false;
	bool terminalRequested = false;
	bool finished = false;
#ifdef ENABLE_SAMPLING
	LineageReference lineage = actorCollectionLineage(*currentLineage);
#endif

	friend class Runner;
};

void Runner::start(Future<Void> task) {
	if (!task.isReady()) {
		registered = true;
		task.addCallbackAndClear(this);
		return;
	}
	if (task.isError()) {
		error(task.getError());
	} else {
		fire(Void());
	}
}

void Runner::fire(Void const&) {
#ifdef ENABLE_SAMPLING
	LineageReference callbackLineage = actorCollectionLineage(lineage);
	LineageScope scope(&callbackLineage);
#endif
	ActorCollectionRuntime* runtime = owner;
	detach();
	runtime->onCompleted(this);
}

void Runner::error(Error e) {
#ifdef ENABLE_SAMPLING
	LineageReference callbackLineage = actorCollectionLineage(lineage);
	LineageScope scope(&callbackLineage);
#endif
	ActorCollectionRuntime* runtime = owner;
	detach();
	if (e.code() != error_code_actor_cancelled) {
		runtime->onError(e);
	}
}

void ActorCollectionRuntime::AddActorCallback::fire(Future<Void> const& actor) {
#ifdef ENABLE_SAMPLING
	LineageReference callbackLineage = actorCollectionLineage(owner->lineage);
	LineageScope scope(&callbackLineage);
#endif
	owner->onAdded(actor);
}

void ActorCollectionRuntime::AddActorCallback::fire(Future<Void>&& actor) {
#ifdef ENABLE_SAMPLING
	LineageReference callbackLineage = actorCollectionLineage(owner->lineage);
	LineageScope scope(&callbackLineage);
#endif
	owner->onAdded(std::move(actor));
}

void ActorCollectionRuntime::AddActorCallback::error(Error e) {
#ifdef ENABLE_SAMPLING
	LineageReference callbackLineage = actorCollectionLineage(owner->lineage);
	LineageScope scope(&callbackLineage);
#endif
	owner->onAddError(e);
}

static Future<Void> actorCollectionImpl(FutureStream<Future<Void>> addActor,
                                        int* pCount,
                                        double* lastChangeTime,
                                        double* idleTime,
                                        double* allTime,
                                        bool returnWhenEmptied,
                                        NoThrowOnCancel = {}) {
	ActorCollectionRuntime runtime(std::move(addActor), pCount, lastChangeTime, idleTime, allTime, returnWhenEmptied);
	Future<Void> result = runtime.getResult();
	runtime.start();
	co_await result;
}

static Future<Void> actorCollectionUntilEmpty(FutureStream<Future<Void>> addActor,
                                              int* pCount,
                                              double* lastChangeTime,
                                              double* idleTime,
                                              double* allTime) {
	ActorCollectionRuntime runtime(std::move(addActor), pCount, lastChangeTime, idleTime, allTime, true);
	Future<Void> result = runtime.getResult();
	runtime.start();
	co_await result;
}

Future<Void> actorCollection(FutureStream<Future<Void>> const& addActor,
                             int* const& pCount,
                             double* const& lastChangeTime,
                             double* const& idleTime,
                             double* const& allTime,
                             bool const& returnWhenEmptied) {
	if (returnWhenEmptied) {
		return actorCollectionUntilEmpty(addActor, pCount, lastChangeTime, idleTime, allTime);
	}
	return actorCollectionImpl(addActor, pCount, lastChangeTime, idleTime, allTime, returnWhenEmptied);
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

TEST_CASE("/flow/actorCollection/chooseWhen") {
	Promise<Void> promise;
	co_await Choose()
	    .When(delay(0), [&promise](Void const&) { promise.send(Void()); })
	    .When(promise.getFuture(), [](Void const&) { ASSERT(false); })
	    .run();
	ASSERT(promise.isSet());
}

Future<Void> failIfNotCancelled() {
	co_await delay(0);
	ASSERT(false);
}

static Future<Void> recordActorCancellation(Future<Void> pending, std::vector<int>* cancelled, int index) {
	try {
		co_await pending;
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			cancelled->push_back(index);
		}
		throw;
	}
}

static Future<Void> signalSiblingOnActorCancellation(Future<Void> pending, Promise<Void> sibling, bool sendError) {
	try {
		co_await pending;
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			if (sendError) {
				sibling.sendError(operation_failed());
			} else {
				sibling.send(Void());
			}
		}
		throw;
	}
}

static Future<Void> recancelCollectionOnActorCancellation(Future<Void> pending,
                                                          ActorCollection* collection,
                                                          bool returnWhenEmptied,
                                                          bool clearCollection,
                                                          int* cancellationCount) {
	try {
		co_await pending;
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			++*cancellationCount;
			if (clearCollection) {
				collection->clear(returnWhenEmptied);
			} else {
				collection->getResult().cancel();
			}
		}
		throw;
	}
}

// test contract that actors are cancelled when the actor collection is cleared
TEST_CASE("/flow/actorCollection/testCancel") {
	ActorCollection actorCollection(false);
	int actors = deterministicRandom()->randomInt(1, 1000);
	for (int i = 0; i < actors; i++) {
		actorCollection.add(failIfNotCancelled());
	}
	actorCollection.clear(false);
	co_await delay(0);
}

TEST_CASE("/flow/actorCollection/testCancelOrder") {
	constexpr int actorCount = 4;
	for (bool returnWhenEmptied : { false, true }) {
		ActorCollection collection(returnWhenEmptied);
		std::vector<Promise<Void>> pending(actorCount);
		std::vector<int> cancelled;
		for (int index = 0; index < actorCount; ++index) {
			collection.add(recordActorCancellation(pending[index].getFuture(), &cancelled, index));
		}
		collection.clear(returnWhenEmptied);
		ASSERT_EQ(cancelled.size(), pending.size());
		for (int index = 0; index < actorCount; ++index) {
			ASSERT_EQ(cancelled[index], index);
		}
	}
	return Void();
}

TEST_CASE("/flow/actorCollection/testCancelReentrantSiblingCompletion") {
	for (bool returnWhenEmptied : { false, true }) {
		for (bool sendError : { false, true }) {
			ActorCollection collection(returnWhenEmptied);
			Promise<Void> pending;
			Promise<Void> sibling;
			collection.add(signalSiblingOnActorCancellation(pending.getFuture(), sibling, sendError));
			collection.add(sibling.getFuture());
			collection.clear(returnWhenEmptied);
			ASSERT(sibling.isSet());
			ASSERT_EQ(sibling.isError(), sendError);
		}
	}
	return Void();
}

TEST_CASE("/flow/actorCollection/testCancelReentrantCollection") {
	for (bool returnWhenEmptied : { false, true }) {
		for (bool clearCollection : { false, true }) {
			ActorCollection collection(returnWhenEmptied);
			Promise<Void> pending;
			int cancellationCount = 0;
			collection.add(recancelCollectionOnActorCancellation(
			    pending.getFuture(), &collection, returnWhenEmptied, clearCollection, &cancellationCount));
			collection.clear(returnWhenEmptied);
			ASSERT_EQ(cancellationCount, 1);
			ASSERT(!collection.getResult().isReady());
		}
	}
	return Void();
}

#ifdef ENABLE_SAMPLING
// Captures the active actor lineage when a collection result is delivered synchronously.
class ActorCollectionLineageObserver final : public Callback<Void>, NonCopyable {
public:
	void fire(Void const&) override { observe(); }

	void error(Error e) override {
		observedError = e;
		observe();
	}

	void assertObserved(bool expectError) const {
		ASSERT_EQ(actorName, std::string("actorCollection"));
		ASSERT_EQ(observedError.present(), expectError);
	}

private:
	void observe() {
		actorName = currentLineage->actorName();
		Callback<Void>::remove();
	}

	std::string actorName;
	Optional<Error> observedError;
};

TEST_CASE("/flow/actorCollection/testSamplingLineage") {
	for (bool readyActor : { false, true }) {
		PromiseStream<Future<Void>> addActor;
		ActorCollectionRuntime collection(addActor.getFuture(), nullptr, nullptr, nullptr, nullptr, true);
		collection.start();

		ActorCollectionLineageObserver observer;
		Future<Void> result = collection.getResult();
		result.addCallbackAndClear(&observer);

		if (readyActor) {
			addActor.send(Future<Void>(Void()));
		} else {
			Promise<Void> pending;
			addActor.send(pending.getFuture());
			pending.send(Void());
		}
		observer.assertObserved(false);
	}

	PromiseStream<Future<Void>> addActor;
	ActorCollectionRuntime collection(addActor.getFuture(), nullptr, nullptr, nullptr, nullptr, false);
	collection.start();
	ActorCollectionLineageObserver observer;
	Future<Void> result = collection.getResult();
	result.addCallbackAndClear(&observer);
	addActor.sendError(operation_failed());
	observer.assertObserved(true);
	return Void();
}
#endif

Future<Void> failedActor() {
	return operation_failed();
}

TEST_CASE("/flow/actorCollection/testReady") {
	ActorCollection actorCollection(true);
	actorCollection.add(Void());
	co_await actorCollection.getResult();
}

TEST_CASE("/flow/actorCollection/testReadyWhilePending") {
	ActorCollection actorCollection(true);
	Promise<Void> pending;
	actorCollection.add(pending.getFuture());
	actorCollection.add(Void());
	co_await delay(0);
	ASSERT(!actorCollection.getResult().isReady());
	pending.send(Void());
	co_await actorCollection.getResult();
}

TEST_CASE("/flow/actorCollection/testQueuedReadyWhilePending") {
	PromiseStream<Future<Void>> addActor;
	Promise<Void> pending;
	int count = 0;
	addActor.send(Void());
	addActor.send(pending.getFuture());

	Future<Void> collection = actorCollection(addActor.getFuture(), &count, nullptr, nullptr, nullptr, true);
	ASSERT_EQ(count, 1);
	ASSERT(!collection.isReady());

	pending.send(Void());
	co_await collection;
	ASSERT_EQ(count, 0);
}

TEST_CASE("/flow/actorCollection/testReadyError") {
	ActorCollection actorCollection(false);
	actorCollection.add(failedActor());
	try {
		co_await actorCollection.getResult();
		ASSERT(false);
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_operation_failed);
	}
}

TEST_CASE("/flow/actorCollection/testAddStreamError") {
	PromiseStream<Future<Void>> addActor;
	Future<Void> collection = actorCollection(addActor.getFuture());
	addActor.sendError(operation_failed());
	try {
		co_await collection;
		ASSERT(false);
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_operation_failed);
	}
}

TEST_CASE("/flow/actorCollection/testPendingErrorCancels") {
	ActorCollection actorCollection(false);
	Promise<Void> pending;
	actorCollection.add(failIfNotCancelled());
	actorCollection.add(pending.getFuture());
	pending.sendError(operation_failed());
	try {
		co_await actorCollection.getResult();
		ASSERT(false);
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_operation_failed);
	}
	co_await delay(0);
}

// test contract that even if the actor collection has stopped and new actors are added to the promise stream, they are
// all cancelled when resetting actor
TEST_CASE("/flow/actorCollection/testCancelPromiseStream") {
	ActorCollection actorCollection(false);
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
	co_await delay(0);
}
