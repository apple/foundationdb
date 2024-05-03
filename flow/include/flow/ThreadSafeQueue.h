
// ThreadSafeQueue<T> is a multi-producer, single-consumer queue.

// It is almost but not quite lock-free (the exception is that if a thread is
// stopped in a very narrow window in push(), pop() will "block" in the sense
// of returning Optional<T>() to the consumer until the former thread makes progress)

// It has an extra facility for event-loop integration: calling canSleep() before blocking
// the consumer thread and waking the consumer thread whenever push() returns true will permit
// the consumer thread to block when the queue is empty with a minimum of overhead.  A caller
// can ignore this facility by simply not calling canSleep and ignoring the return value of push().

// Based in part on the queue at
// http://www.1024cores.net/home/lock-free-algorithms/queues/intrusive-mpsc-node-based-queue which is covered by this
// BSD license:
/* Copyright (c) 2010-2011 Dmitry Vyukov. All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the
following conditions are met:

   1. Redistributions of source code must retain the above copyright notice, this list of
      conditions and the following disclaimer.

   2. Redistributions in binary form must reproduce the above copyright notice, this list
      of conditions and the following disclaimer in the documentation and/or other materials
      provided with the distribution.

THIS SOFTWARE IS PROVIDED BY DMITRY VYUKOV "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL DMITRY
VYUKOV OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
DAMAGE.

The views and conclusions contained in the software and documentation are those of the authors and should not be
interpreted as representing official policies, either expressed or implied, of Dmitry Vyukov.*/

#pragma once

#include <atomic>

#if VALGRIND
#include <drd.h>
#endif

template <class T>
class ThreadSafeQueue : NonCopyable {
	struct BaseNode {
		std::atomic<BaseNode*> next;
		BaseNode() : next(nullptr) {}
	};

	struct Node : BaseNode, FastAllocated<Node> {
		T data;
		Node(T const& data) : data(data) {}
		Node(T&& data) : data(std::move(data)) {}
	};
	std::atomic<BaseNode*> head;
	BaseNode* tail;
	BaseNode stub, sleeping;
	bool sleepy;

	BaseNode* popNode() {
		BaseNode* tail = this->tail;
		BaseNode* next = tail->next.load();
#if VALGRIND
		ANNOTATE_HAPPENS_BEFORE(&tail->next);
#endif
		if (tail == &this->stub) {
			if (!next) {
				return nullptr;
			}
			this->tail = next;
			tail = next;
			next = next->next.load();
#if VALGRIND
			ANNOTATE_HAPPENS_BEFORE(&next->next);
#endif
		}
		if (next) {
			this->tail = next;
			return tail;
		}
		BaseNode* head = this->head.load();
#if VALGRIND
		ANNOTATE_HAPPENS_BEFORE(&this->head);
#endif
		if (tail != head) {
			return nullptr;
		}
#if VALGRIND
		ANNOTATE_HAPPENS_AFTER(&this->stub.next);
#endif
		this->stub.next.store(nullptr);
		pushNode(&this->stub);
		next = tail->next.load();
#if VALGRIND
		ANNOTATE_HAPPENS_BEFORE(&tail->next);
#endif
		if (next) {
			this->tail = next;
			return tail;
		}
		return nullptr;
	}

	// Pushes n at the end of the queue and returns the node immediately before it
	BaseNode* pushNode(BaseNode* n) {
#if VALGRIND
		ANNOTATE_HAPPENS_AFTER(&head);
#endif
		BaseNode* prev = head.exchange(n);
#if VALGRIND
		ANNOTATE_HAPPENS_BEFORE(&head);
		ANNOTATE_HAPPENS_AFTER(&prev->next);
#endif
		prev->next.store(n);
		return prev;
	}

public:
	ThreadSafeQueue() {
#if VALGRIND
		ANNOTATE_HAPPENS_AFTER(&this->head);
#endif
		this->head.store(&this->stub);
		this->tail = &this->stub;
		this->sleepy = false;
	}
	~ThreadSafeQueue() {
		while (pop().present())
			;
	}

	// If push() returns true, the consumer may be sleeping and should be woken
	template <class U>
	bool push(U&& data) {
		Node* n = new Node(std::forward<U>(data));
		return pushNode(n) == &sleeping;
	}

	///////////// The below functions may only be called by a single, consumer thread //////////////////

	// If canSleep returns true, then the queue is empty and the next push() will return true
	bool canSleep() {
		if (sleepy) {
			return false; // We already have sleeping in the queue from a previous call to canSleep.  Pop it and then
			              // maybe you can sleep!
		}
		if (this->tail != &stub || this->tail->next.load()) {
#if VALGRIND
			ANNOTATE_HAPPENS_BEFORE(&this->tail->next);
#endif
			return false; // There is definitely something in the queue.  This is a rejection test not needed for
			              // correctness, but avoids calls to pushNode
		}
#if VALGRIND
		ANNOTATE_HAPPENS_BEFORE(&this->tail->next);
#endif
		// sleeping is definitely not in the queue, so we can safely try...
		sleeping.next.store(nullptr);
		bool ok = pushNode(&sleeping) == &stub;
		sleepy = true; // sleeping is in the queue, regardless of whether it's the first thing; we need to pop it before
		               // sleeping again
		return ok;
	}

	Optional<T> pop() {
		BaseNode* b = popNode();
		if (b == &sleeping) {
			sleepy = false;
			b = popNode();
		}
		if (b == &sleeping)
			ASSERT(false);
		if (b == &stub)
			ASSERT(false);
		if (!b)
			return Optional<T>();

		Node* n = (Node*)b;
		T data = std::move(n->data);
		delete n;
		return Optional<T>(std::move(data));
	}
};
