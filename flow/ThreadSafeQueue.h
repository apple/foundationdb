
// ThreadSafeQueue<T> is a multi-producer, single-consumer queue.  

// It is almost but not quite lock-free (the exception is that if a thread is 
// stopped in a very narrow window in push(), pop() will "block" in the sense
// of returning Optional<T>() to the consumer until the former thread makes progress)

// It has an extra facility for event-loop integration: calling canSleep() before blocking
// the consumer thread and waking the consumer thread whenever push() returns true will permit
// the consumer thread to block when the queue is empty with a minimum of overhead.  A caller
// can ignore this facility by simply not calling canSleep and ignoring the return value of push().

// Based in part on the queue at http://www.1024cores.net/home/lock-free-algorithms/queues/intrusive-mpsc-node-based-queue
// which is covered by this BSD license:
/* Copyright (c) 2010-2011 Dmitry Vyukov. All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

   1. Redistributions of source code must retain the above copyright notice, this list of
	  conditions and the following disclaimer.

   2. Redistributions in binary form must reproduce the above copyright notice, this list
	  of conditions and the following disclaimer in the documentation and/or other materials
	  provided with the distribution.

THIS SOFTWARE IS PROVIDED BY DMITRY VYUKOV "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL DMITRY VYUKOV OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

The views and conclusions contained in the software and documentation are those of the authors and should not be interpreted as representing official policies, either expressed or implied, of Dmitry Vyukov.*/

template <class T>
class ThreadSafeQueue : NonCopyable {
	struct BaseNode {
		BaseNode* volatile  next;
	};

	struct Node : BaseNode, FastAllocated<Node>
	{
		T data;
		Node( T const& data ) : data(data) {}
	};
	BaseNode* volatile  head;
	BaseNode*           tail;
	BaseNode stub, sleeping;
	bool sleepy;

	BaseNode* popNode() {
		BaseNode* tail = this->tail;
		BaseNode* next = tail->next;
		if (tail == &this->stub) {
			if (0 == next)
				return 0;
			this->tail = next;
			tail = next;
			next = next->next;
		}
		if (next)
		{
			this->tail = next;
			return tail;
		}
		BaseNode* head = this->head;
		if (tail != head)
			return 0;
		pushNode( &this->stub );
		next = tail->next;
		if (next)
		{
			this->tail = next;
			return tail;
		}
		return 0;
	}

	// Pushes n at the end of the queue and returns the node immediately before it
	BaseNode* pushNode( BaseNode* n ) {
		n->next = 0;
		BaseNode* prev = interlockedExchangePtr( &head, n );
		prev->next = n;
		return prev;
	}
public:
	ThreadSafeQueue() {
		this->head = &this->stub;
		this->tail = &this->stub;
		this->stub.next = 0;
		this->sleeping.next = 0;
		this->sleepy = false;
	}
	~ThreadSafeQueue() {
		while (pop().present());
	}

	// If push() returns true, the consumer may be sleeping and should be woken
	bool push( T const& data ) {
		Node* n = new Node(data);
		n->data = data;
		return pushNode( n ) == &sleeping;
	}

	///////////// The below functions may only be called by a single, consumer thread //////////////////

	// If canSleep returns true, then the queue is empty and the next push() will return true
	bool canSleep() {
		if (sleepy) return false;  // We already have sleeping in the queue from a previous call to canSleep.  Pop it and then maybe you can sleep!
		if (this->tail != &stub || this->tail->next != 0) return false;  // There is definitely something in the queue.  This is a rejection test not needed for correctness, but avoids calls to pushNode
		// sleeping is definitely not in the queue, so we can safely try...
		bool ok = pushNode( &sleeping ) == &stub;
		sleepy = true; // sleeping is in the queue, regardless of whether it's the first thing; we need to pop it before sleeping again
		return ok;
	}

	Optional<T> pop() {
		BaseNode* b = popNode();
		if (b == &sleeping) { sleepy = false; b = popNode(); }
		if ( b == &sleeping ) ASSERT(false);
		if ( b == &stub ) ASSERT(false);
		if (!b) return Optional<T>();

		Node* n = (Node*)b;
		T data = std::move(n->data);
		delete n;
		return Optional<T>( std::move(data) );
	}
};