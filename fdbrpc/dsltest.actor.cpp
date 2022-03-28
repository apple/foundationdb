/*
 * dsltest.actor.cpp
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

#include <iostream>
#include <algorithm>
#include "flow/FastRef.h"
#undef ERROR
#include "fdbrpc/simulator.h"
#include "fdbrpc/ActorFuzz.h"
#include "flow/DeterministicRandom.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

void* allocateLargePages(int total);

bool testFuzzActor(Future<int> (*actor)(FutureStream<int> const&, PromiseStream<int> const&, Future<Void> const&),
                   const char* desc,
                   std::vector<int> const& expectedOutput) {
	// Run the test 5 times with different "timing"
	int i, outCount;
	bool ok = true;
	for (int trial = 0; trial < 5; trial++) {
		PromiseStream<int> in, out;
		Promise<Void> err;
		int before = deterministicRandom()->randomInt(0, 4);
		int errorBefore = before + deterministicRandom()->randomInt(0, 4);
		// printf("\t\tTrial #%d: %d, %d\n", trial, before, errorBefore);
		if (errorBefore <= before)
			err.sendError(operation_failed());
		for (i = 0; i < before; i++) {
			in.send((i + 1) * 1000);
		}
		Future<int> ret = (*actor)(in.getFuture(), out, err.getFuture());
		while (i < 1000000 && !ret.isReady()) {
			i++;
			if (errorBefore == i)
				err.sendError(operation_failed());
			in.send(i * 1000);
		}
		if (ret.isReady()) {
			if (ret.isError())
				out.send(ret.getError().code());
			else
				out.send(ret.get());
		} else {
			printf("\tERROR: %s did not return after consuming %d input values\n", desc, i);
			if (trial)
				printf("\t\tResult was inconsistent between runs!  (Trial %d)\n", trial);
			ok = false;
			// return false;
		}

		outCount = -1;
		while (out.getFuture().isReady()) {
			int o = out.getFuture().pop();
			outCount++;
			if (outCount < expectedOutput.size() && expectedOutput[outCount] != o) {
				printf("\tERROR: %s output #%d incorrect: %d != expected %d\n",
				       desc,
				       outCount,
				       o,
				       expectedOutput[outCount]);
				if (trial)
					printf("\t\tResult was inconsistent between runs!\n");
				ok = false;
				// return false;
			}
		}
		if (outCount + 1 != expectedOutput.size()) {
			printf(
			    "\tERROR: %s output length incorrect: %d != expected %zu\n", desc, outCount + 1, expectedOutput.size());
			if (trial)
				printf("\t\tResult was inconsistent between runs!\n");
			ok = false;
			// return false;
		}

		// We might have put in values that weren't actually consumed...
		while (in.getFuture().isReady()) {
			in.getFuture().pop();
			i--;
		}
	}
	// printf("\t%s: OK, %d input values -> %d output values\n", desc, i, outCount);
	return ok;
}

#if 0
void memoryTest2() {
	const int Size = 2000 << 20;
	const int Reads = 4 << 20;
	const int MaxThreads = 4;

	char* block = new char[Size];
	memset(block, 0, Size);

	char** random = new char*[ Reads * MaxThreads ];
	random[0] = block;
	for(int i=1; i<Reads*MaxThreads; ) {
		char *s = &block[ deterministicRandom()->randomInt(0, Size) ];
		random[i++] = s;
		/*for(int j=0; j<10 && i<Reads*MaxThreads; j++,i++) {
			random[i] = s + deterministicRandom()->randomInt(0, 4096);
			if (random[i] >= block+Size) random[i] -= Size;
		}*/
	}

	for(int threads=1; threads<=MaxThreads; threads++) {
		double tstart = timer();

		std::vector<ThreadFuture<Void>> done;
		for(int t=0; t<threads; t++) {
			char** r = random + Reads*t;
			done.push_back(
				inThread<Void>( [r,Reads] () -> Void {
					for(int i=0; i<Reads; i++)
						if ( *r[i] )
							std::cout << "Does not happen" << std::endl;
					return Void();
				}));
		}
		waitForAll(done).getBlocking();
		double duration = timer() - tstart;

		std::cout << format("%d threads: %f sec, %0.2fM/sec", threads, duration, Reads*threads/1e6/duration) << std::endl;
	}
}
#endif

enum { MaxTraversalsPerThread = 64 };

void showNumaStatus();
void* numaAllocate(size_t size);

#if 0
void memoryTest() {
	//memoryTest2();
	//return;

	showNumaStatus();

	const int N = 128<<20;	// 128 = 1GB
	const int N2 = 8<<20;
	std::cout << "Preparing memory test with " << N / 1e6 * sizeof(void*) << " MB" << std::endl;
	void **x;
	if (0) {
		std::cout << "  NUMA large pages" << std::endl;
		x = (void**)numaAllocate(size_t(N)*sizeof(void*));
	} else if (1) {
		std::cout << "  Normal pages" << std::endl;
		x = new void*[ N ];
		printf("  at %p\n", x);
	} else {
		std::cout << "  Large pages" << std::endl;
		x = (void**)allocate(N*sizeof(void*), true);
	}
	memset(x, 0, ((int64_t)N) * sizeof(void*));

	showNumaStatus();

	if (1) {
		std::cout <<"  Random permutation" << std::endl;
		// Random cyclic permutation
		for(int i=0; i<N; i++)
			x[i] = &x[i];
		// Sattolo's algorithm
		for(int n = N-1; n >= 1; n--) {
			int k = deterministicRandom()->randomInt(0, n);  //random.IRandomX(0, n-1);
			std::swap( x[k], x[n] );
		}
	} else {
		std::cout <<"  Sequential permutation" << std::endl;
		// Sequential
		for(int i=0; i<N-1; i++)
			x[i] = &x[i+1];
		x[N-1] = &x[0];
	}
	void **p = x;
	for(int i=0; i<N; i++) {
		p = (void**)*p;
		if (p == x) {
			std::cout << "Cycle " << i << std::endl;
			if (i != N-1) terminate();
		}
	}

	const int MT = 16;
	for(int TraversalsPerThread = 1; TraversalsPerThread <= MaxTraversalsPerThread; TraversalsPerThread *= 2)
	{
		const int PseudoThreads = MT * TraversalsPerThread;
		void **starts[MT*MaxTraversalsPerThread];
		for(int t=0; t<PseudoThreads; t++) {
			starts[t] = &x[ N/PseudoThreads * t ];
			//starts[t] = &x[ deterministicRandom()->randomInt(0,N) ];
		}
		for(int T=1; T<=MT; T+=T) {
			double start = timer();
			std::vector< Future<double> > done;
			for(int t=0; t<T; t++) {
				void*** start = starts + t*TraversalsPerThread;
				done.push_back(
					inThread<double>( [start,N2,TraversalsPerThread] () -> double {
						void **p[MaxTraversalsPerThread];
						for(int j=0; j<TraversalsPerThread; j++)
							p[j] = start[j];
						for(int i=0; i<N2; i++)
							for(int j=0; j<TraversalsPerThread; j++) {
								p[j] = (void**)*p[j];
								if (TraversalsPerThread > 1)
									_mm_prefetch( (const char*)p[j], _MM_HINT_T0 );
							}
						for(int j=0; j<TraversalsPerThread; j++)
							if (p[j] == p[(j+1)%TraversalsPerThread])
								std::cout << "N";
						return timer();
					}));
			}
			double firstEnd = 1e30;
			for(int t=0; t<T; t++) {
				done[t].getBlocking();
				firstEnd = std::min(firstEnd, done[t].get());
			}
			double end = timer();
			printf("  %2dx%2d traversals: %5.3fs, %6.1f M/sec, %4.1f%%\n", T, (int)TraversalsPerThread, end-start,
				N2 / 1e6 * (T*TraversalsPerThread) / (end-start),
				(firstEnd-start)/(end-start)*100.0);
		}
	}

	//delete[] x;	// TODO: Free large pages
}
#endif

ACTOR template <int N, class X>
[[flow_allow_discard]] Future<X> addN(Future<X> in) {
	X i = wait(in);
	return i + N;
}

ACTOR template <class A, class B>
[[flow_allow_discard]] Future<Void> switchTest(FutureStream<A> as, Future<B> oneb) {
	loop choose {
		when(A a = waitNext(as)) { std::cout << "A " << a << std::endl; }
		when(B b = wait(oneb)) {
			std::cout << "B " << b << std::endl;
			break;
		}
	}
	loop {
		std::cout << "Done!" << std::endl;
		return Void();
	}
}

class TestBuffer : public ReferenceCounted<TestBuffer> {
public:
	static TestBuffer* create(int length) {
#if defined(__INTEL_COMPILER)
		return new TestBuffer(length);
#else
		auto b = (TestBuffer*)new int[(length + 7) / 4];
		new (b) TestBuffer(length);
		return b;
#endif
	}
#if !defined(__INTEL_COMPILER)
	void operator delete(void* buf) {
		std::cout << "Freeing buffer" << std::endl;
		delete[](int*) buf;
	}
#endif

	int size() const { return length; }
	uint8_t* begin() { return data; }
	uint8_t* end() { return data + length; }
	const uint8_t* begin() const { return data; }
	const uint8_t* end() const { return data + length; }

private:
	TestBuffer(int length) noexcept : length(length) {}
	int length;
	uint8_t data[1];
};

int fastKeyCount = 0;

class FastKey : public FastAllocated<FastKey>, public ReferenceCounted<FastKey> {
public:
	FastKey() : length(0) {}
	FastKey(char* b, int length) : length(length) {
		ASSERT(length <= sizeof(data));
		memcpy(data, b, length);
	}
	~FastKey() { fastKeyCount++; }
	int size() const { return length; }
	uint8_t* begin() { return data; }
	uint8_t* end() { return data + length; }
	const uint8_t* begin() const { return data; }
	const uint8_t* end() const { return data + length; }

private:
	int length;
	uint8_t data[252];
};

struct TestB : FastAllocated<TestB> {
	char x[65];
};

void fastAllocTest() {
	double t;

	std::vector<void*> d;
	for (int i = 0; i < 1000000; i++) {
		d.push_back(FastAllocator<64>::allocate());
		int r = deterministicRandom()->randomInt(0, 1000000);
		if (r < d.size()) {
			FastAllocator<64>::release(d[r]);
			d[r] = d.back();
			d.pop_back();
		}
	}
	std::sort(d.begin(), d.end());
	if (std::unique(d.begin(), d.end()) != d.end())
		std::cout << "Pointer returned twice!?" << std::endl;

	for (int i = 0; i < 2; i++) {
		void* p = FastAllocator<64>::allocate();
		void* q = FastAllocator<64>::allocate();
		std::cout << (intptr_t)p << " " << (intptr_t)q << std::endl;
		FastAllocator<64>::release(p);
		FastAllocator<64>::release(q);
	}

	t = timer();
	for (int i = 0; i < 1000000; i++)
		(void)FastAllocator<64>::allocate();
	t = timer() - t;
	std::cout << "Allocations: " << (1 / t) << "M/sec" << std::endl;

	t = timer();
	for (int i = 0; i < 1000000; i++)
		FastAllocator<64>::release(FastAllocator<64>::allocate());
	t = timer() - t;
	std::cout << "Allocate/Release pairs: " << (1 / t) << "M/sec" << std::endl;

	t = timer();
	void* pp[100];
	for (int i = 0; i < 10000; i++) {
		for (int j = 0; j < 100; j++)
			pp[j] = FastAllocator<64>::allocate();
		for (int j = 0; j < 100; j++)
			FastAllocator<64>::release(pp[j]);
	}
	t = timer() - t;
	std::cout << "Allocate/Release interleaved(100): " << (1 / t) << "M/sec" << std::endl;

	t = timer();
	for (int i = 0; i < 1000000; i++)
		delete new TestB;
	t = timer() - t;
	std::cout << "Allocate/Release TestB pairs: " << (1 / t) << "M/sec" << std::endl;

#if FLOW_THREAD_SAFE
	t = timer();
	std::vector<Future<bool>> results;
	for (int i = 0; i < 4; i++)
		results.push_back(inThread<bool>([]() -> bool {
			TestB* pp[100];
			for (int i = 0; i < 10000; i++) {
				for (int j = 0; j < 100; j++)
					pp[j] = new TestB;
				for (int j = 0; j < 100; j++)
					delete pp[j];
			}
			return true;
		}));
	waitForAll(results).getBlocking();
	t = timer() - t;
	std::cout << "Threaded Allocate/Release TestB interleaved (100): " << results.size() << " x " << (1 / t) << "M/sec"
	          << std::endl;
#endif

	volatile int32_t v = 0;

	t = timer();
	for (int i = 0; i < 10000000; i++)
		interlockedIncrement(&v);
	t = timer() - t;
	std::cout << "interlocked increment: " << 10.0 / t << "M/sec " << v << std::endl;

	v = 5;
	t = timer();
	for (int i = 0; i < 10000000; i++) {
		interlockedCompareExchange(&v, 5, 5);
	}
	t = timer() - t;
	std::cout << "1 state machine: " << 10.0 / t << "M/sec " << v << std::endl;

	v = 0;
	t = timer();
	for (int i = 0; i < 10000000; i++)
		v++;
	t = timer() - t;
	std::cout << "volatile increment: " << 10.0 / t << "M/sec " << v << std::endl;

	{
		Reference<TestBuffer> b(TestBuffer::create(1000));
		memcpy(b->begin(), "Hello, world!", 14);

		t = timer();
		for (int i = 0; i < 10000000; i++) {
			Reference<TestBuffer> r = std::move(b);
			b = std::move(r);
		}
		t = timer() - t;
		std::cout << "move Reference<Buffer>: " << 10.0 / t << "M/sec " << std::endl;

		t = timer();
		for (int i = 0; i < 10000000; i++) {
			Reference<TestBuffer> r = b;
		}
		t = timer() - t;
		std::cout << "copy (1) Reference<Buffer>: " << 10.0 / t << "M/sec " << std::endl;

		Reference<TestBuffer> c = b;
		t = timer();
		for (int i = 0; i < 10000000; i++) {
			Reference<TestBuffer> r = b;
		}
		t = timer() - t;
		std::cout << "copy (2) Reference<Buffer>: " << 10.0 / t << "M/sec " << std::endl;

		std::cout << (const char*)b->begin() << std::endl;
	}
	t = timer();
	for (int i = 0; i < 10000000; i++) {
		delete new FastKey;
	}
	t = timer() - t;
	std::cout << "delete new FastKey: " << 10.0 / t << "M/sec " << fastKeyCount << std::endl;

	t = timer();
	for (int i = 0; i < 10000000; i++) {
		Reference<FastKey> r(new FastKey);
	}
	t = timer() - t;
	std::cout << "new Reference<FastKey>: " << 10.0 / t << "M/sec " << fastKeyCount << std::endl;
}

template <class PromiseT>
Future<Void> threadSafetySender(std::vector<PromiseT>& v, Event& start, Event& ready, int iterations) {
	for (int i = 0; i < iterations; i++) {
		start.block();
		if (v.size() == 0)
			return Void();
		for (int i = 0; i < v.size(); i++)
			v[i].send(Void());
		ready.set();
	}
	return Void();
}

ACTOR [[flow_allow_discard]] void threadSafetyWaiter(Future<Void> f, int32_t* count) {
	wait(f);
	interlockedIncrement(count);
}
ACTOR [[flow_allow_discard]] void threadSafetyWaiter(FutureStream<Void> f, int n, int32_t* count) {
	while (n--) {
		waitNext(f);
		interlockedIncrement(count);
	}
}

#if 0
void threadSafetyTest() {
	double t = timer();

	int N = 10000, V = 100;

	std::vector<Promise<Void>> v;
	Event start, ready;
	Future<Void> sender = inThread<Void>( [&] { return threadSafetySender( v, start, ready, N ); } );

	for(int i=0; i<N; i++) {
		v.clear();
		for (int j = 0; j < V; j++)
			v.push_back(Promise<Void>());
		std::vector<Future<Void>> f( v.size() );
		for(int i=0; i<v.size(); i++)
			f[i] = v[i].getFuture();
		std::random_shuffle( f.begin(), f.end() );

		start.set();
		int32_t count = 0;
		for(int i=0; i<f.size(); i++)
			threadSafetyWaiter( f[i], &count );
		ready.block();

		if (count != V)
			std::cout << "Thread safety error: " << count << std::endl;
	}

	t = timer()-t;
	std::cout << "Thread safety test (2t): " << (V*N/1e6/t) << "M/sec" << std::endl;
}

void threadSafetyTest2() {
	double t = timer();

	int N = 1000, V = 100;

	// std::vector<PromiseStream<Void>> streams( 100 );
	std::vector<PromiseStream<Void>> streams;
	for (int i = 0; i < 100; i++)
		streams.push_back(PromiseStream<Void>());
	std::vector<PromiseStream<Void>> v;
	Event start, ready;
	Future<Void> sender = inThread<Void>( [&] { return threadSafetySender( v, start, ready, N ); } );

	for(int i=0; i<N; i++) {
		std::vector<int> counts( streams.size() );
		v.clear();
		for(int k=0; k<V; k++) {
			int i = deterministicRandom()->randomInt(0, (int)streams.size());
			counts[i]++;
			v.push_back( streams[i] );
		}

		start.set();
		int32_t count = 0;
		for(int i=0; i<streams.size(); i++)
			threadSafetyWaiter( streams[i].getFuture(), counts[i], &count );
		ready.block();

		if (count != V)
			std::cout << "Thread safety error: " << count << std::endl;
	}

	t = timer()-t;
	std::cout << "Thread safety test 2 (2t): " << (V*N/1e6/t) << "M/sec" << std::endl;
}

volatile int32_t cancelled = 0, returned = 0;

ACTOR [[flow_allow_discard]] Future<Void> returnCancelRacer( Future<Void> f ) {
	try {
		wait(f);
	} catch ( Error& ) {
		interlockedIncrement( &cancelled );
		throw;
	}
	interlockedIncrement( &returned );
	return Void();
}

void returnCancelRaceTest() {
	int N = 100, M = 100;
	for(int i=0; i<N; i++) {
		std::vector< Promise<Void> > promises;
		std::vector< Future<Void> > futures;
		for(int i=0; i < M; i++) {
			promises.push_back( Promise<Void>() );
			futures.push_back( returnCancelRacer( promises.back().getFuture() ) );
		}
		std::random_shuffle( futures.begin(), futures.end() );

		// FIXME: Doesn't work as written with auto-reset
		// events. Probably not particularly racy as written. Test may
		// FAIL or PASS at whim.

		Event ev1, ev2;
		ThreadFuture<Void> b = inThread<Void>( [&] ()->Void {
			ev1.block();
			for(int i=0; i<promises.size(); i++)
				futures[i] = Future<Void>();
			return Void();
		} );
		ThreadFuture<Void> a = inThread<Void>([&]()->Void {
			ev2.block();
			for(int i=0; i<promises.size(); i++) {
				promises[i].send(Void());
				for( volatile int32_t dummy = 0; dummy < 10; dummy ++ );
			}
			return Void();
		} );
		ev1.set(); ev2.set();
		a.getBlocking();
		b.getBlocking();
	}

	bool ok = cancelled && returned && cancelled+returned == N*M;
	printf("ReturnCancelRaceTest: %s\n", ok ? "PASS" : "FAIL");
	printf("  %d cancelled, %d returned\n", cancelled, returned);
}
#endif

ACTOR [[flow_allow_discard]] Future<int> chooseTest(Future<int> a, Future<int> b) {
	choose {
		when(int A = wait(a)) { return A; }
		when(int B = wait(b)) { return B; }
	}
}

void showArena(ArenaBlock* a, ArenaBlock* parent) {
	printf("ArenaBlock %p (<-%p): %d bytes, %d refs\n", a, parent, a->size(), a->debugGetReferenceCount());
	if (!a->isTiny()) {
		int o = a->nextBlockOffset;
		while (o) {
			ArenaBlockRef* r = (ArenaBlockRef*)((char*)a->getData() + o);

			// If alignedBuffer is valid then print its pointer and size, else recurse
			if (r->aligned4kBufferSize != 0) {
				printf("AlignedBuffer %p (<-%p) %u bytes\n", r->aligned4kBuffer, a, r->aligned4kBufferSize);
			} else {
				showArena(r->next, a);
			}

			o = r->nextBlockOffset;
		}
	}
}

void arenaTest() {
	BinaryWriter wr(AssumeVersion(g_network->protocolVersion()));
	{
		Arena arena;
		VectorRef<StringRef> test;
		test.push_back(arena, StringRef(arena, LiteralStringRef("Hello")));
		test.push_back(arena, StringRef(arena, LiteralStringRef(", ")));
		test.push_back(arena, StringRef(arena, LiteralStringRef("World!")));

		for (auto i = test.begin(); i != test.end(); ++i)
			for (auto j = i->begin(); j != i->end(); ++j)
				std::cout << *j;
		std::cout << std::endl;

		wr << test;
	}
	{
		Arena arena2;
		VectorRef<StringRef> test2;
		BinaryReader reader(wr.getData(), wr.getLength(), AssumeVersion(g_network->protocolVersion()));
		reader >> test2 >> arena2;

		for (auto i = test2.begin(); i != test2.end(); ++i)
			for (auto j = i->begin(); j != i->end(); ++j)
				std::cout << *j;
		std::cout << std::endl;
	}

	double t = timer();
	for (int i = 0; i < 100; i++) {
		Arena ar;
		for (int i = 0; i < 10000000; i++)
			new (ar) char[10];
	}
	printf("100 x 10M x 10B allocated+freed from Arenas: %f sec\n", timer() - t);

	// printf("100M x 8bytes allocations: %d bytes used\n", 0);//ar.getSize());
	// showArena( ar.impl.getPtr(), 0 );
};

ACTOR [[flow_allow_discard]] void testStream(FutureStream<int> xs) {
	loop {
		int x = waitNext(xs);
		std::cout << x << std::endl;
	}
}

ACTOR [[flow_allow_discard]] Future<Void> actorTest1(bool b) {
	printf("1");
	if (b)
		throw future_version();
	return Void();
}

ACTOR [[flow_allow_discard]] void actorTest2(bool b) {
	printf("2");
	if (b)
		throw future_version();
}

ACTOR [[flow_allow_discard]] Future<Void> actorTest3(bool b) {
	try {
		if (b)
			throw future_version();
	} catch (Error&) {
		printf("3");
		return Void();
	}
	printf("\nactorTest3 failed\n");
	return Void();
}

ACTOR [[flow_allow_discard]] Future<Void> actorTest4(bool b) {
	state double tstart = now();
	try {
		if (b)
			throw operation_failed();
	} catch (...) {
		wait(delay(1));
	}
	if (now() < tstart + 1)
		printf("actorTest4 failed");
	else
		printf("4");
	return Void();
}

ACTOR [[flow_allow_discard]] Future<bool> actorTest5() {
	state bool caught = false;

	loop {
		loop {
			state bool inloop = false;
			if (caught) {
				printf("5");
				return true;
			}
			try {
				loop {
					if (inloop) {
						printf("\nactorTest5 failed\n");
						return false;
					}
					inloop = true;
					if (1)
						throw operation_failed();
				}
			} catch (Error&) {
				caught = true;
			}
		}
	}
}

ACTOR [[flow_allow_discard]] Future<bool> actorTest6() {
	state bool caught = false;
	loop {
		if (caught) {
			printf("6");
			return true;
		}
		try {
			if (1)
				throw operation_failed();
		} catch (Error&) {
			caught = true;
		}
	}
}

ACTOR [[flow_allow_discard]] Future<bool> actorTest7() {
	try {
		loop {
			loop {
				if (1)
					throw operation_failed();
				if (1) {
					printf("actorTest7 failed (1)\n");
					return false;
				}
				if (0)
					break;
			}
			if (1) {
				printf("actorTest7 failed (2)\n");
				return false;
			}
		}
	} catch (Error&) {
		printf("7");
		return true;
	}
}

ACTOR [[flow_allow_discard]] Future<bool> actorTest8() {
	state bool caught = false;
	state Future<bool> set = true;

	loop {
		state bool inloop = false;
		if (caught) {
			printf("8");
			return true;
		}
		try {
			loop {
				if (inloop) {
					printf("\nactorTest8 failed\n");
					return false;
				}
				bool b = wait(set);
				inloop = true;
				if (1)
					throw operation_failed();
			}
		} catch (Error&) {
			caught = true;
		}
	}
}

ACTOR [[flow_allow_discard]] Future<bool> actorTest9A(Future<Void> setAfterCalling) {
	state int count = 0;
	loop {
		if (count == 4) {
			printf("9");
			return true;
		}
		if (count && count != 4) {
			printf("\nactorTest9 failed\n");
			return false;
		}
		loop {
			loop {
				wait(setAfterCalling);
				loop {
					loop {
						count++;
						break;
					}
					wait(Future<Void>(Void()));
					count++;
					break;
				}
				count++;
				break;
			}
			count++;
			break;
		}
		// loopDepth < 0 ???
	}
}

Future<bool> actorTest9() {
	Promise<Void> p;
	Future<bool> f = actorTest9A(p.getFuture());
	p.send(Void());
	return f;
}

ACTOR [[flow_allow_discard]] Future<Void> actorTest10A(FutureStream<int> inputStream, Future<Void> go) {
	state int i;
	for (i = 0; i < 5; i++) {
		wait(go);
		int input = waitNext(inputStream);
	}
	return Void();
}

void actorTest10() {
	PromiseStream<int> ins;
	Promise<Void> go;
	for (int x = 0; x < 2; x++)
		ins.send(x);
	Future<Void> a = actorTest10A(ins.getFuture(), go.getFuture());
	go.send(Void());
	for (int x = 0; x < 3; x++)
		ins.send(x);
	if (!a.isReady())
		printf("\nactorTest10 failed\n");
	else
		printf("10");
}

ACTOR [[flow_allow_discard]] Future<Void> cancellable() {
	wait(Never());
	return Void();
}

ACTOR [[flow_allow_discard]] Future<Void> simple() {
	return Void();
}

ACTOR [[flow_allow_discard]] Future<Void> simpleWait() {
	wait(Future<Void>(Void()));
	return Void();
}

ACTOR [[flow_allow_discard]] Future<int> simpleRet(Future<int> x) {
	int i = wait(x);
	return i;
}

template <int i>
Future<int> chain(Future<int> const& x);

ACTOR template <int i>
[[flow_allow_discard]] Future<int> achain(Future<int> x) {
	int k = wait(chain<i>(x));
	return k + 1;
}

template <int i>
Future<int> chain(Future<int> const& x) {
	return achain<i - 1>(x);
}

template <>
Future<int> chain<0>(Future<int> const& x) {
	return x;
}

ACTOR [[flow_allow_discard]] Future<int> chain2(Future<int> x, int i);

ACTOR [[flow_allow_discard]] Future<int> chain2(Future<int> x, int i) {
	if (i > 1) {
		int k = wait(chain2(x, i - 1));
		return k + 1;
	} else {
		int k = wait(x);
		return k + i;
	}
}

ACTOR [[flow_allow_discard]] Future<Void> cancellable2() {
	try {
		wait(Never());
		return Void();
	} catch (Error& e) {
		throw;
	}
}

ACTOR [[flow_allow_discard]] Future<int> introLoadValueFromDisk(Future<std::string> filename) {
	std::string file = wait(filename);

	if (file == "/dev/threes")
		return 3;
	else
		ASSERT(false);
	return 0; // does not happen
}

ACTOR [[flow_allow_discard]] Future<int> introAdd(Future<int> a, Future<int> b) {
	state int x = wait(a);
	int y = wait(b);
	return x + y; // x would be undefined here if it was not "state"
}

ACTOR [[flow_allow_discard]] Future<int> introFirst(Future<int> a, Future<int> b) {
	choose {
		when(int x = wait(a)) { return x; }
		when(int x = wait(b)) { return x; }
	}
}

struct AddReply {
	int sum;
	AddReply() {}
	AddReply(int x) : sum(x) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, sum);
	}
};

struct AddRequest {
	int a, b;
	Promise<AddReply> reply; // Self-addressed envelope

	AddRequest() {}
	AddRequest(int a, int b) : a(a), b(b) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, a, b, reply);
	}
};

ACTOR [[flow_allow_discard]] void introAddServer(PromiseStream<AddRequest> add) {
	loop choose {
		when(AddRequest req = waitNext(add.getFuture())) {
			printf("%d + %d = %d\n", req.a, req.b, req.a + req.b);
			req.reply.send(req.a + req.b);
		}
	}
}

void introPromiseFuture() {
	Promise<int> myPromise;

	Future<int> myFuture = myPromise.getFuture();

	myPromise.send(12345);

	ASSERT(myFuture.isReady() && myFuture.get() == 12345);
}

void introActor() {
	Future<int> f = introLoadValueFromDisk(std::string("/dev/threes"));
	ASSERT(f.get() == 3);

	Promise<int> a, b;
	Future<int> sum = introAdd(a.getFuture(), b.getFuture());
	b.send(3);
	ASSERT(!sum.isReady());
	a.send(2);
	ASSERT(sum.get() == 5);

	Promise<int> c, d;
	Future<int> first = introFirst(c.getFuture(), d.getFuture());
	ASSERT(!first.isReady());
	// d.send(100);
	d.sendError(operation_failed());
	ASSERT(first.isError() && first.getError().code() == error_code_operation_failed);
	// ASSERT( first.getBlocking() == 100 );

	PromiseStream<AddRequest> addInterface;
	introAddServer(addInterface);

	Future<AddReply> reply = addInterface.getReply(AddRequest(5, 2));
	ASSERT(reply.get().sum == 7);

	printf("OK\n");
}

template <int N>
void chainTest() {
	auto startt = timer();
	for (int i = 0; i < 100000; i++) {
		Promise<int> p;
		Future<int> f = chain<N>(p.getFuture());
		p.send(i);
		ASSERT(f.get() == i + N);
	}
	auto endt = timer();
	printf("chain<%d>: %0.3f M/sec\n", N, 0.1 / (endt - startt));

	startt = timer();
	for (int i = 0; i < 100000; i++) {
		Promise<int> p;
		Future<int> f = chain2(p.getFuture(), N);
		p.send(i);
		ASSERT(f.get() == i + N);
	}
	endt = timer();
	printf("chain2<%d>: %0.3f M/sec\n", N, 0.1 / (endt - startt));
}

ACTOR [[flow_allow_discard]] void cycle(FutureStream<Void> in, PromiseStream<Void> out, int* ptotal) {
	loop {
		waitNext(in);
		(*ptotal)++;
		out.send(Void());
	}
}

ACTOR [[flow_allow_discard]] Future<Void> cycleTime(int nodes, int times) {
	state std::vector<PromiseStream<Void>> n(nodes);
	state int total = 0;

	// 1->2, 2->3, ..., n-1->0
	for (int i = 1; i < nodes; i++)
		cycle(n[i].getFuture(), n[(i + 1) % nodes], &total);

	state double startT = timer();
	n[1].send(Void());
	loop {
		waitNext(n[0].getFuture());
		if (!--times)
			break;
		n[1].send(Void());
	}

	printf("Ring test: %d nodes, %d total ops, %.3f seconds\n", nodes, total, timer() - startT);
	return Void();
}

void sleeptest() {
#ifdef __linux__
	int times[] = { 0, 100, 500, 1000, 5000, 100000, 500000, 1000000 };
	for (int j = 0; j < 8; j++) {
		double b = timer();
		int n = std::min(100, 4000000 / (1 + times[j]));
		for (int i = 0; i < n; i++) {
			timespec ts;
			ts.tv_sec = times[j] / 1000000;
			ts.tv_nsec = (times[j] % 1000000) * 1000;
			clock_nanosleep(CLOCK_MONOTONIC, 0, &ts, nullptr);
			// nanosleep(&ts, nullptr);
		}
		double t = timer() - b;
		printf("Sleep test (%dus x %d): %0.1f\n", times[j], n, double(t) / n * 1e6);
	}
#endif
}

void asyncMapTest() {
	Future<Void> c;

	{
		AsyncMap<int, int> m1;
		m1.set(10, 1);
		ASSERT(m1.get(10) == 1);
		ASSERT(m1.get(20) == 0);
		Future<Void> a = m1.onChange(10);
		Future<Void> b = m1.onChange(20);
		c = m1.onChange(30);
		ASSERT(!a.isReady() && !b.isReady());
		m1.set(10, 0);
		ASSERT(a.isReady() && !a.isError() && !b.isReady() && m1.get(10) == 0);
		m1.set(20, 5);
		ASSERT(b.isReady() && !b.isError() && m1.get(20) == 5);

		a = m1.onChange(10);
		b = m1.onChange(20);
		m1.triggerRange(15, 25);
		ASSERT(!a.isReady() && b.isReady() && !b.isError() && m1.get(20) == 5);
	}
	ASSERT(c.isReady() && c.isError() && c.getError().code() == error_code_broken_promise);

	printf("AsyncMap: OK\n");

	double startt;
	AsyncMap<int, int> m2;
	startt = timer();
	for (int i = 0; i < 1000000; i++) {
		m2.set(5, 0);
		m2.set(5, 1);
	}
	printf("  set(not present/present): %0.1fM/sec\n", 2.0 / (timer() - startt));
	startt = timer();
	for (int i = 0; i < 1000000; i++) {
		m2.set(5, 1);
		m2.set(5, 2);
	}
	printf("  set(present/present): %0.1fM/sec\n", 2.0 / (timer() - startt));
	startt = timer();
	for (int i = 0; i < 1000000; i++) {
		m2.set(5, 1);
	}
	printf("  set(no change): %0.1fM/sec\n", 1.0 / (timer() - startt));

	m2.set(5, 5);
	startt = timer();
	for (int i = 0; i < 1000000; i++)
		m2.onChange(5);
	printf("  onChange(present, cancelled): %0.1fM/sec\n", 1.0 / (timer() - startt));
	startt = timer();
	for (int i = 0; i < 1000000; i++)
		m2.onChange(10);
	printf("  onChange(not present, cancelled): %0.1fM/sec\n", 1.0 / (timer() - startt));
	startt = timer();
	for (int i = 0; i < 1000000; i++) {
		auto f = m2.onChange(10);
		m2.set(10, 1);
		m2.set(10, 0);
	}
	printf("  onChange(not present, set): %0.1fM/sec\n", 1.0 / (timer() - startt));
	startt = timer();
	for (int i = 0; i < 1000000; i++) {
		auto f = m2.onChange(5);
		m2.set(5, i + 1);
	}
	printf("  onChange(present, set): %0.1fM/sec\n", 1.0 / (timer() - startt));
}

extern void net2_test();

void dsltest() {
	double startt, endt;

	setThreadLocalDeterministicRandomSeed(40);

	asyncMapTest();

	net2_test();
	// sleeptest();

	Future<Void> ctf = cycleTime(1000, 1000);
	ctf.get();

	introPromiseFuture();
	introActor();
	// return;

	printf("Actor control flow tests: ");
	actorTest1(true);
	actorTest2(true);
	actorTest3(true);
	// if (g_network == &g_simulator)
	// g_simulator.run( actorTest4(true) );
	actorTest5();
	actorTest6();
	actorTest7();
	actorTest8();
	actorTest9();
	actorTest10();

	printf("\n");

	printf("Running actor fuzz tests:\n");
	// Only include this test outside of Windows because of MSVC compiler bug
#ifndef WIN32
	auto afResults = actorFuzzTests();
#else
	std::pair<int, int> afResults(0, 0);
#endif
	printf("Actor fuzz tests: %d/%d passed\n", afResults.first, afResults.second);
	startt = timer();
	for (int i = 0; i < 1000000; i++)
		deterministicRandom()->random01();
	endt = timer();
	printf("Random01: %0.2f M/sec\n", 1.0 / (endt - startt));

	startt = timer();
	for (int i = 0; i < 1000000; i++)
		Promise<Void>();
	endt = timer();
	printf("Promises: %0.2f M/sec\n", 1.0 / (endt - startt));

	startt = timer();
	for (int i = 0; i < 1000000; i++)
		Promise<Void>().send(Void());
	endt = timer();
	printf("Promises (with send): %0.2f M/sec\n", 1.0 / (endt - startt));

	startt = timer();
	for (int i = 0; i < 1000000; i++) {
		Promise<Void> p;
		Future<Void> f = p.getFuture();
		p.send(Void());
		f.get();
	}
	endt = timer();
	printf("Promise/Future/send roundtrip: %0.2f M/sec\n", 1.0 / (endt - startt));

	Promise<Void> p;

	startt = timer();
	for (int i = 0; i < 1000000; i++)
		p.getFuture();
	endt = timer();
	printf("Futures: %0.2f M/sec\n", 1.0 / (endt - startt));

	startt = timer();
	for (int i = 0; i < 1000000; i++)
		PromiseStream<Void>();
	endt = timer();
	printf("PromiseStreams: %0.2f M/sec\n", 1.0 / (endt - startt));

	startt = timer();
	for (int i = 0; i < 1000000; i++)
		PromiseStream<Void>().send(Void());
	endt = timer();
	printf("PromiseStreams (with send): %0.2f M/sec\n", 1.0 / (endt - startt));

	startt = timer();
	for (int i = 0; i < 1000000; i++) {
		PromiseStream<Void> p;
		FutureStream<Void> f = p.getFuture();
		p.send(Void());
		f.pop();
	}
	endt = timer();
	printf("PromiseStream/FutureStream/send/popBlocking roundtrip: %0.2f M/sec\n", 1.0 / (endt - startt));

	startt = timer();
	{
		PromiseStream<int> ps;
		for (int i = 0; i < 1000000; i++) {
			ps.send(i);
		}
	}
	endt = timer();
	printf("PromiseStream queued send: %0.2f M/sec\n", 1.0 / (endt - startt));

	startt = timer();
	for (int i = 0; i < 1000000; i++)
		cancellable();
	endt = timer();
	printf("Cancellations: %0.2f M/sec\n", 1.0 / (endt - startt));

	startt = timer();
	for (int i = 0; i < 1000000; i++)
		cancellable2();
	endt = timer();
	printf("Cancellations with catch: %0.2f M/sec\n", 1.0 / (endt - startt));

	startt = timer();
	for (int i = 0; i < 1000000; i++)
		simple();
	endt = timer();
	printf("Actor creation: %0.2f M/sec\n", 1.0 / (endt - startt));

	startt = timer();
	for (int i = 0; i < 1000000; i++)
		simpleWait();
	endt = timer();
	printf("With trivial wait: %0.2f M/sec\n", 1.0 / (endt - startt));

	startt = timer();
	for (int i = 0; i < 1000000; i++) {
		Promise<int> p;
		Future<int> f = simpleRet(p.getFuture());
		p.send(i);
		ASSERT(f.get() == i);
	}
	endt = timer();
	printf("Bounce int through actor: %0.2f M/sec\n", 1.0 / (endt - startt));

	startt = timer();
	for (int i = 0; i < 1000000; i++) {
		Promise<int> p;
		Future<int> f = simpleRet(p.getFuture());
		Future<int> g = simpleRet(p.getFuture());
		p.send(i);
		ASSERT(f.get() == i);
		ASSERT(g.get() == i);
	}
	endt = timer();
	printf("Bounce int through two actors in parallel: %0.2f M/sec\n", 1.0 / (endt - startt));

	/*chainTest<1>();
	chainTest<4>();
	chainTest<16>();
	chainTest<64>();

	startt = timer();
	for(int i=0; i<1000000; i++)
	    try {
	        throw success();
	    } catch (Error&) {
	    }
	endt = timer();
	printf("C++ exception: %0.2f M/sec\n", 1.0/(endt-startt));*/

	arenaTest();

	{
		Promise<int> a, b;
		Future<int> c = chooseTest(a.getFuture(), b.getFuture());
		a.send(1);
		b.send(2);
		std::cout << "c=" << c.get() << std::endl;
	}

	{
		Promise<double> i;
		Future<double> d = addN<20>(i.getFuture());
		i.send(1.1);
		std::cout << d.get() << std::endl;
	}

	{
		Promise<double> i;
		i.sendError(operation_failed());
		Future<double> d = addN<20>(i.getFuture());
		if (d.isError() && d.getError().code() == error_code_operation_failed)
			std::cout << "Error transmitted OK" << std::endl;
		else
			std::cout << "Error not transmitted!" << std::endl;
	}

	/*{
	    int na = Actor::allActors.size();
	    PromiseStream<int> t;
	    testStream(t.getFuture());
	    if (Actor::allActors.size() != na+1)
	        std::cout << "Actor not created!" << std::endl;
	    t = PromiseStream<int>();
	    if (Actor::allActors.size() != na)
	        std::cout << "Actor not cleaned up!" << std::endl;
	}*/

	PromiseStream<int> as;
	Promise<double> bs;
	as.send(4);
	Future<Void> sT = switchTest(as.getFuture(), bs.getFuture());
	as.send(5);
	// sT = move(Future<Void>());
	as.send(6);
	bs.send(10.1);
	as.send(7);

	fastAllocTest();

#if FLOW_THREAD_SAFE
	returnCancelRaceTest();
	threadSafetyTest();
	threadSafetyTest2();
#else
	printf("Thread safety disabled.\n");
#endif
}

/*ACTOR Future<Void> pingServer( FutureStream<Promise<bool>> requests, int rate ) {
    state int count = 0;
    loop {
        Promise<bool> req = waitNext( requests );
        req.send( (++count)%rate != 0 );
    }
}

ACTOR Future<int> ping( PromiseStream<Promise<bool>> server ) {
    state int count = 0;
    loop {
        bool result = wait( server.getReply<bool>() );

        count++;
        if (!result)
            break;
    }
    return count;
}

void pingtest() {
    double start = timer();
    PromiseStream<Promise<bool>> serverInterface;
    Future<Void> pS = pingServer( serverInterface.getFuture(), 5000000 );
    Future<int> count = ping( serverInterface );
    double end = timer();
    std::cout << count.get() << " pings completed in " << (end-start) << " sec" << std::endl;
}*/

void copyTest() {
	double start, elapsed;

	Arena arena;
	StringRef s(new (arena) uint8_t[10 << 20], 10 << 20);

	{
		start = timer();
		for (int i = 0; i < 100; i++) {
			StringRef k = s;
			(void)k;
		}
		elapsed = timer() - start;

		printf("StringRef->StringRef: %fs/GB\n", elapsed);
	}

	{
		start = timer();
		for (int i = 0; i < 100; i++)
			Standalone<StringRef> a = s;
		elapsed = timer() - start;

		printf("StringRef->Standalone: %fs/GB\n", elapsed);
	}

	{
		Standalone<StringRef> sa = s;
		start = timer();
		for (int i = 0; i < 100; i++)
			Standalone<StringRef> a = sa;
		elapsed = timer() - start;

		printf("Standalone->Standalone: %fs/GB\n", elapsed);
	}

	{
		Standalone<StringRef> sa = s, sb;
		start = timer();
		for (int i = 0; i < 50; i++) {
			sb = std::move(sa);
			sa = std::move(sb);
		}
		elapsed = timer() - start;
		printf("move(Standalone)->Standalone: %fs/GB\n", elapsed);
	}
}

/*ACTOR void badTest( FutureStream<int> is ) {
    state PromiseStream<int> js;

    loop choose {
        when( int j = waitNext( js.getFuture() ) ) {
            std::cout << "J" << j << std::endl;
        }
        when( int i = waitNext( is ) ) {
            std::cout << "I" << i << std::endl;
            js.send( i );
            std::cout << "-I" << i << std::endl;
        }
    }
}

void dsltest() {
    PromiseStream<int> is;
    badTest( is.getFuture() );
    is.send(1);
    is.send(2);
    is.send(3);
    throw not_implemented();
}
void pingtest() {}*/
