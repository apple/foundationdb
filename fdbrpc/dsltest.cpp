/*
 * dsltest.cpp
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

#include <iostream>
#include <algorithm>
#include "flow/FastRef.h"
#undef ERROR
#include "fdbrpc/simulator.h"
#include "ActorFuzz.h"
#include "flow/DeterministicRandom.h"
#include "flow/ProtocolVersion.h"
#include "flow/ThreadHelper.h"
#include "flow/CoroUtils.h"
#include "flow/UnitTest.h"

void* allocateLargePages(int total);

bool testFuzzActor(Future<int> (*actor)(FutureStream<int>, PromiseStream<int>, Future<Void>),
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

template <int N, class X>
Future<X> addN(Future<X> in) {
	Future<X> input(std::move(in));
	X i = co_await input;
	co_return i + N;
}

template <class A, class B>
Future<Void> switchTest(FutureStream<A> as, Future<B> oneb) {
	FutureStream<A> inputs(std::move(as));
	Future<B> done(std::move(oneb));
	while (true) {
		auto res = co_await race(inputs, done);
		if (res.index() == 0) {
			A a = std::get<0>(std::move(res));
			std::cout << "A " << a << std::endl;
		} else if (res.index() == 1) {
			B b = std::get<1>(std::move(res));
			std::cout << "B " << b << std::endl;
			break;
		} else {
			UNREACHABLE();
		}
	}
	std::cout << "Done!" << std::endl;
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
		delete[] (int*)buf;
	}
#endif

	int size() const { return length; }
	uint8_t* begin() { return data; }
	uint8_t* end() { return data + length; }
	const uint8_t* begin() const { return data; }
	const uint8_t* end() const { return data + length; }

private:
	explicit TestBuffer(int length) noexcept : length(length) {}
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

Future<Void> threadSafetyWaiter(Future<Void> f, int32_t* count, Uncancellable = Uncancellable()) {
	co_await f;
	interlockedIncrement(count);
}
Future<Void> threadSafetyWaiter(FutureStream<Void> f, int n, int32_t* count, Uncancellable = Uncancellable()) {
	while (n--) {
		co_await f;
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

Future<Void> returnCancelRacer( Future<Void> f ) {
	Future<Void> input(std::move(f));
	try {
		co_await input;
	} catch ( Error& ) {
		interlockedIncrement( &cancelled );
		throw;
	}
	interlockedIncrement( &returned );
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

Future<int> chooseTest(Future<int> a, Future<int> b) {
	// Release both inputs before completion, even when callers retain the result future.
	Future<int> first(std::move(a));
	Future<int> second(std::move(b));
	auto res = co_await race(first, second);
	if (res.index() == 0) {
		co_return std::get<0>(res);
	} else if (res.index() == 1) {
		co_return std::get<1>(res);
	}
	UNREACHABLE();
}

void showArena(ArenaBlock* a, ArenaBlock* parent) {
	printf("ArenaBlock %p (<-%p): %d bytes, %d refs\n", a, parent, a->size(), a->debugGetReferenceCount());
	if (!a->isTiny()) {
		int o = a->nextBlockOffset;
		while (o) {
			auto* r = (ArenaBlockRef*)((char*)a->getData() + o);

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
	// The standalone DSL test runs before network initialization.
	const ProtocolVersion protocolVersion = currentProtocolVersion();
	BinaryWriter wr(AssumeVersion(protocolVersion));
	{
		Arena arena;
		VectorRef<StringRef> test;
		test.push_back(arena, StringRef(arena, "Hello"_sr));
		test.push_back(arena, StringRef(arena, ", "_sr));
		test.push_back(arena, StringRef(arena, "World!"_sr));

		for (auto i = test.begin(); i != test.end(); ++i)
			for (auto j = i->begin(); j != i->end(); ++j)
				std::cout << *j;
		std::cout << std::endl;

		wr << test;
	}
	{
		Arena arena2;
		VectorRef<StringRef> test2;
		BinaryReader reader(wr.getData(), wr.getLength(), AssumeVersion(protocolVersion));
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

Future<Void> testStream(FutureStream<int> xs, Uncancellable = Uncancellable()) {
	while (true) {
		int x = co_await xs;
		std::cout << x << std::endl;
	}
}

Future<Void> actorTest1(bool b) {
	printf("1");
	if (b)
		throw future_version();
	co_return;
}

Future<Void> actorTest2(bool b, Uncancellable = Uncancellable()) {
	printf("2");
	if (b)
		throw future_version();
	co_return;
}

Future<Void> actorTest3(bool b) {
	try {
		if (b)
			throw future_version();
	} catch (Error&) {
		printf("3");
		co_return;
	}
	printf("\nactorTest3 failed\n");
}

Future<Void> actorTest4(bool b) {
	double tstart = now();
	bool caught = false;
	try {
		if (b)
			throw operation_failed();
	} catch (...) {
		caught = true;
	}
	if (caught)
		co_await delay(1);
	if (now() < tstart + 1)
		printf("actorTest4 failed");
	else
		printf("4");
}

Future<bool> actorTest5() {
	bool caught = false;

	while (true) {
		while (true) {
			bool inloop = false;
			if (caught) {
				printf("5");
				co_return true;
			}
			try {
				while (true) {
					if (inloop) {
						printf("\nactorTest5 failed\n");
						co_return false;
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

Future<bool> actorTest6() {
	bool caught = false;
	while (true) {
		if (caught) {
			printf("6");
			co_return true;
		}
		try {
			if (1)
				throw operation_failed();
		} catch (Error&) {
			caught = true;
		}
	}
}

Future<bool> actorTest7() {
	try {
		while (true) {
			while (true) {
				if (1)
					throw operation_failed();
				if (1) {
					printf("actorTest7 failed (1)\n");
					co_return false;
				}
				if (0)
					break;
			}
			if (1) {
				printf("actorTest7 failed (2)\n");
				co_return false;
			}
		}
	} catch (Error&) {
		printf("7");
		co_return true;
	}
}

Future<bool> actorTest8() {
	bool caught = false;
	Future<bool> set = true;

	while (true) {
		bool inloop = false;
		if (caught) {
			printf("8");
			co_return true;
		}
		try {
			while (true) {
				if (inloop) {
					printf("\nactorTest8 failed\n");
					co_return false;
				}
				co_await success(set);
				inloop = true;
				if (1)
					throw operation_failed();
			}
		} catch (Error&) {
			caught = true;
		}
	}
}

Future<bool> actorTest9A(Future<Void> setAfterCalling) {
	Future<Void> start(std::move(setAfterCalling));
	int count = 0;
	while (true) {
		if (count == 4) {
			printf("9");
			co_return true;
		}
		if (count && count != 4) {
			printf("\nactorTest9 failed\n");
			co_return false;
		}
		while (true) {
			while (true) {
				co_await start;
				while (true) {
					while (true) {
						count++;
						break;
					}
					co_await Future<Void>(Void());
					count++;
					break;
				}
				count++;
				break;
			}
			count++;
			break;
		}
	}
}

Future<bool> actorTest9() {
	Promise<Void> p;
	Future<bool> f = actorTest9A(p.getFuture());
	p.send(Void());
	return f;
}

Future<Void> actorTest10A(FutureStream<int> inputStream, Future<Void> go) {
	FutureStream<int> inputs(std::move(inputStream));
	Future<Void> start(std::move(go));
	for (int i = 0; i < 5; i++) {
		co_await start;
		int input = co_await inputs;
		(void)input;
	}
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

Future<Void> cancellable() {
	co_await Future<Void>(Never());
}

Future<Void> simple() {
	co_return;
}

Future<Void> simpleWait() {
	co_await Future<Void>(Void());
}

Future<int> simpleRet(Future<int> x) {
	Future<int> input(std::move(x));
	int i = co_await input;
	co_return i;
}

template <int i>
Future<int> chain(Future<int> const& x);

template <int i>
Future<int> achain(Future<int> x) {
	Future<int> input(std::move(x));
	int k = co_await chain<i>(input);
	co_return k + 1;
}

template <int i>
Future<int> chain(Future<int> const& x) {
	return achain<i - 1>(x);
}

template <>
Future<int> chain<0>(Future<int> const& x) {
	return x;
}

Future<int> chain2(Future<int> x, int i);

Future<int> chain2(Future<int> x, int i) {
	Future<int> input(std::move(x));
	if (i > 1) {
		int k = co_await chain2(input, i - 1);
		co_return k + 1;
	} else {
		int k = co_await input;
		co_return k + i;
	}
}

Future<Void> cancellable2() {
	try {
		co_await Future<Void>(Never());
	} catch (Error& e) {
		throw;
	}
}

Future<int> introLoadValueFromDisk(Future<std::string> filename) {
	Future<std::string> input(std::move(filename));
	std::string file = co_await input;

	if (file == "/dev/threes")
		co_return 3;
	else
		ASSERT(false);
	co_return 0; // does not happen
}

Future<int> introAdd(Future<int> a, Future<int> b) {
	Future<int> first(std::move(a));
	Future<int> second(std::move(b));
	int x = co_await first;
	int y = co_await second;
	co_return x + y;
}

Future<int> introFirst(Future<int> a, Future<int> b) {
	Future<int> first(std::move(a));
	Future<int> second(std::move(b));
	auto res = co_await race(first, second);
	if (res.index() == 0) {
		co_return std::get<0>(res);
	} else if (res.index() == 1) {
		co_return std::get<1>(res);
	}
	UNREACHABLE();
}

struct AddReply {
	int sum;
	AddReply() = default;
	explicit(false) AddReply(int x) : sum(x) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, sum);
	}
};

struct AddRequest {
	int a, b;
	Promise<AddReply> reply; // Self-addressed envelope

	AddRequest() = default;
	AddRequest(int a, int b) : a(a), b(b) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, a, b, reply);
	}
};

Future<Void> introAddServer(PromiseStream<AddRequest> add, Uncancellable = Uncancellable()) {
	while (true) {
		auto res = co_await race(add.getFuture());
		if (res.index() == 0) {
			AddRequest req = std::get<0>(std::move(res));
			printf("%d + %d = %d\n", req.a, req.b, req.a + req.b);
			req.reply.send(req.a + req.b);
		} else {
			UNREACHABLE();
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

Future<Void> cycle(FutureStream<Void> in, PromiseStream<Void> out, int* ptotal, Uncancellable = Uncancellable()) {
	while (true) {
		co_await in;
		(*ptotal)++;
		out.send(Void());
	}
}

Future<Void> cycleTime(int nodes, int times) {
	std::vector<PromiseStream<Void>> n(nodes);
	int total = 0;

	// 1->2, 2->3, ..., n-1->0
	for (int i = 1; i < nodes; i++)
		cycle(n[i].getFuture(), n[(i + 1) % nodes], &total);

	double startT = timer();
	n[1].send(Void());
	while (true) {
		co_await n[0].getFuture();
		if (!--times)
			break;
		n[1].send(Void());
	}

	printf("Ring test: %d nodes, %d total ops, %.3f seconds\n", nodes, total, timer() - startT);
}

namespace {

Future<int> cancellationTrackedProducer(Promise<Void> cancelled) {
	Promise<Void> notification(std::move(cancelled));
	try {
		co_await Future<Void>(Never());
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled)
			notification.send(Void());
		throw;
	}
	UNREACHABLE();
}

} // namespace

TEST_CASE("/fdbrpc/Coroutines/ControlFlow") {
	Future<Void> failed = actorTest1(true);
	ASSERT(failed.isReady() && failed.isError());
	ASSERT(failed.getError().code() == error_code_future_version);
	co_await actorTest1(false);
	co_await actorTest3(true);
	ASSERT(actorTest5().get());
	ASSERT(actorTest6().get());
	ASSERT(actorTest7().get());
	ASSERT(actorTest8().get());

	Promise<Void> nestedLoopStart;
	Future<bool> nestedLoops = actorTest9A(nestedLoopStart.getFuture());
	ASSERT(!nestedLoops.isReady());
	nestedLoopStart.send(Void());
	ASSERT(nestedLoops.isReady() && nestedLoops.get());
	ASSERT(nestedLoopStart.getFutureReferenceCount() == 0);

	PromiseStream<int> inputs;
	Promise<Void> go;
	inputs.send(1);
	inputs.send(2);
	Future<Void> reader = actorTest10A(inputs.getFuture(), go.getFuture());
	ASSERT(!reader.isReady());
	go.send(Void());
	ASSERT(!reader.isReady());
	inputs.send(3);
	inputs.send(4);
	ASSERT(!reader.isReady());
	inputs.send(5);
	ASSERT(reader.isReady() && !reader.isError());
	ASSERT(inputs.getFutureReferenceCount() == 0);
	ASSERT(go.getFutureReferenceCount() == 0);

	Future<Void> caught = actorTest4(true);
	ASSERT(!caught.isReady());
	co_await caught;
}

TEST_CASE("/fdbrpc/Coroutines/FireAndForget") {
	int32_t count = 0;
	Promise<Void> ready;
	threadSafetyWaiter(ready.getFuture(), &count);
	ASSERT(count == 0);
	ready.send(Void());
	ASSERT(count == 1);

	PromiseStream<Void> stream;
	threadSafetyWaiter(stream.getFuture(), 2, &count);
	stream.send(Void());
	ASSERT(count == 2);
	stream.send(Void());
	ASSERT(count == 3);
	stream.send(Void());
	ASSERT(count == 3);

	Future<Void> ring = cycleTime(4, 5);
	ASSERT(ring.isReady() && !ring.isError());
	return Void();
}

TEST_CASE("/fdbrpc/Coroutines/Race") {
	for (auto firstReady : { &chooseTest, &introFirst }) {
		ASSERT(firstReady(1, 2).get() == 1);
		ASSERT(firstReady(1, operation_failed()).get() == 1);
		Future<int> failed = firstReady(operation_failed(), 2);
		ASSERT(failed.isError() && failed.getError().code() == error_code_operation_failed);

		Promise<Void> readyLoserCancelled;
		Future<int> readyWinner = firstReady(1, cancellationTrackedProducer(readyLoserCancelled));
		ASSERT(readyWinner.isReady() && readyWinner.get() == 1);
		ASSERT(readyLoserCancelled.getFuture().isReady() && !readyLoserCancelled.getFuture().isError());

		Promise<int> winner;
		Promise<Void> pendingLoserCancelled;
		Future<int> pendingWinner = firstReady(winner.getFuture(), cancellationTrackedProducer(pendingLoserCancelled));
		ASSERT(!pendingWinner.isReady() && !pendingLoserCancelled.getFuture().isReady());
		winner.send(3);
		ASSERT(pendingWinner.isReady() && pendingWinner.get() == 3);
		ASSERT(pendingLoserCancelled.getFuture().isReady() && !pendingLoserCancelled.getFuture().isError());

		Promise<int> first, second;
		Future<int> selected = firstReady(first.getFuture(), second.getFuture());
		ASSERT(!selected.isReady());
		second.send(2);
		ASSERT(selected.isReady() && selected.get() == 2);
		first.sendError(operation_failed());
		ASSERT(selected.get() == 2);

		Promise<int> cancelledFirst, cancelledSecond;
		Future<int> cancelled = firstReady(cancelledFirst.getFuture(), cancelledSecond.getFuture());
		cancelled.cancel();
		ASSERT(cancelled.isError() && cancelled.getError().code() == error_code_actor_cancelled);
		cancelledFirst.send(1);
		cancelledSecond.send(2);
		ASSERT(cancelled.isError() && cancelled.getError().code() == error_code_actor_cancelled);
	}
	PromiseStream<int> inputs;
	Future<Void> switched = switchTest(inputs.getFuture(), Future<int>(0));
	ASSERT(switched.isReady() && !switched.isError());
	ASSERT(inputs.getFutureReferenceCount() == 0);
	return Void();
}

TEST_CASE("/fdbrpc/Coroutines/Cancellation") {
	for (auto pending : { &cancellable, &cancellable2 }) {
		Future<Void> result = pending();
		ASSERT(!result.isReady());
		result.cancel();
		ASSERT(result.isReady() && result.isError());
		ASSERT(result.getError().code() == error_code_actor_cancelled);
	}
	for (auto forward : { &simpleRet, &addN<1, int>, &achain<2> }) {
		Promise<Void> producerCancelled;
		Future<int> result = forward(cancellationTrackedProducer(producerCancelled));
		ASSERT(!result.isReady());
		result.cancel();
		ASSERT(result.isReady() && result.isError() && result.getError().code() == error_code_actor_cancelled);
		ASSERT(producerCancelled.getFuture().isReady() && !producerCancelled.getFuture().isError());
	}
	Promise<Void> producerCancelled;
	Future<int> failedSum = introAdd(operation_failed(), cancellationTrackedProducer(producerCancelled));
	ASSERT(failedSum.isError() && failedSum.getError().code() == error_code_operation_failed);
	ASSERT(producerCancelled.getFuture().isReady() && !producerCancelled.getFuture().isError());
	return Void();
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

void dsltest() {
	double startt, endt;

	setThreadLocalDeterministicRandomSeed(40);

	asyncMapTest();

	Future<Void> ctf = cycleTime(1000, 1000);
	ctf.get();

	introPromiseFuture();
	introActor();
	// return;

	printf("Actor control flow tests: ");
	actorTest1(true);
	actorTest2(true);
	actorTest3(true);
	// if (g_network == g_simulator)
	// g_simulator->run( actorTest4(true) );
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
