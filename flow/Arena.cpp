/*
 * Arena.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2020 Apple Inc. and the FoundationDB project authors
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

#include "Arena.h"

#include "flow/UnitTest.h"

// See https://dox.ipxe.org/memcheck_8h_source.html and https://dox.ipxe.org/valgrind_8h_source.html for an explanation
// of valgrind client requests
#ifdef VALGRIND_ARENA
#include <memcheck.h>
#else
// Since VALGRIND_ARENA is not set, we don't want to pay the performance penalty for precise tracking of arenas. We'll
// make these macros noops just for this translation unit.
#undef VALGRIND_MAKE_MEM_NOACCESS
#undef VALGRIND_MAKE_MEM_DEFINED
#undef VALGRIND_MAKE_MEM_UNDEFINED
#define VALGRIND_MAKE_MEM_NOACCESS(addr, size) ((void)(addr), (void)(size))
#define VALGRIND_MAKE_MEM_DEFINED(addr, size) ((void)(addr), (void)(size))
#define VALGRIND_MAKE_MEM_UNDEFINED(addr, size) ((void)(addr), (void)(size))
#endif

// For each use of arena-internal memory (e.g. ArenaBlock::getSize()), unpoison the memory before use and
// poison it when done.
// When creating a new ArenaBlock, poison the memory that will be later allocated to users.
// When allocating memory to a user, mark that memory as undefined.

namespace {
void allow_access(ArenaBlock* b) {
	if (b) {
		VALGRIND_MAKE_MEM_DEFINED(b, ArenaBlock::TINY_HEADER);
		int headerSize = b->isTiny() ? ArenaBlock::TINY_HEADER : sizeof(ArenaBlock);
		VALGRIND_MAKE_MEM_DEFINED(b, headerSize);
	}
}
void disallow_access(ArenaBlock* b) {
	if (b) {
		int headerSize = b->isTiny() ? ArenaBlock::TINY_HEADER : sizeof(ArenaBlock);
		VALGRIND_MAKE_MEM_NOACCESS(b, headerSize);
	}
}
} // namespace

Arena::Arena() : impl(nullptr) {}
Arena::Arena(size_t reservedSize) : impl(0) {
	UNSTOPPABLE_ASSERT(reservedSize < std::numeric_limits<int>::max());
	if (reservedSize) {
		allow_access(impl.getPtr());
		ArenaBlock::create((int)reservedSize, impl);
		disallow_access(impl.getPtr());
	}
}
Arena::Arena(const Arena& r) = default;
Arena::Arena(Arena&& r) noexcept = default;
Arena& Arena::operator=(const Arena& r) = default;
Arena& Arena::operator=(Arena&& r) noexcept = default;
void Arena::dependsOn(const Arena& p) {
	if (p.impl) {
		allow_access(impl.getPtr());
		allow_access(p.impl.getPtr());
		ArenaBlock::dependOn(impl, p.impl.getPtr());
		disallow_access(p.impl.getPtr());
		if (p.impl.getPtr() != impl.getPtr()) {
			disallow_access(impl.getPtr());
		}
	}
}
size_t Arena::getSize() const {
	if (impl) {
		allow_access(impl.getPtr());
		auto result = impl->totalSize();
		disallow_access(impl.getPtr());
		return result;
	}
	return 0;
}
bool Arena::hasFree(size_t size, const void* address) {
	if (impl) {
		allow_access(impl.getPtr());
		auto result = impl->unused() >= size && impl->getNextData() == address;
		disallow_access(impl.getPtr());
		return result;
	}
	return false;
}

void ArenaBlock::addref() {
	VALGRIND_MAKE_MEM_DEFINED(this, sizeof(ThreadSafeReferenceCounted<ArenaBlock>));
	ThreadSafeReferenceCounted<ArenaBlock>::addref();
	VALGRIND_MAKE_MEM_NOACCESS(this, sizeof(ThreadSafeReferenceCounted<ArenaBlock>));
}

void ArenaBlock::delref() {
	VALGRIND_MAKE_MEM_DEFINED(this, sizeof(ThreadSafeReferenceCounted<ArenaBlock>));
	if (delref_no_destroy()) {
		destroy();
	} else {
		VALGRIND_MAKE_MEM_NOACCESS(this, sizeof(ThreadSafeReferenceCounted<ArenaBlock>));
	}
}

bool ArenaBlock::isTiny() const {
	return tinySize != NOT_TINY;
}
int ArenaBlock::size() const {
	if (isTiny())
		return tinySize;
	else
		return bigSize;
}
int ArenaBlock::used() const {
	if (isTiny())
		return tinyUsed;
	else
		return bigUsed;
}
int ArenaBlock::unused() const {
	if (isTiny())
		return tinySize - tinyUsed;
	else
		return bigSize - bigUsed;
}
const void* ArenaBlock::getData() const {
	return this;
}
const void* ArenaBlock::getNextData() const {
	return (const uint8_t*)getData() + used();
}
size_t ArenaBlock::totalSize() {
	if (isTiny()) {
		return size();
	}

	size_t s = size();
	int o = nextBlockOffset;
	while (o) {
		ArenaBlockRef* r = (ArenaBlockRef*)((char*)getData() + o);
		VALGRIND_MAKE_MEM_DEFINED(r, sizeof(ArenaBlockRef));
		allow_access(r->next);
		s += r->next->totalSize();
		disallow_access(r->next);
		o = r->nextBlockOffset;
		VALGRIND_MAKE_MEM_NOACCESS(r, sizeof(ArenaBlockRef));
	}
	return s;
}
// just for debugging:
void ArenaBlock::getUniqueBlocks(std::set<ArenaBlock*>& a) {
	a.insert(this);
	if (isTiny()) return;

	int o = nextBlockOffset;
	while (o) {
		ArenaBlockRef* r = (ArenaBlockRef*)((char*)getData() + o);
		VALGRIND_MAKE_MEM_DEFINED(r, sizeof(ArenaBlockRef));
		r->next->getUniqueBlocks(a);
		o = r->nextBlockOffset;
		VALGRIND_MAKE_MEM_NOACCESS(r, sizeof(ArenaBlockRef));
	}
	return;
}

int ArenaBlock::addUsed(int bytes) {
	if (isTiny()) {
		int t = tinyUsed;
		tinyUsed += bytes;
		return t;
	} else {
		int t = bigUsed;
		bigUsed += bytes;
		return t;
	}
}

void ArenaBlock::makeReference(ArenaBlock* next) {
	ArenaBlockRef* r = (ArenaBlockRef*)((char*)getData() + bigUsed);
	VALGRIND_MAKE_MEM_DEFINED(r, sizeof(ArenaBlockRef));
	r->next = next;
	r->nextBlockOffset = nextBlockOffset;
	VALGRIND_MAKE_MEM_NOACCESS(r, sizeof(ArenaBlockRef));
	nextBlockOffset = bigUsed;
	bigUsed += sizeof(ArenaBlockRef);
}

void ArenaBlock::dependOn(Reference<ArenaBlock>& self, ArenaBlock* other) {
	other->addref();
	if (!self || self->isTiny() || self->unused() < sizeof(ArenaBlockRef))
		create(SMALL, self)->makeReference(other);
	else
		self->makeReference(other);
}

void* ArenaBlock::allocate(Reference<ArenaBlock>& self, int bytes) {
	ArenaBlock* b = self.getPtr();
	allow_access(b);
	if (!self || self->unused() < bytes) {
		auto* tmp = b;
		b = create(bytes, self);
		disallow_access(tmp);
	}

	void* result = (char*)b->getData() + b->addUsed(bytes);
	disallow_access(b);
	VALGRIND_MAKE_MEM_UNDEFINED(result, bytes);
	return result;
}

// Return an appropriately-sized ArenaBlock to store the given data
ArenaBlock* ArenaBlock::create(int dataSize, Reference<ArenaBlock>& next) {
	ArenaBlock* b;
	if (dataSize <= SMALL - TINY_HEADER && !next) {
		if (dataSize <= 16 - TINY_HEADER) {
			b = (ArenaBlock*)FastAllocator<16>::allocate();
			b->tinySize = 16;
			INSTRUMENT_ALLOCATE("Arena16");
		} else if (dataSize <= 32 - TINY_HEADER) {
			b = (ArenaBlock*)FastAllocator<32>::allocate();
			b->tinySize = 32;
			INSTRUMENT_ALLOCATE("Arena32");
		} else {
			b = (ArenaBlock*)FastAllocator<64>::allocate();
			b->tinySize = 64;
			INSTRUMENT_ALLOCATE("Arena64");
		}
		b->tinyUsed = TINY_HEADER;

	} else {
		int reqSize = dataSize + sizeof(ArenaBlock);
		if (next) reqSize += sizeof(ArenaBlockRef);

		if (reqSize < LARGE) {
			// Each block should be larger than the previous block, up to a limit, to minimize allocations
			// Worst-case allocation pattern: 1 +10 +17 +42 +67 +170 +323 +681 +1348 +2728 +2210 +2211 (+1K +3K+1 +4K)*
			// Overhead: 4X for small arenas, 3X intermediate, 1.33X for large arenas
			int prevSize = next ? next->size() : 0;
			reqSize = std::max(reqSize, std::min(prevSize * 2, std::max(LARGE - 1, reqSize * 4)));
		}

		if (reqSize < LARGE) {
			if (reqSize <= 128) {
				b = (ArenaBlock*)FastAllocator<128>::allocate();
				b->bigSize = 128;
				INSTRUMENT_ALLOCATE("Arena128");
			} else if (reqSize <= 256) {
				b = (ArenaBlock*)FastAllocator<256>::allocate();
				b->bigSize = 256;
				INSTRUMENT_ALLOCATE("Arena256");
			} else if (reqSize <= 512) {
				b = (ArenaBlock*)FastAllocator<512>::allocate();
				b->bigSize = 512;
				INSTRUMENT_ALLOCATE("Arena512");
			} else if (reqSize <= 1024) {
				b = (ArenaBlock*)FastAllocator<1024>::allocate();
				b->bigSize = 1024;
				INSTRUMENT_ALLOCATE("Arena1024");
			} else if (reqSize <= 2048) {
				b = (ArenaBlock*)FastAllocator<2048>::allocate();
				b->bigSize = 2048;
				INSTRUMENT_ALLOCATE("Arena2048");
			} else if (reqSize <= 4096) {
				b = (ArenaBlock*)FastAllocator<4096>::allocate();
				b->bigSize = 4096;
				INSTRUMENT_ALLOCATE("Arena4096");
			} else {
				b = (ArenaBlock*)FastAllocator<8192>::allocate();
				b->bigSize = 8192;
				INSTRUMENT_ALLOCATE("Arena8192");
			}
			b->tinySize = b->tinyUsed = NOT_TINY;
			b->bigUsed = sizeof(ArenaBlock);
		} else {
#ifdef ALLOC_INSTRUMENTATION
			allocInstr["ArenaHugeKB"].alloc((reqSize + 1023) >> 10);
#endif
			b = (ArenaBlock*)new uint8_t[reqSize];
			b->tinySize = b->tinyUsed = NOT_TINY;
			b->bigSize = reqSize;
			b->bigUsed = sizeof(ArenaBlock);

			if (FLOW_KNOBS && g_allocation_tracing_disabled == 0 &&
			    nondeterministicRandom()->random01() < (reqSize / FLOW_KNOBS->HUGE_ARENA_LOGGING_BYTES)) {
				++g_allocation_tracing_disabled;
				hugeArenaSample(reqSize);
				--g_allocation_tracing_disabled;
			}
			g_hugeArenaMemory.fetch_add(reqSize);

			// If the new block has less free space than the old block, make the old block depend on it
			if (next && !next->isTiny() && next->unused() >= reqSize - dataSize) {
				b->nextBlockOffset = 0;
				b->setrefCountUnsafe(1);
				next->makeReference(b);
				return b;
			}
		}
		b->nextBlockOffset = 0;
		if (next) b->makeReference(next.getPtr());
	}
	b->setrefCountUnsafe(1);
	next.setPtrUnsafe(b);
	VALGRIND_MAKE_MEM_NOACCESS(reinterpret_cast<uint8_t*>(b) + b->used(), b->unused());
	return b;
}

void ArenaBlock::destroy() {
	// If the stack never contains more than one item, nothing will be allocated from stackArena.
	// If stackArena is used, it will always be a linked list, so destroying *it* will not create another arena
	ArenaBlock* tinyStack = this;
	allow_access(this);
	Arena stackArena;
	VectorRef<ArenaBlock*> stack(&tinyStack, 1);

	while (stack.size()) {
		ArenaBlock* b = stack.end()[-1];
		stack.pop_back();
		allow_access(b);

		if (!b->isTiny()) {
			int o = b->nextBlockOffset;
			while (o) {
				ArenaBlockRef* br = (ArenaBlockRef*)((char*)b->getData() + o);
				VALGRIND_MAKE_MEM_DEFINED(br, sizeof(ArenaBlockRef));
				allow_access(br->next);
				if (br->next->delref_no_destroy()) stack.push_back(stackArena, br->next);
				disallow_access(br->next);
				o = br->nextBlockOffset;
			}
		}
		b->destroyLeaf();
	}
}

void ArenaBlock::destroyLeaf() {
	if (isTiny()) {
		if (tinySize <= 16) {
			FastAllocator<16>::release(this);
			INSTRUMENT_RELEASE("Arena16");
		} else if (tinySize <= 32) {
			FastAllocator<32>::release(this);
			INSTRUMENT_RELEASE("Arena32");
		} else {
			FastAllocator<64>::release(this);
			INSTRUMENT_RELEASE("Arena64");
		}
	} else {
		if (bigSize <= 128) {
			FastAllocator<128>::release(this);
			INSTRUMENT_RELEASE("Arena128");
		} else if (bigSize <= 256) {
			FastAllocator<256>::release(this);
			INSTRUMENT_RELEASE("Arena256");
		} else if (bigSize <= 512) {
			FastAllocator<512>::release(this);
			INSTRUMENT_RELEASE("Arena512");
		} else if (bigSize <= 1024) {
			FastAllocator<1024>::release(this);
			INSTRUMENT_RELEASE("Arena1024");
		} else if (bigSize <= 2048) {
			FastAllocator<2048>::release(this);
			INSTRUMENT_RELEASE("Arena2048");
		} else if (bigSize <= 4096) {
			FastAllocator<4096>::release(this);
			INSTRUMENT_RELEASE("Arena4096");
		} else if (bigSize <= 8192) {
			FastAllocator<8192>::release(this);
			INSTRUMENT_RELEASE("Arena8192");
		} else {
#ifdef ALLOC_INSTRUMENTATION
			allocInstr["ArenaHugeKB"].dealloc((bigSize + 1023) >> 10);
#endif
			g_hugeArenaMemory.fetch_sub(bigSize);
			delete[](uint8_t*) this;
		}
	}
}

namespace {
template <template <class> class VectorRefLike>
void testRangeBasedForLoop() {
	VectorRefLike<StringRef> xs;
	Arena a;
	int size = deterministicRandom()->randomInt(0, 100);
	for (int i = 0; i < size; ++i) {
		xs.push_back_deep(a, StringRef(std::to_string(i)));
	}
	ASSERT(xs.size() == size);
	int i = 0;
	for (const auto& x : xs) {
		ASSERT(x == StringRef(std::to_string(i++)));
	}
	ASSERT(i == size);
}

template <template <class> class VectorRefLike>
void testIteratorIncrement() {
	VectorRefLike<StringRef> xs;
	Arena a;
	int size = deterministicRandom()->randomInt(0, 100);
	for (int i = 0; i < size; ++i) {
		xs.push_back_deep(a, StringRef(std::to_string(i)));
	}
	ASSERT(xs.size() == size);
	{
		int i = 0;
		for (auto iter = xs.begin(); iter != xs.end();) {
			ASSERT(*iter++ == StringRef(std::to_string(i++)));
		}
		ASSERT(i == size);
	}
	{
		int i = 0;
		for (auto iter = xs.begin(); iter != xs.end() && i < xs.size() - 1;) {
			ASSERT(*++iter == StringRef(std::to_string(++i)));
		}
	}
	{
		int i = 0;
		for (auto iter = xs.begin(); iter < xs.end();) {
			ASSERT(*iter == StringRef(std::to_string(i)));
			iter += 1;
			i += 1;
		}
	}
	if (size > 0) {
		int i = xs.size() - 1;
		for (auto iter = xs.end() - 1; iter >= xs.begin();) {
			ASSERT(*iter == StringRef(std::to_string(i)));
			iter -= 1;
			i -= 1;
		}
	}
	{
		int i = 0;
		for (auto iter = xs.begin(); iter < xs.end();) {
			ASSERT(*iter == StringRef(std::to_string(i)));
			iter = iter + 1;
			i += 1;
		}
	}
	if (size > 0) {
		int i = xs.size() - 1;
		for (auto iter = xs.end() - 1; iter >= xs.begin();) {
			ASSERT(*iter == StringRef(std::to_string(i)));
			iter = iter - 1;
			i -= 1;
		}
	}
}

template <template <class> class VectorRefLike>
void testReverseIterator() {
	VectorRefLike<StringRef> xs;
	Arena a;
	int size = deterministicRandom()->randomInt(0, 100);
	for (int i = 0; i < size; ++i) {
		xs.push_back_deep(a, StringRef(std::to_string(i)));
	}
	ASSERT(xs.size() == size);

	int i = xs.size() - 1;
	for (auto iter = xs.rbegin(); iter != xs.rend();) {
		ASSERT(*iter++ == StringRef(std::to_string(i--)));
	}
	ASSERT(i == -1);
}

template <template <class> class VectorRefLike>
void testAppend() {
	VectorRefLike<StringRef> xs;
	Arena a;
	int size = deterministicRandom()->randomInt(0, 100);
	for (int i = 0; i < size; ++i) {
		xs.push_back_deep(a, StringRef(std::to_string(i)));
	}
	VectorRefLike<StringRef> ys;
	ys.append(a, xs.begin(), xs.size());
	ASSERT(xs.size() == ys.size());
	ASSERT(std::equal(xs.begin(), xs.end(), ys.begin()));
}

template <template <class> class VectorRefLike>
void testCopy() {
	Standalone<VectorRefLike<StringRef>> xs;
	int size = deterministicRandom()->randomInt(0, 100);
	for (int i = 0; i < size; ++i) {
		xs.push_back_deep(xs.arena(), StringRef(std::to_string(i)));
	}
	Arena a;
	VectorRefLike<StringRef> ys(a, xs);
	xs = Standalone<VectorRefLike<StringRef>>();
	int i = 0;
	for (const auto& y : ys) {
		ASSERT(y == StringRef(std::to_string(i++)));
	}
	ASSERT(i == size);
}

template <template <class> class VectorRefLike>
void testVectorLike() {
	testRangeBasedForLoop<VectorRefLike>();
	testIteratorIncrement<VectorRefLike>();
	testReverseIterator<VectorRefLike>();
	testAppend<VectorRefLike>();
	testCopy<VectorRefLike>();
}
} // namespace

// Fix number of template parameters
template <class T>
using VectorRefProxy = VectorRef<T>;
TEST_CASE("/flow/Arena/VectorRef") {
	testVectorLike<VectorRefProxy>();
	return Void();
}

// Fix number of template parameters
template <class T>
using SmallVectorRefProxy = SmallVectorRef<T>;
TEST_CASE("/flow/Arena/SmallVectorRef") {
	testVectorLike<SmallVectorRefProxy>();
	return Void();
}

// Fix number of template parameters
template <class T>
using SmallVectorRef10Proxy = SmallVectorRef<T, 10>;
TEST_CASE("/flow/Arena/SmallVectorRef10") {
	testVectorLike<SmallVectorRef10Proxy>();
	return Void();
}
