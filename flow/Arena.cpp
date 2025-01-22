/*
 * Arena.cpp
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

#include "flow/Arena.h"

#include "flow/UnitTest.h"
#include "flow/ScopeExit.h"

#include "flow/config.h"

// We don't align memory properly, and we need to tell lsan about that.
extern "C" const char* __lsan_default_options(void) {
	return "use_unaligned=1";
}

#ifdef ADDRESS_SANITIZER
#include <sanitizer/asan_interface.h>
#endif

// See https://dox.ipxe.org/memcheck_8h_source.html and https://dox.ipxe.org/valgrind_8h_source.html for an explanation
// of valgrind client requests
#if VALGRIND
#include <memcheck.h>
#endif

// For each use of arena-internal memory (e.g. ArenaBlock::getSize()), unpoison the memory before use and
// poison it when done.
// When creating a new ArenaBlock, poison the memory that will be later allocated to users.
// When allocating memory to a user, mark that memory as undefined.

namespace {
#if VALGRIND
void allowAccess(ArenaBlock* b) {
	if (valgrindPrecise() && b) {
		VALGRIND_MAKE_MEM_DEFINED(b, ArenaBlock::TINY_HEADER);
		int headerSize = b->isTiny() ? ArenaBlock::TINY_HEADER : sizeof(ArenaBlock);
		VALGRIND_MAKE_MEM_DEFINED(b, headerSize);
	}
}
void disallowAccess(ArenaBlock* b) {
	if (valgrindPrecise() && b) {
		int headerSize = b->isTiny() ? ArenaBlock::TINY_HEADER : sizeof(ArenaBlock);
		VALGRIND_MAKE_MEM_NOACCESS(b, headerSize);
	}
}
void makeNoAccess(void* addr, size_t size) {
	if (valgrindPrecise()) {
		VALGRIND_MAKE_MEM_NOACCESS(addr, size);
	}
}
void makeDefined(void* addr, size_t size) {
	if (valgrindPrecise()) {
		VALGRIND_MAKE_MEM_DEFINED(addr, size);
	}
}
void makeUndefined(void* addr, size_t size) {
	if (valgrindPrecise()) {
		VALGRIND_MAKE_MEM_UNDEFINED(addr, size);
	}
}
#elif defined(ADDRESS_SANITZER)
void allowAccess(ArenaBlock* b) {
	if (b) {
		ASAN_UNPOISON_MEMORY_REGION(b, ArenaBlock::TINY_HEADER);
		int headerSize = b->isTiny() ? ArenaBlock::TINY_HEADER : sizeof(ArenaBlock);
		ASAN_UNPOISON_MEMORY_REGION(b, headerSize);
	}
}
void disallowAccess(ArenaBlock* b) {
	if (b) {
		int headerSize = b->isTiny() ? ArenaBlock::TINY_HEADER : sizeof(ArenaBlock);
		ASAN_POISON_MEMORY_REGION(b, headerSize);
	}
}
void makeNoAccess(void* addr, size_t size) {
	ASAN_POISON_MEMORY_REGION(addr, size);
}
void makeDefined(void* addr, size_t size) {
	ASAN_UNPOISON_MEMORY_REGION(addr, size);
}
void makeUndefined(void* addr, size_t size) {
	ASAN_UNPOISON_MEMORY_REGION(addr, size);
}
#else
void allowAccess(ArenaBlock*) {}
void disallowAccess(ArenaBlock*) {}
void makeNoAccess(void*, size_t) {}
void makeDefined(void*, size_t) {}
void makeUndefined(void*, size_t) {}
#endif
} // namespace

Arena::Arena() : impl(nullptr) {}
Arena::Arena(size_t reservedSize) : impl(0) {
	UNSTOPPABLE_ASSERT(reservedSize < std::numeric_limits<int>::max());
	if (reservedSize) {
		allowAccess(impl.getPtr());
		ArenaBlock::create((int)reservedSize, impl);
		disallowAccess(impl.getPtr());
	}
}
Arena::Arena(const Arena& r) = default;
Arena::Arena(Arena&& r) noexcept = default;
Arena& Arena::operator=(const Arena& r) = default;
Arena& Arena::operator=(Arena&& r) noexcept = default;
void Arena::dependsOn(const Arena& p) {
	// x.dependsOn(y) is a no-op if they refer to the same ArenaBlocks.
	// They will already have the same lifetime.
	if (p.impl && p.impl.getPtr() != impl.getPtr()) {
		allowAccess(impl.getPtr());
		allowAccess(p.impl.getPtr());
		ArenaBlock::dependOn(impl, p.impl.getPtr());
		disallowAccess(p.impl.getPtr());
		disallowAccess(impl.getPtr());
	}
}

void* Arena::allocate4kAlignedBuffer(uint32_t size) {
	return ArenaBlock::dependOn4kAlignedBuffer(impl, size);
}

size_t Arena::getSize(FastInaccurateEstimate fastInaccurateEstimate) const {
	if (impl) {
		allowAccess(impl.getPtr());
		size_t result;
		if (fastInaccurateEstimate) {
			result = impl->estimatedTotalSize();
		} else {
			std::unordered_set<ArenaBlock*> visited;
			result = impl->totalSize(visited);
		}

		disallowAccess(impl.getPtr());
		return result;
	}
	return 0;
}

bool Arena::hasFree(size_t size, const void* address) {
	if (impl) {
		allowAccess(impl.getPtr());
		auto result = impl->unused() >= size && impl->getNextData() == address;
		disallowAccess(impl.getPtr());
		return result;
	}
	return false;
}

void ArenaBlock::addref() {
	makeDefined(this, sizeof(ThreadSafeReferenceCounted<ArenaBlock>));
	ThreadSafeReferenceCounted<ArenaBlock>::addref();
	makeNoAccess(this, sizeof(ThreadSafeReferenceCounted<ArenaBlock>));
}

void ArenaBlock::delref() {
	makeDefined(this, sizeof(ThreadSafeReferenceCounted<ArenaBlock>));
	if (delref_no_destroy()) {
		destroy();
	} else {
		makeNoAccess(this, sizeof(ThreadSafeReferenceCounted<ArenaBlock>));
	}
}

bool ArenaBlock::isSecure() const {
	return secure;
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

size_t ArenaBlock::totalSize(std::unordered_set<ArenaBlock*>& visited) const {
	if (isTiny()) {
		return size();
	}

	// Walk the entire tree to get an accurate size and store it in the estimate for
	// each block, recursively.
	totalSizeEstimate = size();
	int o = nextBlockOffset;
	while (o) {
		ArenaBlockRef* r = (ArenaBlockRef*)((char*)getData() + o);
		makeDefined(r, sizeof(ArenaBlockRef));
		if (r->aligned4kBufferSize != 0) {
			totalSizeEstimate += r->aligned4kBufferSize;
		} else {
			allowAccess(r->next);
			// Skip visited arenas and avoid stack overflow for circular references
			if (visited.insert(r->next).second) {
				totalSizeEstimate += r->next->totalSize(visited);
			}
			disallowAccess(r->next);
		}
		o = r->nextBlockOffset;
		makeNoAccess(r, sizeof(ArenaBlockRef));
	}
	return totalSizeEstimate;
}
size_t ArenaBlock::estimatedTotalSize() const {
	if (isTiny()) {
		return size();
	}
	return totalSizeEstimate;
}

void ArenaBlock::wipeUsed() {
	int dataOffset = isTiny() ? TINY_HEADER : sizeof(ArenaBlock);
	void* dataBegin = (char*)getData() + dataOffset;
	int dataSize = used() - dataOffset;
	makeDefined(dataBegin, dataSize);
	::memset(dataBegin, 0, dataSize);
	makeNoAccess(dataBegin, dataSize);
}

// just for debugging:
void ArenaBlock::getUniqueBlocks(std::set<ArenaBlock*>& a) {
	a.insert(this);
	if (isTiny())
		return;

	int o = nextBlockOffset;
	while (o) {
		ArenaBlockRef* r = (ArenaBlockRef*)((char*)getData() + o);
		makeDefined(r, sizeof(ArenaBlockRef));

		// If next is valid recursively count its blocks
		if (r->aligned4kBufferSize == 0) {
			r->next->getUniqueBlocks(a);
		}

		o = r->nextBlockOffset;
		makeNoAccess(r, sizeof(ArenaBlockRef));
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
	makeDefined(r, sizeof(ArenaBlockRef));
	r->aligned4kBufferSize = 0;
	r->next = next;
	r->nextBlockOffset = nextBlockOffset;
	makeNoAccess(r, sizeof(ArenaBlockRef));
	nextBlockOffset = bigUsed;
	bigUsed += sizeof(ArenaBlockRef);
	totalSizeEstimate += next->estimatedTotalSize();
}

void* ArenaBlock::make4kAlignedBuffer(uint32_t size) {
	ArenaBlockRef* r = (ArenaBlockRef*)((char*)getData() + bigUsed);
	makeDefined(r, sizeof(ArenaBlockRef));
	r->aligned4kBufferSize = size;
	r->aligned4kBuffer = allocateFast4kAligned(size);
	// printf("Arena::aligned4kBuffer alloc size=%u ptr=%p\n", size, r->aligned4kBuffer);
	r->nextBlockOffset = nextBlockOffset;
	auto result = r->aligned4kBuffer;
	makeNoAccess(r, sizeof(ArenaBlockRef));
	nextBlockOffset = bigUsed;
	bigUsed += sizeof(ArenaBlockRef);
	totalSizeEstimate += size;
	return result;
}

void ArenaBlock::dependOn(Reference<ArenaBlock>& self, ArenaBlock* other) {
	other->addref();
	if (!self || self->isTiny() || self->unused() < sizeof(ArenaBlockRef)) {
		create(SMALL, self)->makeReference(other);
	} else {
		ASSERT(self->getData() != other->getData());
		self->makeReference(other);
	}
}

void* ArenaBlock::dependOn4kAlignedBuffer(Reference<ArenaBlock>& self, uint32_t size) {
	if (!self || self->isTiny() || self->unused() < sizeof(ArenaBlockRef)) {
		return create(SMALL, self)->make4kAlignedBuffer(size);
	} else {
		return self->make4kAlignedBuffer(size);
	}
}

void* ArenaBlock::allocate(Reference<ArenaBlock>& self, int bytes, IsSecureMem isSecure) {
	ArenaBlock* b = self.getPtr();
	allowAccess(b);
	if (!self || self->unused() < bytes) {
		auto* tmp = b;
		b = create(bytes, self);
		disallowAccess(tmp);
	}

	void* result = (char*)b->getData() + b->addUsed(bytes);
	if (isSecure)
		b->secure = 1;
	disallowAccess(b);
	makeUndefined(result, bytes);
	return result;
}

// Return an appropriately-sized ArenaBlock to store the given data
ArenaBlock* ArenaBlock::create(int dataSize, Reference<ArenaBlock>& next) {
	ArenaBlock* b;
	// all blocks are initialized with no-wipe by default. allocate() sets it, if needed.
	if (dataSize <= SMALL - TINY_HEADER && !next) {
		static_assert(sizeof(ArenaBlock) <= 32); // Need to allocate at least sizeof(ArenaBlock) for an ArenaBlock*. See
		                                         // https://github.com/apple/foundationdb/issues/6753
		if (dataSize <= 32 - TINY_HEADER) {
			b = (ArenaBlock*)FastAllocator<32>::allocate();
			b->tinySize = 32;
			INSTRUMENT_ALLOCATE("Arena32");
		} else {
			b = (ArenaBlock*)FastAllocator<64>::allocate();
			b->tinySize = 64;
			INSTRUMENT_ALLOCATE("Arena64");
		}
		b->tinyUsed = TINY_HEADER;
		b->secure = 0;

	} else {
		int reqSize = dataSize + sizeof(ArenaBlock);
		if (next)
			reqSize += sizeof(ArenaBlockRef);

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
				b = (ArenaBlock*)allocateAndMaybeKeepalive(512);
				b->bigSize = 512;
				INSTRUMENT_ALLOCATE("Arena512");
			} else if (reqSize <= 1024) {
				b = (ArenaBlock*)allocateAndMaybeKeepalive(1024);
				b->bigSize = 1024;
				INSTRUMENT_ALLOCATE("Arena1024");
			} else if (reqSize <= 2048) {
				b = (ArenaBlock*)allocateAndMaybeKeepalive(2048);
				b->bigSize = 2048;
				INSTRUMENT_ALLOCATE("Arena2048");
			} else if (reqSize <= 4096) {
				b = (ArenaBlock*)allocateAndMaybeKeepalive(4096);
				b->bigSize = 4096;
				INSTRUMENT_ALLOCATE("Arena4096");
			} else {
				b = (ArenaBlock*)allocateAndMaybeKeepalive(8192);
				b->bigSize = 8192;
				INSTRUMENT_ALLOCATE("Arena8192");
			}
			b->totalSizeEstimate = b->bigSize;
			b->tinySize = b->tinyUsed = NOT_TINY;
			b->bigUsed = sizeof(ArenaBlock);
			b->secure = 0;
		} else {
#ifdef ALLOC_INSTRUMENTATION
			allocInstr["ArenaHugeKB"].alloc((reqSize + 1023) >> 10);
#endif
			b = (ArenaBlock*)allocateAndMaybeKeepalive(reqSize);
			b->tinySize = b->tinyUsed = NOT_TINY;
			b->bigSize = reqSize;
			b->totalSizeEstimate = b->bigSize;
			b->bigUsed = sizeof(ArenaBlock);
			b->secure = 0;

#if !DEBUG_DETERMINISM
			if (FLOW_KNOBS && g_allocation_tracing_disabled == 0 &&
			    nondeterministicRandom()->random01() < (reqSize / FLOW_KNOBS->HUGE_ARENA_LOGGING_BYTES)) {
				++g_allocation_tracing_disabled;
				hugeArenaSample(reqSize);
				--g_allocation_tracing_disabled;
			}
#endif
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
		if (next)
			b->makeReference(next.getPtr());
	}
	b->setrefCountUnsafe(1);
	next.setPtrUnsafe(b);
	makeNoAccess(reinterpret_cast<uint8_t*>(b) + b->used(), b->unused());
	return b;
}

void ArenaBlock::destroy() {
	// If the stack never contains more than one item, nothing will be allocated from stackArena.
	// If stackArena is used, it will always be a linked list, so destroying *it* will not create another arena
	ArenaBlock* tinyStack = this;
	allowAccess(this);
	Arena stackArena;
	VectorRef<ArenaBlock*> stack(&tinyStack, 1);

	while (stack.size()) {
		ArenaBlock* b = stack.end()[-1];
		stack.pop_back();
		allowAccess(b);

		if (!b->isTiny()) {
			int o = b->nextBlockOffset;
			while (o) {
				ArenaBlockRef* br = (ArenaBlockRef*)((char*)b->getData() + o);
				makeDefined(br, sizeof(ArenaBlockRef));

				// If aligned4kBuffer is valid, free it
				if (br->aligned4kBufferSize != 0) {
					// printf("Arena::aligned4kBuffer free %p\n", br->aligned4kBuffer);
					freeFast4kAligned(br->aligned4kBufferSize, br->aligned4kBuffer);
				} else {
					allowAccess(br->next);
					if (br->next->delref_no_destroy())
						stack.push_back(stackArena, br->next);
					disallowAccess(br->next);
				}

				o = br->nextBlockOffset;
			}
		}
		b->destroyLeaf();
	}
}

void ArenaBlock::destroyLeaf() {
	if (secure) {
		wipeUsed();
	}
	if (isTiny()) {
		if (tinySize <= 32) {
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
			freeOrMaybeKeepalive(this);
			INSTRUMENT_RELEASE("Arena512");
		} else if (bigSize <= 1024) {
			freeOrMaybeKeepalive(this);
			INSTRUMENT_RELEASE("Arena1024");
		} else if (bigSize <= 2048) {
			freeOrMaybeKeepalive(this);
			INSTRUMENT_RELEASE("Arena2048");
		} else if (bigSize <= 4096) {
			freeOrMaybeKeepalive(this);
			INSTRUMENT_RELEASE("Arena4096");
		} else if (bigSize <= 8192) {
			freeOrMaybeKeepalive(this);
			INSTRUMENT_RELEASE("Arena8192");
		} else {
#ifdef ALLOC_INSTRUMENTATION
			allocInstr["ArenaHugeKB"].dealloc((bigSize + 1023) >> 10);
#endif
			g_hugeArenaMemory.fetch_sub(bigSize);
			freeOrMaybeKeepalive(this);
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

TEST_CASE("/flow/Arena/OptionalHash") {
	std::hash<Optional<int>> hashFunc{};
	Optional<int> a;
	Optional<int> b;
	Optional<int> c = 1;
	Optional<int> d = 1;
	Optional<int> e = 2;

	ASSERT(hashFunc(a) == hashFunc(b));
	ASSERT(hashFunc(a) != hashFunc(c));
	ASSERT(hashFunc(c) == hashFunc(d));
	ASSERT(hashFunc(c) != hashFunc(e));
	ASSERT(hashFunc(a) == hashFunc(a));
	ASSERT(hashFunc(c) == hashFunc(c));

	return Void();
}

TEST_CASE("/flow/Arena/DefaultBoostHash") {
	boost::hash<std::pair<Optional<int>, StringRef>> hashFunc;

	auto a = std::make_pair(Optional<int>(), "foo"_sr);
	auto b = std::make_pair(Optional<int>(), "foo"_sr);
	auto c = std::make_pair(Optional<int>(), "bar"_sr);
	auto d = std::make_pair(Optional<int>(1), "foo"_sr);
	auto e = std::make_pair(Optional<int>(1), "foo"_sr);

	ASSERT(hashFunc(a) == hashFunc(b));
	ASSERT(hashFunc(a) != hashFunc(c));
	ASSERT(hashFunc(a) != hashFunc(d));
	ASSERT(hashFunc(d) == hashFunc(e));
	ASSERT(hashFunc(a) == hashFunc(a));
	ASSERT(hashFunc(d) == hashFunc(d));

	return Void();
}

TEST_CASE("/flow/Arena/Size") {
	Arena a;
	int fastSize, slowSize;

	// Size estimates are accurate unless dependencies are added to an Arena via another Arena
	// handle which points to a non-root node.
	//
	// Note that the ASSERT argument order matters, the estimate must be calculated first as
	// the full accurate calculation will update the estimate
	makeString(40, a);
	fastSize = a.getSize(FastInaccurateEstimate::True);
	slowSize = a.getSize();
	ASSERT_EQ(fastSize, slowSize);

	makeString(700, a);
	fastSize = a.getSize(FastInaccurateEstimate::True);
	slowSize = a.getSize();
	ASSERT_EQ(fastSize, slowSize);

	// Copy a at a point where it points to a large block with room for block references
	Arena b = a;

	// copy a at a point where there isn't room for more block references
	makeString(1000, a);
	Arena c = a;

	makeString(1000, a);
	makeString(1000, a);
	fastSize = a.getSize(FastInaccurateEstimate::True);
	slowSize = a.getSize();
	ASSERT_EQ(fastSize, slowSize);

	Standalone<StringRef> s = makeString(500);
	a.dependsOn(s.arena());
	fastSize = a.getSize(FastInaccurateEstimate::True);
	slowSize = a.getSize();
	ASSERT_EQ(fastSize, slowSize);

	Standalone<StringRef> s2 = makeString(500);
	a.dependsOn(s2.arena());
	fastSize = a.getSize(FastInaccurateEstimate::True);
	slowSize = a.getSize();
	ASSERT_EQ(fastSize, slowSize);

	// Add a dependency to b, which will fit in b's root and update b's size estimate
	Standalone<StringRef> s3 = makeString(100);
	b.dependsOn(s3.arena());
	fastSize = b.getSize(FastInaccurateEstimate::True);
	slowSize = b.getSize();
	ASSERT_EQ(fastSize, slowSize);

	// But now a's size estimate is out of date because the new reference in b's root is still
	// in a's tree
	fastSize = a.getSize(FastInaccurateEstimate::True);
	slowSize = a.getSize();
	ASSERT_LT(fastSize, slowSize);

	// Now that a full size calc has been done on a, the estimate is up to date.
	fastSize = a.getSize(FastInaccurateEstimate::True);
	slowSize = a.getSize();
	ASSERT_EQ(fastSize, slowSize);

	// Add a dependency to c, which will NOT fit in c's root, so it will be added to a new
	// root for c and that root will not be in a's tree so a's size and estimate remain
	// unchanged and the same.  The size and estimate of c will also match.
	Standalone<StringRef> s4 = makeString(100);
	c.dependsOn(s4.arena());
	fastSize = c.getSize(FastInaccurateEstimate::True);
	slowSize = c.getSize();
	ASSERT_EQ(fastSize, slowSize);
	fastSize = a.getSize(FastInaccurateEstimate::True);
	slowSize = a.getSize();
	ASSERT_EQ(fastSize, slowSize);

	return Void();
}

// Test that x.dependsOn(x) works, and is effectively a no-op.
TEST_CASE("/flow/Arena/SelfRef") {
	Arena a(4096);

	// This should be a no-op.
	a.dependsOn(a);

	return Void();
}

TEST_CASE("flow/StringRef/eat") {
	StringRef str = "test/case"_sr;
	StringRef first = str.eat("/");
	ASSERT(first == "test"_sr);
	ASSERT(str == "case"_sr);

	str = "test/case"_sr;
	first = str.eat("/"_sr);
	ASSERT(first == "test"_sr);
	ASSERT(str == "case"_sr);

	str = "testcase"_sr;
	first = str.eat("/"_sr);
	ASSERT(first == "testcase"_sr);
	ASSERT(str == ""_sr);

	str = "testcase/"_sr;
	first = str.eat("/"_sr);
	ASSERT(first == "testcase"_sr);
	ASSERT(str == ""_sr);

	str = "test/case/extra"_sr;
	first = str.eat("/"_sr);
	ASSERT(first == "test"_sr);
	ASSERT(str == "case/extra"_sr);

	bool hasSep;
	str = "test/case"_sr;
	first = str.eat("/"_sr, &hasSep);
	ASSERT(hasSep);
	ASSERT(first == "test"_sr);
	ASSERT(str == "case"_sr);

	str = "testcase"_sr;
	first = str.eat("/", &hasSep);
	ASSERT(!hasSep);
	ASSERT(first == "testcase"_sr);
	ASSERT(str == ""_sr);

	return Void();
}

struct TestOptionalMapClass {
	StringRef value;
	Optional<StringRef> optionalValue;

	const StringRef constValue;
	const Optional<StringRef> constOptionalValue;

	StringRef getValue() const { return value; }
	Optional<StringRef> getOptionalValue() const { return optionalValue; }

	StringRef const& getValueRef() const { return value; }
	Optional<StringRef> const& getOptionalValueRef() const { return optionalValue; }

	StringRef sub(int x) const { return value.substr(x); }
	Optional<StringRef> optionalSub(int x) const { return optionalValue.map<StringRef>(&StringRef::substr, (int)x); }

	TestOptionalMapClass(StringRef value, bool setOptional)
	  : value(value), constValue(value),
	    optionalValue(setOptional ? Optional<StringRef>(value) : Optional<StringRef>()),
	    constOptionalValue(setOptional ? Optional<StringRef>(value) : Optional<StringRef>()) {}
};

struct TestOptionalMapClassRef : public TestOptionalMapClass, public ReferenceCounted<TestOptionalMapClassRef> {
	TestOptionalMapClassRef(StringRef value, bool setOptional) : TestOptionalMapClass(value, setOptional) {}
};

void checkResults(std::vector<Optional<StringRef>> const& results,
                  StringRef value,
                  bool shouldBeEmpty,
                  std::string context) {
	if (shouldBeEmpty) {
		for (int i = 0; i < results.size(); ++i) {
			if (results[i].present()) {
				fmt::print("Unexpected result {} at index {} in {}\n", results[i].get().printable(), i, context);
				ASSERT(false);
			}
		}
	} else {
		for (int i = 0; i < results.size(); ++i) {
			if (!results[i].present()) {
				fmt::print("Missing result {} at index {} in {}\n", value.printable(), i, context);
				ASSERT(false);
			}

			if (i < results.size() - 1) {
				if (results[i].get() != value) {
					fmt::print("Incorrect result {} at index {} in {}: expected {}\n",
					           results[i].get().printable(),
					           i,
					           context,
					           value.printable());
					ASSERT(false);
				}
			} else {
				if (results[i].get() != value.substr(5)) {
					fmt::print("Incorrect result {} at index {} in {}: expected {}\n",
					           results[i].get().printable(),
					           i,
					           context,
					           value.substr(5).printable());
					ASSERT(false);
				}
			}
		}
	}
}

template <bool IsRef, class T>
void checkOptional(Optional<T> val) {
	StringRef value;
	bool isEmpty = !val.present();
	bool isFlatMapEmpty = isEmpty;
	std::vector<Optional<StringRef>> mapResults;
	std::vector<Optional<StringRef>> flatMapResults;

	if constexpr (IsRef) {
		isEmpty = isFlatMapEmpty = isEmpty || !val.get();
		if (!isEmpty) {
			value = val.get()->value;
			isFlatMapEmpty = !val.get()->optionalValue.present();
		}
		mapResults.push_back(val.mapRef(&TestOptionalMapClass::value));
		mapResults.push_back(val.mapRef(&TestOptionalMapClass::constValue));
		mapResults.push_back(val.mapRef(&TestOptionalMapClass::getValue));
		mapResults.push_back(val.mapRef(&TestOptionalMapClass::getValueRef));
		mapResults.push_back(val.mapRef(&TestOptionalMapClass::sub, 5));

		flatMapResults.push_back(val.flatMap([](auto t) { return t ? t->optionalValue : Optional<StringRef>(); }));
		flatMapResults.push_back(val.flatMapRef(&TestOptionalMapClass::optionalValue));
		flatMapResults.push_back(val.flatMapRef(&TestOptionalMapClass::constOptionalValue));
		flatMapResults.push_back(val.flatMapRef(&TestOptionalMapClass::getOptionalValue));
		flatMapResults.push_back(val.flatMapRef(&TestOptionalMapClass::getOptionalValueRef));
		flatMapResults.push_back(val.flatMapRef(&TestOptionalMapClass::optionalSub, 5));
	} else {
		if (!isEmpty) {
			value = val.get().value;
			isFlatMapEmpty = !val.get().optionalValue.present();
		}
		mapResults.push_back(val.map([](auto t) { return t.value; }));
		mapResults.push_back(val.map(&TestOptionalMapClass::value));
		mapResults.push_back(val.map(&TestOptionalMapClass::constValue));
		mapResults.push_back(val.map(&TestOptionalMapClass::getValue));
		mapResults.push_back(val.map(&TestOptionalMapClass::getValueRef));
		mapResults.push_back(val.map(&TestOptionalMapClass::sub, 5));

		flatMapResults.push_back(val.flatMap([](auto t) { return t.optionalValue; }));
		flatMapResults.push_back(val.flatMap(&TestOptionalMapClass::optionalValue));
		flatMapResults.push_back(val.flatMap(&TestOptionalMapClass::constOptionalValue));
		flatMapResults.push_back(val.flatMap(&TestOptionalMapClass::getOptionalValue));
		flatMapResults.push_back(val.flatMap(&TestOptionalMapClass::getOptionalValueRef));
		flatMapResults.push_back(val.flatMap(&TestOptionalMapClass::optionalSub, 5));
	}

	checkResults(mapResults, value, isEmpty, IsRef ? "ref map" : "non-ref map");
	checkResults(flatMapResults, value, isFlatMapEmpty, IsRef ? "ref flat map" : "non-ref flat map");
}

TEST_CASE("/flow/Arena/OptionalMap") {
	// Optional<T>
	checkOptional<false>(Optional<TestOptionalMapClass>());
	checkOptional<false>(Optional<TestOptionalMapClass>(TestOptionalMapClass("test_string"_sr, false)));
	checkOptional<false>(Optional<TestOptionalMapClass>(TestOptionalMapClass("test_string"_sr, true)));

	// Optional<Reference<T>>
	checkOptional<true>(Optional<Reference<TestOptionalMapClassRef>>());
	checkOptional<true>(Optional<Reference<TestOptionalMapClassRef>>(Reference<TestOptionalMapClassRef>()));
	checkOptional<true>(
	    Optional<Reference<TestOptionalMapClassRef>>(makeReference<TestOptionalMapClassRef>("test_string"_sr, false)));
	checkOptional<true>(
	    Optional<Reference<TestOptionalMapClassRef>>(makeReference<TestOptionalMapClassRef>("test_string"_sr, true)));

	// Optional<T*>
	checkOptional<true>(Optional<TestOptionalMapClass*>());
	checkOptional<true>(Optional<TestOptionalMapClass*>(nullptr));

	auto ptr = new TestOptionalMapClass("test_string"_sr, false);
	checkOptional<true>(Optional<TestOptionalMapClass*>(ptr));
	delete ptr;

	ptr = new TestOptionalMapClass("test_string"_sr, true);
	checkOptional<true>(Optional<TestOptionalMapClass*>(ptr));
	delete ptr;

	return Void();
}

TEST_CASE("/flow/Arena/Secure") {
	auto& rng = *deterministicRandom();
	auto sizes = std::vector<int>{ 1 };
	for (auto i = 2; i <= ArenaBlock::LARGE * 2; i *= 2) {
		sizes.push_back(i);
		// randomly select one value between this pow2 and the next
		sizes.push_back(rng.randomInt(sizes.back() + 1, sizes.back() * 2));
	}
	// temporarily disable allocation tracing: runs with hugeArenaSample seems to cause test to fail for some reason
	g_allocation_tracing_disabled++;
	auto reenableAllocTracingOnUnwind = ScopeExit([]() { g_allocation_tracing_disabled--; });
	for (auto iter = 0; iter < 100; iter++) {
		auto const allocsPerArena = rng.randomInt(1, 80);
		std::vector<std::pair<uint8_t*, int>> buffers;
		// below scope object keeps deallocated memory alive to test if wipe-after-use behaves correctly
		auto keepaliveScope = keepalive_allocator::ActiveScope{};
		{
			Arena arena;
			for (auto i = 0; i < allocsPerArena; i++) {
				auto const len = sizes[rng.randomInt(0, sizes.size())];
				auto const buf = new (arena, WipeAfterUse{}) uint8_t[len];
				for (auto i = 0; i < len; i++)
					buf[i] = rng.randomInt(1, 256);
				buffers.push_back(std::make_pair(buf, len));
			}
		}
		// make sure the buffers have been zeroed out
		for (auto const& buffer : buffers) {
			auto buf = buffer.first;
			auto len = buffer.second;
			makeDefined(buf, len);
			auto poisonOnUnwind = ScopeExit([buf, len]() { makeNoAccess(buf, len); });
			for (auto i = 0; i < len; i++) {
				if (buf[i] != 0) {
					fmt::print("Non-zero byte found at iter {} size {} offset {}\n", iter + 1, len, i);
					ASSERT(false);
				}
			}
		}
	}
	return Void();
}
