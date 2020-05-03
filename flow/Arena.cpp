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

#include "flow/Arena.h"
#include "flow/Error.h"

void ArenaBlock::delref() {
	if (delref_no_destroy()) destroy();
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
	if (isTiny()) return size();

	size_t s = size();
	int o = nextBlockOffset;
	while (o) {
		ArenaBlockRef* r = (ArenaBlockRef*)((char*)getData() + o);
		s += r->next->totalSize();
		o = r->nextBlockOffset;
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
		r->next->getUniqueBlocks(a);
		o = r->nextBlockOffset;
	}
	return;
}

int ArenaBlock::addUsed(int bytes) {
	if (isTiny()) {
		int t = tinyUsed;
		tinyUsed += bytes;
		UNSTOPPABLE_ASSERT(tinyUsed <= tinySize);
		return t;
	} else {
		int t = bigUsed;
		bigUsed += bytes;
		UNSTOPPABLE_ASSERT(bigUsed <= bigSize);
		return t;
	}
}

void ArenaBlock::makeReference(ArenaBlock* next) {
	// TODO: Align memory here?
	ArenaBlockRef* r = (ArenaBlockRef*)((char*)getData() + bigUsed);
	r->next = next;
	r->nextBlockOffset = nextBlockOffset;
	nextBlockOffset = bigUsed;
	bigUsed += sizeof(ArenaBlockRef);
}

void ArenaBlock::dependOn(Reference<ArenaBlock>& self, ArenaBlock* other) {
	other->addref();
	if (!self || self->isTiny() || !self->canAlloc(sizeof(ArenaBlockRef)))
		create(SMALL, self)->makeReference(other);
	else
		self->makeReference(other);
}

static int alignment_for(int size) {
	if (size <= 1) {
		return 1;
	} else if (size <= 2) {
		return 2;
	} else if (size <= 4) {
		return 4;
	} else if (size <= 8) {
		return 8;
	}
	return 16;
}

void* ArenaBlock::allocate(Reference<ArenaBlock>& self, int bytes) {
	ArenaBlock* b = self.getPtr();
	if (!self || !self->canAlloc(bytes))
		b = create(bytes, self);

	auto a = b->alignment(bytes);
	auto res = (char*)b->getData() + a + b->addUsed(bytes + a);
	ASSERT(reinterpret_cast<uintptr_t>(res) % alignment_for(bytes) == 0);
	return res;
}

static inline int sizeWithOffset(int bytes, int offset) {
	if (bytes == 0) {
		return 0;
	}
	auto a = alignment_for(bytes);
	auto off = offset % a;
	if (off == 0) {
		return bytes;
	}
	return a - off + bytes;
}

// Return an appropriately-sized ArenaBlock to store the given data
ArenaBlock* ArenaBlock::create(int dataSize, Reference<ArenaBlock>& next) {
	ArenaBlock* b;
	auto tinyBytes = sizeWithOffset(dataSize, TINY_HEADER);
	if (tinyBytes <= SMALL - TINY_HEADER && !next) {
		if (tinyBytes <= 16 - TINY_HEADER) {
			b = (ArenaBlock*)FastAllocator<16>::allocate();
			b->tinySize = 16;
			INSTRUMENT_ALLOCATE("Arena16");
		} else if (tinyBytes <= 32 - TINY_HEADER) {
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
		dataSize = sizeWithOffset(dataSize, sizeof(ArenaBlock));
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
	return b;
}

void ArenaBlock::destroy() {
	// If the stack never contains more than one item, nothing will be allocated from stackArena.
	// If stackArena is used, it will always be a linked list, so destroying *it* will not create another arena
	ArenaBlock* tinyStack = this;
	Arena stackArena;
	VectorRef<ArenaBlock*> stack(&tinyStack, 1);

	while (stack.size()) {
		ArenaBlock* b = stack.end()[-1];
		stack.pop_back();

		if (!b->isTiny()) {
			int o = b->nextBlockOffset;
			while (o) {
				ArenaBlockRef* br = (ArenaBlockRef*)((char*)b->getData() + o);
				if (br->next->delref_no_destroy()) stack.push_back(stackArena, br->next);
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

bool ArenaBlock::canAlloc(int bytes) const {
	if (bytes == 0) return true;
	return used() + bytes + alignment(bytes) <= size();
}

static inline int alignment_from(int bytes, uintptr_t addr) {
	if (bytes == 0) return 0;
	auto a = alignment_for(bytes);
	auto off = int(addr % a);
	if (off == 0) {
		return 0;
	}
	return a - off;
}

int ArenaBlock::alignment(int bytes) const {
	return alignment_from(bytes, reinterpret_cast<uintptr_t>(this) + used());
}
