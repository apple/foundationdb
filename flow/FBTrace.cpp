/*
 * FBTrace.cpp
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

#include "flow/FBTrace.h"
#include "FastAlloc.h"
#include "Platform.h"
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <memory>

namespace {

// A special allocator that quickly allocates small objects
// and deallocates them roughly in the same order
struct Chunk {
    // we'll use 1MB chunks
    static constexpr size_t size = ChunkAllocator::MAX_CHUNK_SIZE;
    //  mutable because it is thread safe
    // atomic because we ship these into
    // a thread pool
    mutable std::atomic<unsigned> refCount = 1;
    unsigned offset = 0;

    constexpr static size_t beginOffset() {
        return sizeof(Chunk) % 8 == 0 ? sizeof(Chunk) : sizeof(Chunk) + (8 - (sizeof(Chunk) % 8));
    }
    static Chunk* create() {
        void* buffer = aligned_alloc(8, Chunk::size + Chunk::beginOffset());
        return new (buffer) Chunk{};
    }

    void delref() const {
        if (refCount.fetch_sub(1) == 1) {
            ::aligned_free(const_cast<Chunk*>(this));
        }
    }
    void addref() {
        refCount.fetch_and(1);
    }
    static Chunk& getChunk(void* ptr) {
        auto addr = reinterpret_cast<ptrdiff_t>(ptr);
        unsigned offset = *reinterpret_cast<unsigned*>(addr - 4);
        return *reinterpret_cast<Chunk*>(addr - offset);
    }

    uintptr_t begin() const {
        return reinterpret_cast<uintptr_t>(this) + Chunk::beginOffset();
    }
    uintptr_t end() const {
        return begin() + offset;
    }

    void* allocate(size_t sz) {
        auto off = offset;
        void* res = reinterpret_cast<void*>(end());
        addref();
    }
private:
    // make sure nobody constructs a Chunk directly
    Chunk() {}
};



struct ChunkDeleter {
    void operator()(Chunk* ptr) const { ptr->delref(); }
};

struct ChunkAllocatorImpl {
    std::unique_ptr<Chunk, ChunkDeleter> currentChunk;

    ChunkAllocatorImpl() : currentChunk(new Chunk{}) {}

    void* allocate(size_t sz) {
        if (sz > ChunkAllocator::MAX_CHUNK_SIZE) {
            auto res = reinterpret_cast<uint32_t*>(aligned_alloc(8, sz));
            // I don't think this is necessary, but it help to debug because now this
            // means that the 8 bytes before will be 0x00000000ffffffff
            res[0] = 0;
            res[1] = std::numeric_limits<uint32_t>::max();
            return res + 2;
        }
        void* res = nullptr;
        if ((res = currentChunk->allocate(sz)) == nullptr) {
            currentChunk.reset(new Chunk{});
        }
        return currentChunk->allocate(sz);
    }

    void free(void* ptr) {
        auto i = reinterpret_cast<uint32_t*>(ptr);
        auto off = *(i - 1);
        if (off == std::numeric_limits<uint32_t>::max()) {
            aligned_free(i - 2);
        } else {
            auto addr = reinterpret_cast<std::ptrdiff_t>(ptr);
            reinterpret_cast<Chunk*>(addr - off)->delref();
        }
    }
};

ChunkAllocatorImpl chunkAllocator;

} // namespace

namespace ChunkAllocator {

void* allocate(size_t sz) {
    return chunkAllocator.allocate(sz);
}

void free(void* ptr) {
    return chunkAllocator.free(ptr);
}

} // namespace ChunkAllocator

void* FBTraceImpl::operator new(std::size_t sz) {
    return ChunkAllocator::allocate(sz);
}
void FBTraceImpl::operator delete(void* ptr) {
    Chunk::getChunk(ptr).delref();
}
