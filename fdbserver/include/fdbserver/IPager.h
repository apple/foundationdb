/*
 * IPager.h
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

#ifndef FDBSERVER_IPAGER_H
#define FDBSERVER_IPAGER_H
#include "flow/Error.h"
#include "flow/FastAlloc.h"
#include "flow/ProtocolVersion.h"
#include <cstddef>
#include <stdint.h>
#pragma once

#include "fdbserver/IKeyValueStore.h"

#include "flow/flow.h"
#include "fdbclient/FDBTypes.h"
#define XXH_INLINE_ALL
#include "flow/xxhash.h"

typedef uint32_t LogicalPageID;
typedef uint32_t PhysicalPageID;
#define invalidLogicalPageID std::numeric_limits<LogicalPageID>::max()
#define invalidPhysicalPageID std::numeric_limits<PhysicalPageID>::max()

typedef uint32_t QueueID;
#define invalidQueueID std::numeric_limits<QueueID>::max()

// Pager Events
enum class PagerEvents { CacheLookup = 0, CacheHit, CacheMiss, PageWrite, MAXEVENTS };
static const char* const PagerEventsStrings[] = { "Lookup", "Hit", "Miss", "Write", "Unknown" };
// Reasons for page level events.
enum class PagerEventReasons { PointRead = 0, RangeRead, RangePrefetch, Commit, LazyClear, MetaData, MAXEVENTREASONS };
static const char* const PagerEventReasonsStrings[] = {
	"Get", "GetR", "GetRPF", "Commit", "LazyClr", "Meta", "Unknown"
};

static const unsigned int nonBtreeLevel = 0;
static const std::vector<std::pair<PagerEvents, PagerEventReasons>> possibleEventReasonPairs = {
	{ PagerEvents::CacheLookup, PagerEventReasons::Commit },
	{ PagerEvents::CacheLookup, PagerEventReasons::LazyClear },
	{ PagerEvents::CacheLookup, PagerEventReasons::PointRead },
	{ PagerEvents::CacheLookup, PagerEventReasons::RangeRead },
	{ PagerEvents::CacheHit, PagerEventReasons::Commit },
	{ PagerEvents::CacheHit, PagerEventReasons::LazyClear },
	{ PagerEvents::CacheHit, PagerEventReasons::PointRead },
	{ PagerEvents::CacheHit, PagerEventReasons::RangeRead },
	{ PagerEvents::CacheMiss, PagerEventReasons::Commit },
	{ PagerEvents::CacheMiss, PagerEventReasons::LazyClear },
	{ PagerEvents::CacheMiss, PagerEventReasons::PointRead },
	{ PagerEvents::CacheMiss, PagerEventReasons::RangeRead },
	{ PagerEvents::PageWrite, PagerEventReasons::Commit },
	{ PagerEvents::PageWrite, PagerEventReasons::LazyClear },
};
static const std::vector<std::pair<PagerEvents, PagerEventReasons>> L0PossibleEventReasonPairs = {
	{ PagerEvents::CacheLookup, PagerEventReasons::RangePrefetch },
	{ PagerEvents::CacheLookup, PagerEventReasons::MetaData },
	{ PagerEvents::CacheHit, PagerEventReasons::RangePrefetch },
	{ PagerEvents::CacheHit, PagerEventReasons::MetaData },
	{ PagerEvents::CacheMiss, PagerEventReasons::RangePrefetch },
	{ PagerEvents::CacheMiss, PagerEventReasons::MetaData },
	{ PagerEvents::PageWrite, PagerEventReasons::MetaData },
};

enum EncodingType : uint8_t {
	XXHash64 = 0,
	// For testing purposes
	XOREncryption = 1
};

enum PageType : uint8_t {
	HeaderPage = 0,
	BackupHeaderPage = 1,
	BTreeNode = 2,
	BTreeSuperNode = 3,
	QueuePageStandalone = 4,
	QueuePageInExtent = 5
};

// Encryption key ID
typedef uint64_t KeyID;

// EncryptionKeyRef is somewhat multi-variant, it will contain members representing the union
// of all fields relevant to any implemented encryption scheme.  They are generally of
// the form
//   Page Fields - fields which come from or are stored in the Page
//   Secret Fields - fields which are only known by the Key Provider
// but it is up to each encoding and provider which fields are which and which ones are used
struct EncryptionKeyRef {

	EncryptionKeyRef(){};
	EncryptionKeyRef(Arena& arena, const EncryptionKeyRef& toCopy) : secret(arena, toCopy.secret), id(toCopy.id) {}
	int expectedSize() const { return secret.size(); }

	StringRef secret;
	Optional<KeyID> id;
};
typedef Standalone<EncryptionKeyRef> EncryptionKey;

// Interface used by pager to get encryption keys by ID when reading pages from disk
// and by the BTree to get encryption keys to use for new pages
class IEncryptionKeyProvider {
public:
	virtual ~IEncryptionKeyProvider() {}

	// Get an EncryptionKey with Secret Fields populated based on the given Page Fields.
	// It is up to the implementation which fields those are.
	// The output Page Fields must match the input Page Fields.
	virtual Future<EncryptionKey> getSecrets(const EncryptionKeyRef& key) = 0;

	// Get encryption key that should be used for a given user Key-Value range
	virtual Future<EncryptionKey> getByRange(const KeyRef& begin, const KeyRef& end) = 0;
};

// This is a hacky way to attach an additional object of an arbitrary type at runtime to another object.
// It stores an arbitrary void pointer and a void pointer function to call when the ArbitraryObject
// is destroyed.
// It has helper operator= methods for storing heap-allocated T's or Reference<T>'s in into it via
//   x = thing;
// Examples:
//   ArbitraryObject x;
//   x.set(new Widget());  // x owns the new object
//   x.set(Reference<SomeClass>(new SomeClass());    // x holds a reference now too
//   x.setReference(new SomeReferenceCountedType()); //
struct ArbitraryObject {
	ArbitraryObject() : ptr(nullptr), onDestruct(nullptr) {}
	ArbitraryObject(const ArbitraryObject&) = delete;

	~ArbitraryObject() { destructOnly(); }

	bool valid() const { return ptr != nullptr; }

	template <typename T>
	void operator=(T* p) {
		destructOnly();
		ptr = p;
		onDestruct = [](void* ptr) { delete (T*)ptr; };
	}

	template <typename T>
	void operator=(Reference<T>& r) {
		destructOnly();
		ptr = r.getPtr();
		r.getPtr()->addref();
		onDestruct = [](void* ptr) { ((T*)ptr)->delref(); };
	}

	template <typename T>
	void operator=(Reference<T>&& r) {
		destructOnly();
		ptr = r.extractPtr();
		onDestruct = [](void* ptr) { ((T*)ptr)->delref(); };
	}

	template <typename T>
	T* getPtr() {
		return (T*)ptr;
	}

	template <typename T>
	Reference<T> getReference() {
		return Reference<T>::addRef((T*)ptr);
	}

	void reset() {
		destructOnly();
		ptr = nullptr;
		onDestruct = nullptr;
	}

	// ptr can be set to any arbitrary thing.  If it is not null at destruct time then
	// onDestruct(ptr) will be called if onDestruct is not null.
	void* ptr = nullptr;
	void (*onDestruct)(void*) = nullptr;

private:
	// Call onDestruct(ptr) if needed but don't reset any state
	void destructOnly() {
		if (ptr != nullptr && onDestruct != nullptr) {
			onDestruct(ptr);
		}
	}
};

// ArenaPage represents a data page meant to be stored on disk, located in a block of
// 4k-aligned memory held by an Arena
//
// Page Format:
//    PageHeader - describes main header version, encoding type, and offsets of subheaders and payload.
//    MainHeader - structure based on header version.  It is responsible for protecting all bytes
//                 of PageHeader, MainHeader, and EncodingHeader with some sort of checksum.
//    EncodingHeader - structure based on encoding type.  It is responsible for protecting and
//                     possibly encrypting all payload bytes.
//    Payload - User accessible bytes, protected and possibly encrypted based on the encoding
//
// preWrite() must be called before writing a page to disk to update checksums and encrypt as needed
// After reading a page from disk,
//   postReadHeader() must be called to verify the verison, main, and encoding headers
//   postReadPayload() must be called, after potentially setting encryption secret, to verify and possibly
//                     decrypt the payload
class ArenaPage : public ReferenceCounted<ArenaPage>, public FastAllocated<ArenaPage> {
public:
	// This is the header version that new page init() calls will use.
	// It is not necessarily the latest header version, as read/modify support for
	// a new header version may be added prior to using that version as the default
	// for new pages as part of downgrade support.
	static constexpr uint8_t HEADER_WRITE_VERSION = 1;

	ArenaPage(int logicalSize, int bufferSize) : logicalSize(logicalSize), bufferSize(bufferSize), pPayload(nullptr) {
		if (bufferSize > 0) {
			buffer = (uint8_t*)arena.allocate4kAlignedBuffer(bufferSize);

			// Zero unused region
			memset(buffer + logicalSize, 0, bufferSize - logicalSize);
		} else {
			buffer = nullptr;
		}
	};

	~ArenaPage() {}

	// Before using these, either init() or postReadHeader and postReadPayload() must be called
	const uint8_t* data() const { return pPayload; }
	uint8_t* mutateData() const { return (uint8_t*)pPayload; }
	int dataSize() const { return payloadSize; }

	StringRef dataAsStringRef() const { return StringRef((uint8_t*)pPayload, payloadSize); }

	const uint8_t* rawData() const { return buffer; }
	uint8_t* rawData() { return buffer; }
	int rawSize() const { return bufferSize; }

#pragma pack(push, 1)

	// The next few structs describe the byte-packed physical structure.  The fields of Page
	// cannot change, but new header versions and encoding types can be added and existing
	// header versions and encoding type headers could change size as offset information
	// is stored to enable efficient jumping to the encoding header or payload.
	// Page members are only initialized in init()
	struct PageHeader {
		uint8_t headerVersion;
		EncodingType encodingType;

		// Encoding header comes after main header
		uint8_t encodingHeaderOffset;

		// Payload comes after encoding header
		uint8_t payloadOffset;

		// Get main header pointer, casting to its type
		template <typename T>
		T* getMainHeader() const {
			return (T*)(this + 1);
		}

		// Get encoding header pointer, casting to its type
		template <typename T>
		T* getEncodingHeader() const {
			return (T*)((uint8_t*)this + encodingHeaderOffset);
		}

		// Get payload pointer
		uint8_t* getPayload() const { return (uint8_t*)this + payloadOffset; }
	};

	// Redwood header version 1
	// Protects all headers with a 64-bit XXHash checksum
	// Most other fields are forensic in nature and are not required to be set for correct
	// behavior but they can faciliate forensic investigation of data on disk.  Some of them
	// could be used for sanity checks at runtime.
	struct RedwoodHeaderV1 {
		PageType pageType;
		// The meaning of pageSubType is based on pageType
		//   For Queue pages, pageSubType is the QueueID
		//   For BTree nodes, pageSubType is Height (also stored in BTreeNode)
		uint8_t pageSubType;
		// Format identifier, normally specific to the page Type and SubType
		uint8_t pageFormat;
		XXH64_hash_t checksum;

		// Physical page ID of first block on disk of the ArenaPage
		PhysicalPageID firstPhysicalPageID;
		// The first logical page ID the ArenaPage was referenced by when last written
		LogicalPageID lastKnownLogicalPageID;
		// The first logical page ID of the parent of this ArenaPage when last written
		LogicalPageID lastKnownParentLogicalPageID;

		// Time and write version as of the last update to this page.
		// Note that for relocated pages, writeVersion should not be updated.
		double writeTime;
		Version writeVersion;

		// Update checksum
		void updateChecksum(uint8_t* headerBytes, int len) {
			// Checksum is within the checksum input so clear it first
			checksum = 0;
			checksum = XXH3_64bits(headerBytes, len);
		}

		// Verify checksum
		void verifyChecksum(uint8_t* headerBytes, int len) {
			// Checksum is within the checksum input so save it and restore it afterwards
			XXH64_hash_t saved = checksum;
			checksum = 0;
			XXH64_hash_t calculated = XXH3_64bits(headerBytes, len);
			checksum = saved;

			if (saved != calculated) {
				throw page_header_checksum_failed();
			}
		}
	};

	// An encoding that validates the payload with an XXHash checksum
	struct XXHashEncodingHeader {
		XXH64_hash_t checksum;
		void encode(uint8_t* payload, int len, PhysicalPageID seed) {
			checksum = XXH3_64bits_withSeed(payload, len, seed);
		}
		void decode(uint8_t* payload, int len, PhysicalPageID seed) {
			if (checksum != XXH3_64bits_withSeed(payload, len, seed)) {
				throw page_decoding_failed();
			}
		}
	};

	// A dummy "encrypting" encoding which uses XOR with a 1 byte secret key on
	// the payload to obfuscate it and protects the payload with an XXHash checksum.
	struct XOREncryptionEncodingHeader {
		// Checksum is on unencrypted payload
		XXH64_hash_t checksum;
		uint8_t keyID;

		void encode(uint8_t secret, uint8_t* payload, int len, PhysicalPageID seed) {
			checksum = XXH3_64bits_withSeed(payload, len, seed);
			for (int i = 0; i < len; ++i) {
				payload[i] ^= secret;
			}
		}
		void decode(uint8_t secret, uint8_t* payload, int len, PhysicalPageID seed) {
			for (int i = 0; i < len; ++i) {
				payload[i] ^= secret;
			}
			if (checksum != XXH3_64bits_withSeed(payload, len, seed)) {
				throw page_decoding_failed();
			}
		}
	};
#pragma pack(pop)

	// Get the size of the encoding header based on type
	// Note that this is only to be used in operations involving new pages to calculate the payload offset.  For
	// existing pages, the payload offset is stored in the page.
	static int encodingHeaderSize(EncodingType t) {
		if (t == EncodingType::XXHash64) {
			return sizeof(XXHashEncodingHeader);
		} else if (t == EncodingType::XOREncryption) {
			return sizeof(XOREncryptionEncodingHeader);
		} else {
			throw page_encoding_not_supported();
		}
	}

	// Get the usable size for a new page of pageSize using HEADER_WRITE_VERSION with encoding type t
	static int getUsableSize(int pageSize, EncodingType t) {
		return pageSize - sizeof(PageHeader) - sizeof(RedwoodHeaderV1) - encodingHeaderSize(t);
	}

	// Initialize the header for a new page so that the payload can be written to
	// Pre:  Buffer is allocated and logical size is set
	// Post: Page header is initialized and space is reserved for subheaders for
	//       HEADER_WRITE_VERSION main header and the given encoding type.
	//       Payload can be written to with mutateData() and dataSize()
	void init(EncodingType t, PageType pageType, uint8_t pageSubType, uint8_t pageFormat = 0) {
		// Carefully cast away constness to modify page header
		PageHeader* p = const_cast<PageHeader*>(page);
		p->headerVersion = HEADER_WRITE_VERSION;
		p->encodingHeaderOffset = sizeof(PageHeader) + sizeof(RedwoodHeaderV1);
		p->encodingType = t;
		p->payloadOffset = page->encodingHeaderOffset + encodingHeaderSize(t);

		pPayload = page->getPayload();
		payloadSize = logicalSize - (pPayload - buffer);

		RedwoodHeaderV1* h = page->getMainHeader<RedwoodHeaderV1>();
		h->pageType = pageType;
		h->pageSubType = pageSubType;
		h->pageFormat = pageFormat;

		// Write dummy values for these in new pages. They should be updated when possible before calling preWrite()
		// when modifying existing pages
		h->lastKnownLogicalPageID = invalidLogicalPageID;
		h->lastKnownParentLogicalPageID = invalidLogicalPageID;
		h->writeVersion = invalidVersion;
	}

	// Get the logical page buffer as a StringRef
	Standalone<StringRef> asStringRef() const { return Standalone<StringRef>(StringRef(buffer, logicalSize)); }

	// Get a new ArenaPage that contains a copy of this page's data.
	// extra is not copied to the returned page
	Reference<ArenaPage> clone() const {
		ArenaPage* p = new ArenaPage(logicalSize, bufferSize);
		memcpy(p->buffer, buffer, logicalSize);

		// Non-verifying header parse just to initialize members
		p->postReadHeader(invalidPhysicalPageID, false);
		p->encryptionKey = encryptionKey;

		return Reference<ArenaPage>(p);
	}

	// Get an ArenaPage which depends on this page's Arena and references some of its memory
	Reference<ArenaPage> getSubPage(int offset, int len) const {
		ASSERT(offset + len <= logicalSize);
		ArenaPage* p = new ArenaPage(len, 0);
		p->buffer = buffer + offset;
		p->arena.dependsOn(arena);

		// Non-verifying header parse just to initialize component pointers
		p->postReadHeader(invalidPhysicalPageID, false);
		p->encryptionKey = encryptionKey;

		return Reference<ArenaPage>(p);
	}

	// The next two functions set mostly forensic info that may help in an investigation to identify data on disk.  The
	// exception is pageID which must be set to the physical page ID on disk where the page is written or post-read
	// verification will fail.
	void setWriteInfo(PhysicalPageID pageID, Version writeVersion) {
		if (page->headerVersion == 1) {
			RedwoodHeaderV1* h = page->getMainHeader<RedwoodHeaderV1>();
			h->firstPhysicalPageID = pageID;
			h->writeVersion = writeVersion;
			h->writeTime = now();
		}
	}

	// These should be updated before writing a BTree page.  Note that the logical ID that refers to a page can change
	// after the page is written, if its parent is updated to point directly to its physical page ID.  Therefore, the
	// last known logical page ID should always be updated before writing an updated version of a BTree page.
	void setLogicalPageInfo(LogicalPageID lastKnownLogicalPageID, LogicalPageID lastKnownParentLogicalPageID) {
		if (page->headerVersion == 1) {
			RedwoodHeaderV1* h = page->getMainHeader<RedwoodHeaderV1>();
			h->lastKnownLogicalPageID = lastKnownLogicalPageID;
			h->lastKnownParentLogicalPageID = lastKnownParentLogicalPageID;
		}
	}

	// Must be called before writing to disk to update headers and encrypt page
	// Pre:   Encoding-specific header fields are set if needed
	//        Secret is set if needed
	// Post:  Main and Encoding subheaders are updated
	//        Payload is possibly encrypted
	void preWrite(PhysicalPageID pageID) const {
		// Explicitly check payload definedness to make the source of valgrind errors more clear.
		// Without this check, calculating a checksum on a payload with undefined bytes does not
		// cause a valgrind error but the resulting checksum is undefined which causes errors later.
		ASSERT(VALGRIND_CHECK_MEM_IS_DEFINED(pPayload, payloadSize) == 0);

		if (page->encodingType == EncodingType::XXHash64) {
			page->getEncodingHeader<XXHashEncodingHeader>()->encode(pPayload, payloadSize, pageID);
		} else if (page->encodingType == EncodingType::XOREncryption) {
			ASSERT(encryptionKey.secret.size() == 1);
			XOREncryptionEncodingHeader* xh = page->getEncodingHeader<XOREncryptionEncodingHeader>();
			xh->keyID = encryptionKey.id.orDefault(0);
			xh->encode(encryptionKey.secret[0], pPayload, payloadSize, pageID);
		} else {
			throw page_encoding_not_supported();
		}

		if (page->headerVersion == 1) {
			page->getMainHeader<RedwoodHeaderV1>()->updateChecksum(buffer, pPayload - buffer);
		} else {
			throw page_header_version_not_supported();
		}
	}

	// Must be called after reading from disk to verify all non-payload bytes
	// Pre:   Bytes from storage medium copied into raw buffer space
	// Post:  Page headers outside of payload are verified (unless verify is false)
	//        encryptionKey is updated with information from encoding header if needed
	//        Payload is accessible via data(), dataSize(), etc.
	//
	// Exceptions are thrown for unknown header types or pages which fail verification
	void postReadHeader(PhysicalPageID pageID, bool verify = true) {
		pPayload = page->getPayload();
		payloadSize = logicalSize - (pPayload - buffer);

		// Populate encryption key with relevant fields from page
		if (page->encodingType == EncodingType::XOREncryption) {
			encryptionKey.id = page->getEncodingHeader<XOREncryptionEncodingHeader>()->keyID;
		}

		if (page->headerVersion == 1) {
			if (verify) {
				RedwoodHeaderV1* h = page->getMainHeader<RedwoodHeaderV1>();
				h->verifyChecksum(buffer, pPayload - buffer);
				if (pageID != h->firstPhysicalPageID) {
					throw page_header_wrong_page_id();
				}
			}
		} else {
			throw page_header_version_not_supported();
		}
	}

	// Pre:   postReadHeader has been called, encoding-specific parameters (such as the encryption secret) have been set
	// Post:  Payload has been verified and decrypted if necessary
	void postReadPayload(PhysicalPageID pageID) {
		if (page->encodingType == EncodingType::XXHash64) {
			page->getEncodingHeader<XXHashEncodingHeader>()->decode(pPayload, payloadSize, pageID);
		} else if (page->encodingType == EncodingType::XOREncryption) {
			ASSERT(encryptionKey.secret.size() == 1);
			page->getEncodingHeader<XOREncryptionEncodingHeader>()->decode(
			    encryptionKey.secret[0], pPayload, payloadSize, pageID);
		} else {
			throw page_encoding_not_supported();
		}
	}

	const Arena& getArena() const { return arena; }

	static bool isEncodingTypeEncrypted(EncodingType t) { return t == EncodingType::XOREncryption; }

	// Returns true if the page's encoding type employs encryption
	bool isEncrypted() const { return isEncodingTypeEncrypted(getEncodingType()); }

private:
	Arena arena;

	// The logical size of the page, which can be smaller than bufferSize, which is only of
	// practical purpose in simulation to use arbitrarily small page sizes to test edge cases
	// with shorter execution time
	int logicalSize;

	// The 4k-aligned physical size of allocated memory for the page which also represents the
	// block size to be written to disk
	int bufferSize;

	// buffer is a pointer to the page's memory
	// For convenience, it is unioned with a Page pointer which defines the page structure
	union {
		uint8_t* buffer;
		const PageHeader* page;
	};

	// Pointer and length of page space available to the user
	// These are accessed very often so they are stored directly
	uint8_t* pPayload;
	int payloadSize;

public:
	EncodingType getEncodingType() const { return page->encodingType; }

	PhysicalPageID getPhysicalPageID() const {
		if (page->headerVersion == 1) {
			return page->getMainHeader<RedwoodHeaderV1>()->firstPhysicalPageID;
		} else {
			throw page_header_version_not_supported();
		}
	}

	// Used by encodings that do encryption
	EncryptionKey encryptionKey;

	mutable ArbitraryObject extra;
};

class IPagerSnapshot {
public:
	virtual Future<Reference<const ArenaPage>> getPhysicalPage(PagerEventReasons reason,
	                                                           unsigned int level,
	                                                           LogicalPageID pageID,
	                                                           int priority,
	                                                           bool cacheable,
	                                                           bool nohit) = 0;
	virtual Future<Reference<const ArenaPage>> getMultiPhysicalPage(PagerEventReasons reason,
	                                                                unsigned int level,
	                                                                VectorRef<LogicalPageID> pageIDs,
	                                                                int priority,
	                                                                bool cacheable,
	                                                                bool nohit) = 0;
	virtual Version getVersion() const = 0;

	virtual Key getMetaKey() const = 0;

	virtual ~IPagerSnapshot() {}

	virtual void addref() = 0;
	virtual void delref() = 0;

	ArbitraryObject extra;
};

// This API is probably too customized to the behavior of DWALPager and probably needs some changes to be more generic.
class IPager2 : public IClosable {
public:
	virtual std::string getName() const = 0;

	// Returns an ArenaPage that can be passed to writePage. The data in the returned ArenaPage might not be zeroed.
	virtual Reference<ArenaPage> newPageBuffer(size_t blocks = 1) = 0;

	// Returns the usable size of pages returned by the pager (i.e. the size of the page that isn't pager overhead).
	// For a given pager instance, separate calls to this function must return the same value.
	// Only valid to call after recovery is complete.
	virtual int getPhysicalPageSize() const = 0;
	virtual int getLogicalPageSize() const = 0;
	virtual int getPagesPerExtent() const = 0;

	// Write detail fields with pager stats to a trace event
	virtual void toTraceEvent(TraceEvent& e) const = 0;

	// Allocate a new page ID for a subsequent write.  The page will be considered in-use after the next commit
	// regardless of whether or not it was written to.
	virtual Future<LogicalPageID> newPageID() = 0;

	virtual Future<LogicalPageID> newExtentPageID(QueueID queueID) = 0;
	virtual QueueID newLastQueueID() = 0;

	// Replace the contents of a page with new data across *all* versions.
	// Existing holders of a page reference for pageID, read from any version,
	// may see the effects of this write.
	virtual void updatePage(PagerEventReasons reason,
	                        unsigned int level,
	                        Standalone<VectorRef<LogicalPageID>> pageIDs,
	                        Reference<ArenaPage> data) = 0;
	// Try to atomically update the contents of a page as of version v in the next commit.
	// If the pager is unable to do this at this time, it may choose to write the data to a new page ID
	// instead and return the new page ID to the caller.  Otherwise the original pageID argument will be returned.
	// If a new page ID is returned, the old page ID will be freed as of version v
	virtual Future<LogicalPageID> atomicUpdatePage(PagerEventReasons reason,
	                                               unsigned int level,
	                                               LogicalPageID pageID,
	                                               Reference<ArenaPage> data,
	                                               Version v) = 0;

	// Free pageID to be used again after the commit that moves oldestVersion past v
	virtual void freePage(LogicalPageID pageID, Version v) = 0;

	virtual void freeExtent(LogicalPageID pageID) = 0;

	// If id is remapped, delete the original as of version v and return the page it was remapped to.  The caller
	// is then responsible for referencing and deleting the returned page ID.
	virtual LogicalPageID detachRemappedPage(LogicalPageID id, Version v) = 0;

	// Returns the latest data (regardless of version) for a page by LogicalPageID
	// The data returned will be the later of
	//   - the most recent committed atomic
	//   - the most recent non-atomic write
	// Cacheable indicates that the page should be added to the page cache (if applicable?) as a result of this read.
	// NoHit indicates that the read should not be considered a cache hit, such as when preloading pages that are
	// considered likely to be needed soon.
	virtual Future<Reference<ArenaPage>> readPage(PagerEventReasons reason,
	                                              unsigned int level,
	                                              PhysicalPageID pageIDs,
	                                              int priority,
	                                              bool cacheable,
	                                              bool noHit) = 0;
	virtual Future<Reference<ArenaPage>> readMultiPage(PagerEventReasons reason,
	                                                   unsigned int level,
	                                                   VectorRef<PhysicalPageID> pageIDs,
	                                                   int priority,
	                                                   bool cacheable,
	                                                   bool noHit) = 0;

	virtual Future<Reference<ArenaPage>> readExtent(LogicalPageID pageID) = 0;
	virtual void releaseExtentReadLock() = 0;

	// Temporary methods for testing
	virtual Future<Standalone<VectorRef<LogicalPageID>>> getUsedExtents(QueueID queueID) = 0;
	virtual void pushExtentUsedList(QueueID queueID, LogicalPageID extID) = 0;
	virtual void extentCacheClear() = 0;
	virtual int64_t getPageCacheCount() = 0;
	virtual int64_t getExtentCacheCount() = 0;

	// Get a snapshot of the metakey and all pages as of the version v which must be >= getOldestVersion()
	// Note that snapshots at any version may still see the results of updatePage() calls.
	// The snapshot shall be usable until setOldVersion() is called with a version > v.
	virtual Reference<IPagerSnapshot> getReadSnapshot(Version v) = 0;

	// Atomically make durable all pending page writes, page frees, and update the user commit
	// record at version v
	// v must be higher than the highest committed version
	virtual Future<Void> commit(Version v, Value commitRecord) = 0;

	// Get the latest committed user commit record
	virtual Value getCommitRecord() const = 0;

	virtual StorageBytes getStorageBytes() const = 0;

	virtual int64_t getPageCount() = 0;

	// Count of pages in use by the pager client (including retained old page versions)
	virtual Future<int64_t> getUserPageCount() = 0;

	// Future returned is ready when pager has been initialized from disk and is ready for reads and writes.
	// It is invalid to call most other functions until init() is ready.
	// TODO: Document further.
	virtual Future<Void> init() = 0;

	// Returns latest committed version
	virtual Version getLastCommittedVersion() const = 0;

	// Returns the oldest readable version as of the most recent committed version
	virtual Version getOldestReadableVersion() const = 0;

	// Sets the oldest readable version to be put into affect at the next commit.
	// The pager can reuse pages that were freed at a version less than v.
	// If any snapshots are in use at a version less than v, the pager can either forcefully
	// invalidate them or keep their versions around until the snapshots are no longer in use.
	virtual void setOldestReadableVersion(Version v) = 0;

	// Advance the commit version and the oldest readble version and commit until the remap queue is empty.
	virtual Future<Void> clearRemapQueue() = 0;

	// Get a pointer to an integer representing a byte count penalty the pager should apply against usable page cache
	// memory. This is used to track significant memory usage external to the pager.  Such usages should
	// increment/decrement the value at this pointer based on their memory footprint.
	virtual int64_t* getPageCachePenaltySource() = 0;

protected:
	~IPager2() {} // Destruction should be done using close()/dispose() from the IClosable interface
};

// The null key provider is useful to simplify page decoding.
// It throws an error for any key info requested.
class NullKeyProvider : public IEncryptionKeyProvider {
public:
	virtual ~NullKeyProvider() {}
	Future<EncryptionKey> getSecrets(const EncryptionKeyRef& key) override { throw encryption_key_not_found(); }
	Future<EncryptionKey> getByRange(const KeyRef& begin, const KeyRef& end) override {
		throw encryption_key_not_found();
	}
};

// Key provider for dummy XOR encryption scheme
class XOREncryptionKeyProvider : public IEncryptionKeyProvider {
public:
	XOREncryptionKeyProvider(std::string filename) {
		ASSERT(g_network->isSimulated());

		// Choose a deterministic random filename (without path) byte for secret generation
		// Remove any leading directory names
		size_t lastSlash = filename.find_last_of("\\/");
		if (lastSlash != filename.npos) {
			filename.erase(0, lastSlash);
		}
		xorWith = filename.empty() ? 0x5e
		                           : (uint8_t)filename[XXH3_64bits(filename.data(), filename.size()) % filename.size()];
	}

	virtual ~XOREncryptionKeyProvider() {}

	virtual Future<EncryptionKey> getSecrets(const EncryptionKeyRef& key) override {
		if (!key.id.present()) {
			throw encryption_key_not_found();
		}
		EncryptionKey s = key;
		uint8_t secret = ~(uint8_t)key.id.get() ^ xorWith;
		s.secret = StringRef(s.arena(), &secret, 1);
		return s;
	}

	virtual Future<EncryptionKey> getByRange(const KeyRef& begin, const KeyRef& end) override {
		EncryptionKeyRef k;
		k.id = end.empty() ? 0 : *(end.end() - 1);
		return getSecrets(k);
	}

	uint8_t xorWith;
};

#endif
