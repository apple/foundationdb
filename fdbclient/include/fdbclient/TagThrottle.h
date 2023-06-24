/**
 * TagThrottle.h
 */

#pragma once

#include "fmt/format.h"
#include "flow/Arena.h"
#include "flow/Error.h"
#include "flow/flow.h"
#include "flow/network.h"
#include "flow/ThreadHelper.actor.h"
#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/CommitTransaction.h"
#include "fdbclient/Tuple.h"

typedef StringRef TransactionTagRef;
typedef Standalone<TransactionTagRef> TransactionTag;

FDB_BOOLEAN_PARAM(Capitalize);

class TagSet {
public:
	typedef std::vector<TransactionTagRef>::const_iterator const_iterator;

	TagSet() : bytes(0) {}

	void addTag(TransactionTagRef tag);
	size_t size() const;

	const_iterator begin() const { return tags.begin(); }

	const_iterator end() const { return tags.end(); }

	void clear() {
		tags.clear();
		bytes = 0;
	}

	template <class Context>
	void save(uint8_t* out, Context&) const {
		uint8_t* start = out;
		for (const auto& tag : *this) {
			*(out++) = (uint8_t)tag.size();

			std::copy(tag.begin(), tag.end(), out);
			out += tag.size();
		}

		ASSERT((size_t)(out - start) == size() + bytes);
	}

	template <class Context>
	void load(const uint8_t* data, size_t size, Context& context) {
		// const uint8_t *start = data;
		const uint8_t* end = data + size;
		while (data < end) {
			uint8_t len = *(data++);
			// Tags are already deduplicated
			const auto& tag = tags.emplace_back(context.tryReadZeroCopy(data, len), len);
			data += len;
			bytes += tag.size();
		}

		ASSERT(data == end);

		// Deserialized tag sets share the arena with the request that contained them
		// For this reason, persisting a TagSet that shares memory with other request
		// members should be done with caution.
		arena = context.arena();
	}

	size_t getBytes() const { return bytes; }

	const Arena& getArena() const { return arena; }

	// Used by fdbcli commands
	std::string toString(Capitalize = Capitalize::False) const;

private:
	size_t bytes;
	Arena arena;
	// Currently there are never >= 256 tags, so
	// std::vector is faster than std::set. This may
	// change if we allow more tags in the future.
	std::vector<TransactionTagRef> tags;
};

template <>
struct dynamic_size_traits<TagSet> : std::true_type {
	// May be called multiple times during one serialization
	template <class Context>
	static size_t size(const TagSet& t, Context&) {
		return t.size() + t.getBytes();
	}

	// Guaranteed to be called only once during serialization
	template <class Context>
	static void save(uint8_t* out, const TagSet& t, Context& c) {
		t.save(out, c);
	}

	// Context is an arbitrary type that is plumbed by reference throughout the
	// load call tree.
	template <class Context>
	static void load(const uint8_t* data, size_t size, TagSet& t, Context& context) {
		t.load(data, size, context);
	}
};

struct ClientTrCommitCostEstimation {
	int opsCount = 0;
	uint64_t writeCosts = 0;
	std::deque<std::pair<int, uint64_t>> clearIdxCosts;
	uint32_t expensiveCostEstCount = 0;
	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, opsCount, writeCosts, clearIdxCosts, expensiveCostEstCount);
	}
};

namespace ThrottleApi {

class ThroughputQuotaValue {
public:
	int64_t reservedQuota{ 0 };
	int64_t totalQuota{ 0 };
	bool isValid() const;
	Tuple pack() const;
	static ThroughputQuotaValue unpack(Tuple const& val);
	bool operator==(ThroughputQuotaValue const&) const;
};

Key getTagQuotaKey(TransactionTagRef);

template <class Tr>
void setTagQuota(Reference<Tr> tr, TransactionTagRef tag, int64_t reservedQuota, int64_t totalQuota) {
	ThroughputQuotaValue tagQuotaValue;
	tagQuotaValue.reservedQuota = reservedQuota;
	tagQuotaValue.totalQuota = totalQuota;
	if (!tagQuotaValue.isValid()) {
		throw invalid_throttle_quota_value();
	}
	tr->set(getTagQuotaKey(tag), tagQuotaValue.pack().pack());
}

}; // namespace ThrottleApi

template <class Value>
using TransactionTagMap = std::unordered_map<TransactionTag, Value, std::hash<TransactionTagRef>>;

template <class Value>
using UIDTransactionTagMap = std::unordered_map<UID, TransactionTagMap<Value>>;
