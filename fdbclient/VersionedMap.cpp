#include "fdbclient/VersionedMap.h"
#include "flow/TreeBenchmark.h"
#include "flow/UnitTest.h"

template <typename K>
struct VersionedMapHarness {
	using map = VersionedMap<K, int>;
	using key_type = K;

	struct result {
		typename map::iterator it;

		result(typename map::iterator it) : it(it) {}

		result& operator++() {
			++it;
			return *this;
		}

		const K& operator*() const { return it.key(); }

		const K& operator->() const { return it.key(); }

		bool operator==(result const& k) const { return it == k.it; }
		bool operator!=(result const& k) const { return !(*this == k); }
	};

	map s;

	void insert(K const& k) { s.insert(k, 1); }
	result find(K const& k) const { return result(s.atLatest().find(k)); }
	result not_found() const { return result(s.atLatest().end()); }
	result begin() const { return result(s.atLatest().begin()); }
	result end() const { return result(s.atLatest().end()); }
	result lower_bound(K const& k) const { return result(s.atLatest().lower_bound(k)); }
	result upper_bound(K const& k) const { return result(s.atLatest().upper_bound(k)); }
	void erase(K const& k) { s.erase(k); }
};

TEST_CASE("performance/map/int/VersionedMap") {
    VersionedMapHarness<int> tree;

	treeBenchmark(tree, *randomInt);

	return Void();
}

TEST_CASE("performance/map/StringRef/VersionedMap") {
	Arena arena;
    VersionedMapHarness<StringRef> tree;
    
	treeBenchmark(tree, [&arena]() { return randomStr(arena); });

	return Void();
}

void forceLinkVersionedMapTests() {}
