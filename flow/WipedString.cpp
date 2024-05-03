#include "flow/FastAlloc.h"
#include "flow/ScopeExit.h"
#include "flow/UnitTest.h"
#include "flow/WipedString.h"
#include "flow/ObjectSerializer.h"
#include "flow/Net2Packet.h"
#include "flow/serialize.h"

static_assert(detail::is_wipe_enabled<ObjectWriter::SaveContext>);

TEST_CASE("/flow/WipedString/basic") {
	auto& rng = *deterministicRandom();
	for (auto iter = 0; iter < 100; iter++) {
		auto randomString = rng.randomAlphaNumeric(rng.randomInt(1, 1000));
		// keeps arena-allocated memory from being really freed, for test purposes
		auto kaScope = keepalive_allocator::ActiveScope();
		const uint8_t* begin = nullptr;
		int size = 0;
		{
			StringRef rs(randomString);
			WipedString ws(rs);
			begin = ws.contents().begin();
			size = ws.contents().size();
		}
		for (auto i = 0; i < size; i++) {
			ASSERT_EQ(begin[i], 0);
		}
	}
	return Void();
}

namespace unit_tests {

void fillRandom(int& i) {
	i = deterministicRandom()->randomInt(0, 10000);
}

void fillRandom(int64_t& i) {
	i = deterministicRandom()->randomInt64(0, 1e15);
}

void fillRandom(double& d) {
	auto i64 = deterministicRandom()->randomInt64(0, 1e15);
	static_assert(sizeof(i64) == sizeof(double));
	d = *reinterpret_cast<double*>(&i64);
}

void fillRandom(VectorRef<int>& vi, Arena& arena, int minLen = 1, int maxLen = 100) {
	vi.resize(arena, deterministicRandom()->randomInt(minLen, maxLen + 1));
	for (auto i = 0; i < vi.size(); i++)
		fillRandom(vi[i]);
}

void fillRandom(StringRef& s, Arena& arena, int minLen = 1, int maxLen = 100) {
	const auto len = deterministicRandom()->randomInt(minLen, maxLen + 1);
	s = StringRef(arena, deterministicRandom()->randomAlphaNumeric(len));
}

void fillRandom(VectorRef<StringRef>& vs,
                Arena& arena,
                int minLen = 1,
                int maxLen = 100,
                int minElementLen = 1,
                int maxElementLen = 100) {
	vs.resize(arena, deterministicRandom()->randomInt(minLen, maxLen + 1));
	for (auto i = 0; i < vs.size(); i++)
		fillRandom(vs[i], arena, minElementLen, maxElementLen);
}

void fillRandom(WipedString& ws, Arena& arena, int minLen = 1, int maxLen = 1000) {
	StringRef s;
	fillRandom(s, arena, minLen, maxLen);
	ws = WipedString(s);
}

void fillRandom(Optional<WipedString>& ows, Arena& arena, int minLen = 1, int maxLen = 1000) {
	WipedString ws;
	fillRandom(ws, arena, minLen, maxLen);
	ows = ws;
}

void fillRandom(std::vector<WipedString>& vws,
                Arena& arena,
                int minLen = 1,
                int maxLen = 100,
                int minElementLen = 10,
                int maxElementLen = 1000) {
	auto vs = VectorRef<StringRef>();
	fillRandom(vs, arena, minLen, maxLen);
	for (auto s : vs) {
		vws.push_back(WipedString(s));
	}
}

// WipedString embedded in the middle of struct, with variable-length objects
struct WS_A {
	constexpr static FileIdentifier file_identifier = 1557618;
	Arena a;
	int64_t i;
	double d;
	VectorRef<int> vi;
	WipedString ws;
	StringRef s;

	static WS_A makeRandom() {
		WS_A o;
		fillRandom(o.i);
		fillRandom(o.d);
		fillRandom(o.vi, o.a, 0, 1000);
		fillRandom(o.ws, o.a, 100, 1000);
		fillRandom(o.s, o.a, 0, 1000);
		return o;
	}

	static WS_A makeMax() {
		WS_A o;
		fillRandom(o.i);
		fillRandom(o.d);
		fillRandom(o.vi, o.a, 9000, 10000);
		fillRandom(o.ws, o.a, 9000, 10000);
		fillRandom(o.s, o.a, 9000, 10000);
		return o;
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, i, d, vi, ws, s, a);
	}
};

// optional WipedString
struct WS_B {
	constexpr static FileIdentifier file_identifier = 7621124;
	Arena a;
	Optional<WipedString> ows;
	int64_t i;
	StringRef s;
	VectorRef<int> vi;
	VectorRef<StringRef> vs;

	static WS_B makeRandom() {
		WS_B o;
		fillRandom(o.ows, o.a, 100, 10000);
		fillRandom(o.i);
		fillRandom(o.s, o.a, 0, 10000);
		fillRandom(o.vi, o.a, 0, 10000);
		fillRandom(o.vs, o.a, 1, 10, 100, 1000);
		return o;
	}

	static WS_B makeMax() {
		WS_B o;
		fillRandom(o.ows, o.a, 9000, 10000);
		fillRandom(o.i);
		fillRandom(o.s, o.a, 9000, 10000);
		fillRandom(o.vi, o.a, 9000, 10000);
		fillRandom(o.vs, o.a, 50, 200, 100, 1000);
		return o;
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, ows, i, s, vi, vs, a);
	}
};

// vector of WipedStrings
struct WS_C {
	constexpr static FileIdentifier file_identifier = 151112;
	Arena a;
	int64_t i;
	StringRef s;
	VectorRef<int> vi;
	std::vector<WipedString> vws;
	VectorRef<StringRef> vs;

	static WS_C makeRandom() {
		WS_C o;
		fillRandom(o.i);
		fillRandom(o.s, o.a, 100, 1000);
		fillRandom(o.vi, o.a, 100, 1000);
		fillRandom(o.vws, o.a, 10, 100, 100, 1000);
		fillRandom(o.vs, o.a, 10, 100, 100, 200);
		return o;
	}

	static WS_C makeMax() {
		WS_C o;
		fillRandom(o.i);
		fillRandom(o.s, o.a, 9000, 10000);
		fillRandom(o.vi, o.a, 1000, 10000);
		fillRandom(o.vws, o.a, 10, 20, 1000, 10000);
		fillRandom(o.vs, o.a, 10, 100, 100, 200);
		return o;
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, i, s, vi, vws, vs, a);
	}
};

template <class GenerateObjectFunc>
void testWipeAfterPacketSerialize(GenerateObjectFunc&& fn) {
	// Note that kaScope is not created before object creation.
	// This is because ArenaBlock wiping is not a target of this test function
	auto obj = fn();
	{
		// Emulate network packet writing with keepalive allocator active
		auto pq = UnsentPacketQueue();
		auto kaScope = keepalive_allocator::ActiveScope();
		auto pb = pq.getWriteBuffer();
		auto pw = PacketWriter(pb, nullptr /* ReliablePacket* */, AssumeVersion(g_network->protocolVersion()));
		// Below call serializes the object, marking sensitive areas for wiping in the process,
		// because the serialized objects all have WipedString in it.
		SerializeSource<decltype(obj)>(obj).serializePacketWriter(pw);
		pq.setWriteBuffer(pw.finish());
		auto const& wipedSet = keepalive_allocator::getWipedAreaSet();
		ASSERT_GT(wipedSet.size(), 0);
		for (auto [begin, size] : wipedSet) {
			for (auto i = 0; i < size; i++) {
				// This weakly verifies that memory is filled with serialized string.
				ASSERT(std::isalnum(begin[i]));
			}
		}

		// This should normally deallocate the packet buffers after wiping the sensitive region.
		// With keepalive_allocator active, however, the memory is kept alive for post-free inspection.
		pq.discardAll();
		for (auto [begin, size] : wipedSet) {
			for (auto i = 0; i < size; i++)
				ASSERT_EQ(begin[i], 0);
		}
	}
}

} // namespace unit_tests

TEST_CASE("/flow/WipedString/serialize/modest") {
	for (auto i = 0; i < 100; i++) {
		unit_tests::testWipeAfterPacketSerialize([]() { return unit_tests::WS_A::makeRandom(); });
		unit_tests::testWipeAfterPacketSerialize([]() { return unit_tests::WS_B::makeRandom(); });
		unit_tests::testWipeAfterPacketSerialize([]() { return unit_tests::WS_C::makeRandom(); });
	}
	return Void();
}

TEST_CASE("/flow/WipedString/serialize/maximal") {
	// Test with larger test objects fewer times to test wiping memory with larger allocation/serialization context
	for (auto i = 0; i < 10; i++) {
		unit_tests::testWipeAfterPacketSerialize([]() { return unit_tests::WS_A::makeMax(); });
		unit_tests::testWipeAfterPacketSerialize([]() { return unit_tests::WS_B::makeMax(); });
		unit_tests::testWipeAfterPacketSerialize([]() { return unit_tests::WS_C::makeMax(); });
	}
	return Void();
}

void forceLinkWipedStringTests() {}
