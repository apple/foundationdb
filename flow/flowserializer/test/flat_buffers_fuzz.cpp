#include <algorithm>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <random>
#include <thread>
#include <type_traits>
#include <vector>

#include <boost/iterator/counting_iterator.hpp>
#include <boost/variant.hpp>

#include "flow/flat_buffers.h"

#include "flatbuffers/flatbuffers.h"
#include "flatbuffers/minireflect.h"

#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include "doctest.h"

// Flowbuffers serializer
#include "table.h"

namespace theirs {
#include "table_generated.h"
} // namespace theirs

namespace ours {

template <class T, class U>
std::enable_if_t<std::is_integral_v<T> || std::is_floating_point_v<T> || std::is_integral_v<U> ||
                 std::is_floating_point_v<U>>
Verify(T lhs, U rhs, std::string context);

void Randomize(std::mt19937_64& r, bool& x);
void Randomize(std::mt19937_64& r, char& x);

template <class T>
std::enable_if_t<std::is_integral_v<T> && !std::is_same_v<T, bool> && !std::is_same_v<T, char>> Randomize(
    std::mt19937_64& r,
    T& x);

template <class T>
std::enable_if_t<std::is_floating_point_v<T>> Randomize(std::mt19937_64& r, T& x);

template <class T>
std::enable_if_t<std::is_integral_v<T> || std::is_floating_point_v<T>> Randomize(std::mt19937_64& r,
                                                                                 T& x,
                                                                                 flatbuffers::FlatBufferBuilder&);

template <class Tuple, size_t... I>
void RandomizeTupleHelper(std::mt19937_64& r, Tuple& x, std::index_sequence<I...>);

template <class... T>
void Randomize(std::mt19937_64& r, std::tuple<T...>& x);

template <class T, class Vector>
void Verify(const std::vector<T>& lhs, const Vector* rhs, std::string context);

template <class Vector>
void Verify(const std::vector<bool>& lhs, const Vector* rhs, std::string context);

template <class T>
void Randomize(std::mt19937_64& r, std::vector<T>& xs);

template <class T>
void Randomize(std::mt19937_64& r,
               flatbuffers::Offset<flatbuffers::Vector<const T*>>& result,
               flatbuffers::FlatBufferBuilder& fbb);

template <class T>
std::enable_if_t<!std::is_pointer_v<T>> Randomize(std::mt19937_64& r,
                                                  flatbuffers::Offset<flatbuffers::Vector<T>>& result,
                                                  flatbuffers::FlatBufferBuilder& fbb);

template <class Union, int I, class T, class... Ts>
void RandomizeUnionHelper(std::mt19937_64& r, int i, Union& x);

template <class... T>
void Randomize(std::mt19937_64& r, std::variant<T...>& x);

#include "table_fdbflatbuffers.h"

template <class T, class U>
std::enable_if_t<std::is_integral_v<T> || std::is_floating_point_v<T> || std::is_integral_v<U> ||
                 std::is_floating_point_v<U>>
Verify(T lhs, U rhs, std::string context) {
	CHECK_MESSAGE(lhs == rhs, context);
}

void Randomize(std::mt19937_64& r, bool& x) {
	x = static_cast<bool>(std::uniform_int_distribution<int>(0, 1)(r));
}

template <class T>
std::enable_if_t<std::is_integral_v<T> && !std::is_same_v<T, bool> && !std::is_same_v<T, char>> Randomize(
    std::mt19937_64& r,
    T& x) {
	// Divide by 3 to fix ubsan complaint in uniform_int_distribution
	std::uniform_int_distribution<T> dist(std::numeric_limits<T>::min() / 3, std::numeric_limits<T>::max() / 3);
	x = dist(r);
}

void Randomize(std::mt19937_64& r, char& x) {
	int8_t i;
	Randomize(r, i);
	x = static_cast<char>(i);
}

template <class T>
std::enable_if_t<std::is_floating_point_v<T>> Randomize(std::mt19937_64& r, T& x) {
	std::uniform_real_distribution<T> dist(std::numeric_limits<T>::min(), std::numeric_limits<T>::max());
	x = dist(r);
}

template <class T>
std::enable_if_t<std::is_integral_v<T> || std::is_floating_point_v<T>> Randomize(std::mt19937_64& r,
                                                                                 T& x,
                                                                                 flatbuffers::FlatBufferBuilder&) {
	Randomize(r, x);
}

template <class Tuple, size_t... I>
void RandomizeTupleHelper(std::mt19937_64& r, Tuple& x, std::index_sequence<I...>) {
	(Randomize(r, std::get<I>(x)), ...);
}

template <class... T>
void Randomize(std::mt19937_64& r, std::tuple<T...>& x) {
	RandomizeTupleHelper(r, x, std::index_sequence_for<T...>{});
}

template <class T, class Vector>
void Verify(const std::vector<T>& lhs, const Vector* rhs, std::string context) {
	int i = 0;
	CHECK_MESSAGE(lhs.size() == rhs->size(), context);
	for (const auto& x : lhs) {
		Verify(x, (*rhs)[i], context + "[" + std::to_string(i) + "]");
		++i;
	}
}

template <class Vector>
void Verify(const std::vector<bool>& lhs, const Vector* rhs, std::string context) {
	CHECK_MESSAGE(lhs.size() == rhs->size(), context);
	for (int i = 0; i < lhs.size(); ++i) {
		bool x = lhs[i];
		bool y = (*rhs)[i];
		Verify(x, y, context + "[" + std::to_string(i) + "]");
	}
}

template <class T>
void Randomize(std::mt19937_64& r, std::vector<T>& xs) {
	int len = std::geometric_distribution<>{ 0.1 }(r);
	xs.resize(len);
	for (int i = 0; i < len; ++i) {
		T t;
		Randomize(r, t);
		xs[i] = t;
	}
}

template <class T>
void Randomize(std::mt19937_64& r,
               flatbuffers::Offset<flatbuffers::Vector<const T*>>& result,
               flatbuffers::FlatBufferBuilder& fbb) {
	int len = std::geometric_distribution<>{ 0.1 }(r);
	std::vector<T> xs(len);
	for (auto& x : xs) {
		Randomize(r, x, fbb);
	}
	result = fbb.CreateVectorOfStructs(xs);
}

template <class T>
std::enable_if_t<!std::is_pointer_v<T>> Randomize(std::mt19937_64& r,
                                                  flatbuffers::Offset<flatbuffers::Vector<T>>& result,
                                                  flatbuffers::FlatBufferBuilder& fbb) {
	int len = std::geometric_distribution<>{ 0.1 }(r);
	std::vector<T> xs(len);
	for (auto& x : xs) {
		Randomize(r, x, fbb);
	}
	result = fbb.CreateVector(xs);
}

template <class Union, int I, class T, class... Ts>
void RandomizeUnionHelper(std::mt19937_64& r, int i, Union& x) {
	if (i == I) {
		T t;
		Randomize(r, t);
		x = t;
	} else if constexpr (I > 0) {
		RandomizeUnionHelper<Union, I - 1, Ts...>(r, i, x);
	}
}

template <class... T>
void Randomize(std::mt19937_64& r, std::variant<T...>& x) {
	int discriminant = std::uniform_int_distribution<int>{ 0, sizeof...(T) }(r);
	RandomizeUnionHelper<std::variant<T...>, sizeof...(T) - 1, T...>(r, discriminant, x);
}

} // namespace ours

using namespace std;

namespace {
struct Arena {
	std::vector<std::pair<uint8_t*, size_t>> allocated;

	uint8_t* operator()(size_t sz) {
		auto res = new uint8_t[sz];
		allocated.emplace_back(res, sz);
		return res;
	}

	size_t get_size(uint8_t* ptr) {
		for (auto p : allocated) {
			if (p.first == ptr) {
				return p.second;
			}
		}
		return -1;
	}

	~Arena() {
		for (auto p : allocated) {
			delete[] p.first;
		}
	}
};

struct DummyContext {
	Arena a;
	Arena& arena() { return a; }
};

} // namespace

void print_buffer(const uint8_t* out, int len) {
	std::cout << std::hex << std::setfill('0');
	for (int i = 0; i < len; ++i) {
		if (i % 8 == 0) {
			std::cout << std::endl;
			std::cout << std::setw(4) << i << ": ";
		}
		std::cout << std::setw(2) << (int)out[i] << " ";
	}
	std::cout << std::endl << std::dec;
}

namespace {
struct TestContext {
	Arena& _arena;
	Arena& arena() { return _arena; }
	uint8_t* allocate(size_t size) { return _arena(size); }
	TestContext& context() { return *this; }
};

// Randomly generate instance of our type, serialize, verify, convert to
// their type and compare.
void testOursToTheirs(std::mt19937_64& r, int i) {
	ours::Table0 ours;
	ours::Randomize(r, ours);
	Arena arena;
	TestContext context{ arena };
	auto* serialized = detail::save(context, ours, FileIdentifier{});

	flatbuffers::Verifier verifier(serialized, arena.get_size(serialized));
	auto result = theirs::testfb::VerifyTable0Buffer(verifier);
	CHECK(result);
	ours::Verify(ours, theirs::testfb::GetTable0(serialized), "SavePath[" + std::to_string(i) + "]: ");
}

// Randomly generate instance of their type, serialize, verify, convert to
// our type and compare.
void testTheirsToOurs(std::mt19937_64& r, int i) {
	flatbuffers::FlatBufferBuilder fbb;
	flatbuffers::Offset<theirs::testfb::Table0> theirs;
	ours::Randomize(r, theirs, fbb);
	fbb.Finish(theirs);

	ours::Table0 ours;
	Arena arena;
	TestContext context{ arena };
	detail::load(ours, fbb.GetBufferPointer(), context);
	ours::Verify(ours, theirs::testfb::GetTable0(fbb.GetBufferPointer()), "LoadPath[" + std::to_string(i) + "]: ");
}

void testOurNewSerialize(std::mt19937_64& r, int i) {
	// Generate a random instance
	testfb::Table0 ours_new;
	ours::Randomize(r, ours_new);

	// Serialize using our new serializer
	flowserializer::Writer w;
	auto [bufPtr, bufSize] = ours_new.write(w);

	// Deserialize using flat buffers and verify
	flatbuffers::Verifier verifier(bufPtr, bufSize);
	auto result = theirs::testfb::VerifyTable0Buffer(verifier);
	CHECK(result);
	ours::Verify(ours_new, theirs::testfb::GetTable0(bufPtr), "NewSavePath[" + std::to_string(i) + "]: ");
}

void testOurNewDeserialize(std::mt19937_64& r, int i) {
	// Generate a random instance and serialize using
	// the old serializer
	ours::Table0 ours_old;
	ours::Randomize(r, ours_old);
	Arena arena;
	TestContext context{ arena };
	auto* serialized = detail::save(context, ours_old, FileIdentifier{});

	// Deserialize using our new serializer
	ObjectReader reader(serialized, Unversioned());
	testfb::Table0 ours_new = testfb::Table0::read(reader);

	// Convert to the old format and verify
	ours::Verify(ours_new, theirs::testfb::GetTable0(serialized), "NewLoadPath[" + std::to_string(i) + "]: ");
}

void doFuzzOld() {
	std::mt19937_64 r(ours::kSeed);

	// Fuzz
	for (int i = 0; i < 100; ++i) {
		testOursToTheirs(r, i);
		testTheirsToOurs(r, i);
	}
}

void doFuzzNew() {
	std::mt19937_64 r(ours::kSeed);

	// Fuzz
	for (int i = 0; i < 100; ++i) {
		testOurNewSerialize(r, i);
		testOurNewDeserialize(r, i);
	}
}
} // namespace

TEST_CASE("Serialize") {
	std::mt19937_64 r(ours::kSeed);
	testOurNewSerialize(r, 0);
}

TEST_CASE("Deserialize") {
	std::mt19937_64 r(ours::kSeed);
	testOurNewDeserialize(r, 0);
}

TEST_CASE("FuzzOld") {
	std::thread t1{ doFuzzOld };
	std::thread t2{ doFuzzOld };
	t1.join();
	t2.join();
}

TEST_CASE("FuzzNew") {
	std::thread t1{ doFuzzNew };
	std::thread t2{ doFuzzNew };
	t1.join();
	t2.join();
}