/*
 * flow.cpp
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

#include "flow/flow.h"

#include <stdarg.h>

#include <cinttypes>

#include <openssl/err.h>
#include <openssl/rand.h>

#include <fmt/format.h>

#include "flow/DeterministicRandom.h"
#include "flow/Error.h"
#include "flow/Hostname.h"
#include "flow/rte_memcpy.h"
#include "flow/UnitTest.h"

#ifdef WITH_FOLLY_MEMCPY
#include "folly_memcpy.h"
#endif

std::atomic<bool> startSampling = false;
LineageReference rootLineage;
thread_local LineageReference* currentLineage = &rootLineage;

LineagePropertiesBase::~LineagePropertiesBase() {}

#ifdef ENABLE_SAMPLING
ActorLineage::ActorLineage() : properties(), parent(*currentLineage) {}
#else
ActorLineage::ActorLineage() : properties() {}
#endif

ActorLineage::~ActorLineage() {
	for (auto property : properties) {
		delete property.properties;
	}
}

#ifdef ENABLE_SAMPLING
LineageReference getCurrentLineage() {
	if (!currentLineage->isValid() || !currentLineage->isAllocated()) {
		currentLineage->allocate();
	}
	return *currentLineage;
}

void sample(LineageReference* lineagePtr);

void replaceLineage(LineageReference* lineage) {
	if (!startSampling) {
		currentLineage = lineage;
	} else {
		startSampling = false;
		sample(currentLineage);
		currentLineage = lineage;
	}
}
#endif

using namespace std::literals;

const std::string_view StackLineage::name = "StackLineage"sv;

#if (defined(__linux__) || defined(__FreeBSD__)) && defined(__AVX__) && !defined(MEMORY_SANITIZER) && !DEBUG_DETERMINISM
// For benchmarking; need a version of rte_memcpy that doesn't live in the same compilation unit as the test.
void* rte_memcpy_noinline(void* __restrict __dest, const void* __restrict __src, size_t __n) {
	return rte_memcpy(__dest, __src, __n);
}

// This compilation unit will be linked in to the main binary, so this should override glibc memcpy
__attribute__((visibility("default"))) void* memcpy(void* __restrict __dest, const void* __restrict __src, size_t __n) {
	// folly_memcpy is faster for small copies, but rte seems to win out in most other circumstances
	return rte_memcpy(__dest, __src, __n);
}
#else
void* rte_memcpy_noinline(void* __restrict __dest, const void* __restrict __src, size_t __n) {
	return memcpy(__dest, __src, __n);
}
#endif // (defined (__linux__) || defined (__FreeBSD__)) && defined(__AVX__) && !defined(MEMORY_SANITIZER)

INetwork* g_network = nullptr;

FILE* randLog = nullptr;
thread_local Reference<IRandom> seededRandom;
Reference<IRandom> seededDebugRandom;
uint64_t debug_lastLoadBalanceResultEndpointToken = 0;
bool noUnseed = false;

void setThreadLocalDeterministicRandomSeed(uint32_t seed) {
	seededRandom = Reference<IRandom>(new DeterministicRandom(seed, true));
	seededDebugRandom = Reference<IRandom>(new DeterministicRandom(seed));
}

Reference<IRandom> debugRandom() {
	return seededDebugRandom;
}

Reference<IRandom> deterministicRandom() {
	if (!seededRandom) {
		seededRandom = Reference<IRandom>(new DeterministicRandom(platform::getRandomSeed(), true));
	}
	return seededRandom;
}

Reference<IRandom> nondeterministicRandom() {
	static thread_local Reference<IRandom> random;
	if (!random) {
		random = Reference<IRandom>(new DeterministicRandom(platform::getRandomSeed()));
	}
	return random;
}

std::string UID::toString() const {
	return fmt::format("{:016x}{:016x}", part[0], part[1]);
}

UID UID::fromString(std::string const& s) {
	ASSERT(s.size() == 32);
	uint64_t a = 0, b = 0;
	int r = sscanf(s.c_str(), "%16" SCNx64 "%16" SCNx64, &a, &b);
	ASSERT(r == 2);
	return UID(a, b);
}

UID UID::fromStringThrowsOnFailure(std::string const& s) {
	if (s.size() != 32) {
		// invalid string size
		throw operation_failed();
	}
	uint64_t a = 0, b = 0;
	int r = sscanf(s.c_str(), "%16" SCNx64 "%16" SCNx64, &a, &b);
	if (r != 2) {
		throw operation_failed();
	}
	return UID(a, b);
}

std::string UID::shortString() const {
	return format("%016llx", part[0]);
}

void detectFailureAfter(int const& address, double const& delay);

Optional<uint64_t> parse_with_suffix(std::string const& toparse, std::string const& default_unit) {
	char* endptr;

	uint64_t ret = strtoull(toparse.c_str(), &endptr, 10);

	if (endptr == toparse.c_str()) {
		return Optional<uint64_t>();
	}

	std::string unit;

	if (*endptr == '\0') {
		if (!default_unit.empty()) {
			unit = default_unit;
		} else {
			return Optional<uint64_t>();
		}
	} else {
		unit = endptr;
	}

	if (!unit.compare("B")) {
		// Nothing to do
	} else if (!unit.compare("KB")) {
		ret *= int64_t(1e3);
	} else if (!unit.compare("KiB")) {
		ret *= 1LL << 10;
	} else if (!unit.compare("MB")) {
		ret *= int64_t(1e6);
	} else if (!unit.compare("MiB")) {
		ret *= 1LL << 20;
	} else if (!unit.compare("GB")) {
		ret *= int64_t(1e9);
	} else if (!unit.compare("GiB")) {
		ret *= 1LL << 30;
	} else if (!unit.compare("TB")) {
		ret *= int64_t(1e12);
	} else if (!unit.compare("TiB")) {
		ret *= 1LL << 40;
	} else {
		return Optional<uint64_t>();
	}

	return ret;
}

// Parses a duration with one of the following suffixes and returns the duration in seconds
// s - seconds
// m - minutes
// h - hours
// d - days
Optional<uint64_t> parseDuration(std::string const& str, std::string const& defaultUnit) {
	char* endptr;
	uint64_t ret = strtoull(str.c_str(), &endptr, 10);

	if (endptr == str.c_str()) {
		return Optional<uint64_t>();
	}

	std::string unit;
	if (*endptr == '\0') {
		if (!defaultUnit.empty()) {
			unit = defaultUnit;
		} else {
			return Optional<uint64_t>();
		}
	} else {
		unit = endptr;
	}

	if (!unit.compare("s")) {
		// Nothing to do
	} else if (!unit.compare("m")) {
		ret *= 60;
	} else if (!unit.compare("h")) {
		ret *= 60 * 60;
	} else if (!unit.compare("d")) {
		ret *= 24 * 60 * 60;
	} else {
		return Optional<uint64_t>();
	}

	return ret;
}

int vsformat(std::string& outputString, const char* form, va_list args) {
	char buf[200];

	va_list args2;
	va_copy(args2, args);
	int size = vsnprintf(buf, sizeof(buf), form, args2);
	va_end(args2);

	if (size >= 0 && size < sizeof(buf)) {
		outputString = std::string(buf, size);
		return size;
	}

#ifdef _WIN32
	// Microsoft's non-standard vsnprintf doesn't return a correct size, but just an error, so determine the necessary
	// size
	va_copy(args2, args);
	size = _vscprintf(form, args2);
	va_end(args2);
#endif

	if (size < 0) {
		return -1;
	}

	CODE_PROBE(true, "large format result");

	outputString.resize(size + 1);
	size = vsnprintf(&outputString[0], outputString.size(), form, args);
	if (size < 0 || size >= outputString.size()) {
		return -1;
	}

	outputString.resize(size);
	return size;
}

std::string format(const char* form, ...) {
	va_list args;
	va_start(args, form);

	std::string str;
	int result = vsformat(str, form, args);
	va_end(args);

	ASSERT(result >= 0);
	return str;
}

Standalone<StringRef> strinc(StringRef const& str) {
	int index;
	for (index = str.size() - 1; index >= 0; index--)
		if (str[index] != 255)
			break;

	// Must not be called with a string that consists only of zero or more '\xff' bytes.
	ASSERT(index >= 0);

	Standalone<StringRef> r = str.substr(0, index + 1);
	uint8_t* p = mutateString(r);
	p[r.size() - 1]++;
	return r;
}

StringRef strinc(StringRef const& str, Arena& arena) {
	int index;
	for (index = str.size() - 1; index >= 0; index--)
		if (str[index] != 255)
			break;

	// Must not be called with a string that consists only of zero or more '\xff' bytes.
	ASSERT(index >= 0);

	StringRef r(arena, str.substr(0, index + 1));
	uint8_t* p = mutateString(r);
	p[r.size() - 1]++;
	return r;
}

StringRef addVersionStampAtEnd(StringRef const& str, Arena& arena) {
	int32_t size = str.size();
	uint8_t* s = new (arena) uint8_t[size + 14];
	memcpy(s, str.begin(), size);
	memset(&s[size], 0, 10);
	memcpy(&s[size + 10], &size, 4);
	return StringRef(s, size + 14);
}

Standalone<StringRef> addVersionStampAtEnd(StringRef const& str) {
	Standalone<StringRef> r;
	((StringRef&)r) = addVersionStampAtEnd(str, r.arena());
	return r;
}

// Make OpenSSL use DeterministicRandom as RNG source such that simulation runs stay deterministic w/ e.g. signature ops
void bindDeterministicRandomToOpenssl() {
	// TODO: implement ifdef branch for 3.x using provider API
#ifndef OPENSSL_IS_BORINGSSL
	static const RAND_METHOD method = {
		// replacement for RAND_seed(), which reseeds OpenSSL RNG
		[](const void*, int) -> int { return 1; },
		// replacement for RAND_bytes(), which fills given buffer with random byte sequence
		[](unsigned char* buf, int length) -> int {
		    if (g_network)
			    ASSERT_ABORT(g_network->isSimulated());
		    deterministicRandom()->randomBytes(buf, length);
		    return 1;
		},
		// replacement for RAND_cleanup(), a no-op for simulation
		[]() -> void {},
		// replacement for RAND_add(), which reseeds OpenSSL RNG with randomness hint
		[](const void*, int, double) -> int { return 1; },
		// replacement for default pseudobytes getter (same as RAND_bytes by default)
		[](unsigned char* buf, int length) -> int {
		    if (g_network)
			    ASSERT_ABORT(g_network->isSimulated());
		    deterministicRandom()->randomBytes(buf, length);
		    return 1;
		},
		// status function for PRNG readiness check
		[]() -> int { return 1; },
	};

	if (1 != ::RAND_set_rand_method(&method)) {
		auto ec = ::ERR_get_error();
		char msg[256]{
			0,
		};
		if (ec) {
			::ERR_error_string_n(ec, msg, sizeof(msg));
		}
		fprintf(stderr,
		        "ERROR: Failed to bind DeterministicRandom to OpenSSL RNG\n"
		        "       OpenSSL error message: '%s'\n",
		        msg);
		throw internal_error();
	} else {
		printf("DeterministicRandom successfully bound to OpenSSL RNG\n");
	}
#else // OPENSSL_IS_BORINGSSL
	static const RAND_METHOD method = {
		[](const void*, int) -> void {},
		[](unsigned char* buf, unsigned long length) -> int {
		    if (g_network)
			    ASSERT_ABORT(g_network->isSimulated());
		    ASSERT(length <= std::numeric_limits<int>::max());
		    deterministicRandom()->randomBytes(buf, length);
		    return 1;
		},
		[]() -> void {},
		[](const void*, int, double) -> void {},
		[](unsigned char* buf, unsigned long length) -> int {
		    if (g_network)
			    ASSERT_ABORT(g_network->isSimulated());
		    ASSERT(length <= std::numeric_limits<int>::max());
		    deterministicRandom()->randomBytes(buf, length);
		    return 1;
		},
		[]() -> int { return 1; },
	};
	::RAND_set_rand_method(&method);
	printf("DeterministicRandom successfully bound to OpenSSL RNG\n");
#endif // OPENSSL_IS_BORINGSSL
}

int nChooseK(int n, int k) {
	assert(n >= k && k >= 0);
	if (k == 0) {
		return 1;
	}
	if (k > n / 2) {
		return nChooseK(n, n - k);
	}

	long ret = 1;

	// To avoid integer overflow, we do n/1 * (n-1)/2 * (n-2)/3 * (n-i+1)/i, where i = k
	for (int i = 1; i <= k; ++i) {
		ret *= n - i + 1;
		ret /= i;
	}
	ASSERT(ret <= INT_MAX);

	return ret;
}

namespace {
// Simple message for flatbuffers unittests
struct Int {
	constexpr static FileIdentifier file_identifier = 12345;
	uint32_t value;
	Int() = default;
	Int(uint32_t value) : value(value) {}
	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, value);
	}
};
} // namespace

TEST_CASE("/flow/FlatBuffers/ErrorOr") {
	{
		ErrorOr<Int> in(worker_removed());
		ErrorOr<Int> out;
		ObjectWriter writer(Unversioned());
		writer.serialize(in);
		Standalone<StringRef> copy = writer.toStringRef();
		ArenaObjectReader reader(copy.arena(), copy, Unversioned());
		reader.deserialize(out);
		ASSERT(out.isError());
		ASSERT(out.getError().code() == in.getError().code());
	}
	{
		ErrorOr<Int> in(deterministicRandom()->randomUInt32());
		ErrorOr<Int> out;
		ObjectWriter writer(Unversioned());
		writer.serialize(in);
		Standalone<StringRef> copy = writer.toStringRef();
		ArenaObjectReader reader(copy.arena(), copy, Unversioned());
		reader.deserialize(out);
		ASSERT(!out.isError());
		ASSERT(out.get().value == in.get().value);
	}
	return Void();
}

TEST_CASE("/flow/FlatBuffers/Optional") {
	{
		Optional<Int> in;
		Optional<Int> out;
		ObjectWriter writer(Unversioned());
		writer.serialize(in);
		Standalone<StringRef> copy = writer.toStringRef();
		ArenaObjectReader reader(copy.arena(), copy, Unversioned());
		reader.deserialize(out);
		ASSERT(!out.present());
	}
	{
		Optional<Int> in(deterministicRandom()->randomUInt32());
		Optional<Int> out;
		ObjectWriter writer(Unversioned());
		writer.serialize(in);
		Standalone<StringRef> copy = writer.toStringRef();
		ArenaObjectReader reader(copy.arena(), copy, Unversioned());
		reader.deserialize(out);
		ASSERT(out.present());
		ASSERT(out.get().value == in.get().value);
	}
	return Void();
}

TEST_CASE("/flow/FlatBuffers/Standalone") {
	{
		Standalone<StringRef> in(std::string("foobar"));
		StringRef out;
		ObjectWriter writer(Unversioned());
		writer.serialize(in);
		Standalone<StringRef> copy = writer.toStringRef();
		ArenaObjectReader reader(copy.arena(), copy, Unversioned());
		reader.deserialize(out);
		ASSERT(in == out);
	}
	{
		StringRef in = "foobar"_sr;
		Standalone<StringRef> out;
		ObjectWriter writer(Unversioned());
		writer.serialize(in);
		Standalone<StringRef> copy = writer.toStringRef();
		ArenaObjectReader reader(copy.arena(), copy, Unversioned());
		reader.deserialize(out);
		ASSERT(in == out);
	}
	return Void();
}

// we need to make sure at least one test of each prefix exists, otherwise
// the noSim test fails if we compile without RocksDB
TEST_CASE("noSim/noopTest") {
	return Void();
}

struct TestErrorOrMapClass {
	StringRef value;
	ErrorOr<StringRef> errorOrValue;

	const StringRef constValue;
	const ErrorOr<StringRef> constErrorOrValue;

	StringRef getValue() const { return value; }
	ErrorOr<StringRef> getErrorOrValue() const { return errorOrValue; }

	StringRef const& getValueRef() const { return value; }
	ErrorOr<StringRef> const& getErrorOrValueRef() const { return errorOrValue; }

	StringRef sub(int x) const { return value.substr(x); }
	ErrorOr<StringRef> errorOrSub(int x) const { return errorOrValue.map<StringRef>(&StringRef::substr, (int)x); }

	TestErrorOrMapClass(StringRef value, Optional<Error> optionalValueError)
	  : value(value), constValue(value),
	    errorOrValue(!optionalValueError.present() ? ErrorOr<StringRef>(value)
	                                               : ErrorOr<StringRef>(optionalValueError.get())),
	    constErrorOrValue(!optionalValueError.present() ? ErrorOr<StringRef>(value)
	                                                    : ErrorOr<StringRef>(optionalValueError.get())) {}
};

struct TestErrorOrMapClassRef : public TestErrorOrMapClass, public ReferenceCounted<TestErrorOrMapClassRef> {
	TestErrorOrMapClassRef(StringRef value, Optional<Error> optionalValueError)
	  : TestErrorOrMapClass(value, optionalValueError) {}
};

void checkResults(std::vector<ErrorOr<StringRef>> const& results,
                  StringRef value,
                  Optional<Error> expectedError,
                  std::string context) {
	if (expectedError.present()) {
		for (int i = 0; i < results.size(); ++i) {
			if (results[i].present() || results[i].getError().code() != expectedError.get().code()) {
				fmt::print("Unexpected result at index {} in {}\n", i, context);
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
void checkErrorOr(ErrorOr<T> val) {
	StringRef value;
	bool isError = !val.present();
	bool isFlatMapError = isError;
	Optional<Error> expectedError;
	std::vector<ErrorOr<StringRef>> mapResults;
	std::vector<ErrorOr<StringRef>> flatMapResults;

	if (isError) {
		expectedError = val.getError();
	}

	if constexpr (IsRef) {
		if (!isError && !val.get()) {
			isError = isFlatMapError = true;
			expectedError = default_error_or();
		}
		if (!isError) {
			value = val.get()->value;
			isFlatMapError = !val.get()->errorOrValue.present();
			if (isFlatMapError) {
				expectedError = val.get()->errorOrValue.getError();
			}
		}
		mapResults.push_back(val.mapRef(&TestErrorOrMapClass::value));
		mapResults.push_back(val.mapRef(&TestErrorOrMapClass::constValue));
		mapResults.push_back(val.mapRef(&TestErrorOrMapClass::getValue));
		mapResults.push_back(val.mapRef(&TestErrorOrMapClass::getValueRef));
		mapResults.push_back(val.mapRef(&TestErrorOrMapClass::sub, 5));

		flatMapResults.push_back(val.flatMap([](auto t) { return t ? t->errorOrValue : ErrorOr<StringRef>(); }));
		flatMapResults.push_back(val.flatMapRef(&TestErrorOrMapClass::errorOrValue));
		flatMapResults.push_back(val.flatMapRef(&TestErrorOrMapClass::constErrorOrValue));
		flatMapResults.push_back(val.flatMapRef(&TestErrorOrMapClass::getErrorOrValue));
		flatMapResults.push_back(val.flatMapRef(&TestErrorOrMapClass::getErrorOrValueRef));
		flatMapResults.push_back(val.flatMapRef(&TestErrorOrMapClass::errorOrSub, 5));
	} else {
		if (!isError) {
			value = val.get().value;
			isFlatMapError = !val.get().errorOrValue.present();
			if (isFlatMapError) {
				expectedError = val.get().errorOrValue.getError();
			}
		}
		mapResults.push_back(val.map([](auto t) { return t.value; }));
		mapResults.push_back(val.map(&TestErrorOrMapClass::value));
		mapResults.push_back(val.map(&TestErrorOrMapClass::constValue));
		mapResults.push_back(val.map(&TestErrorOrMapClass::getValue));
		mapResults.push_back(val.map(&TestErrorOrMapClass::getValueRef));
		mapResults.push_back(val.map(&TestErrorOrMapClass::sub, 5));

		flatMapResults.push_back(val.flatMap([](auto t) { return t.errorOrValue; }));
		flatMapResults.push_back(val.flatMap(&TestErrorOrMapClass::errorOrValue));
		flatMapResults.push_back(val.flatMap(&TestErrorOrMapClass::constErrorOrValue));
		flatMapResults.push_back(val.flatMap(&TestErrorOrMapClass::getErrorOrValue));
		flatMapResults.push_back(val.flatMap(&TestErrorOrMapClass::getErrorOrValueRef));
		flatMapResults.push_back(val.flatMap(&TestErrorOrMapClass::errorOrSub, 5));
	}

	checkResults(mapResults, value, isError ? expectedError : Optional<Error>(), IsRef ? "ref map" : "non-ref map");
	checkResults(flatMapResults,
	             value,
	             isFlatMapError ? expectedError : Optional<Error>(),
	             IsRef ? "ref flat map" : "non-ref flat map");
}

TEST_CASE("/flow/ErrorOr/Map") {
	// ErrorOr<T>
	checkErrorOr<false>(ErrorOr<TestErrorOrMapClass>());
	checkErrorOr<false>(ErrorOr<TestErrorOrMapClass>(transaction_too_old()));
	checkErrorOr<false>(ErrorOr<TestErrorOrMapClass>(TestErrorOrMapClass("test_string"_sr, {})));
	checkErrorOr<false>(ErrorOr<TestErrorOrMapClass>(TestErrorOrMapClass("test_string"_sr, default_error_or())));
	checkErrorOr<false>(ErrorOr<TestErrorOrMapClass>(TestErrorOrMapClass("test_string"_sr, transaction_too_old())));

	// ErrorOr<Reference<T>>
	checkErrorOr<true>(ErrorOr<Reference<TestErrorOrMapClassRef>>());
	checkErrorOr<true>(ErrorOr<Reference<TestErrorOrMapClassRef>>(Reference<TestErrorOrMapClassRef>()));
	checkErrorOr<true>(ErrorOr<Reference<TestErrorOrMapClassRef>>(transaction_too_old()));
	checkErrorOr<true>(ErrorOr<Reference<TestErrorOrMapClassRef>>(
	    makeReference<TestErrorOrMapClassRef>("test_string"_sr, Optional<Error>())));
	checkErrorOr<true>(ErrorOr<Reference<TestErrorOrMapClassRef>>(
	    makeReference<TestErrorOrMapClassRef>("test_string"_sr, Optional<Error>(default_error_or()))));
	checkErrorOr<true>(ErrorOr<Reference<TestErrorOrMapClassRef>>(
	    makeReference<TestErrorOrMapClassRef>("test_string"_sr, Optional<Error>(transaction_too_old()))));

	// ErrorOr<T*>
	checkErrorOr<true>(ErrorOr<TestErrorOrMapClass*>());
	checkErrorOr<true>(ErrorOr<TestErrorOrMapClass*>(nullptr));
	checkErrorOr<true>(ErrorOr<TestErrorOrMapClassRef*>(transaction_too_old()));

	auto ptr = new TestErrorOrMapClass("test_string"_sr, Optional<Error>());
	checkErrorOr<true>(ErrorOr<TestErrorOrMapClass*>(ptr));
	delete ptr;

	ptr = new TestErrorOrMapClass("test_string"_sr, default_error_or());
	checkErrorOr<true>(ErrorOr<TestErrorOrMapClass*>(ptr));
	delete ptr;

	ptr = new TestErrorOrMapClass("test_string"_sr, transaction_too_old());
	checkErrorOr<true>(ErrorOr<TestErrorOrMapClass*>(ptr));
	delete ptr;

	return Void();
}
