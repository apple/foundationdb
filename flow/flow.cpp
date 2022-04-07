/*
 * flow.cpp
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

#include "flow/flow.h"
#include "flow/DeterministicRandom.h"
#include "flow/UnitTest.h"
#include "flow/rte_memcpy.h"
#include "flow/folly_memcpy.h"
#include <stdarg.h>
#include <cinttypes>

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
	return format("%016llx%016llx", part[0], part[1]);
}

UID UID::fromString(std::string const& s) {
	ASSERT(s.size() == 32);
	uint64_t a = 0, b = 0;
	int r = sscanf(s.c_str(), "%16" SCNx64 "%16" SCNx64, &a, &b);
	ASSERT(r == 2);
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

	TEST(true); // large format result

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

namespace {

std::vector<bool> buggifyActivated{ false, false };
std::map<BuggifyType, std::map<std::pair<std::string, int>, int>> typedSBVars;

} // namespace

std::vector<double> P_BUGGIFIED_SECTION_ACTIVATED{ .25, .25 };
std::vector<double> P_BUGGIFIED_SECTION_FIRES{ .25, .25 };

double P_EXPENSIVE_VALIDATION = .05;

int getSBVar(std::string const& file, int line, BuggifyType type) {
	if (!buggifyActivated[int(type)])
		return 0;

	const auto& flPair = std::make_pair(file, line);
	auto& SBVars = typedSBVars[type];
	if (!SBVars.count(flPair)) {
		SBVars[flPair] = deterministicRandom()->random01() < P_BUGGIFIED_SECTION_ACTIVATED[int(type)];
		g_traceBatch.addBuggify(SBVars[flPair], line, file);
		if (g_network)
			g_traceBatch.dump();
	}

	return SBVars[flPair];
}

void clearBuggifySections(BuggifyType type) {
	typedSBVars[type].clear();
}

bool validationIsEnabled(BuggifyType type) {
	return buggifyActivated[int(type)];
}

bool isBuggifyEnabled(BuggifyType type) {
	return buggifyActivated[int(type)];
}

void enableBuggify(bool enabled, BuggifyType type) {
	buggifyActivated[int(type)] = enabled;
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
		StringRef in = LiteralStringRef("foobar");
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
