/*
 * RandomKeyValueUtils.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2023 Apple Inc. and the FoundationDB project authors
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
#pragma once

#include <string>
#include <set>
#include <vector>
#include <algorithm>

#include "flow/Arena.h"
#include "flow/Error.h"
#include "flow/IRandom.h"
#include "fdbclient/FDBTypes.h"

template <typename T>
struct IGenerator {
	virtual T next() = 0;
	virtual T last() const = 0;
	virtual std::string toString() const = 0;
	virtual T next(int distance, bool wrap = false) { throw unsupported_operation(); }
	virtual ~IGenerator(){};
};

struct IKeyGenerator : public IGenerator<Key> {
	virtual int getMaxKeyLen() const = 0;
};

// Random unsigned int generator which generates integers between and including first and last
// Distribution can be uniform, skewed small, or skewed large
// String Definition Format:  [^]first[..last]
//   last is optional and defaults to first
//   If ^ is present, the generated numbers skew toward first, otherwise are uniform random
//   If either first or last begins with a letter character it will be interpreted as its ASCII byte value.
struct RandomIntGenerator : IGenerator<unsigned int> {
	enum Skew { LARGE, SMALL, NONE };

	unsigned int min;
	unsigned int max;
	unsigned int val;
	bool alpha = false;
	Skew skew = NONE;

	unsigned int parseInt(StringRef s) {
		if (s.size() == 0) {
			return 0;
		} else if (std::isalpha(s[0])) {
			alpha = true;
			return (unsigned int)s[0];
		} else {
			return atol(s.toString().c_str());
		}
	}

	RandomIntGenerator(unsigned int only = 0) : min(only), max(only) {}
	RandomIntGenerator(unsigned int first, unsigned int last, bool skewTowardFirst = false) : min(first), max(last) {
		if (first != last && skewTowardFirst) {
			skew = (first < last) ? SMALL : LARGE;
		}
		if (min > max) {
			std::swap(min, max);
		}
	}
	RandomIntGenerator(const char* cstr) : RandomIntGenerator(std::string(cstr)) {}
	RandomIntGenerator(std::string str) : RandomIntGenerator(StringRef(str)) {}
	RandomIntGenerator(StringRef str) {
		bool skewTowardFirst = false;
		if (!str.empty() && str[0] == '^') {
			skewTowardFirst = true;
			str = str.substr(1);
		}

		StringRef first = str.eat("..");
		StringRef last = str;
		if (last.size() == 0) {
			last = first;
		}

		min = parseInt(first);
		max = parseInt(last);
		if (skewTowardFirst && min != max) {
			skew = (min < max) ? SMALL : LARGE;
		}
		if (min > max) {
			std::swap(min, max);
		}
	}

	// Generate and return a random number
	unsigned int next() {
		switch (skew) {
		case SMALL:
			return val = deterministicRandom()->randomSkewedUInt32(min, max + 1);
		case LARGE:
			return val = max - deterministicRandom()->randomSkewedUInt32(min, max + 1);
		case NONE:
		default:
			return val = deterministicRandom()->randomInt(min, max + 1);
		}
	}
	// Return the last random number returned by next()
	unsigned int last() const { return val; }

	std::string formatLimit(int x) const {
		return (alpha && std::isalpha(x)) ? fmt::format("{}", (char)x) : fmt::format("{}", x);
	}

	std::string toString() const {
		if (min == max) {
			return fmt::format("{}", min);
		}
		if (skew == NONE || skew == SMALL) {
			return fmt::format("{}{}..{}", (skew == NONE) ? "" : "^", formatLimit(min), formatLimit(max));
		}
		ASSERT(skew == LARGE);
		return fmt::format("^{}..{}", formatLimit(max), formatLimit(min));
	}
};

// Random string generator
// Generates random strings of a random size from a size int generator and made of random chars
// from a random char int generator
//
// String Definition Format:  sizeRange[/byteRange]
// sizeRange and byteRange are RandomIntGenerators
// The default `byteRange` is 0:255
struct RandomStringGenerator : IKeyGenerator {
	RandomStringGenerator() {}
	RandomStringGenerator(RandomIntGenerator size, RandomIntGenerator byteset) : size(size), bytes(byteset) {}
	RandomStringGenerator(const char* cstr) : RandomStringGenerator(std::string(cstr)) {}
	RandomStringGenerator(std::string str) : RandomStringGenerator(StringRef(str)) {}
	RandomStringGenerator(StringRef str) {
		StringRef sSize = str.eat("/");
		StringRef sBytes = str;
		if (sBytes.size() == 0) {
			sBytes = "0:255"_sr;
		}
		size = RandomIntGenerator(sSize.toString());
		bytes = RandomIntGenerator(sBytes);
	}

	RandomIntGenerator size;
	RandomIntGenerator bytes;
	Standalone<StringRef> val;

	Standalone<StringRef> next() override {
		val = makeString(size.next());
		for (int i = 0; i < val.size(); ++i) {
			mutateString(val)[i] = (uint8_t)bytes.next();
		}
		return val;
	}

	Standalone<StringRef> last() const override { return val; };

	std::string toString() const override { return fmt::format("{}/{}", size.toString(), bytes.toString()); }

	int getMaxKeyLen() const override { return size.max - 1; }
};

// Same construction, definition, and usage as RandomStringGenerator but sacrifices randomness
// and uniqueness for performance.
// It uses a large pre-generated string and generates random substrings from it.
struct RandomValueGenerator {
	template <typename... Args>
	RandomValueGenerator(Args&&... args) : strings(std::forward<Args>(args)...) {
		// Make a similar RandomStringGenerator to generate the noise block from
		noise = RandomStringGenerator(RandomIntGenerator(std::max<int>(2e6, strings.size.max)), strings.bytes).next();
	}

	RandomStringGenerator strings;
	Standalone<StringRef> noise;
	Value val;

	Value next() {
		int len = strings.size.next();
		val = Value(noise.substr(deterministicRandom()->randomInt(0, noise.size() - len + 1), len), noise.arena());
		return val;
	}

	Value last() const { return val; };

	std::string toString() const { return fmt::format("{}", strings.toString()); }

	int getMaxValLen() const { return strings.size.max - 1; }
};

// Base class for randomly generated key sets
// Returns a random or nearby key at some distance from a vector of keys generated at init time.
// Requires a RandomIntGenerator as the index generator for selecting which random next key to return.  The given index
// generator should have a min of 0 and if it doesn't its min will be updated to 0.
struct RandomStringSetGeneratorBase : IKeyGenerator {
	Arena arena;
	std::vector<KeyRef> keys;
	RandomIntGenerator indexGenerator;
	int iVal;
	KeyRange rangeVal;
	int maxKeyLen;

	template <typename KeyGen>
	void init(RandomIntGenerator originalIndexGenerator, KeyGen& keyGen) {
		indexGenerator = originalIndexGenerator;
		indexGenerator.min = 0;
		maxKeyLen = keyGen.getMaxKeyLen();
		ASSERT(indexGenerator.max > 0);
		std::set<Key> uniqueKeys;
		int inserts = 0;
		// for smaller indexGenerator.max, give it more insert try, as it may not find enough unique keys with 3 * max.
		// It adds roughly log * 100. For example, even for max is 1, it will try at least 100 times.
		const uint maxInsertTry = 3 * indexGenerator.max + (((sizeof(uint) * 8) - clz(indexGenerator.max)) * 100);
		while (uniqueKeys.size() < indexGenerator.max) {
			auto k = keyGen.next();
			uniqueKeys.insert(k);
			if (++inserts > maxInsertTry) {
				// StringGenerator cardinality is too low, unable to find enough unique keys.
				ASSERT(false);
			}
		}
		// Adjust indexGenerator max down by 1 because indices are 0-based.
		--indexGenerator.max;

		for (auto& k : uniqueKeys) {
			keys.push_back(KeyRef(arena, k));
		}
		iVal = 0;
	}

	Key last() const override { return Key(keys[iVal], arena); };
	KeyRange lastRange() const { return rangeVal; }

	Key next() override {
		iVal = indexGenerator.next();
		return last();
	}

	// Next sequential with some jump distance and optional wrap-around which is false
	Key next(int distance, bool wrap = false) override {
		iVal += distance;
		if (wrap) {
			iVal %= keys.size();
		} else {
			iVal = std::clamp<int>(iVal, 0, keys.size() - 1);
		}

		return last();
	}

	KeyRange nextRange(int width) {
		int begin = indexGenerator.next();
		int end = (begin + width) % keys.size();
		if (begin > end) {
			std::swap(begin, end);
		}
		rangeVal = KeyRange(KeyRangeRef(keys[begin], keys[end]), arena);
		return rangeVal;
	}

	KeyRange nextRange() { return nextRange(deterministicRandom()->randomSkewedUInt32(0, keys.size())); }

	int getMaxKeyLen() const override { return maxKeyLen; }
};

template <typename StringGenT>
struct RandomStringSetGenerator : public RandomStringSetGeneratorBase {
	RandomStringSetGenerator(RandomIntGenerator indexGen, StringGenT stringGen)
	  : indexGen(indexGen), stringGen(stringGen) {
		init(indexGen, stringGen);
	}
	RandomStringSetGenerator(const char* cstr) : RandomStringSetGenerator(std::string(cstr)) {}
	RandomStringSetGenerator(std::string str) : RandomStringSetGenerator(StringRef(str)) {}
	RandomStringSetGenerator(StringRef str) {
		indexGen = str.eat("::");
		stringGen = str;
		init(indexGen, stringGen);
	}

	RandomIntGenerator indexGen;
	StringGenT stringGen;

	std::string toString() const { return fmt::format("{}::{}", indexGen.toString(), stringGen.toString()); }
};

typedef RandomStringSetGenerator<RandomStringGenerator> RandomKeySetGenerator;

// Generate random keys which are composed of tuple segments from a list of RandomKeySets
// String Definition Format: RandomKeySet[,RandomKeySet]...
struct RandomKeyTupleGenerator : public IKeyGenerator {
	RandomKeyTupleGenerator(){};
	RandomKeyTupleGenerator(std::vector<RandomKeySetGenerator> tupleParts) : tuples(tupleParts) {}
	RandomKeyTupleGenerator(std::string s) : RandomKeyTupleGenerator(StringRef(s)) {}
	RandomKeyTupleGenerator(StringRef s) {
		while (!s.empty()) {
			tuples.push_back(s.eat(","));
		}
	}

	std::vector<RandomKeySetGenerator> tuples;
	Key val;

	Key next() override {
		int totalBytes = 0;
		for (auto& t : tuples) {
			totalBytes += t.next().size();
		}
		val = makeString(totalBytes);
		totalBytes = 0;

		for (auto& t : tuples) {
			memcpy(mutateString(val) + totalBytes, t.last().begin(), t.last().size());
			totalBytes += t.last().size();
		}
		return val;
	}

	Key last() const override { return val; };

	std::string toString() const override {
		std::string s;
		for (auto const& t : tuples) {
			if (!s.empty()) {
				s += ',';
			}
			s += t.toString();
		}
		return s;
	}

	int getMaxKeyLen() const override {
		int maxKeyLen = 0;
		for (const auto& g : tuples) {
			maxKeyLen += g.getMaxKeyLen();
		}
		return maxKeyLen;
	}
};

typedef RandomStringSetGenerator<RandomKeyTupleGenerator> RandomKeyTupleSetGenerator;

// RandomKeyGenerator is a helper function to contain multiple KeyGenerators
// TODO: ideally, if all RandomGenerators support IGenerator interface and can be nested with each other, there's no
//  need to have this class. But that also requires more complicated string syntax to build the generators.
struct RandomKeyGenerator : IKeyGenerator {
	std::vector<std::unique_ptr<IKeyGenerator>> keyGenerators;
	Key val;

	void addKeyGenerator(std::unique_ptr<IKeyGenerator> gen) { keyGenerators.emplace_back(std::move(gen)); }

	Key next() override {
		int totalBytes = 0;
		for (auto& g : keyGenerators) {
			totalBytes += g->next().size();
		}
		val = makeString(totalBytes);
		totalBytes = 0;
		for (const auto& g : keyGenerators) {
			memcpy(mutateString(val) + totalBytes, g->last().begin(), g->last().size());
			totalBytes += g->last().size();
		}
		return val;
	}

	Key last() const override { return val; };

	// TODO: the string is only for output, RandomKeyGenerator currently doesn't support constructing from a string.
	std::string toString() const override {
		std::string s;
		for (auto const& g : keyGenerators) {
			s += "[" + g->toString() + "]";
		}
		return s;
	}

	int getMaxKeyLen() const override {
		int maxKeyLen = 0;
		for (const auto& g : keyGenerators) {
			maxKeyLen += g->getMaxKeyLen();
		}
		return maxKeyLen;
	}
};

struct RandomMutationGenerator {
	RandomKeyTupleSetGenerator keys;
	RandomValueGenerator valueGen;
};
