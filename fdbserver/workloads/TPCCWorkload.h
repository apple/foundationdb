/*
 * TPCCWorkload.h
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
#ifndef FDBSERVER_TPCCWORKLOAD_H
#define FDBSERVER_TPCCWORKLOAD_H
#pragma once
#include "flow/Arena.h"
#include "fdbclient/FDBTypes.h"
#include <boost/preprocessor.hpp>
#include <iomanip>

namespace TPCCWorkload {

// Schema
#define EXPAND(...) __VA_ARGS__
#define EMPTY()
#define DEFER(x) x EMPTY()
// An indirection macro to avoid direct recursion
#define BOOST_PP_SEQ_FOR_EACH_ID() BOOST_PP_SEQ_FOR_EACH generators
#define ROW_CONCAT(prefix, name) prefix##name
#define ROW_TO_STRING(str) #str
#define ROW_ELEMENT_NAME(prefix, element) ROW_CONCAT(prefix, element)
#define ROW_MEMBER(r, data, elem)                                                                                      \
	BOOST_PP_TUPLE_ELEM(0, elem)                                                                                       \
	ROW_ELEMENT_NAME(data, BOOST_PP_TUPLE_ELEM(1, elem));
#define ROW_MEMBERS_SEQ(prefix, seq) BOOST_PP_SEQ_FOR_EACH(ROW_MEMBER, prefix, seq)
#define ROW_MEMBERS(prefix, tuple) ROW_MEMBERS_SEQ(prefix, BOOST_PP_TUPLE_TO_SEQ(tuple))

#define ROW_SERIALIZE_ELEMENT(r, data, elem) , ROW_ELEMENT_NAME(data, BOOST_PP_TUPLE_ELEM(1, elem))
#define ROW_SERIALIZE_ELEMENTS(prefix, seq) BOOST_PP_SEQ_FOR_EACH(ROW_SERIALIZE_ELEMENT, prefix, seq)
#define ROW_SERIALIZE(prefix, tuple) ar ROW_SERIALIZE_ELEMENTS(prefix, BOOST_PP_TUPLE_TO_SEQ(tuple))

#define ROW_KEY_STEP(r, data, elem) , ROW_ELEMENT_NAME(data, elem)
#define ROW_KEY_LIST_SEQ_EXP(prefix, seq) BOOST_PP_SEQ_FOR_EACH(ROW_KEY_STEP, prefix, seq)
#define ROW_KEY_LIST_SEQ(prefix, seq) ROW_KEY_LIST_SEQ_EXP(prefix, seq)
#define ROW_KEY_LIST(prefix, a) ROW_KEY_LIST_SEQ(prefix, BOOST_PP_ARRAY_TO_SEQ(a))
#define ROW_KEY_LIST_TUPLE(prefix, tuple) ROW_KEY_LIST_SEQ(prefix, BOOST_PP_TUPLE_TO_SEQ(tuple))

#define ROW_KEY_HAS_KEY(Name, prefix, primary_key)                                                                     \
	static constexpr bool HAS_KEY = true;                                                                              \
	StringRef key() {                                                                                                  \
		auto s = generateKey(#Name, KEY_SIZE ROW_KEY_LIST(prefix, primary_key));                                       \
		return StringRef(arena, s);                                                                                    \
	}                                                                                                                  \
	KeyRangeRef keyRange(int dontInclude) {                                                                            \
		auto s = generateKey(#Name, KEY_SIZE - dontInclude ROW_KEY_LIST(prefix, primary_key));                         \
		KeyRef begin = StringRef(arena, reinterpret_cast<const uint8_t*>(s.c_str()), s.size() + 1);                    \
		KeyRef end = StringRef(arena, reinterpret_cast<const uint8_t*>(s.c_str()), s.size() + 1);                      \
		auto sBegin = mutateString(begin);                                                                             \
		sBegin[s.size()] = uint8_t('/');                                                                               \
		auto sEnd = mutateString(end);                                                                                 \
		sEnd[s.size()] = uint8_t('0');                                                                                 \
		return KeyRangeRef(begin, end);                                                                                \
	}
#define ROW_KEY_NO_KEY static constexpr bool HAS_KEY = false;
#define ROW_KEY_IMPL(Name, prefix, primary_key, sz)                                                                    \
	BOOST_PP_IF(sz, ROW_KEY_HAS_KEY(Name, prefix, primary_key), ROW_KEY_NO_KEY)
#define ROW_KEY(Name, prefix, primary_key) ROW_KEY_IMPL(Name, prefix, primary_key, BOOST_PP_ARRAY_SIZE(primary_key))

#define ROW_INDEX_NAME_KEY(name) ROW_CONCAT(name, Key)
#define ROW_INDEX_NAME_IMPL2(name) ROW_TO_STRING(name)
#define ROW_INDEX_NAME_IMPL(indexName, name) ROW_INDEX_NAME_IMPL2(ROW_CONCAT(indexName, name))
#define ROW_INDEX_NAME(nameTuple, index)                                                                               \
	ROW_INDEX_NAME_IMPL(BOOST_PP_TUPLE_ELEM(0, index), BOOST_PP_TUPLE_ELEM(0, nameTuple))
#define ROW_GENERATE_INDEX(r, data, index)                                                                             \
	StringRef ROW_INDEX_NAME_KEY(BOOST_PP_TUPLE_ELEM(0, index))(int dontInclude = 0) {                                 \
		auto s = generateKey(ROW_INDEX_NAME(data, index),                                                              \
		                     BOOST_PP_TUPLE_SIZE(index) - dontInclude -                                                \
		                         1 ROW_KEY_LIST_TUPLE(BOOST_PP_TUPLE_ELEM(1, data), BOOST_PP_TUPLE_POP_FRONT(index))); \
		return StringRef(arena, s);                                                                                    \
	}
#define ROW_GENERATE_INDEXES_LIST(Name, prefix, indexes)                                                               \
	BOOST_PP_LIST_FOR_EACH(ROW_GENERATE_INDEX, (Name, prefix), indexes)
#define ROW_GENERATE_INDEXES(Name, prefix, indexes)                                                                    \
	ROW_GENERATE_INDEXES_LIST(Name, prefix, BOOST_PP_ARRAY_TO_LIST(indexes))
#define ROW_INDEXES(Name, prefix, indexes)                                                                             \
	BOOST_PP_IF(BOOST_PP_ARRAY_SIZE(indexes), ROW_GENERATE_INDEXES(Name, prefix, indexes), BOOST_PP_EMPTY())

#define ROW(Name, prefix, tuple, primary_key, indexes)                                                                 \
	struct Name {                                                                                                      \
		constexpr static FileIdentifier file_identifier = __COUNTER__;                                                 \
		Arena arena;                                                                                                   \
		ROW_MEMBERS(prefix, tuple)                                                                                     \
		template <class Ar>                                                                                            \
		void serialize(Ar& ar) {                                                                                       \
			serializer(ROW_SERIALIZE(prefix, tuple));                                                                  \
		}                                                                                                              \
		static constexpr int KEY_SIZE = BOOST_PP_ARRAY_SIZE(primary_key);                                              \
		ROW_KEY(Name, prefix, primary_key)                                                                             \
		ROW_INDEXES(Name, prefix, indexes)                                                                             \
	}

template <class Value>
struct KeyStreamer {
	void operator()(std::stringstream& ss, const Value& v) { ss << v; }
};

template <>
struct KeyStreamer<StringRef> {
	void operator()(std::stringstream& ss, const StringRef& v) { ss << v.toString(); }
};

template <>
struct KeyStreamer<int> {
	void operator()(std::stringstream& ss, const int v) { ss << std::setfill('0') << std::setw(6) << v; }
};

template <>
struct KeyStreamer<short> {
	void operator()(std::stringstream& ss, const int v) { ss << std::setfill('0') << std::setw(6) << v; }
};

template <class... Values>
struct KeyGenerator;

template <class Head, class... Tail>
struct KeyGenerator<Head, Tail...> {
	static void generate(std::stringstream& ss, int max, Head h, Tail... tail) {
		KeyStreamer<Head> streamer;
		if (max > 0) {
			ss << '/';
			streamer(ss, h);
			KeyGenerator<Tail...>::generate(ss, max - 1, tail...);
		}
	}
};

template <>
struct KeyGenerator<> {
	static void generate(std::stringstream&, int) {}
};

template <class... Values>
std::string generateKey(const std::string& table, int max, Values... values) {
	std::stringstream ss;
	ss << table;
	if (max > 0) {
		KeyGenerator<Values...>::generate(ss, max, values...);
	}
	return ss.str();
}

ROW(Warehouse,
    w_,
    ((int, id),
     (StringRef, name),
     (StringRef, street_1),
     (StringRef, street_2),
     (StringRef, city),
     (StringRef, state),
     (StringRef, zip),
     (double, tax),
     (double, ytd)),
    (1, (id)),
    (0, ()));

ROW(District,
    d_,
    ((int, id),
     (int, w_id),
     (StringRef, name),
     (StringRef, street_1),
     (StringRef, street_2),
     (StringRef, city),
     (StringRef, state),
     (StringRef, zip),
     (double, tax),
     (double, ytd),
     (int, next_o_id)),
    (2, (w_id, id)),
    (0, ()));

ROW(Customer,
    c_,
    ((int, id),
     (int, d_id),
     (int, w_id),
     (StringRef, first),
     (StringRef, last),
     (StringRef, middle),
     (StringRef, street_1),
     (StringRef, street_2),
     (StringRef, city),
     (StringRef, state),
     (StringRef, zip),
     (StringRef, phone),
     (double, since),
     (StringRef, credit),
     (double, credit_lim),
     (double, discount),
     (double, balance),
     (double, ytd_payment),
     (unsigned, payment_cnt),
     (unsigned, delivery_count),
     (StringRef, data)),
    (3, (w_id, d_id, id)),
    (1, ((indexLast, w_id, d_id, last, id))));

ROW(History,
    h_,
    ((int, c_id),
     (int, c_d_id),
     (int, c_w_id),
     (int, d_id),
     (int, w_id),
     (double, date),
     (double, amount),
     (StringRef, data)),
    (0, ()),
    (0, ()));

ROW(NewOrder, no_, ((int, o_id), (int, d_id), (int, w_id)), (3, (w_id, d_id, o_id)), (0, ()));

ROW(Order,
    o_,
    ((int, id),
     (int, d_id),
     (int, w_id),
     (int, c_id),
     (double, entry_d),
     (Optional<short>, carrier_id),
     (short, ol_cnt),
     (bool, all_local)),
    (3, (w_id, d_id, id)),
    (0, ()));

ROW(OrderLine,
    ol_,
    ((int, o_id),
     (int, d_id),
     (int, w_id),
     (short, number),
     (int, i_id),
     (int, supply_w_id),
     (Optional<double>, delivery_d),
     (short, quantity),
     (double, amount),
     (StringRef, dist_info)),
    (4, (w_id, d_id, o_id, number)),
    (0, ()));

ROW(Item, i_, ((int, id), (int, im_id), (StringRef, name), (double, price), (StringRef, data)), (1, (id)), (0, ()));

ROW(Stock,
    s_,
    ((int, i_id),
     (int, w_id),
     (short, quantity),
     (StringRef, dist_01),
     (StringRef, dist_02),
     (StringRef, dist_03),
     (StringRef, dist_04),
     (StringRef, dist_05),
     (StringRef, dist_06),
     (StringRef, dist_07),
     (StringRef, dist_08),
     (StringRef, dist_09),
     (StringRef, dist_10),
     (int, ytd),
     (short, order_cnt),
     (short, remote_cnt),
     (StringRef, data)),
    (2, (w_id, i_id)),
    (0, ()));

#undef FLOW_ACOMPILER_STATE
#define FLOW_ACOMPILER_STATE 1

struct GlobalState {
	constexpr static FileIdentifier file_identifier = 1064821;
	int CLoad, CRun, CDelta, CId, COlIID;

	GlobalState() {
		CLoad = deterministicRandom()->randomInt(0, 256);
		while (true) {
			CDelta = deterministicRandom()->randomInt(65, 120);
			if (!(CDelta == 96 || CDelta == 112)) {
				break;
			}
		}

		if (CDelta > CLoad) {
			CRun = CLoad + CDelta;
		} else {
			CRun = deterministicRandom()->coinflip() ? CLoad + CDelta : CLoad - CDelta;
		}
		CId = deterministicRandom()->randomInt(1, 3001);
		COlIID = deterministicRandom()->randomInt(1, 100001);
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, CLoad, CRun, CDelta, CId, COlIID);
	}

	StringRef key() const { return LiteralStringRef("GlobalState"); }
};

const std::vector<std::string> syllables = {
	"BAR", "UGHT", "ABLE", "RI", "PRES", "SE", "ANTI", "ALLY", "ATION", "ING",
};

} // namespace TPCCWorkload

#endif
