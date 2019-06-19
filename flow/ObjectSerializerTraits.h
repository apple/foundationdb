/*
 * ObjectSerializerTraits.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

#include <type_traits>
#include <cstdint>
#include <cstddef>
#include <memory>
#include <functional>
#include <vector>
#include <variant>

template <class T, typename = void>
struct is_fb_function_t : std::false_type {};

template<class T>
struct is_fb_function_t<T, typename std::enable_if<T::is_fb_visitor>::type> : std::true_type {};

template <class T>
constexpr bool is_fb_function = is_fb_function_t<T>::value;

template <class Visitor, class... Items>
typename std::enable_if<is_fb_function<Visitor>, void>::type serializer(Visitor& visitor, Items&... items) {
	visitor(items...);
}


template <class... Ts>
struct pack {};

template <int i, class... Ts>
struct index_impl;

template <int i, class T, class... Ts>
struct index_impl<i, pack<T, Ts...>> {
	using type = typename index_impl<i - 1, pack<Ts...>>::type;
};

template <class T, class... Ts>
struct index_impl<0, pack<T, Ts...>> {
	using type = T;
};

template <int i, class Pack>
using index_t = typename index_impl<i, Pack>::type;

struct Block {
	const uint8_t* data;
	size_t size;
};

template <class T>
Block unownedPtr(T* t, size_t s) {
	return Block{ t, s };
}

template <class T, typename = void>
struct scalar_traits : std::false_type {
	constexpr static size_t size = 0;
	static void save(uint8_t*, const T&);

	// Context is an arbitrary type that is plumbed by reference throughout the
	// load call tree.
	template <class Context>
	static void load(const uint8_t*, T&, Context&);
};


template <class T>
struct dynamic_size_traits : std::false_type {
	static Block save(const T&);
	static void serialization_done(const T&); // Optional. Called after the last call to save.

	// Context is an arbitrary type that is plumbed by reference throughout the
	// load call tree.
	template <class Context>
	static void load(const uint8_t*, size_t, T&, Context&);
};

template <class T>
struct serializable_traits : std::false_type {
	template <class Archiver>
	static void serialize(Archiver& ar, T& v);
};

template <class VectorLike>
struct vector_like_traits : std::false_type {
	// Write this at the beginning of the buffer
	using value_type = uint8_t;
	using iterator = void;
	using insert_iterator = void;

	static size_t num_entries(VectorLike&);
	template <class Context>
	static void reserve(VectorLike&, size_t, Context&);

	static insert_iterator insert(VectorLike&);
	static iterator begin(const VectorLike&);
};

template <class UnionLike>
struct union_like_traits : std::false_type {
	using Member = UnionLike;
	using alternatives = pack<>;
	static uint8_t index(const Member&);
	static bool empty(const Member& variant);

	template <int i>
	static const index_t<i, alternatives>& get(const Member&);

	template <int i, class Alternative>
	static const void assign(Member&, const Alternative&);

	template <class Context>
	static void done(Member&, Context&);
};

// TODO(anoyes): Implement things that are currently using scalar traits with
// struct-like traits.
template <class StructLike>
struct struct_like_traits : std::false_type {
	using Member = StructLike;
	using types = pack<>;

	template <int i>
	static const index_t<i, types>& get(const Member&);

	template <int i>
	static const void assign(Member&, const index_t<i, types>&);

	template <class Context>
	static void done(Member&, Context&);
};

template <class... Alternatives>
struct union_like_traits<std::variant<Alternatives...>> : std::true_type {
	using Member = std::variant<Alternatives...>;
	using alternatives = pack<Alternatives...>;
	static uint8_t index(const Member& variant) { return variant.index(); }
	static bool empty(const Member& variant) { return false; }

	template <int i>
	static const index_t<i, alternatives>& get(const Member& variant) {
		return std::get<index_t<i, alternatives>>(variant);
	}

	template <size_t i, class Alternative>
	static const void assign(Member& member, const Alternative& a) {
		static_assert(std::is_same_v<index_t<i, alternatives>, Alternative>);
		member = a;
	}
};
