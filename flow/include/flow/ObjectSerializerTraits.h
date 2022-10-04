/*
 * ObjectSerializerTraits.h
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

template <class T>
struct is_fb_function_t<T, typename std::enable_if<T::is_fb_visitor>::type> : std::true_type {};

template <class T>
constexpr bool is_fb_function = is_fb_function_t<T>::value;

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

template <class T, typename = void>
struct fb_must_appear_last_t : std::false_type {};

template <class T>
struct fb_must_appear_last_t<T, typename std::enable_if<T::fb_must_appear_last>::type>
  : std::conditional_t<T::fb_must_appear_last, std::true_type, std::false_type> {};

template <class T>
constexpr bool fb_must_appear_last = fb_must_appear_last_t<T>::value;

template <class Item, class... Items>
constexpr bool fb_appears_last_property_helper(pack<Item, Items...>) {
	if constexpr (sizeof...(Items) == 0) {
		return true;
	} else {
		return !fb_must_appear_last<Item> && fb_appears_last_property_helper(pack<Items...>{});
	}
}
template <class... Items>
constexpr bool fb_appears_last_property(pack<Items...>) {
	if constexpr (sizeof...(Items) == 0) {
		return true;
	} else {
		return fb_appears_last_property_helper(pack<Items...>{});
	}
}

template <class Visitor, class... Items>
typename std::enable_if<is_fb_function<Visitor>, void>::type serializer(Visitor& visitor, Items&... items) {
	static_assert(fb_appears_last_property(pack<Items...>{}),
	              "An argument to a serializer call that must appear last (Arena?) does not appear last");
	visitor(items...);
}

template <class T, typename = void>
struct scalar_traits : std::false_type {
	constexpr static size_t size = 0;
	template <class Context>
	static void save(uint8_t*, const T&, Context&);

	// Context is an arbitrary type that is plumbed by reference throughout the
	// load call tree.
	template <class Context>
	static void load(const uint8_t*, T&, Context&);
};

template <class T>
struct dynamic_size_traits : std::false_type {
	// May be called multiple times during one serialization. Guaranteed not to be called after save.
	template <class Context>
	static size_t size(const T&, Context&);

	// Guaranteed to be called only once during serialization
	template <class Context>
	static void save(uint8_t*, const T&, Context&);

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

template <class T>
struct serialize_raw : std::false_type {
	template <class Context>
	static uint8_t* save_raw(Context& context, const T& obj);
};

template <class VectorLike>
struct vector_like_traits : std::false_type {
	// Write this at the beginning of the buffer
	using value_type = uint8_t;
	using iterator = void;
	using insert_iterator = void;

	// The number of entries in this vector
	template <class Context>
	static size_t num_entries(VectorLike&, Context&);

	// Return an insert_iterator starting with an empty vector. |size| is the
	// number of elements to be inserted. Implementations may want to allocate
	// enough memory up front to hold |size| elements.
	template <class Context>
	static insert_iterator insert(VectorLike&, size_t size, Context&);

	// Return an iterator to read from this vector.
	template <class Context>
	static iterator begin(const VectorLike&, Context&);
};

template <class UnionLike>
struct union_like_traits : std::false_type {
	using Member = UnionLike;
	using alternatives = pack<>;
	template <class Context>
	static uint8_t index(const Member&, Context&);
	template <class Context>
	static bool empty(const Member& variant, Context&);

	template <int i, class Context>
	static const index_t<i, alternatives>& get(const Member&, Context&);

	template <int i, class Alternative, class Context>
	static void assign(Member&, const Alternative&, Context&);

	template <class Context>
	static void done(Member&, Context&);
};

// TODO(anoyes): Implement things that are currently using scalar traits with
// struct-like traits.
template <class StructLike>
struct struct_like_traits : std::false_type {
	using Member = StructLike;
	using types = pack<>;

	template <int i, class Context>
	static const index_t<i, types>& get(const Member&, Context&);

	template <int i, class Context>
	static void assign(Member&, const index_t<i, types>&, Context&);

	template <class Context>
	static void done(Member&, Context&);
};

template <class... Alternatives>
struct union_like_traits<std::variant<Alternatives...>> : std::true_type {
	using Member = std::variant<Alternatives...>;
	using alternatives = pack<Alternatives...>;
	template <class Context>
	static uint8_t index(const Member& variant, Context&) {
		return variant.index();
	}
	template <class Context>
	static bool empty(const Member& variant, Context&) {
		return false;
	}

	template <int i, class Context>
	static const index_t<i, alternatives>& get(const Member& variant, Context&) {
		return std::get<index_t<i, alternatives>>(variant);
	}

	template <size_t i, class Alternative, class Context>
	static void assign(Member& member, const Alternative& a, Context&) {
		static_assert(std::is_same_v<index_t<i, alternatives>, Alternative>);
		member = a;
	}
};
