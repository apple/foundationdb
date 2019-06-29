/*
 * flat_buffers.h
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

#include <algorithm>
#include <cstring>
#include <functional>
#include <map>
#include <memory>
#include <set>
#include <stdint.h>
#include <string>
#include <tuple>
#include <type_traits>
#include <vector>
#include <cstring>
#include <array>
#include <typeinfo>
#include <typeindex>
#include "flow/FileIdentifier.h"
#include "flow/ObjectSerializerTraits.h"

template <class T = pack<>, class...>
struct concat {
	using type = T;
};
template <class... T1, class... T2, class... Ts>
struct concat<pack<T1...>, pack<T2...>, Ts...> : concat<pack<T1..., T2...>, Ts...> {};
template <class... Ts>
using concat_t = typename concat<Ts...>::type;

template <class... Ts>
constexpr auto pack_size(pack<Ts...>) {
	return sizeof...(Ts);
}

constexpr int RightAlign(int offset, int alignment) {
	return offset % alignment == 0 ? offset : ((offset / alignment) + 1) * alignment;
}

template <class T>
struct object_construction {
	T obj;

	object_construction() : obj() {}
	object_construction(const T& o) : obj(o) {}
	object_construction(T&& o) : obj(std::move(o)) {}

	T& get() { return obj; }
	const T& get() const { return obj; }
	T move() { return std::move(obj); }
};

template <class... Ts>
struct struct_like_traits<std::tuple<Ts...>> : std::true_type {
	using Member = std::tuple<Ts...>;
	using types = pack<Ts...>;

	template <int i>
	static const index_t<i, types>& get(const Member& m) {
		return std::get<i>(m);
	}

	template <int i, class Type>
	static const void assign(Member& m, const Type& t) {
		std::get<i>(m) = t;
	}
};

template <class T>
struct scalar_traits<
    T, std::enable_if_t<std::is_integral<T>::value || std::is_floating_point<T>::value || std::is_enum<T>::value>>
  : std::true_type {
	constexpr static size_t size = sizeof(T);
	static void save(uint8_t* out, const T& t) { memcpy(out, &t, size); }
	template <class Context>
	static void load(const uint8_t* in, T& t, Context&) {
		memcpy(&t, in, size);
	}
};

template <class F, class S>
struct serializable_traits<std::pair<F, S>> : std::true_type {
	template <class Archiver>
	static void serialize(Archiver& ar, std::pair<F, S>& p) {
		serializer(ar, p.first, p.second);
	}
};

template <class T, class Alloc>
struct vector_like_traits<std::vector<T, Alloc>> : std::true_type {
	using Vec = std::vector<T, Alloc>;
	using value_type = typename Vec::value_type;
	using iterator = typename Vec::const_iterator;
	using insert_iterator = std::back_insert_iterator<Vec>;

	static size_t num_entries(const Vec& v) { return v.size(); }
	template <class Context>
	static void reserve(Vec& v, size_t size, Context&) {
		v.clear();
		v.reserve(size);
	}

	static insert_iterator insert(Vec& v) { return std::back_inserter(v); }
	static iterator begin(const Vec& v) { return v.begin(); }
};

template<class T, size_t N>
struct vector_like_traits<std::array<T, N>> : std::true_type {
	using Vec = std::array<T, N>;
	using value_type = typename Vec::value_type;
	using iterator = typename Vec::const_iterator;
	using insert_iterator = typename Vec::iterator;

	static size_t num_entries(const Vec& v) { return N; }
	template <class Context>
	static void reserve(Vec& v, size_t size, Context&) {
	}
	static insert_iterator insert(Vec& v) { return v.begin(); }
	static iterator begin(const Vec& v) { return v.begin(); }
};

template <class Key, class T, class Compare, class Allocator>
struct vector_like_traits<std::map<Key, T, Compare, Allocator>> : std::true_type {
	using Vec = std::map<Key, T, Compare, Allocator>;
	using value_type = std::pair<Key, T>;
	using iterator = typename Vec::const_iterator;
	using insert_iterator = std::insert_iterator<Vec>;

	static size_t num_entries(const Vec& v) { return v.size(); }
	template <class Context>
	static void reserve(Vec& v, size_t size, Context&) {}

	static insert_iterator insert(Vec& v) { return std::inserter(v, v.end()); }
	static iterator begin(const Vec& v) { return v.begin(); }
};

template<class Key, class Compare, class Allocator>
struct vector_like_traits<std::set<Key, Compare, Allocator>> : std::true_type {
	using Vec = std::set<Key, Compare, Allocator>;
	using value_type = Key;
	using iterator = typename Vec::const_iterator;
	using insert_iterator = std::insert_iterator<Vec>;

	static size_t num_entries(const Vec& v) { return v.size(); }
	template <class Context>
	static void reserve(Vec&, size_t, Context&) {}

	static insert_iterator insert(Vec& v) { return std::inserter(v, v.end()); }
	static iterator begin(const Vec& v) { return v.begin(); }
};

template <>
struct dynamic_size_traits<std::string> : std::true_type {
private:
	using T = std::string;

public:
	static WriteRawMemory save(const T& t) {
		return { { unownedPtr(reinterpret_cast<const uint8_t*>(t.data())), t.size() } };
	};

	// Context is an arbitrary type that is plumbed by reference throughout the
	// load call tree.
	template <class Context>
	static void load(const uint8_t* p, size_t n, T& t, Context&) {
		t.assign(reinterpret_cast<const char*>(p), n);
	}
};

namespace detail {

template <class T>
T interpret_as(const uint8_t* current) {
	T t;
	memcpy(&t, current, sizeof(t));
	return t;
}

// Used to select an overload for |MessageWriter::write| that fixes relative
// offsets.
struct RelativeOffset {
	int value;
};
static_assert(sizeof(RelativeOffset) == 4, "");

template <class T>
constexpr bool is_scalar = scalar_traits<T>::value;

template <class T>
constexpr bool is_dynamic_size = dynamic_size_traits<T>::value;

template <class T>
constexpr bool is_union_like = union_like_traits<T>::value;

template <class T>
constexpr bool is_vector_like = vector_like_traits<T>::value;

template <class T>
constexpr bool is_vector_of_union_like = is_vector_like<T>&& is_union_like<typename vector_like_traits<T>::value_type>;

template <class T>
constexpr bool is_struct_like = struct_like_traits<T>::value;

template <class T>
constexpr bool expect_serialize_member =
    !is_scalar<T> && !is_vector_like<T> && !is_union_like<T> && !is_dynamic_size<T> && !is_struct_like<T>;

template <class T>
constexpr bool use_indirection = !(is_scalar<T> || is_struct_like<T>);

using VTable = std::vector<uint16_t>;

template <class T>
struct sfinae_true : std::true_type {};

template <class T>
auto test_deserialization_done(int) -> sfinae_true<decltype(T::deserialization_done)>;

template <class T>
auto test_deserialization_done(long) -> std::false_type;

template <class T>
struct has_deserialization_done : decltype(test_deserialization_done<T>(0)) {};

template <class T>
constexpr int fb_scalar_size = is_scalar<T> ? scalar_traits<T>::size : sizeof(RelativeOffset);

template <size_t offset, size_t index, class... Ts>
struct struct_offset_impl;

template <size_t o, size_t index>
struct struct_offset_impl<o, index> {
	static_assert(index == 0);
	static constexpr auto offset = o;
};

template <size_t o, size_t index, class T, class... Ts>
struct struct_offset_impl<o, index, T, Ts...> {
private:
	static constexpr size_t offset_() {
		if constexpr (index == 0) {
			return RightAlign(o, fb_scalar_size<T>);
		} else {
			return struct_offset_impl<RightAlign(o, fb_scalar_size<T>) + fb_scalar_size<T>, index - 1, Ts...>::offset;
		}
	}

public:
	static_assert(!is_struct_like<T>, "Nested structs not supported yet");
	static constexpr auto offset = offset_();
};

constexpr size_t AlignToPowerOfTwo(size_t s) {
	if (s > 4) {
		return 8;
	} else if (s > 2) {
		return 4;
	} else if (s > 1) {
		return 2;
	} else {
		return 1;
	}
}

template <class... Ts>
constexpr auto align_helper(pack<Ts...>) {
	return std::max({ size_t{ 1 }, AlignToPowerOfTwo(fb_scalar_size<Ts>)... });
}

template <class... T>
constexpr auto struct_size(pack<T...>) {
	return std::max(1, RightAlign(struct_offset_impl<0, sizeof...(T), T...>::offset, align_helper(pack<T...>{})));
}

template <int i, class... T>
constexpr auto struct_offset(pack<T...>) {
	static_assert(i < sizeof...(T));
	return struct_offset_impl<0, i, T...>::offset;
}

static_assert(struct_offset<0>(pack<int>{}) == 0);
static_assert(struct_offset<1>(pack<int, bool>{}) == 4);
static_assert(struct_offset<2>(pack<int, bool, double>{}) == 8);

static_assert(struct_size(pack<>{}) == 1);
static_assert(struct_size(pack<int>{}) == 4);
static_assert(struct_size(pack<int, bool>{}) == 8);
static_assert(struct_size(pack<int, bool, double>{}) == 16);

template <class T>
constexpr int fb_size = is_struct_like<T> ? struct_size(typename struct_like_traits<T>::types{}) : fb_scalar_size<T>;

template <class T>
constexpr int fb_align = is_struct_like<T> ? align_helper(typename struct_like_traits<T>::types{})
                                           : AlignToPowerOfTwo(fb_scalar_size<T>);

template <class T>
struct _SizeOf {
	static constexpr unsigned int size = fb_size<T>;
	static constexpr unsigned int align = fb_align<T>;
};

struct PrecomputeSize {
	// |offset| is measured from the end of the buffer. Precondition: len <=
	// offset.
	void write(const void*, int offset, int len) { current_buffer_size = std::max(current_buffer_size, offset); }

	template <class ToRawMemory>
	void writeRawMemory(ToRawMemory&& to_raw_memory) {
		auto w = std::forward<ToRawMemory>(to_raw_memory)();
		int start = RightAlign(current_buffer_size + w.size() + 4, 4);
		write(nullptr, start, 4);
		start -= 4;
		for (auto& block : w.blocks) {
			write(nullptr, start, block.second);
			start -= block.second;
		}
		writeRawMemories.emplace_back(std::move(w));
	}

	struct Noop {
		void write(const void* src, int offset, int len) {}
		void writeTo(PrecomputeSize& writer, int offset) {

			writer.write(nullptr, offset, size);
			writer.writeToOffsets[writeToIndex] = offset;
		}
		void writeTo(PrecomputeSize& writer) { writeTo(writer, writer.current_buffer_size + size); }
		int size;
		int writeToIndex;
	};

	Noop getMessageWriter(int size) {
		int writeToIndex = writeToOffsets.size();
		writeToOffsets.push_back({});
		return Noop{ size, writeToIndex };
	}

	int current_buffer_size = 0;

	const int buffer_length = -1; // Dummy, the value of this should not affect anything.
	const int vtable_start = -1; // Dummy, the value of this should not affect anything.
	std::vector<int> writeToOffsets;
	std::vector<WriteRawMemory> writeRawMemories;
};

template <class Member, class Context>
void load_helper(Member&, const uint8_t*, Context&);

struct VTableSet;

template <class T>
struct is_array : std::false_type {};

template <class T, size_t size>
struct is_array<std::array<T, size>> : std::true_type {};

struct WriteToBuffer {
	// |offset| is measured from the end of the buffer. Precondition: len <=
	// offset.
	void write(const void* src, int offset, int len) {
		copy_memory(src, offset, len);
		current_buffer_size = std::max(current_buffer_size, offset);
	}

	template <class ToRawMemory>
	void writeRawMemory(ToRawMemory&&) {
		auto& w = *write_raw_memories_iter;
		uint32_t size = w.size();
		int start = RightAlign(current_buffer_size + size + 4, 4);
		write(&size, start, 4);
		start -= 4;
		for (auto& p : w.blocks) {
			if (p.second > 0) {
				write(reinterpret_cast<const void*>(p.first.get()), start, p.second);
			}
			start -= p.second;
		}
		++write_raw_memories_iter;
	}

	WriteToBuffer(int buffer_length, int vtable_start, uint8_t* buffer, std::vector<int> writeToOffsets,
	              std::vector<WriteRawMemory>::iterator write_raw_memories_iter)
	  : buffer_length(buffer_length), vtable_start(vtable_start), buffer(buffer),
	    writeToOffsets(std::move(writeToOffsets)), write_raw_memories_iter(write_raw_memories_iter) {}

	struct MessageWriter {
		template <class T>
		void write(const T* src, int offset, size_t len) {
			if constexpr (std::is_same_v<T, RelativeOffset>) {
				uint32_t fixed_offset = finalLocation - offset - src->value;
				writer.copy_memory(&fixed_offset, finalLocation - offset, len);
			} else if constexpr (is_array<T>::value) {
				writer.copy_memory(src, finalLocation - offset, std::min(src->size(), len));
			} else {
				writer.copy_memory(src, finalLocation - offset, len);
			}
		}
		void writeTo(WriteToBuffer&) { writer.current_buffer_size += size; }
		void writeTo(WriteToBuffer&, int offset) {
			writer.current_buffer_size = std::max(writer.current_buffer_size, offset);
		}
		WriteToBuffer& writer;
		int finalLocation;
		int size;
	};

	MessageWriter getMessageWriter(int size) {
		MessageWriter m{ *this, writeToOffsets[writeToIndex++], size };
		return m;
	}

	const int buffer_length;
	const int vtable_start;
	int current_buffer_size = 0;

private:
	void copy_memory(const void* src, int offset, int len) {
		memcpy(static_cast<void*>(&buffer[buffer_length - offset]), src, len);
	}
	std::vector<int> writeToOffsets;
	std::vector<WriteRawMemory>::iterator write_raw_memories_iter;
	int writeToIndex = 0;
	uint8_t* buffer;
};

template <class Member>
constexpr auto fields_helper() {
	if constexpr (_SizeOf<Member>::size == 0) {
		return pack<>{};
	} else if constexpr (is_union_like<Member>) {
		return pack</*type*/ uint8_t, /*offset*/ uint32_t>{};
	} else if constexpr (is_vector_of_union_like<Member>) {
		return pack</*type vector*/ uint32_t, /*offset vector*/ uint32_t>{};
	} else {
		return pack<Member>{};
	}
}

template <class Member>
using Fields = decltype(fields_helper<Member>());

// TODO(anoyes): Make this `template <int... offsets>` so we can re-use
// identical vtables even if they have different types.
// Also, it's important that get_vtable always returns the same VTable pointer
// so that we can decide equality by comparing the pointers.

extern VTable generate_vtable(size_t numMembers, const std::vector<unsigned>& members,
                              const std::vector<unsigned>& alignments);

template <class... Members>
VTable gen_vtable(pack<Members...> p) {
	return generate_vtable(sizeof...(Members), std::vector<unsigned>{ { _SizeOf<Members>::size... } },
	                       std::vector<unsigned>{ { _SizeOf<Members>::align... } });
}

template <class... Members>
const VTable* get_vtable() {
	static VTable table = gen_vtable(concat_t<Fields<Members>...>{});
	return &table;
}

template <class F, class... Members>
void for_each(F&& f, Members&&... members) {
	(std::forward<F>(f)(std::forward<Members>(members)), ...);
}

struct VTableSet {
	std::map<const VTable*, int> offsets;
	std::vector<uint8_t> packed_tables;
};

struct InsertVTableLambda;

struct TraverseMessageTypes {
	InsertVTableLambda& f;

	bool vtableGeneratedBefore(const std::type_index&);

	template <class Member>
	std::enable_if_t<expect_serialize_member<Member>> operator()(const Member& member) {
		if (vtableGeneratedBefore(typeid(Member))) {
			return;
		}
		if constexpr (serializable_traits<Member>::value) {
			serializable_traits<Member>::serialize(f, const_cast<Member&>(member));
		} else {
			const_cast<Member&>(member).serialize(f);
		}
	};

	template <class T>
	std::enable_if_t<!expect_serialize_member<T> && !is_vector_like<T> && !is_union_like<T>> operator()(const T&) {}

	template <class VectorLike>
	std::enable_if_t<is_vector_like<VectorLike>> operator()(const VectorLike& members) {
		using VectorTraits = vector_like_traits<VectorLike>;
		using T = typename VectorTraits::value_type;
		// we don't need to check for recursion here because the next call
		// to operator() will do that and we don't generate a vtable for the
		// vector-like type itself
		object_construction<T> t;
		(*this)(t.get());
	}

	template <class UnionLike>
	std::enable_if_t<is_union_like<UnionLike>> operator()(const UnionLike& members) {
		using UnionTraits = union_like_traits<UnionLike>;
		static_assert(pack_size(typename UnionTraits::alternatives{}) <= 254,
		              "Up to 254 alternatives are supported for unions");
		union_helper(typename UnionTraits::alternatives{});
	}

private:
	template <class T, class... Ts>
	void union_helper(pack<T, Ts...>) {
		object_construction<T> t;
		(*this)(t.get());
		union_helper(pack<Ts...>{});
	}
	void union_helper(pack<>) {}
};

struct InsertVTableLambda {
	static constexpr bool isDeserializing = false;
	static constexpr bool isSerializing = false;
	static constexpr bool is_fb_visitor = true;
	std::set<const VTable*>& vtables;
	std::set<std::type_index>& known_types;

	template <class... Members>
	void operator()(const Members&... members) {
		vtables.insert(get_vtable<Members...>());
		for_each(TraverseMessageTypes{ *this }, members...);
	}
};

template <class T>
int vec_bytes(const T& begin, const T& end) {
	return sizeof(typename T::value_type) * (end - begin);
}

template <class Root>
VTableSet get_vtableset_impl(const Root& root) {
	std::set<const VTable*> vtables;
	std::set<std::type_index> known_types;
	InsertVTableLambda vlambda{ vtables, known_types };
	if constexpr (serializable_traits<Root>::value) {
		serializable_traits<Root>::serialize(vlambda, const_cast<Root&>(root));
	} else {
		const_cast<Root&>(root).serialize(vlambda);
	}
	size_t size = 0;
	for (const auto* vtable : vtables) {
		size += vec_bytes(vtable->begin(), vtable->end());
	}
	std::vector<uint8_t> packed_tables(size);
	int i = 0;
	std::map<const VTable*, int> offsets;
	for (const auto* vtable : vtables) {
		memcpy(&packed_tables[i], reinterpret_cast<const uint8_t*>(&(*vtable)[0]),
		       vec_bytes(vtable->begin(), vtable->end()));
		offsets[vtable] = i;
		i += vec_bytes(vtable->begin(), vtable->end());
	}
	return VTableSet{ offsets, packed_tables };
}

template <class Root>
const VTableSet* get_vtableset(const Root& root) {
	static VTableSet result = get_vtableset_impl(root);
	return &result;
}

template <class Root, class Writer>
void save_with_vtables(const Root& root, const VTableSet* vtableset, Writer& writer, int* vtable_start,
                       FileIdentifier file_identifier) {
	auto vtable_writer = writer.getMessageWriter(vtableset->packed_tables.size());
	vtable_writer.write(&vtableset->packed_tables[0], 0, vtableset->packed_tables.size());
	RelativeOffset offset = save_helper(const_cast<Root&>(root), writer, vtableset);
	vtable_writer.writeTo(writer);
	*vtable_start = writer.current_buffer_size;
	int root_writer_size = sizeof(uint32_t) + sizeof(file_identifier);
	auto root_writer = writer.getMessageWriter(root_writer_size);
	root_writer.write(&offset, 0, sizeof(offset));
	root_writer.write(&file_identifier, sizeof(offset), sizeof(file_identifier));
	root_writer.writeTo(writer, RightAlign(writer.current_buffer_size + root_writer_size, 8));
}

template <class Writer, class UnionTraits>
struct SaveAlternative {
	Writer& writer;
	const VTableSet* vtables;

	RelativeOffset save(uint8_t type_tag, const typename UnionTraits::Member& member) {
		return save_<0>(type_tag, member);
	}

private:
	template <uint8_t Alternative>
	RelativeOffset save_(uint8_t type_tag, const typename UnionTraits::Member& member) {
		if constexpr (Alternative < pack_size(typename UnionTraits::alternatives{})) {
			if (type_tag == Alternative) {
				auto result = save_helper(UnionTraits::template get<Alternative>(member), writer, vtables);
				if constexpr (use_indirection<index_t<Alternative, typename UnionTraits::alternatives>>) {
					return result;
				}
				writer.write(&result, writer.current_buffer_size + sizeof(result), sizeof(result));
				return RelativeOffset{ writer.current_buffer_size };
			} else {
				return save_<Alternative + 1>(type_tag, member);
			}
		}
		throw std::runtime_error("type_tag out of range. This should never happen.");
	}
};

template <class Context, class UnionTraits>
struct LoadAlternative {
	Context& context;
	const uint8_t* current;

	void load(uint8_t type_tag, typename UnionTraits::Member& member) { return load_<0>(type_tag, member); }

private:
	template <uint8_t Alternative>
	void load_(uint8_t type_tag, typename UnionTraits::Member& member) {
		if constexpr (Alternative < pack_size(typename UnionTraits::alternatives{})) {
			if (type_tag == Alternative) {
				using AlternativeT = index_t<Alternative, typename UnionTraits::alternatives>;
				object_construction<AlternativeT> alternative;
				if constexpr (use_indirection<AlternativeT>) {
					load_helper(alternative.get(), current, context);
				} else {
					uint32_t current_offset = interpret_as<uint32_t>(current);
					current += current_offset;
					load_helper(alternative.get(), current, context);
				}
				UnionTraits::template assign<Alternative>(member, std::move(alternative.move()));
			} else {
				load_<Alternative + 1>(type_tag, member);
			}
		}
	}
};

template <class Writer>
struct SaveVisitorLambda {
	static constexpr bool isDeserializing = false;
	static constexpr bool isSerializing = true;
	static constexpr bool is_fb_visitor = true;
	const VTableSet* vtableset;
	Writer& writer;

	template <class... Members>
	void operator()(const Members&... members) {
		const auto& vtable = *get_vtable<Members...>();
		auto self = writer.getMessageWriter(vtable[1] /* length */);
		int i = 2;
		for_each(
		    [&](const auto& member) {
			    using Member = std::decay_t<decltype(member)>;
			    if constexpr (is_vector_of_union_like<Member>) {
				    using VectorTraits = vector_like_traits<Member>;
				    using T = typename VectorTraits::value_type;
				    using UnionTraits = union_like_traits<T>;
				    uint32_t num_entries = VectorTraits::num_entries(member);
				    auto typeVectorWriter = writer.getMessageWriter(num_entries); // type tags are one byte
				    auto offsetVectorWriter = writer.getMessageWriter(num_entries * sizeof(RelativeOffset));
				    auto iter = VectorTraits::begin(member);
				    for (int i = 0; i < num_entries; ++i) {
					    uint8_t type_tag = UnionTraits::index(*iter);
					    uint8_t fb_type_tag =
					        UnionTraits::empty(*iter) ? 0 : type_tag + 1; // Flatbuffers indexes from 1.
					    typeVectorWriter.write(&fb_type_tag, i, sizeof(fb_type_tag));
					    if (!UnionTraits::empty(*iter)) {
						    RelativeOffset offset =
						        (SaveAlternative<Writer, UnionTraits>{ writer, vtableset }).save(type_tag, *iter);
						    offsetVectorWriter.write(&offset, i * sizeof(offset), sizeof(offset));
					    }
					    ++iter;
				    }
				    int start = RightAlign(writer.current_buffer_size + num_entries, 4) + 4;
				    writer.write(&num_entries, start, sizeof(uint32_t));
				    typeVectorWriter.writeTo(writer, start - sizeof(uint32_t));
				    auto typeVectorOffset = RelativeOffset{ writer.current_buffer_size };
				    start = RightAlign(writer.current_buffer_size + num_entries * sizeof(RelativeOffset), 4) + 4;
				    writer.write(&num_entries, start, sizeof(uint32_t));
				    offsetVectorWriter.writeTo(writer, start - sizeof(uint32_t));
				    auto offsetVectorOffset = RelativeOffset{ writer.current_buffer_size };
				    self.write(&typeVectorOffset, vtable[i++], sizeof(typeVectorOffset));
				    self.write(&offsetVectorOffset, vtable[i++], sizeof(offsetVectorOffset));
			    } else if constexpr (is_union_like<Member>) {
				    using UnionTraits = union_like_traits<Member>;
				    uint8_t type_tag = UnionTraits::index(member);
				    uint8_t fb_type_tag = UnionTraits::empty(member) ? 0 : type_tag + 1; // Flatbuffers indexes from 1.
				    self.write(&fb_type_tag, vtable[i++], sizeof(fb_type_tag));
				    if (!UnionTraits::empty(member)) {
					    RelativeOffset offset =
					        (SaveAlternative<Writer, UnionTraits>{ writer, vtableset }).save(type_tag, member);
					    self.write(&offset, vtable[i++], sizeof(offset));
				    } else {
					    ++i;
				    }
			    } else if constexpr (_SizeOf<Member>::size == 0) {
				    save_helper(member, writer, vtableset);
			    } else {
				    auto result = save_helper(member, writer, vtableset);
				    self.write(&result, vtable[i++], sizeof(result));
			    }
		    },
		    members...);
		int vtable_offset = writer.vtable_start - vtableset->offsets.at(&vtable);
		int start = RightAlign(writer.current_buffer_size + vtable[1] - 4, std::max({ 1, fb_align<Members>... })) + 4;
		int32_t relative = vtable_offset - start;
		self.write(&relative, 0, sizeof(relative));
		self.writeTo(writer, start);
	}
};

template <class Context>
struct LoadMember {
	static constexpr bool isDeserializing = true;
	static constexpr bool isSerializing = false;
	const uint16_t* const vtable;
	const uint8_t* const message;
	const uint16_t vtable_length;
	const uint16_t table_length;
	int& i;
	Context& context;
	template <class Member>
	void operator()(Member& member) {
		if constexpr (is_vector_of_union_like<Member>) {
			if (!field_present()) {
				i += 2;
				return;
			}
			const uint8_t* types_current = &message[vtable[i++]];
			uint32_t types_current_offset = interpret_as<uint32_t>(types_current);
			types_current += types_current_offset;
			types_current += sizeof(uint32_t); // num entries in types vector
			using VectorTraits = vector_like_traits<Member>;
			using T = typename Member::value_type;
			const uint8_t* current = &message[vtable[i++]];
			uint32_t current_offset = interpret_as<uint32_t>(current);
			current += current_offset;
			uint32_t numEntries = interpret_as<uint32_t>(current);
			current += sizeof(uint32_t);
			VectorTraits::reserve(member, numEntries, context);
			auto inserter = VectorTraits::insert(member);
			for (int i = 0; i < numEntries; ++i) {
				T value;
				if (types_current[i] > 0) {
					uint8_t type_tag = types_current[i] - 1; // Flatbuffers indexes from 1.
					(LoadAlternative<Context, union_like_traits<T>>{ context, current }).load(type_tag, value);
				}
				*inserter = std::move(value);
				++inserter;
				current += sizeof(RelativeOffset);
			}
			if constexpr (has_deserialization_done<VectorTraits>::value) {
				VectorTraits::deserialization_done(member);
			}
		} else if constexpr (is_union_like<Member>) {
			if (!field_present()) {
				i += 2;
				return;
			}
			uint8_t fb_type_tag;
			load_helper(fb_type_tag, &message[vtable[i]], context);
			uint8_t type_tag = fb_type_tag - 1; // Flatbuffers indexes from 1.
			++i;
			if (field_present() && fb_type_tag > 0) {
				(LoadAlternative<Context, union_like_traits<Member>>{ context, &message[vtable[i]] })
				    .load(type_tag, member);
			}
			++i;
		} else if constexpr (_SizeOf<Member>::size == 0) {
			load_helper(member, nullptr, context);
		} else {
			if (field_present()) {
				load_helper(member, &message[vtable[i]], context);
			}
			++i;
		}
	}

private:
	bool field_present() { return i < vtable_length && vtable[i] >= 4; }
};

template <size_t i>
struct int_type {
	static constexpr int value = i;
};

template <class F, size_t... I>
void for_each_i_impl(F&& f, std::index_sequence<I...>) {
	for_each(std::forward<F>(f), int_type<I>{}...);
}

template <size_t I, class F>
void for_each_i(F&& f) {
	for_each_i_impl(std::forward<F>(f), std::make_index_sequence<I>{});
}

template <class>
struct LoadSaveHelper {
	template <class U, class Context>
	std::enable_if_t<is_scalar<U>> load(U& member, const uint8_t* current, Context& context) {
		scalar_traits<U>::load(current, member, context);
	}

	template <class U, class Context>
	std::enable_if_t<is_struct_like<U>> load(U& member, const uint8_t* current, Context& context) {
		using StructTraits = struct_like_traits<U>;
		using types = typename StructTraits::types;
		for_each_i<pack_size(types{})>([&](auto i_type) {
			constexpr int i = decltype(i_type)::value;
			using type = index_t<i, types>;
			object_construction<type> t;
			load_helper(t.get(), current + struct_offset<i>(types{}), context);
			StructTraits::template assign<i>(member, t.move());
		});
	}

	template <class U, class Context>
	std::enable_if_t<is_dynamic_size<U>> load(U& member, const uint8_t* current, Context& context) {
		uint32_t current_offset = interpret_as<uint32_t>(current);
		current += current_offset;
		uint32_t size = interpret_as<uint32_t>(current);
		current += sizeof(size);
		dynamic_size_traits<U>::load(current, size, member, context);
	}

	template <class Context>
	struct SerializeFun {
		static constexpr bool isDeserializing = true;
		static constexpr bool isSerializing = false;
		static constexpr bool is_fb_visitor = true;

		const uint16_t* vtable;
		const uint8_t* current;
		Context& context;

		SerializeFun(const uint16_t* vtable, const uint8_t* current, Context& context)
		  : vtable(vtable), current(current), context(context) {}

		template <class... Args>
		void operator()(Args&... members) {
			int i = 0;
			uint16_t vtable_length = vtable[i++] / sizeof(uint16_t);
			uint16_t table_length = vtable[i++];
			for_each(LoadMember<Context>{ vtable, current, vtable_length, table_length, i, context }, members...);
		}
	};

	template <class Member, class Context>
	std::enable_if_t<expect_serialize_member<Member>> load(Member& member, const uint8_t* current, Context& context) {
		uint32_t current_offset = interpret_as<uint32_t>(current);
		current += current_offset;
		int32_t vtable_offset = interpret_as<int32_t>(current);
		const uint16_t* vtable = reinterpret_cast<const uint16_t*>(current - vtable_offset);
		SerializeFun<Context> fun(vtable, current, context);
		if constexpr (serializable_traits<Member>::value) {
			serializable_traits<Member>::serialize(fun, member);
		} else {
			member.serialize(fun);
		}
	}

	template <class VectorLike, class Context>
	std::enable_if_t<is_vector_like<VectorLike>> load(VectorLike& member, const uint8_t* current, Context& context) {
		using VectorTraits = vector_like_traits<VectorLike>;
		using T = typename VectorTraits::value_type;
		uint32_t current_offset = interpret_as<uint32_t>(current);
		current += current_offset;
		uint32_t numEntries = interpret_as<uint32_t>(current);
		current += sizeof(uint32_t);
		VectorTraits::reserve(member, numEntries, context);
		auto inserter = VectorTraits::insert(member);
		for (uint32_t i = 0; i < numEntries; ++i) {
			T value;
			load_helper(value, current, context);
			*inserter = std::move(value);
			++inserter;
			current += fb_size<T>;
		}
		if constexpr (has_deserialization_done<VectorTraits>::value) {
			VectorTraits::deserialization_done(member);
		}
	}

	template <class U, class Writer, typename = std::enable_if_t<is_scalar<U>>>
	auto save(const U& message, Writer& writer, const VTableSet*) {
		constexpr auto size = scalar_traits<U>::size;
		std::array<uint8_t, size> result = {};
		if constexpr (size > 0) {
			scalar_traits<U>::save(&result[0], message);
		}
		return result;
	}

	template <class U, class Writer>
	auto save(const U& message, Writer& writer, const VTableSet* vtables,
	          std::enable_if_t<is_struct_like<U>, int> _ = 0) {
		using StructTraits = struct_like_traits<U>;
		using types = typename StructTraits::types;
		constexpr auto size = struct_size(types{});
		std::array<uint8_t, size> struct_bytes = {};
		for_each_i<pack_size(types{})>([&](auto i_type) {
			constexpr int i = decltype(i_type)::value;
			auto result = save_helper(StructTraits::template get<i>(message), writer, vtables);
			memcpy(&struct_bytes[struct_offset<i>(types{})], &result, sizeof(result));
		});
		return struct_bytes;
	}

	template <class U, class Writer, typename = std::enable_if_t<is_dynamic_size<U>>>
	RelativeOffset save(const U& message, Writer& writer, const VTableSet*,
	                    std::enable_if_t<is_dynamic_size<U>, int> _ = 0) {
		writer.writeRawMemory([&]() { return dynamic_size_traits<U>::save(message); });
		return RelativeOffset{ writer.current_buffer_size };
	}

	template <class Member, class Writer>
	RelativeOffset save(const Member& member, Writer& writer, const VTableSet* vtables,
	                    std::enable_if_t<expect_serialize_member<Member>, int> _ = 0) {
		SaveVisitorLambda<Writer> l{ vtables, writer };
		if constexpr (serializable_traits<Member>::value) {
			serializable_traits<Member>::serialize(l, const_cast<Member&>(member));
		} else {
			const_cast<Member&>(member).serialize(l);
		}
		return RelativeOffset{ writer.current_buffer_size };
	}

	template <class VectorLike, class Writer, typename = std::enable_if_t<is_vector_like<VectorLike>>>
	RelativeOffset save(const VectorLike& members, Writer& writer, const VTableSet* vtables) {
		using VectorTraits = vector_like_traits<VectorLike>;
		using T = typename VectorTraits::value_type;
		constexpr auto size = fb_size<T>;
		uint32_t num_entries = VectorTraits::num_entries(members);
		uint32_t len = num_entries * size;
		auto self = writer.getMessageWriter(len);
		auto iter = VectorTraits::begin(members);
		for (uint32_t i = 0; i < num_entries; ++i) {
			auto result = save_helper(*iter, writer, vtables);
			self.write(&result, i * size, size);
			++iter;
		}
		int start = RightAlign(writer.current_buffer_size + len, std::min(4, fb_align<T>)) + 4;
		writer.write(&num_entries, start, sizeof(uint32_t));
		self.writeTo(writer, start - sizeof(uint32_t));
		return RelativeOffset{ writer.current_buffer_size };
	}
};

template <class Alloc>
struct LoadSaveHelper<std::vector<bool, Alloc>> {
	template <class Context>
	void load(std::vector<bool, Alloc>& member, const uint8_t* current, Context& context) {
		uint32_t current_offset = interpret_as<uint32_t>(current);
		current += current_offset;
		uint32_t length = interpret_as<uint32_t>(current);
		current += sizeof(uint32_t);
		member.clear();
		member.resize(length);
		bool m;
		for (uint32_t i = 0; i < length; ++i) {
			load_helper(m, current, context);
			member[i] = m;
			current += fb_size<bool>;
		}
	}

	template <class Writer>
	RelativeOffset save(const std::vector<bool, Alloc>& members, Writer& writer, const VTableSet* vtables) {
		uint32_t len = members.size();
		int start = RightAlign(writer.current_buffer_size + sizeof(uint32_t) + len, sizeof(uint32_t));
		writer.write(&len, start, sizeof(uint32_t));
		int i = 0;
		for (bool b : members) {
			writer.write(&b, start - sizeof(uint32_t) - i++, 1);
		}
		return RelativeOffset{ writer.current_buffer_size };
	}
};

template <class Member, class Context>
void load_helper(Member& member, const uint8_t* current, Context& context) {
	LoadSaveHelper<Member> helper;
	helper.load(member, current, context);
}

template <class Member, class Writer>
auto save_helper(const Member& member, Writer& writer, const VTableSet* vtables) {
	LoadSaveHelper<Member> helper;
	return helper.save(member, writer, vtables);
}

} // namespace detail

namespace detail {

template <class... Members>
struct FakeRoot {
	std::tuple<Members&...> members;

	FakeRoot(Members&... members) : members(members...) {}

	template <class Archive>
	void serialize(Archive& archive) {
		serialize_impl(archive, std::index_sequence_for<Members...>{});
	}

private:
	template <class Archive, size_t... is>
	void serialize_impl(Archive& archive, std::index_sequence<is...>) {
		serializer(archive, std::get<is>(members)...);
	}
};

template <class... Members>
auto fake_root(Members&... members) {
	return FakeRoot<Members...>(members...);
}

template <class Allocator, class Root>
uint8_t* save(Allocator& allocator, const Root& root, FileIdentifier file_identifier) {
	const auto* vtableset = get_vtableset(root);
	PrecomputeSize precompute_size;
	int vtable_start;
	save_with_vtables(root, vtableset, precompute_size, &vtable_start, file_identifier);
	uint8_t* out = allocator(precompute_size.current_buffer_size);
	memset(out, 0, precompute_size.current_buffer_size);
	WriteToBuffer writeToBuffer{ precompute_size.current_buffer_size, vtable_start, out,
		                         std::move(precompute_size.writeToOffsets), precompute_size.writeRawMemories.begin() };
	save_with_vtables(root, vtableset, writeToBuffer, &vtable_start, file_identifier);
	return out;
}

template <class Root, class Context>
void load(Root& root, const uint8_t* in, Context& context) {
	detail::load_helper(root, in, context);
}

} // namespace detail

template <class Allocator, class... Members>
uint8_t* save_members(Allocator& allocator, FileIdentifier file_identifier, Members&... members) {
	const auto& root = detail::fake_root(members...);
	return detail::save(allocator, root, file_identifier);
}

template <class Context, class... Members>
void load_members(const uint8_t* in, Context& context, Members&... members) {
	auto root = detail::fake_root(members...);
	detail::load(root, in, context);
}

inline FileIdentifier read_file_identifier(const uint8_t* in) {
	FileIdentifier result;
	memcpy(&result, in + sizeof(result), sizeof(result));
	return result;
}

// members of unions must be tables in flatbuffers, so you can use this to
// introduce the indirection only when necessary.
template <class T>
struct EnsureTable {
	static_assert(HasFileIdentifier<T>::value);
	constexpr static FileIdentifier file_identifier = FileIdentifierFor<T>::value;
	EnsureTable() = default;
	EnsureTable(const object_construction<T>& t) : t(t) {}
	EnsureTable(const T& t) : t(t) {}
	template <class Archive>
	void serialize(Archive& ar) {
		if constexpr (is_fb_function<Archive>) {
			if constexpr (detail::expect_serialize_member<T>) {
				if constexpr (serializable_traits<T>::value) {
					serializable_traits<T>::serialize(ar, t.get());
				} else {
					t.get().serialize(ar);
				}
			} else {
				serializer(ar, t.get());
			}
		} else {
			serializer(ar, t.get());
		}
	}
	T& asUnderlyingType() { return t.get(); }

private:
	object_construction<T> t;
};

