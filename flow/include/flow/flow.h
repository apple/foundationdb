/*
 * flow.h
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

#ifndef FLOW_FLOW_H
#define FLOW_FLOW_H
#include "flow/ActorContext.h"
#include "flow/Arena.h"
#include "flow/FastRef.h"
#pragma once

#ifdef _MSC_VER
#pragma warning(disable : 4244 4267) // SOMEDAY: Carefully check for integer overflow issues (e.g. size_t to int
// conversions like this suppresses)
#pragma warning(disable : 4345)
#pragma warning(error : 4239)
#endif

#include <algorithm>
#include <array>
#include <iosfwd>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <stack>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "flow/Buggify.h"
#include "flow/CodeProbe.h"
#include "flow/Deque.h"
#include "flow/Error.h"
#include "flow/FastAlloc.h"
#include "flow/FileIdentifier.h"
#include "flow/IRandom.h"
#include "flow/Platform.h"
#include "flow/ThreadPrimitives.h"
#include "flow/WriteOnlySet.h"
#include "flow/network.h"
#include "flow/serialize.h"

#ifdef WITH_SWIFT
#include <swift/bridging>

// Flow_CheckedContinuation.h depends on this header, so we first parse it
// without relying on any imported Swift types.
#ifndef SWIFT_HIDE_CHECKED_CONTINUTATION
#include "SwiftModules/Flow_CheckedContinuation.h"
#endif /* SWIFT_HIDE_CHECKED_CONTINUATION */

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wnullability-completeness"
#endif /* WITH_SWIFT */

#include "pthread.h"

#include <boost/version.hpp>

#define TEST(condition)                                                                                                \
	do {                                                                                                               \
		static_assert(false, "TEST macros are deprecated, please use CODE_PROBE instead");                             \
	} while (false)


extern Optional<uint64_t> parse_with_suffix(std::string const& toparse, std::string const& default_unit = "");
extern Optional<uint64_t> parseDuration(std::string const& str, std::string const& defaultUnit = "");
extern std::string format(const char* form, ...);

// On success, returns the number of characters written. On failure, returns a negative number.
extern int vsformat(std::string& outputString, const char* form, va_list args);

extern Standalone<StringRef> strinc(StringRef const& str);
extern StringRef strinc(StringRef const& str, Arena& arena);
extern Standalone<StringRef> addVersionStampAtEnd(StringRef const& str);
extern StringRef addVersionStampAtEnd(StringRef const& str, Arena& arena);

// Return the number of combinations to choose k items out of n choices
int nChooseK(int n, int k);

template <typename Iter>
StringRef concatenate(Iter b, Iter const& e, Arena& arena) {
	int rsize = 0;
	Iter i = b;
	while (i != e) {
		rsize += i->size();
		++i;
	}
	uint8_t* s = new (arena) uint8_t[rsize];
	uint8_t* p = s;
	while (b != e) {
		memcpy(p, b->begin(), b->size());
		p += b->size();
		++b;
	}
	return StringRef(s, rsize);
}

template <typename Iter>
Standalone<StringRef> concatenate(Iter b, Iter const& e) {
	Standalone<StringRef> r;
	((StringRef&)r) = concatenate(b, e, r.arena());
	return r;
}

class Void {
public:
	constexpr static FileIdentifier file_identifier = 2010442;
	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar);
	}
};

class Never {};

template <class T>
class ErrorOr : public ComposedIdentifier<T, 2> {
	std::variant<Error, T> value;

public:
	using ValueType = T;

	ErrorOr() : ErrorOr(default_error_or()) {}
	ErrorOr(Error const& error) : value(std::in_place_type<Error>, error) {}

	template <class U>
	ErrorOr(U const& t) : value(std::in_place_type<T>, t) {}

	ErrorOr(Arena& a, ErrorOr<T> const& o) {
		if (o.present()) {
			value = std::variant<Error, T>(std::in_place_type<T>, a, o.get());
		} else {
			value = std::variant<Error, T>(std::in_place_type<Error>, o.getError());
		}
	}
	int expectedSize() const { return present() ? get().expectedSize() : 0; }

	template <class R>
	ErrorOr<R> castTo() const {
		return map<R>([](const T& v) { return (R)v; });
	}

private:
	template <class F>
	using MapRet = std::decay_t<std::invoke_result_t<F, T>>;

	template <class F>
	using EnableIfNotMemberPointer =
	    std::enable_if_t<!std::is_member_object_pointer_v<F> && !std::is_member_function_pointer_v<F>>;

public:
	// If the ErrorOr is set, calls the function f on the value and returns the value. Otherwise, returns an ErrorOr
	// with the same error value as this ErrorOr.
	template <class F, typename = EnableIfNotMemberPointer<F>>
	ErrorOr<MapRet<F>> map(const F& f) const& {
		return present() ? ErrorOr<MapRet<F>>(f(get())) : ErrorOr<MapRet<F>>(getError());
	}
	template <class F, typename = EnableIfNotMemberPointer<F>>
	ErrorOr<MapRet<F>> map(const F& f) && {
		return present() ? ErrorOr<MapRet<F>>(f(std::move(*this).get())) : ErrorOr<MapRet<F>>(getError());
	}

	// Converts an ErrorOr<T> to an ErrorOr<R> of one of its value's members
	//
	// v.map(&T::member) is equivalent to v.map<R>([](T t) { return t.member; })
	template <class R, class Rp = std::decay_t<R>>
	std::enable_if_t<std::is_class_v<T>, ErrorOr<Rp>> map(
	    R std::conditional_t<std::is_class_v<T>, T, Void>::*member) const& {
		return present() ? ErrorOr<Rp>(get().*member) : ErrorOr<Rp>(getError());
	}
	template <class R, class Rp = std::decay_t<R>>
	std::enable_if_t<std::is_class_v<T>, ErrorOr<Rp>> map(
	    R std::conditional_t<std::is_class_v<T>, T, Void>::*member) && {
		return present() ? ErrorOr<Rp>(std::move(*this).get().*member) : ErrorOr<Rp>(getError());
	}

	// Converts an ErrorOr<T> to an ErrorOr<R> of a value returned by a member function of T
	//
	// v.map(&T::memberFunc, arg1, arg2, ...) is equivalent to
	// v.map<R>([](T t) { return t.memberFunc(arg1, arg2, ...); })
	template <class R, class... Args, class Rp = std::decay_t<R>>
	std::enable_if_t<std::is_class_v<T>, ErrorOr<Rp>> map(
	    R (std::conditional_t<std::is_class_v<T>, T, Void>::*memberFunc)(Args...) const,
	    Args&&... args) const& {
		return present() ? ErrorOr<Rp>((get().*memberFunc)(std::forward<Args>(args)...)) : ErrorOr<Rp>(getError());
	}
	template <class R, class... Args, class Rp = std::decay_t<R>>
	std::enable_if_t<std::is_class_v<T>, ErrorOr<Rp>> map(
	    R (std::conditional_t<std::is_class_v<T>, T, Void>::*memberFunc)(Args...) const,
	    Args&&... args) && {
		return present() ? ErrorOr<Rp>((std::move(*this).get().*memberFunc)(std::forward<Args>(args)...))
		                 : ErrorOr<Rp>(getError());
	}

	// Given T that is a pointer or pointer-like type to type P (e.g. T=P* or T=Reference<P>), converts an ErrorOr<T>
	// to an ErrorOr<R> of one its value's members. If the value is present and false-like (null), then
	// returns a default constructed ErrorOr<R>.
	//
	// v.mapRef(&P::member) is equivalent to ErrorOr<R>(v.get()->member) if v is present and non-null
	template <class P, class R, class Rp = std::decay_t<R>>
	std::enable_if_t<std::is_class_v<T> || std::is_pointer_v<T>, ErrorOr<Rp>> mapRef(R P::*member) const& {

		if (!present()) {
			return ErrorOr<Rp>(getError());
		} else if (!get()) {
			return ErrorOr<Rp>();
		}

		P& p = *get();
		return p.*member;
	}

	// Given T that is a pointer or pointer-like type to type P (e.g. T=P* or T=Reference<P>), converts an ErrorOr<T>
	// to an ErrorOr<R> of a value returned by a member function of P. If the optional value is present and false-like
	// (null), then returns a default constructed ErrorOr<R>.
	//
	// v.map(&T::memberFunc, arg1, arg2, ...) is equivalent to ErrorOr<R>(v.get()->memberFunc(arg1, arg2, ...)) if v is
	// present and non-null
	template <class P, class R, class... Args, class Rp = std::decay_t<R>>
	std::enable_if_t<std::is_class_v<T> || std::is_pointer_v<T>, ErrorOr<Rp>> mapRef(R (P::*memberFunc)(Args...) const,
	                                                                                 Args&&... args) const& {
		if (!present()) {
			return ErrorOr<Rp>(getError());
		} else if (!get()) {
			return ErrorOr<Rp>();
		}

		P& p = *get();
		return (p.*memberFunc)(std::forward<Args>(args)...);
	}

	// Similar to map with a mapped type of ErrorOr<R>, but flattens the result. For example, if the mapped result is of
	// type ErrorOr<R>, map will return ErrorOr<ErrorOr<R>> while flatMap will return ErrorOr<R>
	template <class... Args>
	auto flatMap(Args&&... args) const& {
		auto val = map(std::forward<Args>(args)...);
		using R = typename decltype(val)::ValueType::ValueType;

		if (val.present()) {
			return val.get();
		} else {
			return ErrorOr<R>(val.getError());
		}
	}
	template <class... Args>
	auto flatMap(Args&&... args) && {
		auto val = std::move(*this).map(std::forward<Args>(args)...);
		using R = typename decltype(val)::ValueType::ValueType;

		if (val.present()) {
			return val.get();
		} else {
			return ErrorOr<R>(val.getError());
		}
	}

	// Similar to mapRef with a mapped type of ErrorOr<R>, but flattens the result. For example, if the mapped result is
	// of type ErrorOr<R>, mapRef will return ErrorOr<ErrorOr<R>> while flatMapRef will return ErrorOr<R>
	template <class... Args>
	auto flatMapRef(Args&&... args) const& {
		auto val = mapRef(std::forward<Args>(args)...);
		using R = typename decltype(val)::ValueType::ValueType;

		if (val.present()) {
			return val.get();
		} else {
			return ErrorOr<R>(val.getError());
		}
	}
	template <class... Args>
	auto flatMapRef(Args&&... args) && {
		auto val = std::move(*this).mapRef(std::forward<Args>(args)...);
		using R = typename decltype(val)::ValueType::ValueType;

		if (val.present()) {
			return val.get();
		} else {
			return ErrorOr<R>(val.getError());
		}
	}

	bool present() const { return std::holds_alternative<T>(value); }
	T& get() & {
		UNSTOPPABLE_ASSERT(present());
		return std::get<T>(value);
	}
	T const& get() const& {
		UNSTOPPABLE_ASSERT(present());
		return std::get<T>(value);
	}
	T&& get() && {
		UNSTOPPABLE_ASSERT(present());
		return std::get<T>(std::move(value));
	}
	template <class U>
	T orDefault(U&& defaultValue) const& {
		return present() ? get() : std::forward<U>(defaultValue);
	}
	template <class U>
	T orDefault(U&& defaultValue) && {
		return present() ? std::move(*this).get() : std::forward<U>(defaultValue);
	}

	bool isError() const { return std::holds_alternative<Error>(value); }
	bool isError(int code) const { return isError() && getError().code() == code; }
	Error const& getError() const {
		ASSERT(isError());
		return std::get<Error>(value);
	}
};

template <class Archive, class T>
void load(Archive& ar, ErrorOr<T>& value) {
	Error error;
	ar >> error;
	if (error.code() != invalid_error_code) {
		T t;
		ar >> t;
		value = ErrorOr<T>(t);
	} else {
		value = ErrorOr<T>(error);
	}
}

template <class Archive, class T>
void save(Archive& ar, ErrorOr<T> const& value) {
	if (value.present()) {
		ar << Error{}; // invalid error code
		ar << value.get();
	} else {
		ar << value.getError();
	}
}

template <class T>
struct union_like_traits<ErrorOr<T>> : std::true_type {
	using Member = ErrorOr<T>;
	using alternatives = pack<Error, T>;
	template <class Context>
	static uint8_t index(const Member& variant, Context&) {
		return variant.present() ? 1 : 0;
	}
	template <class Context>
	static bool empty(const Member& variant, Context&) {
		return false;
	}

	template <int i, class Context>
	static const index_t<i, alternatives>& get(const Member& m, Context&) {
		if constexpr (i == 0) {
			return m.getError();
		} else {
			static_assert(i == 1, "ErrorOr only has two members");
			return m.get();
		}
	}

	template <int i, class Alternative, class Context>
	static void assign(Member& m, const Alternative& a, Context&) {
		if constexpr (i == 0) {
			m = a;
		} else {
			static_assert(i == 1);
			m = a;
		}
	}
};

template <class T>
class CachedSerialization {
public:
	constexpr static FileIdentifier file_identifier = FileIdentifierFor<T>::value;

	// FIXME: this code will not work for caching a direct serialization from ObjectWriter, because it adds an ErrorOr,
	// we should create a separate SerializeType for direct serialization
	enum class SerializeType { None, Binary, Object };

	CachedSerialization() : cacheType(SerializeType::None) {}
	explicit CachedSerialization(const T& data) : data(data), cacheType(SerializeType::None) {}

	const T& read() const { return data; }

	T& mutate() {
		cacheType = SerializeType::None;
		return data;
	}

	// This should only be called from the ObjectSerializer load function
	Standalone<StringRef> getCache() const {
		if (cacheType != SerializeType::Object) {
			cache = ObjectWriter::toValue(ErrorOr<EnsureTable<T>>(data), AssumeVersion(g_network->protocolVersion()));
			cacheType = SerializeType::Object;
		}
		return cache;
	}

	bool operator==(CachedSerialization<T> const& rhs) const { return data == rhs.data; }
	bool operator!=(CachedSerialization<T> const& rhs) const { return !(*this == rhs); }
	bool operator<(CachedSerialization<T> const& rhs) const { return data < rhs.data; }

	template <class Ar>
	void serialize(Ar& ar) {
		if constexpr (is_fb_function<Ar>) {
			// Suppress vtable collection. Save and load are implemented via the specializations below
		} else {
			if (Ar::isDeserializing) {
				cache = Standalone<StringRef>();
				cacheType = SerializeType::None;
				serializer(ar, data);
			} else {
				if (cacheType != SerializeType::Binary) {
					cache = BinaryWriter::toValue(data, AssumeVersion(g_network->protocolVersion()));
					cacheType = SerializeType::Binary;
				}
				ar.serializeBytes(const_cast<uint8_t*>(cache.begin()), cache.size());
			}
		}
	}

private:
	T data;
	mutable SerializeType cacheType;
	mutable Standalone<StringRef> cache;
};

// this special case is needed - the code expects
// Standalone<T> and T to be equivalent for serialization
namespace detail {

template <class T, class Context>
struct LoadSaveHelper<CachedSerialization<T>, Context> : Context {
	LoadSaveHelper(const Context& context) : Context(context), helper(context) {}

	void load(CachedSerialization<T>& member, const uint8_t* current) { helper.load(member.mutate(), current); }

	template <class Writer>
	RelativeOffset save(const CachedSerialization<T>& member, Writer& writer, const VTableSet* vtables) {
		throw internal_error();
	}

private:
	LoadSaveHelper<T, Context> helper;
};

} // namespace detail

template <class V>
struct serialize_raw<ErrorOr<EnsureTable<CachedSerialization<V>>>> : std::true_type {
	template <class Context>
	static uint8_t* save_raw(Context& context, const ErrorOr<EnsureTable<CachedSerialization<V>>>& obj) {
		auto cache = obj.present() ? obj.get().asUnderlyingType().getCache()
		                           : ObjectWriter::toValue(ErrorOr<EnsureTable<V>>(obj.getError()),
		                                                   AssumeVersion(g_network->protocolVersion()));
		uint8_t* out = context.allocate(cache.size());
		memcpy(out, cache.begin(), cache.size());
		return out;
	}
};

template <class T>
struct Callback {
	Callback<T>*prev, *next;

	virtual void fire(T const&) {}
	virtual void fire(T&&) {}
	virtual void error(Error) {}
	virtual void unwait() {}

	void insert(Callback<T>* into) {
		// Add this (uninitialized) callback just after `into`
		this->prev = into;
		this->next = into->next;
		into->next->prev = this;
		into->next = this;
	}

	void insertBack(Callback<T>* into) {
		// Add this (uninitialized) callback just before `into`
		this->next = into;
		this->prev = into->prev;
		into->prev->next = this;
		into->prev = this;
	}

	void insertChain(Callback<T>* into) {
		// Combine this callback's (initialized) chain and `into`'s such that this callback is just after `into`
		auto p = this->prev;
		auto n = into->next;
		this->prev = into;
		into->next = this;
		p->next = n;
		n->prev = p;
	}

	void remove() {
		// Remove this callback from the list it is in, and call unwait() on the head of that list if this was the last
		// callback
		next->prev = prev;
		prev->next = next;
		if (prev == next)
			next->unwait();
	}

	int countCallbacks() const {
		int count = 0;
		for (Callback* c = next; c != this; c = c->next)
			count++;
		return count;
	}
};

template <class T>
struct SingleCallback {
	// Used for waiting on FutureStreams, which don't support multiple callbacks
	SingleCallback<T>* next;

	virtual void fire(T const&) {}
	virtual void fire(T&&) {}
	virtual void error(Error) {}
	virtual void unwait() {}

	void insert(SingleCallback<T>* into) {
		this->next = into->next;
		into->next = this;
	}

	void remove() {
		ASSERT(next->next == this);
		next->next = next;
		next->unwait();
	}
};

struct LineagePropertiesBase {
	virtual ~LineagePropertiesBase();
};

// helper class to make implementation of LineageProperties easier
template <class Derived>
struct LineageProperties : LineagePropertiesBase {
	// Contract:
	//
	// StringRef name = "SomeUniqueName"_str;

	// this has to be implemented by subclasses
	// but can't be made virtual.
	// A user should implement this for any type
	// within the properties class.
	template <class Value>
	bool isSet(Value Derived::*member) const {
		return true;
	}
};

class ActorLineage : public ThreadSafeReferenceCounted<ActorLineage> {
public:
	friend class LineageReference;

	struct Property {
		std::string_view name;
		LineagePropertiesBase* properties;
	};

private:
	std::vector<Property> properties;
	Reference<ActorLineage> parent;
	mutable std::mutex mutex;
	using Lock = std::unique_lock<std::mutex>;
	using Iterator = std::vector<Property>::const_iterator;

	ActorLineage();
	Iterator find(const std::string_view& name) const {
		for (auto it = properties.cbegin(); it != properties.cend(); ++it) {
			if (it->name == name) {
				return it;
			}
		}
		return properties.end();
	}
	Property& findOrInsert(const std::string_view& name) {
		for (auto& property : properties) {
			if (property.name == name) {
				return property;
			}
		}
		properties.emplace_back(Property{ name, nullptr });
		return properties.back();
	}

public:
	~ActorLineage();
	bool isRoot() const {
		Lock _{ mutex };
		return parent.getPtr() == nullptr;
	}
	void makeRoot() {
		Lock _{ mutex };
		parent.clear();
	}
	template <class T, class V>
	V& modify(V T::*member) {
		Lock _{ mutex };
		auto& res = findOrInsert(T::name).properties;
		if (!res) {
			res = new T{};
		}
		T* map = static_cast<T*>(res);
		return map->*member;
	}
	template <class T, class V>
	std::optional<V> get(V T::*member) const {
		Lock _{ mutex };
		auto current = this;
		while (current != nullptr) {
			auto iter = current->find(T::name);
			if (iter != current->properties.end()) {
				T const& map = static_cast<T const&>(*iter->properties);
				if (map.isSet(member)) {
					return map.*member;
				}
			}
			current = current->parent.getPtr();
		}
		return std::optional<V>{};
	}
	template <class T, class V>
	std::vector<V> stack(V T::*member) const {
		Lock _{ mutex };
		auto current = this;
		std::vector<V> res;
		while (current != nullptr) {
			auto iter = current->find(T::name);
			if (iter != current->properties.end()) {
				T const& map = static_cast<T const&>(*iter->properties);
				if (map.isSet(member)) {
					res.push_back(map.*member);
				}
			}
			current = current->parent.getPtr();
		}
		return res;
	}
	Reference<ActorLineage> getParent() { return parent; }
};

// A Reference subclass with knowledge on the true owner of the contained
// ActorLineage object. This class enables lazy allocation of ActorLineages.
// LineageReference copies are generally made by child actors, which should
// create their own ActorLineage when attempting to add lineage properties (see
// getCurrentLineage()).
class LineageReference : public Reference<ActorLineage> {
public:
	LineageReference() : Reference<ActorLineage>(nullptr), actorName_(""), allocated_(false) {}
	explicit LineageReference(ActorLineage* ptr) : Reference<ActorLineage>(ptr), actorName_(""), allocated_(false) {}
	LineageReference(const LineageReference& r) : Reference<ActorLineage>(r), actorName_(""), allocated_(false) {}
	LineageReference(LineageReference&& r)
	  : Reference<ActorLineage>(std::forward<LineageReference>(r)), actorName_(r.actorName_), allocated_(r.allocated_) {
		r.actorName_ = "";
		r.allocated_ = false;
	}

	void setActorName(const char* name) { actorName_ = name; }
	const char* actorName() const { return actorName_; }
	void allocate() {
		Reference<ActorLineage>::setPtrUnsafe(new ActorLineage());
		allocated_ = true;
	}
	bool isAllocated() const { return allocated_; }

private:
	// The actor name has to be a property of the LineageReference because all
	// actors store their own LineageReference copy, but not all actors point
	// to their own ActorLineage.
	const char* actorName_;
	bool allocated_;
};

extern std::atomic<bool> startSampling;
extern thread_local LineageReference* currentLineage;

#ifdef ENABLE_SAMPLING
LineageReference getCurrentLineage();
#else
#define getCurrentLineage()                                                                                            \
	if (false)                                                                                                         \
	(*currentLineage)
#endif
void replaceLineage(LineageReference* lineage);

struct StackLineage : LineageProperties<StackLineage> {
	static const std::string_view name;
	StringRef actorName;
};

#ifdef ENABLE_SAMPLING
struct LineageScope {
	LineageReference* oldLineage;
	LineageScope(LineageReference* with) : oldLineage(currentLineage) { replaceLineage(with); }
	~LineageScope() { replaceLineage(oldLineage); }
};
#endif

// This class can be used in order to modify all lineage properties
// of actors created within a (non-actor) scope
struct LocalLineage {
	LineageReference lineage;
	LineageReference* oldLineage;
	LocalLineage() {
#ifdef ENABLE_SAMPLING
		lineage.allocate();
		oldLineage = currentLineage;
		replaceLineage(&lineage);
#endif
	}
	~LocalLineage() {
#ifdef ENABLE_SAMPLING
		replaceLineage(oldLineage);
#endif
	}
};

// SAV is short for Single Assignment Variable: It can be assigned for only once!
template <class T>
struct SAV : private Callback<T>, FastAllocated<SAV<T>> {
	int promises; // one for each promise (and one for an active actor if this is an actor)
	int futures; // one for each future and one more if there are any callbacks

private:
	typename std::aligned_storage<sizeof(T), __alignof(T)>::type value_storage;

public:
	Error error_state;

	enum { UNSET_ERROR_CODE = -3, NEVER_ERROR_CODE, SET_ERROR_CODE };

	T& value() { return *(T*)&value_storage; }

	SAV(int futures, int promises)
	  : promises(promises), futures(futures), error_state(Error::fromCode(UNSET_ERROR_CODE)) {
		Callback<T>::prev = Callback<T>::next = this;
	}
	~SAV() {
		if (int16_t(error_state.code()) == SET_ERROR_CODE)
			value().~T();
	}

	bool isSet() const { return int16_t(error_state.code()) > NEVER_ERROR_CODE; }
	bool canBeSet() const { return int16_t(error_state.code()) == UNSET_ERROR_CODE; }
	bool isError() const { return int16_t(error_state.code()) > SET_ERROR_CODE; }

	T const& get() const {
		ASSERT(isSet());
		if (isError())
			throw error_state;
		return *(T const*)&value_storage;
	}

	template <class U>
	void send(U&& value) {
		ASSERT(canBeSet());
		new (&value_storage) T(std::forward<U>(value));
		this->error_state = Error::fromCode(SET_ERROR_CODE);

		while (Callback<T>::next != this) {
			Callback<T>::next->fire(this->value());
		}
	}

	void send(Never) {
		ASSERT(canBeSet());
		this->error_state = Error::fromCode(NEVER_ERROR_CODE);
	}

	void sendError(Error err) {
		ASSERT(canBeSet() && int16_t(err.code()) > 0);
		this->error_state = err;
		while (Callback<T>::next != this) {
			Callback<T>::next->error(err);
		}
	}

	template <class U>
	void sendAndDelPromiseRef(U&& value) {
		ASSERT(canBeSet());
		if (promises == 1 && !futures) {
			// No one is left to receive the value, so we can just die
			destroy();
			return;
		}

		new (&value_storage) T(std::forward<U>(value));
		finishSendAndDelPromiseRef();
	}

	void finishSendAndDelPromiseRef() {
		// Call only after value_storage has already been initialized!
		this->error_state = Error::fromCode(SET_ERROR_CODE);
		while (Callback<T>::next != this)
			Callback<T>::next->fire(this->value());

		if (!--promises && !futures)
			destroy();
	}

	void sendAndDelPromiseRef(Never) {
		ASSERT(canBeSet());
		this->error_state = Error::fromCode(NEVER_ERROR_CODE);
		if (!--promises && !futures)
			destroy();
	}

	void sendErrorAndDelPromiseRef(Error err) {
		ASSERT(canBeSet() && int16_t(err.code()) > 0);
		if (promises == 1 && !futures) {
			// No one is left to receive the value, so we can just die
			destroy();
			return;
		}

		this->error_state = err;
		while (Callback<T>::next != this)
			Callback<T>::next->error(err);

		if (!--promises && !futures)
			destroy();
	}

	// this is only used for C++ coroutines
	void finishSendErrorAndDelPromiseRef() {
		if (promises == 1 && !futures) {
			// No one is left to receive the value, so we can just die
			destroy();
			return;
		}
		while (Callback<T>::next != this)
			Callback<T>::next->error(this->error_state);

		if (!--promises && !futures)
			destroy();
	}

	void addPromiseRef() { promises++; }
	void addFutureRef() { futures++; }

	void delPromiseRef() {
		if (promises == 1) {
			if (futures && canBeSet()) {
				sendError(broken_promise());
				ASSERT(promises == 1); // Once there is only one promise, there is no one else with the right to change
				                       // the promise reference count
			}
			promises = 0;
			if (!futures)
				destroy();
		} else
			--promises;
	}
	void delFutureRef() {
		if (!--futures) {
			if (promises)
				cancel();
			else
				destroy();
		}
	}

	int getFutureReferenceCount() const { return futures; }
	int getPromiseReferenceCount() const { return promises; }

	// Derived classes should override destroy.
	virtual void destroy() {
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdelete-non-virtual-dtor"
		delete this;
#pragma clang diagnostic pop
	}

	virtual void cancel() {}

	void addCallbackAndDelFutureRef(Callback<T>* cb) {
		// We are always *logically* dropping one future reference from this, but if we are adding a first callback
		// we also need to add one (since futures is defined as being +1 if there are any callbacks), so net nothing
		if (Callback<T>::next != this)
			delFutureRef();

		cb->insert(this);
	}

	void addYieldedCallbackAndDelFutureRef(Callback<T>* cb) {
		// Same contract as addCallbackAndDelFutureRef, except that the callback is placed at the end of the callback
		// chain rather than at the beginning
		if (Callback<T>::next != this)
			delFutureRef();

		cb->insertBack(this);
	}

	void addCallbackChainAndDelFutureRef(Callback<T>* cb) {
		if (Callback<T>::next != this)
			delFutureRef();
		cb->insertChain(this);
	}

	void unwait() override { delFutureRef(); }
	void fire(T const&) override { ASSERT(false); }
};

template <class T>
class Promise;

#ifdef WITH_SWIFT
#ifndef SWIFT_HIDE_CHECKED_CONTINUTATION
using flow_swift::FlowCheckedContinuation;

template<class T>
class
#ifdef WITH_SWIFT
SWIFT_CONFORMS_TO_PROTOCOL(flow_swift.FlowCallbackForSwiftContinuationT)
#endif
FlowCallbackForSwiftContinuation : Callback<T> {
public:
	using SwiftCC = flow_swift::FlowCheckedContinuation<T>;
	using AssociatedFuture = Future<T>;

private:
	SwiftCC continuationInstance;

public:
	FlowCallbackForSwiftContinuation() : continuationInstance(SwiftCC::init()) {}

	void set(const void* _Nonnull pointerToContinuationInstance, Future<T> f, const void* _Nonnull thisPointer) {
		// Verify Swift did not make a copy of the `self` value for this method
		// call.
		assert(this == thisPointer);

		// FIXME: Propagate `SwiftCC` to Swift using forward
		// interop, without relying on passing it via a `void *`
		// here. That will let us avoid this hack.
		const void* _Nonnull opaqueStorage = pointerToContinuationInstance;
		static_assert(sizeof(SwiftCC) == sizeof(const void*));
		const SwiftCC ccCopy(*reinterpret_cast<const SwiftCC*>(&opaqueStorage));
		// Set the continuation instance.
		continuationInstance.set(ccCopy);
		// Add this callback to the future.
		f.addCallbackAndClear(this);
	}

	void fire(const T& value) override {
		Callback<T>::remove();
		Callback<T>::next = nullptr;
		continuationInstance.resume(value);
	}
	void error(Error error) override {
		Callback<T>::remove();
		Callback<T>::next = nullptr;
		continuationInstance.resumeThrowing(error);
	}
	void unwait() override {
		// TODO(swift): implement
	}
};
#endif /* SWIFT_HIDE_CHECKED_CONTINUATION */
#endif /* WITH_SWIFT*/

template <class T>
class SWIFT_SENDABLE
#ifndef SWIFT_HIDE_CHECKED_CONTINUTATION
#ifdef WITH_SWIFT
SWIFT_CONFORMS_TO_PROTOCOL(flow_swift.FlowFutureOps)
#endif
#endif
    Future {
public:
	using Element = T;
#ifdef WITH_SWIFT
#ifndef SWIFT_HIDE_CHECKED_CONTINUTATION
	using FlowCallbackForSwiftContinuation = FlowCallbackForSwiftContinuation<T>;
#endif
#endif /* WITH_SWIFT */

	T const& get() const { return sav->get(); }
	T getValue() const { return get(); }

	bool isValid() const { return sav != nullptr; }
	bool isReady() const { return sav->isSet(); }
	bool isError() const { return sav->isError(); }
	// returns true if get can be called on this future (counterpart of canBeSet on Promises)
	bool canGet() const { return isValid() && isReady() && !isError(); }
	Error& getError() const {
		ASSERT(isError());
		return sav->error_state;
	}

	Future() : sav(nullptr) {}
	Future(const Future<T>& rhs) : sav(rhs.sav) {
		if (sav)
			sav->addFutureRef();
	}
	Future(Future<T>&& rhs) noexcept : sav(rhs.sav) { rhs.sav = nullptr; }
	Future(const T& presentValue) : sav(new SAV<T>(1, 0)) { sav->send(presentValue); }
	Future(T&& presentValue) : sav(new SAV<T>(1, 0)) { sav->send(std::move(presentValue)); }
	Future(Never) : sav(new SAV<T>(1, 0)) { sav->send(Never()); }
	Future(const Error& error) : sav(new SAV<T>(1, 0)) { sav->sendError(error); }

#ifndef NO_INTELLISENSE
	template <class U>
	Future(const U&, typename std::enable_if<std::is_assignable<T, U>::value, int*>::type = 0) {}
#endif

	~Future() {
		if (sav)
			sav->delFutureRef();
	}
	void operator=(const Future<T>& rhs) {
		if (rhs.sav)
			rhs.sav->addFutureRef();
		if (sav)
			sav->delFutureRef();
		sav = rhs.sav;
	}
	void operator=(Future<T>&& rhs) noexcept {
		if (sav != rhs.sav) {
			if (sav)
				sav->delFutureRef();
			sav = rhs.sav;
			rhs.sav = nullptr;
		}
	}
	bool operator==(const Future& rhs) { return rhs.sav == sav; }
	bool operator!=(const Future& rhs) { return rhs.sav != sav; }

	void cancel() {
		if (sav)
			sav->cancel();
	}

	void addCallbackAndClear(Callback<T>* _Nonnull cb) {
		sav->addCallbackAndDelFutureRef(cb);
		sav = nullptr;
	}

	void addYieldedCallbackAndClear(Callback<T>* _Nonnull cb) {
		sav->addYieldedCallbackAndDelFutureRef(cb);
		sav = nullptr;
	}

	void addCallbackChainAndClear(Callback<T>* cb) {
		sav->addCallbackChainAndDelFutureRef(cb);
		sav = nullptr;
	}

	int getFutureReferenceCount() const { return sav->getFutureReferenceCount(); }
	int getPromiseReferenceCount() const { return sav->getPromiseReferenceCount(); }

	explicit Future(SAV<T>* sav) : sav(sav) {}

private:
	SAV<T>* sav;
	friend class Promise<T>;
};

// This class is used by the flow compiler when generating code around wait statements to avoid confusing situations
// regarding Futures.
//
// For example, the following is legal with Future but not with StrictFuture:
//
// Future<T> x = ...
// T result = wait(x); // This is the correct code
// Future<T> result = wait(x); // This is legal if wait() generates Futures, but it's probably wrong. It's a compilation
// error if wait() generates StrictFutures.
template <class T>
class SWIFT_SENDABLE StrictFuture : public Future<T> {
public:
	inline StrictFuture(Future<T> const& f) : Future<T>(f) {}
	inline StrictFuture(Never n) : Future<T>(n) {}

private:
	StrictFuture(T t) {}
	StrictFuture(Error e) {}
};

template <class T>
class SWIFT_SENDABLE Promise final {
public:
	template <class U>
	void send(U&& value) const {
		sav->send(std::forward<U>(value));
	}

	// Swift can't call method that takes in a universal references (U&&),
	// so provide a callable `send` method that copies the value.
	void sendCopy(const T& valueCopy) const SWIFT_NAME(send(_:)) { sav->send(valueCopy); }

	template <class E>
	void sendError(const E& exc) const {
		sav->sendError(exc);
	}

	SWIFT_CXX_IMPORT_UNSAFE Future<T> getFuture() const {
		sav->addFutureRef();
		return Future<T>(sav);
	}

	bool isSet() const { return sav->isSet(); }
	bool canBeSet() const { return sav->canBeSet(); }
	bool isError() const { return sav->isError(); }

	bool isValid() const { return sav != nullptr; }
	Promise() : sav(new SAV<T>(0, 1)) {}
	Promise(const Promise& rhs) : sav(rhs.sav) { sav->addPromiseRef(); }
	Promise(Promise&& rhs) noexcept : sav(rhs.sav) { rhs.sav = 0; }

	~Promise() {
		if (sav)
			sav->delPromiseRef();
	}

	void operator=(const Promise& rhs) {
		if (rhs.sav)
			rhs.sav->addPromiseRef();
		if (sav)
			sav->delPromiseRef();
		sav = rhs.sav;
	}
	void operator=(Promise&& rhs) noexcept {
		if (sav != rhs.sav) {
			if (sav)
				sav->delPromiseRef();
			sav = rhs.sav;
			rhs.sav = 0;
		}
	}
	void reset() { *this = Promise<T>(); }
	void swap(Promise& other) { std::swap(sav, other.sav); }

	// Beware, these operations are very unsafe
	SAV<T>* extractRawPointer() {
		auto ptr = sav;
		sav = nullptr;
		return ptr;
	}
	explicit Promise<T>(SAV<T>* ptr) : sav(ptr) {}

	int getFutureReferenceCount() const { return sav->getFutureReferenceCount(); }
	int getPromiseReferenceCount() const { return sav->getPromiseReferenceCount(); }

private:
	SAV<T>* sav;
};

template <class T>
struct NotifiedQueue : private SingleCallback<T>
#ifndef WITH_SWIFT
   , FastAllocated<NotifiedQueue<T>> // FIXME(swift): Swift can't deal with this type yet
#endif /* WITH_SWIFT */
{
	int promises; // one for each promise (and one for an active actor if this is an actor)
	int futures; // one for each future and one more if there are any callbacks

	// Invariant: SingleCallback<T>::next==this || (queue.empty() && !error.isValid())
	std::queue<T, Deque<T>> queue;
	Promise<Void> onEmpty;
	Error error;
	Promise<Void> onError;

	NotifiedQueue(int futures, int promises)
	  : promises(promises), futures(futures), onEmpty(nullptr), onError(nullptr) {
		SingleCallback<T>::next = this;
	}

	virtual ~NotifiedQueue() = default;

	bool isReady() const { return !queue.empty() || error.isValid(); }
	bool isError() const { return queue.empty() && error.isValid(); } // the *next* thing queued is an error
	bool hasError() const { return error.isValid(); } // there is an error queued
	uint32_t size() const { return queue.size(); }

	virtual T pop() {
		if (queue.empty()) {
			if (error.isValid())
				throw error;
			throw internal_error();
		}
		auto copy = std::move(queue.front());
		queue.pop();
		if (onEmpty.isValid() && queue.empty()) {
			Promise<Void> hold = onEmpty;
			onEmpty = Promise<Void>(nullptr);
			hold.send(Void());
		}
		return copy;
	}

	template <class U>
	void send(U&& value) {
		if (error.isValid())
			return;

		if (SingleCallback<T>::next != this) {
			SingleCallback<T>::next->fire(std::forward<U>(value));
		} else {
			queue.emplace(std::forward<U>(value));
		}
	}

	void sendError(Error err) {
		if (error.isValid())
			return;

		ASSERT(this->error.code() != error_code_success && this->error.code() != error_code_end_of_stream);
		this->error = err;

		// end_of_stream error is "expected", don't terminate reading stream early for this
		// onError must be triggered before callback, otherwise callback could cause delPromiseRef/delFutureRef. This
		// could destroy *this* in the callback, causing onError to be referenced after this object is destroyed.
		if (err.code() != error_code_end_of_stream && err.code() != error_code_broken_promise && onError.isValid()) {
			ASSERT(onError.canBeSet());
			onError.sendError(err);
		}

		if (shouldFireImmediately()) {
			SingleCallback<T>::next->error(err);
		}
	}

	void addPromiseRef() { promises++; }
	void addFutureRef() { futures++; }

	void delPromiseRef() {
		if (!--promises) {
			if (futures) {
				sendError(broken_promise());
			} else
				destroy();
		}
	}
	void delFutureRef() {
		if (!--futures) {
			if (promises)
				cancel();
			else
				destroy();
		}
	}

	int getFutureReferenceCount() const { return futures; }
	int getPromiseReferenceCount() const { return promises; }

	virtual void destroy() { delete this; }
	virtual void cancel() {}

	void addCallbackAndDelFutureRef(SingleCallback<T>* cb) {
		ASSERT(SingleCallback<T>::next == this);
		cb->insert(this);
	}
	virtual void unwait() override { delFutureRef(); }
	virtual void fire(T const&) override { ASSERT(false); }
	virtual void fire(T&&) override { ASSERT(false); }

protected:
	T popImpl() {
		if (queue.empty()) {
			if (error.isValid())
				throw error;
			throw internal_error();
		}
		auto copy = std::move(queue.front());
		queue.pop();
		if (onEmpty.isValid() && queue.empty()) {
			Promise<Void> hold = onEmpty;
			onEmpty = Promise<Void>(nullptr);
			hold.send(Void());
		}
		return copy;
	}

	bool shouldFireImmediately() { return SingleCallback<T>::next != this; }
};

template <class T>
class SWIFT_SENDABLE FutureStream {
public:
	bool isValid() const { return queue != nullptr; }
	bool isReady() const { return queue->isReady(); }
	bool isError() const {
		// This means that the next thing to be popped is an error - it will be false if there is an error in the stream
		// but some actual data first
		return queue->isError();
	}
	void addCallbackAndClear(SingleCallback<T>* cb) {
		queue->addCallbackAndDelFutureRef(cb);
		queue = nullptr;
	}
	FutureStream() : queue(nullptr) {}
	FutureStream(const FutureStream& rhs) : queue(rhs.queue) { queue->addFutureRef(); }
	FutureStream(FutureStream&& rhs) noexcept : queue(rhs.queue) { rhs.queue = 0; }
	~FutureStream() {
		if (queue)
			queue->delFutureRef();
	}
	void operator=(const FutureStream& rhs) {
		rhs.queue->addFutureRef();
		if (queue)
			queue->delFutureRef();
		queue = rhs.queue;
	}
	void operator=(FutureStream&& rhs) noexcept {
		if (rhs.queue != queue) {
			if (queue)
				queue->delFutureRef();
			queue = rhs.queue;
			rhs.queue = nullptr;
		}
	}
	bool operator==(const FutureStream& rhs) { return rhs.queue == queue; }
	bool operator!=(const FutureStream& rhs) { return rhs.queue != queue; }

	// FIXME: remove annotation after https://github.com/apple/swift/issues/64316 is fixed.
	T pop() __attribute__((swift_attr("import_unsafe"))) { return queue->pop(); }
	Error getError() const {
		ASSERT(queue->isError());
		return queue->error;
	}

	explicit FutureStream(NotifiedQueue<T>* queue) : queue(queue) {}

private:
	NotifiedQueue<T>* queue;
};

template <class Request>
decltype(std::declval<Request>().reply) const& getReplyPromise(Request const& r) {
	return r.reply;
}

template <class Request>
auto const& getReplyPromiseStream(Request const& r) {
	return r.reply;
}

// Neither of these implementations of REPLY_TYPE() works on both MSVC and g++, so...
#ifdef __GNUG__
#define REPLY_TYPE(RequestType) decltype(getReplyPromise(std::declval<RequestType>()).getFuture().getValue())
// #define REPLY_TYPE(RequestType) decltype( getReplyFuture( std::declval<RequestType>() ).getValue() )
#else
template <class T>
struct ReplyType {
	// Doing this calculation directly in the return value declaration for PromiseStream<T>::getReply()
	//   breaks IntelliSense in VS2010; this is a workaround.
	typedef decltype(std::declval<T>().reply.getFuture().getValue()) Type;
};
template <class T>
class ReplyPromise;
template <class T>
struct ReplyType<ReplyPromise<T>> {
	typedef T Type;
};
#define REPLY_TYPE(RequestType) typename ReplyType<RequestType>::Type
#endif

template <class T>
class SWIFT_SENDABLE PromiseStream {
public:
	// stream.send( request )
	//   Unreliable at most once delivery: Delivers request unless there is a connection failure (zero or one times)

	void send(const T& value) { queue->send(value); }
	void sendCopy(T value) { queue->send(value); }
	void send(T&& value) { queue->send(std::move(value)); }
	void sendError(const Error& error) { queue->sendError(error); }

	// stream.getReply( request )
	//   Reliable at least once delivery: Eventually delivers request at least once and returns one of the replies if
	//   communication is possible.  Might deliver request
	//      more than once.
	//   If a reply is returned, request was or will be delivered one or more times.
	//   If cancelled, request was or will be delivered zero or more times.
	template <class X>
	Future<REPLY_TYPE(X)> getReply(const X& value) {
		send(value);
		return getReplyPromise(value).getFuture();
	}
	template <class X>
	Future<REPLY_TYPE(X)> getReply(const X& value, TaskPriority taskID) {
		setReplyPriority(value, taskID);
		return getReplyPromise(value).getFuture();
	}

	template <class X>
	Future<X> getReply() {
		return getReply(Promise<X>());
	}
	template <class X>
	Future<X> getReplyWithTaskID(TaskPriority taskID) {
		Promise<X> reply;
		reply.getEndpoint(taskID);
		return getReply(reply);
	}

	// Not const, because this function gives mutable
	// access to queue
	SWIFT_CXX_IMPORT_UNSAFE FutureStream<T> getFuture() {
		queue->addFutureRef();
		return FutureStream<T>(queue);
	}
	PromiseStream() : queue(new NotifiedQueue<T>(0, 1)) {}
	PromiseStream(const PromiseStream& rhs) : queue(rhs.queue) { queue->addPromiseRef(); }
	PromiseStream(PromiseStream&& rhs) noexcept : queue(rhs.queue) { rhs.queue = 0; }
	void operator=(const PromiseStream& rhs) {
		rhs.queue->addPromiseRef();
		if (queue)
			queue->delPromiseRef();
		queue = rhs.queue;
	}
	void operator=(PromiseStream&& rhs) noexcept {
		if (queue != rhs.queue) {
			if (queue)
				queue->delPromiseRef();
			queue = rhs.queue;
			rhs.queue = 0;
		}
	}
	~PromiseStream() {
		if (queue)
			queue->delPromiseRef();
		// queue = (NotifiedQueue<T>*)0xdeadbeef;
	}

	bool operator==(const PromiseStream<T>& rhs) const { return queue == rhs.queue; }
	bool isReady() const { return queue->isReady(); }
	bool isEmpty() const { return !queue->isReady(); }

	Future<Void> onEmpty() {
		if (isEmpty()) {
			return Void();
		}
		if (!queue->onEmpty.isValid()) {
			queue->onEmpty = Promise<Void>();
		}
		return queue->onEmpty.getFuture();
	}

	int getFutureReferenceCount() const { return queue->getFutureReferenceCount(); }
	int getPromiseReferenceCount() const { return queue->getPromiseReferenceCount(); }

private:
	NotifiedQueue<T>* queue;
};

// Neither of these implementations of REPLY_TYPE() works on both MSVC and g++, so...
#ifdef __GNUG__
#define REPLYSTREAM_TYPE(RequestType) decltype(getReplyPromiseStream(std::declval<RequestType>()).getFuture().pop())
#else
template <class T>
struct ReplyStreamType {
	// Doing this calculation directly in the return value declaration for PromiseStream<T>::getReply()
	//   breaks IntelliSense in VS2010; this is a workaround.
	typedef decltype(std::declval<T>().reply.getFuture().pop()) Type;
};
template <class T>
class ReplyPromiseStream;
template <class T>
struct ReplyStreamType<ReplyPromiseStream<T>> {
	typedef T Type;
};
#define REPLYSTREAM_TYPE(RequestType) typename ReplyStreamType<RequestType>::Type
#endif

// extern int actorCount;

template <class T>
static inline void destruct(T& t) {
	t.~T();
}

template <class ReturnValue>
struct Actor : SAV<ReturnValue> {
#ifdef ENABLE_SAMPLING
	LineageReference lineage = *currentLineage;
#endif
	int8_t actor_wait_state; // -1 means actor is cancelled; 0 means actor is not waiting; 1-N mean waiting in callback
	                         // group #

	Actor() : SAV<ReturnValue>(1, 1), actor_wait_state(0) { /*++actorCount;*/
	}
	// ~Actor() { --actorCount; }

#ifdef ENABLE_SAMPLING
	LineageReference* lineageAddr() { return std::addressof(lineage); }
#endif
};

template <>
struct Actor<void> {
	// This specialization is for a void actor (one not returning a future, hence also uncancellable)

#ifdef ENABLE_SAMPLING
	LineageReference lineage = *currentLineage;
#endif
	int8_t actor_wait_state; // 0 means actor is not waiting; 1-N mean waiting in callback group #

	Actor() : actor_wait_state(0) { /*++actorCount;*/
	}
	// ~Actor() { --actorCount; }

#ifdef ENABLE_SAMPLING
	LineageReference* lineageAddr() { return std::addressof(lineage); }
#endif
};

template <class ActorType, int CallbackNumber, class ValueType>
struct ActorCallback : Callback<ValueType> {
	virtual void fire(ValueType const& value) override {
#ifdef ENABLE_SAMPLING
		LineageScope _(static_cast<ActorType*>(this)->lineageAddr());
#endif
		static_cast<ActorType*>(this)->a_callback_fire(this, value);
	}
	virtual void error(Error e) override {
#ifdef ENABLE_SAMPLING
		LineageScope _(static_cast<ActorType*>(this)->lineageAddr());
#endif
		static_cast<ActorType*>(this)->a_callback_error(this, e);
	}
};

template <class ActorType, int CallbackNumber, class ValueType>
struct ActorSingleCallback : SingleCallback<ValueType> {
	void fire(ValueType const& value) override {
#ifdef ENABLE_SAMPLING
		LineageScope _(static_cast<ActorType*>(this)->lineageAddr());
#endif
		static_cast<ActorType*>(this)->a_callback_fire(this, value);
	}
	void fire(ValueType&& value) override {
#ifdef ENABLE_SAMPLING
		LineageScope _(static_cast<ActorType*>(this)->lineageAddr());
#endif
		static_cast<ActorType*>(this)->a_callback_fire(this, std::move(value));
	}
	void error(Error e) override {
#ifdef ENABLE_SAMPLING
		LineageScope _(static_cast<ActorType*>(this)->lineageAddr());
#endif
		static_cast<ActorType*>(this)->a_callback_error(this, e);
	}
};
inline double now() {
	return g_network->now();
}
inline Future<Void> delay(double seconds, TaskPriority taskID = TaskPriority::DefaultDelay) {
	return g_network->delay(seconds, taskID);
}
inline Future<Void> orderedDelay(double seconds, TaskPriority taskID = TaskPriority::DefaultDelay) {
	return g_network->orderedDelay(seconds, taskID);
}
inline void _swiftEnqueue(void* task) {
	return g_network->_swiftEnqueue(task);
}
inline Future<Void> delayUntil(double time, TaskPriority taskID = TaskPriority::DefaultDelay) {
	return g_network->delay(std::max(0.0, time - g_network->now()), taskID);
}
inline Future<Void> delayJittered(double seconds, TaskPriority taskID = TaskPriority::DefaultDelay) {
	return g_network->delay(seconds * (FLOW_KNOBS->DELAY_JITTER_OFFSET +
	                                   FLOW_KNOBS->DELAY_JITTER_RANGE * deterministicRandom()->random01()),
	                        taskID);
}
inline Future<Void> yield(TaskPriority taskID = TaskPriority::DefaultYield) {
	return g_network->yield(taskID);
}
inline bool check_yield(TaskPriority taskID = TaskPriority::DefaultYield) {
	return g_network->check_yield(taskID);
}

void bindDeterministicRandomToOpenssl();

#ifdef WITH_SWIFT
#pragma clang diagnostic pop
#endif

#include "flow/Coroutines.h"
#include "flow/genericactors.actor.h"
#endif
