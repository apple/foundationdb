/*
 * flow.h
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

#ifndef FLOW_FLOW_H
#define FLOW_FLOW_H
#include "flow/Arena.h"
#include "flow/FastRef.h"
#pragma once

#ifdef _MSC_VER
#pragma warning(disable : 4244 4267) // SOMEDAY: Carefully check for integer overflow issues (e.g. size_t to int
// conversions like this suppresses)
#pragma warning(disable : 4345)
#pragma warning(error : 4239)
#endif

#include <vector>
#include <queue>
#include <stack>
#include <map>
#include <unordered_map>
#include <set>
#include <functional>
#include <iostream>
#include <string>
#include <string_view>
#include <utility>
#include <algorithm>
#include <memory>
#include <mutex>

#include "flow/Platform.h"
#include "flow/FastAlloc.h"
#include "flow/IRandom.h"
#include "flow/serialize.h"
#include "flow/Deque.h"
#include "flow/ThreadPrimitives.h"
#include "flow/network.h"
#include "flow/FileIdentifier.h"
#include "flow/WriteOnlySet.h"

#include <boost/version.hpp>

#define TEST(condition)                                                                                                \
	if (!(condition)) {                                                                                                \
	} else {                                                                                                           \
		static BaseTraceEvent* __test =                                                                                \
		    &(TraceEvent(intToSeverity(FLOW_KNOBS->CODE_COV_TRACE_EVENT_SEVERITY), "CodeCoverage")                     \
		          .detail("File", __FILE__)                                                                            \
		          .detail("Line", __LINE__)                                                                            \
		          .detail("Condition", #condition));                                                                   \
		(void)__test;                                                                                                  \
	}

/*
usage:
if (BUGGIFY) (
// code here is executed on some runs (with probability P_BUGGIFIED_SECTION_ACTIVATED),
//  sometimes --
)
*/

extern std::vector<double> P_BUGGIFIED_SECTION_ACTIVATED, P_BUGGIFIED_SECTION_FIRES;
extern double P_EXPENSIVE_VALIDATION;
enum class BuggifyType : uint8_t { General = 0, Client };
bool isBuggifyEnabled(BuggifyType type);
void clearBuggifySections(BuggifyType type);
int getSBVar(std::string const& file, int line, BuggifyType);
void enableBuggify(bool enabled,
                   BuggifyType type); // Currently controls buggification and (randomized) expensive validation
bool validationIsEnabled(BuggifyType type);

#define BUGGIFY_WITH_PROB(x)                                                                                           \
	(getSBVar(__FILE__, __LINE__, BuggifyType::General) && deterministicRandom()->random01() < (x))
#define BUGGIFY BUGGIFY_WITH_PROB(P_BUGGIFIED_SECTION_FIRES[int(BuggifyType::General)])
#define EXPENSIVE_VALIDATION                                                                                           \
	(validationIsEnabled(BuggifyType::General) && deterministicRandom()->random01() < P_EXPENSIVE_VALIDATION)

extern Optional<uint64_t> parse_with_suffix(std::string const& toparse, std::string const& default_unit = "");
extern Optional<uint64_t> parseDuration(std::string const& str, std::string const& defaultUnit = "");
extern std::string format(const char* form, ...);

// On success, returns the number of characters written. On failure, returns a negative number.
extern int vsformat(std::string& outputString, const char* form, va_list args);

extern Standalone<StringRef> strinc(StringRef const& str);
extern StringRef strinc(StringRef const& str, Arena& arena);
extern Standalone<StringRef> addVersionStampAtEnd(StringRef const& str);
extern StringRef addVersionStampAtEnd(StringRef const& str, Arena& arena);

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

	template <class R>
	ErrorOr<R> map(std::function<R(T)> f) const& {
		return present() ? ErrorOr<R>(f(get())) : ErrorOr<R>(getError());
	}
	template <class R>
	ErrorOr<R> map(std::function<R(T)> f) && {
		return present() ? ErrorOr<R>(f(std::move(*this).get())) : ErrorOr<R>(getError());
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

	int countCallbacks() {
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
	// within the properies class.
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
	const char* actorName() { return actorName_; }
	void allocate() {
		Reference<ActorLineage>::setPtrUnsafe(new ActorLineage());
		allocated_ = true;
	}
	bool isAllocated() { return allocated_; }

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

template <class T>
class Future {
public:
	T const& get() const { return sav->get(); }
	T getValue() const { return get(); }

	bool isValid() const { return sav != 0; }
	bool isReady() const { return sav->isSet(); }
	bool isError() const { return sav->isError(); }
	// returns true if get can be called on this future (counterpart of canBeSet on Promises)
	bool canGet() const { return isValid() && isReady() && !isError(); }
	Error& getError() const {
		ASSERT(isError());
		return sav->error_state;
	}

	Future() : sav(0) {}
	Future(const Future<T>& rhs) : sav(rhs.sav) {
		if (sav)
			sav->addFutureRef();
		// if (sav->endpoint.isValid()) std::cout << "Future copied for " << sav->endpoint.key << std::endl;
	}
	Future(Future<T>&& rhs) noexcept : sav(rhs.sav) {
		rhs.sav = 0;
		// if (sav->endpoint.isValid()) std::cout << "Future moved for " << sav->endpoint.key << std::endl;
	}
	Future(const T& presentValue) : sav(new SAV<T>(1, 0)) { sav->send(presentValue); }
	Future(T&& presentValue) : sav(new SAV<T>(1, 0)) { sav->send(std::move(presentValue)); }
	Future(Never) : sav(new SAV<T>(1, 0)) { sav->send(Never()); }
	Future(const Error& error) : sav(new SAV<T>(1, 0)) { sav->sendError(error); }

#ifndef NO_INTELLISENSE
	template <class U>
	Future(const U&, typename std::enable_if<std::is_assignable<T, U>::value, int*>::type = 0) {}
#endif

	~Future() {
		// if (sav && sav->endpoint.isValid()) std::cout << "Future destroyed for " << sav->endpoint.key << std::endl;
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
			rhs.sav = 0;
		}
	}
	bool operator==(const Future& rhs) { return rhs.sav == sav; }
	bool operator!=(const Future& rhs) { return rhs.sav != sav; }

	void cancel() {
		if (sav)
			sav->cancel();
	}

	void addCallbackAndClear(Callback<T>* cb) {
		sav->addCallbackAndDelFutureRef(cb);
		sav = 0;
	}

	void addYieldedCallbackAndClear(Callback<T>* cb) {
		sav->addYieldedCallbackAndDelFutureRef(cb);
		sav = 0;
	}

	void addCallbackChainAndClear(Callback<T>* cb) {
		sav->addCallbackChainAndDelFutureRef(cb);
		sav = 0;
	}

	int getFutureReferenceCount() const { return sav->getFutureReferenceCount(); }
	int getPromiseReferenceCount() const { return sav->getPromiseReferenceCount(); }

	explicit Future(SAV<T>* sav) : sav(sav) {
		// if (sav->endpoint.isValid()) std::cout << "Future created for " << sav->endpoint.key << std::endl;
	}

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
class StrictFuture : public Future<T> {
public:
	inline StrictFuture(Future<T> const& f) : Future<T>(f) {}
	inline StrictFuture(Never n) : Future<T>(n) {}

private:
	StrictFuture(T t) {}
	StrictFuture(Error e) {}
};

template <class T>
class Promise final {
public:
	template <class U>
	void send(U&& value) const {
		sav->send(std::forward<U>(value));
	}
	template <class E>
	void sendError(const E& exc) const {
		sav->sendError(exc);
	}

	Future<T> getFuture() const {
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
struct NotifiedQueue : private SingleCallback<T>, FastAllocated<NotifiedQueue<T>> {
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

		ASSERT(this->error.code() != error_code_success);
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
class FutureStream {
public:
	bool isValid() const { return queue != 0; }
	bool isReady() const { return queue->isReady(); }
	bool isError() const {
		// This means that the next thing to be popped is an error - it will be false if there is an error in the stream
		// but some actual data first
		return queue->isError();
	}
	void addCallbackAndClear(SingleCallback<T>* cb) {
		queue->addCallbackAndDelFutureRef(cb);
		queue = 0;
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
			rhs.queue = 0;
		}
	}
	bool operator==(const FutureStream& rhs) { return rhs.queue == queue; }
	bool operator!=(const FutureStream& rhs) { return rhs.queue != queue; }

	T pop() { return queue->pop(); }
	Error getError() {
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
//#define REPLY_TYPE(RequestType) decltype( getReplyFuture( std::declval<RequestType>() ).getValue() )
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
class PromiseStream {
public:
	// stream.send( request )
	//   Unreliable at most once delivery: Delivers request unless there is a connection failure (zero or one times)

	void send(const T& value) const { queue->send(value); }
	void send(T&& value) const { queue->send(std::move(value)); }
	void sendError(const Error& error) const { queue->sendError(error); }

	// stream.getReply( request )
	//   Reliable at least once delivery: Eventually delivers request at least once and returns one of the replies if
	//   communication is possible.  Might deliver request
	//      more than once.
	//   If a reply is returned, request was or will be delivered one or more times.
	//   If cancelled, request was or will be delivered zero or more times.
	template <class X>
	Future<REPLY_TYPE(X)> getReply(const X& value) const {
		send(value);
		return getReplyPromise(value).getFuture();
	}
	template <class X>
	Future<REPLY_TYPE(X)> getReply(const X& value, TaskPriority taskID) const {
		setReplyPriority(value, taskID);
		return getReplyPromise(value).getFuture();
	}

	template <class X>
	Future<X> getReply() const {
		return getReply(Promise<X>());
	}
	template <class X>
	Future<X> getReplyWithTaskID(TaskPriority taskID) const {
		Promise<X> reply;
		reply.getEndpoint(taskID);
		return getReply(reply);
	}

	FutureStream<T> getFuture() const {
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

#include "flow/genericactors.actor.h"
#endif
