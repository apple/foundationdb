/*
 * Optional.h
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

#ifndef FLOW_OPTIONAL_H
#define FLOW_OPTIONAL_H

#include <optional>
#include <fmt/format.h>

#include "flow/Traceable.h"
#include "flow/FileIdentifier.h"
#include "flow/Error.h"
#include "flow/swift_support.h"

class Arena;

// Optional is a wrapper for std::optional. There
// are two primary reasons to use this wrapper instead
// of using std::optional directly:
//
// 1) Legacy: A lot of code was written using Optional before
//    std::optional was available.
// 2) When you call get but no value is present Optional gives an
//    assertion failure. std::optional, on the other hand, would
//    throw std::bad_optional_access. It is easier to debug assertion
//    failures, and FDB generally does not handle std exceptions, so
//    assertion failures are preferable. This is the main reason we
//    don't intend to use std::optional directly.
template <class T>
class SWIFT_CONFORMS_TO(flow_swift, FlowOptionalProtocol) Optional : public ComposedIdentifier<T, 4> {
public:
	using ValueType = T;
	using Wrapped = T;

	Optional() = default;

	template <class U>
	Optional(const U& t) : impl(std::in_place, t) {}
	Optional(T&& t) : impl(std::in_place, std::move(t)) {}

	/* This conversion constructor was nice, but combined with the prior constructor it means that Optional<int> can be
	converted to Optional<Optional<int>> in the wrong way (a non-present Optional<int> converts to a non-present
	Optional<Optional<int>>). Use .castTo<>() instead.

	template <class S> Optional(const Optional<S>& o) : valid(o.present()) {
	    if (valid)
	        new (&value) T(o.get());
	}
	*/

	Optional(Arena& a, const Optional<T>& o) {
		if (o.present())
			impl = std::make_optional<T>(a, o.get());
	}
	int expectedSize() const { return present() ? get().expectedSize() : 0; }

	template <class R>
	Optional<R> castTo() const {
		return map([](const T& v) { return (R)v; });
	}

private:
	template <class F>
	using MapRet = std::decay_t<std::invoke_result_t<F, T>>;

	template <class F>
	using EnableIfNotMemberPointer =
	    std::enable_if_t<!std::is_member_object_pointer_v<F> && !std::is_member_function_pointer_v<F>>;

public:
	// If the optional is set, calls the function f on the value and returns the value. Otherwise, returns an empty
	// optional.
	template <class F, typename = EnableIfNotMemberPointer<F>>
	Optional<MapRet<F>> map(const F& f) const& {
		return present() ? Optional<MapRet<F>>(f(get())) : Optional<MapRet<F>>();
	}
	template <class F, typename = EnableIfNotMemberPointer<F>>
	Optional<MapRet<F>> map(const F& f) && {
		return present() ? Optional<MapRet<F>>(f(std::move(*this).get())) : Optional<MapRet<F>>();
	}

	// Converts an Optional<T> to an Optional<R> of one of its value's members
	//
	// v.map(&T::member) is equivalent to v.map([](T v) { return v.member; })
	template <class R, class Rp = std::decay_t<R>>
	std::enable_if_t<std::is_class_v<T>, Optional<Rp>> map(
	    R std::conditional_t<std::is_class_v<T>, T, Void>::*member) const& {
		return present() ? Optional<Rp>(get().*member) : Optional<Rp>();
	}
	template <class R, class Rp = std::decay_t<R>>
	std::enable_if_t<std::is_class_v<T>, Optional<Rp>> map(
	    R std::conditional_t<std::is_class_v<T>, T, Void>::*member) && {
		return present() ? Optional<Rp>(std::move(*this).get().*member) : Optional<Rp>();
	}

	// Converts an Optional<T> to an Optional<R> of a value returned by a member function of T
	//
	// v.map(&T::memberFunc, arg1, arg2, ...) is equivalent to
	// v.map([](T v) { return v.memberFunc(arg1, arg2, ...); })
	template <class R, class... Args, class Rp = std::decay_t<R>>
	std::enable_if_t<std::is_class_v<T>, Optional<Rp>> map(
	    R (std::conditional_t<std::is_class_v<T>, T, Void>::*memberFunc)(Args...) const,
	    Args&&... args) const& {
		return present() ? Optional<Rp>((get().*memberFunc)(std::forward<Args>(args)...)) : Optional<Rp>();
	}
	template <class R, class... Args, class Rp = std::decay_t<R>>
	std::enable_if_t<std::is_class_v<T>, Optional<Rp>> map(
	    R (std::conditional_t<std::is_class_v<T>, T, Void>::*memberFunc)(Args...) const,
	    Args&&... args) && {
		return present() ? Optional<Rp>((std::move(*this).get().*memberFunc)(std::forward<Args>(args)...))
		                 : Optional<Rp>();
	}

	// Given T that is a pointer or pointer-like type to type P (e.g. T=P* or T=Reference<P>), converts an Optional<T>
	// to an Optional<R> of one its value's members. If the optional value is present and false-like (null), then
	// returns an empty Optional<R>.
	//
	// v.mapRef(&P::member) is equivalent to Optional<R>(v.get()->member) if v is present and non-null
	template <class P, class R, class Rp = std::decay_t<R>>
	std::enable_if_t<std::is_class_v<T> || std::is_pointer_v<T>, Optional<Rp>> mapRef(R P::*member) const& {
		if (!present() || !get()) {
			return Optional<Rp>();
		}

		P& p = *get();
		return p.*member;
	}

	// Given T that is a pointer or pointer-like type to type P (e.g. T=P* or T=Reference<P>), converts an Optional<T>
	// to an Optional<R> of a value returned by a member function of P. If the optional value is present and false-like
	// (null), then returns an empty Optional<R>.
	//
	// v.mapRef(&T::memberFunc, arg1, arg2, ...) is equivalent to Optional<R>(v.get()->memberFunc(arg1, arg2, ...)) if v
	// is present and non-null
	template <class P, class R, class... Args, class Rp = std::decay_t<R>>
	std::enable_if_t<std::is_class_v<T> || std::is_pointer_v<T>, Optional<Rp>> mapRef(R (P::*memberFunc)(Args...) const,
	                                                                                  Args&&... args) const& {
		if (!present() || !get()) {
			return Optional<Rp>();
		}
		P& p = *get();
		return (p.*memberFunc)(std::forward<Args>(args)...);
	}

	// Similar to map with a mapped type of Optional<R>, but flattens the result. For example, if the mapped result is
	// of type Optional<R>, map will return Optional<Optional<R>> while flatMap will return Optional<R>
	template <class... Args>
	auto flatMap(Args&&... args) const& {
		auto val = map(std::forward<Args>(args)...);
		using R = typename decltype(val)::ValueType::ValueType;

		if (val.present()) {
			return val.get();
		} else {
			return Optional<R>();
		}
	}
	template <class... Args>
	auto flatMap(Args&&... args) && {
		auto val = std::move(*this).map(std::forward<Args>(args)...);
		using R = typename decltype(val)::ValueType::ValueType;

		if (val.present()) {
			return val.get();
		} else {
			return Optional<R>();
		}
	}

	// Similar to mapRef with a mapped type of Optional<R>, but flattens the result. For example, if the mapped result
	// is of type Optional<R>, mapRef will return Optional<Optional<R>> while flatMapRef will return Optional<R>
	template <class... Args>
	auto flatMapRef(Args&&... args) const& {
		auto val = mapRef(std::forward<Args>(args)...);
		using R = typename decltype(val)::ValueType::ValueType;

		if (val.present()) {
			return val.get();
		} else {
			return Optional<R>();
		}
	}
	template <class... Args>
	auto flatMapRef(Args&&... args) && {
		auto val = std::move(*this).mapRef(std::forward<Args>(args)...);
		using R = typename decltype(val)::ValueType::ValueType;

		if (val.present()) {
			return val.get();
		} else {
			return Optional<R>();
		}
	}

	bool present() const { return impl.has_value(); }
	T& get() & {
		UNSTOPPABLE_ASSERT(impl.has_value());
		return impl.value();
	}
	T const& get() const& {
		UNSTOPPABLE_ASSERT(impl.has_value());
		return impl.value();
	}
	T&& get() && {
		UNSTOPPABLE_ASSERT(impl.has_value());
		return std::move(impl.value());
	}
	template <class U>
	T orDefault(U&& defaultValue) const& {
		return impl.value_or(std::forward<U>(defaultValue));
	}
	template <class U>
	T orDefault(U&& defaultValue) && {
		return std::move(impl).value_or(std::forward<U>(defaultValue));
	}

	// A combination of orDefault() and get()
	// Stores defaultValue in *this if *this was not present, then returns the stored value.
	// Can only be called on lvalues because returning a reference into an rvalue is dangerous.
	template <class U>
	T& withDefault(U&& defaultValue) & {
		if (!impl.has_value()) {
			impl.emplace(std::forward<U>(defaultValue));
		}
		return impl.value();
	}

	// Spaceship operator.  Treats not-present as less-than present.
	int compare(Optional const& rhs) const {
		if (present() == rhs.present()) {
			return present() ? get().compare(rhs.get()) : 0;
		}
		return present() ? 1 : -1;
	}

	bool operator==(Optional const& o) const { return impl == o.impl; }
	bool operator!=(Optional const& o) const { return !(*this == o); }
	// Ordering: If T is ordered, then Optional() < Optional(t) and (Optional(u)<Optional(v))==(u<v)
	bool operator<(Optional const& o) const { return impl < o.impl; }

	const T* operator->() const { return &get(); }
	T* operator->() { return &get(); }
	const T& operator*() const& { return get(); }
	T& operator*() & { return get(); }
	T&& operator*() && { return get(); }

	void reset() { impl.reset(); }
	size_t hash() const { return hashFunc(impl); }

private:
	static inline std::hash<std::optional<T>> hashFunc{};
	std::optional<T> impl;
};

template <class T>
struct Traceable<Optional<T>> : std::conditional<Traceable<T>::value, std::true_type, std::false_type>::type {
	static std::string toString(const Optional<T>& value) {
		return value.present() ? Traceable<T>::toString(value.get()) : "[not set]";
	}
};

template <typename T>
struct fmt::formatter<Optional<T>> : FormatUsingTraceable<Optional<T>> {};

#endif // FLOW_OPTIONAL_H
