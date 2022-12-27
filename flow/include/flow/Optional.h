#ifndef FLOW_OPTIONAL_H
#define FLOW_OPTIONAL_H

#include <optional>

#include "flow/FileIdentifier.h"
#include "flow/Error.h"

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
class Optional : public ComposedIdentifier<T, 4> {
public:
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
		return map<R>([](const T& v) { return (R)v; });
	}

	template <class R>
	Optional<R> map(std::function<R(T)> f) const& {
		return present() ? Optional<R>(f(get())) : Optional<R>();
	}
	template <class R>
	Optional<R> map(std::function<R(T)> f) && {
		return present() ? Optional<R>(f(std::move(*this).get())) : Optional<R>();
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

	void reset() { impl.reset(); }
	size_t hash() const { return hashFunc(impl); }

private:
	static inline std::hash<std::optional<T>> hashFunc{};
	std::optional<T> impl;
};


#endif // FLOW_OPTIONAL_H