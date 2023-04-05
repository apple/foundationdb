/*
 * TypeTraits.h
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

// This file, similar to `type_traits` in the standard library, contains utility types that can be used for template
// metaprogramming. While they can be very useful and simplify certain things, please be aware that their use will
// increase compilation times significantly. Therefore it is not recommended to use them in header file if not
// absolutely necessary.
#pragma once
#include <variant>

// This type class will take two std::variant types and concatenate them
template <class L, class R>
struct variant_concat_t;

template <class... Args1, class... Args2>
struct variant_concat_t<std::variant<Args1...>, std::variant<Args2...>> {
	using type = std::variant<Args1..., Args2...>;
};

// Helper definition for variant_concat_t. Instead of using `typename variant_concat_t<...>::type` one can simply use
// `variant_concat<...>`
template <class L, class R>
using variant_concat = typename variant_concat_t<L, R>::type;

// Takes a std::variant as first argument and applies Fun to all of them. For example: typename
// variant_map_t<std::variant<int, bool>, std::add_pointer_t>::type will be defined as std::variant<int*, bool*>
template <class T, template <class> class Fun>
struct variant_map_t;

template <class... Args, template <class> class Fun>
struct variant_map_t<std::variant<Args...>, Fun> {
	using type = std::variant<Fun<Args>...>;
};

// Helper definition for variant_map_t. Instead of using `typename variant_map<...>::type` one can simple use
// `variant_map<...>` which is equivalent but shorter.
template <class T, template <class> class Fun>
using variant_map = typename variant_map_t<T, Fun>::type;
