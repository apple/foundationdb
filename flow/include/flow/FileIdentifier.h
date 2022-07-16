/*
 * FileIdentifier.h
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
#include <cstdint>
#include <type_traits>

using FileIdentifier = uint32_t;

template <typename T, typename = int>
struct HasFileIdentifierMember : std::false_type {};

template <typename T>
struct HasFileIdentifierMember<T, decltype((void)T::file_identifier, 0)> : std::true_type {};

template <typename T, typename = int>
struct CompositionDepthFor : std::integral_constant<int, 0> {};

template <typename T>
struct CompositionDepthFor<T, decltype((void)T::composition_depth, 0)>
  : std::integral_constant<int, T::composition_depth> {};

template <class T, bool>
struct FileIdentifierForBase;

template <class T>
struct FileIdentifierForBase<T, false> {};

template <class T>
struct FileIdentifierForBase<T, true> {
	static constexpr FileIdentifier value = T::file_identifier;
	static_assert(CompositionDepthFor<T>::value > 0 || T::file_identifier < (1 << 24),
	              "non-composed file identifiers must be less than 2^24");
};

template <class T>
struct FileIdentifierFor : FileIdentifierForBase<T, HasFileIdentifierMember<T>::value> {};

template <typename T, typename = int>
struct HasFileIdentifier : std::false_type {};

template <typename T>
struct HasFileIdentifier<T, decltype((void)FileIdentifierFor<T>::value, 0)> : std::true_type {};

template <class T, uint8_t B, bool = (HasFileIdentifier<T>::value && CompositionDepthFor<T>::value < 2)>
struct ComposedIdentifier;

// Manually specified file identifiers must be less than 2^24.
// The first 8 bits are used to identify the wrapper classes:
//   The 5th-8th bits represent the inner wrapper class
//   The 1st-4th bits represent the outer wrapper class
// Types with more than two level of composition do not get file identifiers.
template <class T, uint8_t B>
struct ComposedIdentifier<T, B, true> {
	static_assert(CompositionDepthFor<T>::value < 2);
	static constexpr int composed_identifier_offset = (CompositionDepthFor<T>::value == 1) ? 28 : 24;
	static_assert(B > 0 && B < 16, "Up to 15 types of composed identifiers allowed");
	static_assert(FileIdentifierFor<T>::value < (1 << composed_identifier_offset));

public:
	static constexpr int composition_depth = CompositionDepthFor<T>::value + 1;
	static constexpr FileIdentifier file_identifier = (B << composed_identifier_offset) | FileIdentifierFor<T>::value;
};

template <class T, uint8_t B>
struct ComposedIdentifier<T, B, false> {};

template <class T, uint32_t B, bool = HasFileIdentifier<T>::value>
struct ComposedIdentifierExternal;
template <class T, uint32_t B>
struct ComposedIdentifierExternal<T, B, false> {};
template <class T, uint32_t B>
struct ComposedIdentifierExternal<T, B, true> {
	static constexpr FileIdentifier value = ComposedIdentifier<T, B>::file_identifier;
};
