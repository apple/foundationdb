/*
 * FileIdentifier.h
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
#include <cstdint>
#include <type_traits>

using FileIdentifier = uint32_t;

struct Empty {};

template <typename T, typename = int>
struct HasFileIdentifierMember : std::false_type {};

template <typename T>
struct HasFileIdentifierMember<T, decltype((void)T::file_identifier, 0)> : std::true_type {};

template <class T, bool>
struct FileIdentifierForBase;

template <class T>
struct FileIdentifierForBase<T, false> {};

template <class T>
struct FileIdentifierForBase<T, true> {
	static constexpr FileIdentifier value = T::file_identifier;
};

template <class T>
struct FileIdentifierFor : FileIdentifierForBase<T, HasFileIdentifierMember<T>::value> {};

template <typename T, typename = int>
struct HasFileIdentifier : std::false_type {};

template <typename T>
struct HasFileIdentifier<T, decltype((void)FileIdentifierFor<T>::value, 0)> : std::true_type {};

template <class T, uint32_t B, bool = HasFileIdentifier<T>::value>
struct ComposedIdentifier;

template <class T, uint32_t B>
struct ComposedIdentifier<T, B, true>
{
	static constexpr FileIdentifier file_identifier = (B << 24) | FileIdentifierFor<T>::value;
};

template <class T, uint32_t B>
struct ComposedIdentifier<T, B, false> {};

template <class T, uint32_t B, bool = HasFileIdentifier<T>::value>
struct ComposedIdentifierExternal;
template <class T, uint32_t B>
struct ComposedIdentifierExternal<T, B, false> {};
template <class T, uint32_t B>
struct ComposedIdentifierExternal<T, B, true> {
	static constexpr FileIdentifier value = ComposedIdentifier<T, B>::file_identifier;
};
