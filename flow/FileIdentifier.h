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

using FileIdentifier = uint32_t;

template <class T>
struct FileIdentifierFor {
	constexpr static FileIdentifier value = T::file_identifier;
};

template <>
struct FileIdentifierFor<int> {
	constexpr static FileIdentifier value = 1;
};

template <>
struct FileIdentifierFor<unsigned> {
	constexpr static FileIdentifier value = 2;
};

template <>
struct FileIdentifierFor<long> {
	constexpr static FileIdentifier value = 3;
};

template <>
struct FileIdentifierFor<unsigned long> {
	constexpr static FileIdentifier value = 4;
};

template <>
struct FileIdentifierFor<long long> {
	constexpr static FileIdentifier value = 5;
};

template <>
struct FileIdentifierFor<unsigned long long> {
	constexpr static FileIdentifier value = 6;
};

template <>
struct FileIdentifierFor<short> {
	constexpr static FileIdentifier value = 7;
};

template <>
struct FileIdentifierFor<unsigned short> {
	constexpr static FileIdentifier value = 8;
};

template <>
struct FileIdentifierFor<signed char> {
	constexpr static FileIdentifier value = 9;
};

template <>
struct FileIdentifierFor<unsigned char> {
	constexpr static FileIdentifier value = 10;
};

template <>
struct FileIdentifierFor<bool> {
	constexpr static FileIdentifier value = 11;
};

template <>
struct FileIdentifierFor<float> {
	constexpr static FileIdentifier value = 7266212;
};

template <>
struct FileIdentifierFor<double> {
	constexpr static FileIdentifier value = 9348150;
};

