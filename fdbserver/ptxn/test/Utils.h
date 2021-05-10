/*
 * Utils.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2021 Apple Inc. and the FoundationDB project authors
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

#ifndef FDBSERVER_PTXN_TEST_UTILS_H
#define FDBSERVER_PTXN_TEST_UTILS_H

#pragma once

#include <vector>

#include "fdbserver/ptxn/test/Driver.h"
#include "fdbserver/ptxn/test/TestTLogPeek.h"
#include "fdbserver/ptxn/TLogInterface.h"

namespace ptxn::test {

// Shortcut for deterministicRanodm()->randomUniqueID();
UID randomUID();

// Constructs a random TeamID
TeamID getNewTeamID();

// Construct numTeams TeamIDs
std::vector<TeamID> generateRandomTeamIDs(const int numTeams);

// Pick one element from a container, randomly
// The container should support the concept of
//  template <typename Container>
//  requires(const Container& container) {
//      { container[0] };
//      { container.size() };
//      { Container::const_reference }
//  }
template <typename Container>
typename Container::const_reference randomlyPick(const Container& container) {
    if (container.size() == 0) {
        throw std::range_error("empty container");
    }
    return container[deterministicRandom()->randomInt(0, container.size())];
}

namespace print {

void print(const TLogCommitReply&);
void print(const TLogPeekRequest&);
void print(const TLogPeekRequest&);
void print(const TLogPeekReply&);
void print(const TestDriverOptions&);
void print(const CommitRecord&);
void print(const ptxn::test::tLogPeek::TestTLogPeekOptions&);

void printCommitRecord(const std::vector<CommitRecord>&);
void printNotValidatedRecords(const std::vector<CommitRecord>&);

} // namespace print

} // namespace ptxn::test

#endif // FDBSERVER_PTXN_TEST_UTILS_H