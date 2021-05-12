/*
 * MessageTypes.cpp
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

#include "fdbserver/ptxn/MessageTypes.h"

namespace ptxn {

VersionSubsequenceMutation::VersionSubsequenceMutation() {}

VersionSubsequenceMutation::VersionSubsequenceMutation(const Version& version_,
                                                       const Subsequence& subsequence_,
                                                       const MutationRef& mutation_)
  : version(version_), subsequence(subsequence_), mutation(mutation_) {}

bool VersionSubsequenceMutation::operator==(const VersionSubsequenceMutation& another) const {
	return version == another.version && subsequence == another.subsequence && mutation.type == another.mutation.type &&
	       mutation.param1 == another.mutation.param1 && mutation.param2 == another.mutation.param2;
}

bool VersionSubsequenceMutation::operator!=(const VersionSubsequenceMutation& another) const {
	return !(*this == another);
}

std::string VersionSubsequenceMutation::toString() const {
	return concatToString("Version ", version, "\tSubsequence ", subsequence, "\tMutation ", mutation.toString());
}

std::ostream& operator<<(std::ostream& stream, const VersionSubsequenceMutation& mutation) {
	stream << mutation.toString();
	return stream;
}

} // namespace ptxn