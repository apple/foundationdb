#
# tests.cmake
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#
# This is a convenience file to separate the java test file listings from the rest
# of the cmake logic so that we don't accidentally break anything when adding/removing
# test files from the cmake construction
#

# Listing of unit tests. These are tests that don't require a running database to complete.
# make sure that they are in the src/junit folder
set(JAVA_JUNIT_TESTS
  api/src/junit/com/apple/foundationdb/tuple/ArrayUtilSortTest.java
  api/src/junit/com/apple/foundationdb/tuple/ArrayUtilTest.java
  api/src/junit/com/apple/foundationdb/tuple/ByteArrayUtilTest.java
  api/src/junit/com/apple/foundationdb/tuple/TupleComparisonTest.java
  api/src/junit/com/apple/foundationdb/tuple/TuplePackingTest.java
  api/src/junit/com/apple/foundationdb/tuple/TupleSerializationTest.java
  )
