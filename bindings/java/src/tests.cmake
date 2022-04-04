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
  src/junit/com/apple/foundationdb/tuple/ArrayUtilSortTest.java
  src/junit/com/apple/foundationdb/tuple/ArrayUtilTest.java
  src/junit/com/apple/foundationdb/tuple/ByteArrayUtilTest.java
  src/junit/com/apple/foundationdb/tuple/TupleComparisonTest.java
  src/junit/com/apple/foundationdb/tuple/TuplePackingTest.java
  src/junit/com/apple/foundationdb/tuple/TupleSerializationTest.java
  src/junit/com/apple/foundationdb/RangeQueryTest.java
  src/junit/com/apple/foundationdb/EventKeeperTest.java
  )

# Resources that are used in unit testing, but are not explicitly test files (JUnit rules, utility
# classes, and so on)
set(JUNIT_RESOURCES
  src/junit/com/apple/foundationdb/FakeFDBTransaction.java
  src/junit/com/apple/foundationdb/FDBLibraryRule.java
)

# Integration tests. These are tests that require a running FDB instance to complete
# successfully. Make sure that they are in the src/integration folder
set(JAVA_INTEGRATION_TESTS
  src/integration/com/apple/foundationdb/DirectoryTest.java
  src/integration/com/apple/foundationdb/RangeQueryIntegrationTest.java
  src/integration/com/apple/foundationdb/BasicMultiClientIntegrationTest.java
  src/integration/com/apple/foundationdb/CycleMultiClientIntegrationTest.java
  src/integration/com/apple/foundationdb/SidebandMultiThreadClientTest.java
  src/integration/com/apple/foundationdb/RepeatableReadMultiThreadClientTest.java
  src/integration/com/apple/foundationdb/MappedRangeQueryIntegrationTest.java
)

# Resources that are used in integration testing, but are not explicitly test files (JUnit rules,
# utility classes, and so forth)
set(JAVA_INTEGRATION_RESOURCES
  src/integration/com/apple/foundationdb/RequiresDatabase.java
  src/integration/com/apple/foundationdb/MultiClientHelper.java
)


