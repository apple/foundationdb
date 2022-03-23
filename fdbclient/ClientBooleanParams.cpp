/*
 * ClientBooleanParams.cpp
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

#include "fdbclient/ClientBooleanParams.h"

FDB_DEFINE_BOOLEAN_PARAM(EnableLocalityLoadBalance);
FDB_DEFINE_BOOLEAN_PARAM(LockAware);
FDB_DEFINE_BOOLEAN_PARAM(Reverse);
FDB_DEFINE_BOOLEAN_PARAM(Snapshot);
FDB_DEFINE_BOOLEAN_PARAM(IsInternal);
FDB_DEFINE_BOOLEAN_PARAM(AddConflictRange);
FDB_DEFINE_BOOLEAN_PARAM(UseMetrics);
FDB_DEFINE_BOOLEAN_PARAM(IsSwitchable);
