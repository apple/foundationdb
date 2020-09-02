/*
 * MasterProxyInterface.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2020 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *	 http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "fdbclient/MasterProxyInterface.h"

#include "fdbclient/Knobs.h"

enum {
	SPLIT_TRANSACTION_MASK = 0b1,

	DISABLE_SPLIT_TRANSACTION = 0b0,
	ENABLE_SPLIT_TRANSACTION = 0b1,

	CONFLICTS_MASK = 0b110,

	CONFLICTS_EVENLY_DISTRIBUTE = 0b000,
	CONFLICTS_TO_ONE_PROXY = 0b010
};

