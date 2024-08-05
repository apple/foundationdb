/*
 * StorageCorruptionBug.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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
#ifndef FDBSERVER_STORAGE_CORRUPTION_BUG_H
#define FDBSERVER_STORAGE_CORRUPTION_BUG_H
#include "flow/SimBugInjector.h"

class StorageCorruptionBug : public ISimBug {
public:
	double corruptionProbability = 0.001;
};
class StorageCorruptionBugID : public IBugIdentifier {
public:
	std::shared_ptr<ISimBug> create() const override { return std::make_shared<StorageCorruptionBug>(); }
};

#endif // FDBSERVER_STORAGE_CORRUPTION_BUG_H
