/*
 * TesterScheduler.h
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

#pragma once

#ifndef APITESTER_SCHEDULER_H
#define APITESTER_SCHEDULER_H

#include <functional>
#include <memory>

namespace FdbApiTester {

using TTaskFct = std::function<void(void)>;

extern const TTaskFct NO_OP_TASK;

class IScheduler {
public:
	virtual ~IScheduler() {}
	virtual void start() = 0;
	virtual void schedule(TTaskFct task) = 0;
	virtual void stop() = 0;
	virtual void join() = 0;
};

std::unique_ptr<IScheduler> createScheduler(int numThreads);

} // namespace FdbApiTester

#endif