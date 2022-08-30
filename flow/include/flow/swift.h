/*
 * network.h
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

#ifndef FLOW_SWIFT_H
#define FLOW_SWIFT_H

#include "flow/ProtocolVersion.h"
#pragma once

#include <array>
#include <regex>
#include <string>
#include <stdint.h>
#include <variant>
#include <atomic>
#include "boost/asio.hpp"
#include "flow/Arena.h"
#include "flow/BooleanParam.h"
#include "flow/IRandom.h"
#include "flow/Trace.h"
#include "flow/WriteOnlySet.h"


// ==== ----------------------------------------------------------------------------------------------------------------

/// A count in nanoseconds.
using JobDelay = unsigned long long;

class Job {
public:
    bool isAsyncTask() const {
        return false;
    }

    int getPriority() const {
        return 0;
    }

    /// Given that we've fully established the job context in the current
    /// thread, actually start running this job.  To establish the context
    /// correctly, call swift_job_run or runJobInExecutorContext.
    void runInFullyEstablishedContext();

    /// Given that we've fully established the job context in the
    /// current thread, and that the job is a simple (non-task) job,
    /// actually start running this job.
    void runSimpleInFullyEstablishedContext() {
        // FIXME: return RunJob(this); // 'return' forces tail call
    }
};

class AsyncTask: public Job {

};

#endif