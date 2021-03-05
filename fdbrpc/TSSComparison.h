/*
 * TSSComparison.h
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

/*
 * This header is to declare the tss comparison function that LoadBalance.Actor.h needs to be aware of to call,
 * But StorageServerInterface.h needs to implement on the types defined in SSI.h.
 */
#ifndef FDBRPC_TSS_COMPARISON_H
#define FDBRPC_TSS_COMPARISON_H

// TODO is there a way to do this static method without a class?
class TSSComparison {
    public:
        template<class T>
        static bool tssCompare(T src, T tss);
};

#endif