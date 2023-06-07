/*
 * swift_fdbserver_cxx_swift_value_conformance.swift
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

import FDBClient
import FDBServer
import flow_swift

@_expose(Cxx)
public struct ExposeTypeConf<T> {
    let x: CInt
}

/*****************************************************************************/
/* Whenever you see a 'type cannot be used in a Swift generic context' error */
/* you need to expose the the type in question via an expose... function.    */
/*                                                                           */
/* These functions function ensures that the value witness table for `T`     */
/* to C++ is exposed in the generated C++ header.                            */
/*****************************************************************************/

// TODO(swift): we should somehow simplify this process, so we don't have to expose manually.

// FIXME: return ExposeTypeConf? for conformance instead once header supports stdlib types.
@_expose(Cxx)
public func exposeConformanceToCxx_UpdateRecoveryDataRequest(
        _: ExposeTypeConf<UpdateRecoveryDataRequest>)  {}

// FIXME: return ExposeTypeConf? for conformance instead once header supports stdlib types.
@_expose(Cxx)
public func exposeConformanceToCxx_GetCommitVersionRequest(
        _: ExposeTypeConf<GetCommitVersionRequest>)  {}

// FIXME: return ExposeTypeConf? for conformance instead once header supports stdlib types.
@_expose(Cxx)
public func exposeConformanceToCxx_GetRawCommittedVersionRequest(
        _: ExposeTypeConf<GetRawCommittedVersionRequest>)  {}

// FIXME: return ExposeTypeConf? for conformance instead once header supports stdlib types.
@_expose(Cxx)
public func exposeConformanceToCxx_ReportRawCommittedVersionRequest(
        _: ExposeTypeConf<ReportRawCommittedVersionRequest>)  {}
