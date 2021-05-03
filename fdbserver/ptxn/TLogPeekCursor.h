/*
 * TLogPeekCursor.h
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

#ifndef FDBSERVER_PTXN_TLOGPEEKCURSOR_H
#define FDBSERVER_PTXN_TLOGPEEKCURSOR_H

#pragma once

#include <iterator>

#include "fdbclient/FDBTypes.h"
#include "fdbserver/ptxn/MessageTypes.h"
#include "fdbserver/ptxn/TLogInterface.h"
#include "fdbserver/ptxn/TLogStorageServerPeekMessageSerializer.h"
#include "flow/Arena.h"

namespace ptxn {

class PeekCursorBase {
public:

    PeekCursorBase(const Version&);

    // Returns the begin verion for the cursor. The cursor will start from the begin version.
    const Version& getBeginVersion() const;

    // Returns the last version being pulled
    const Version& getLastVersion() const;

    // Check if there is any more messages in the remote TLog(s), if so, retrieve the
    // messages locally.
    Future<bool> remoteMoreAvailable();

    // Get one mutation
    const VersionSubsequenceMutation& get() const;

    // Move to the next mutation, return false if there is no more mutation
    void next();

    // Any remaining mutation *LOCALLY* available
    bool hasRemaining() const;

protected:
    // Check if there is any mutations remotely
    virtual Future<bool> remoteMoreAvailableImpl() = 0;

    // Step the local cursor
    virtual void nextImpl() = 0;

    // Get the message
    virtual const VersionSubsequenceMutation& getImpl() const = 0;

    // Check if any remaining mutations
    virtual bool hasRemainingImpl() const = 0;

    // Last version processed
    Version lastVersion;

private:
    // The version the cursor starts
    const Version beginVersion;
};


// Connect to a given TLog server and peeks for mutations with a given TeamID
class ServerTeamPeekCursor : public PeekCursorBase {
    const TeamID teamID;
    TLogInterfaceBase* pTLogInterface;

    // The arena used to store incoming serialized data, if not nullptr, TLogPeekReply arenas will be attached to this
    // arena, enables the access of deserialized data even the cursor is destroyed.
    Arena* pAttachArena;
    TLogStorageServerMessageDeserializer deserializer;
    TLogStorageServerMessageDeserializer::iterator deserializerIter;

public:
    // version_ is the version the cursor starts with
    // teamID_ is the teamID
    // pTLogInterface_ is the interface to the specific TLog server
    // pArena_ is used to 
    ServerTeamPeekCursor(const Version& version_, const TeamID& teamID_, TLogInterfaceBase* pTLogInterface_, Arena* arena_ = nullptr);

    const TeamID& getTeamID() const;

protected:
    virtual Future<bool> remoteMoreAvailableImpl() override;
    virtual void nextImpl() override;
    virtual const VersionSubsequenceMutation& getImpl() const override;
    virtual bool hasRemainingImpl() const override;
};


}   // namespace ptxn

#endif // FDBSERVER_PTXN_TLOGPEEKCURSOR_H