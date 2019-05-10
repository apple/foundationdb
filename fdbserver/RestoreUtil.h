/*
 * RestoreUtil.h
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

// This file defines the commonly used data structure and functions
// that are used by both RestoreWorker and RestoreRoles(Master, Loader, and Applier)

#ifndef FDBSERVER_RESTOREUTIL_H
#define FDBSERVER_RESTOREUTIL_H
#pragma once

#include "fdbclient/Tuple.h"
#include "flow/flow.h"
#include "flow/Stats.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbrpc/IAsyncFile.h"

// TODO: To remove unused command enum. and re-order the command sequence
// RestoreCommandEnum is also used as the phase ID for CMDUID
enum class RestoreCommandEnum {Init = 0,
		Sample_Range_File, Sample_Log_File, Sample_File_Done,
		Loader_Send_Sample_Mutation_To_Applier, Loader_Send_Sample_Mutation_To_Applier_Done, //5
		Calculate_Applier_KeyRange, Get_Applier_KeyRange, Get_Applier_KeyRange_Done, //8
		Assign_Applier_KeyRange, Assign_Applier_KeyRange_Done, //10
		Assign_Loader_Range_File, Assign_Loader_Log_File, Assign_Loader_File_Done,//13
		Loader_Send_Mutations_To_Applier, Loader_Send_Mutations_To_Applier_Done,//15
		Apply_Mutation_To_DB, Apply_Mutation_To_DB_Skip, //17
		Loader_Notify_Appler_To_Apply_Mutation,
		Notify_Loader_ApplierKeyRange, Notify_Loader_ApplierKeyRange_Done, //20
		Finish_Restore, Reset_VersionBatch, Set_WorkerInterface, Collect_RestoreRoleInterface, // 24
		Heart_Beat, Recruit_Role_On_Worker}; 
BINARY_SERIALIZABLE(RestoreCommandEnum);

enum class RestoreRole {Invalid = 0, Master = 1, Loader, Applier};
BINARY_SERIALIZABLE( RestoreRole );

extern std::vector<std::string> RestoreRoleStr;
extern int numRoles;

std::string getRoleStr(RestoreRole role);

// Restore command's UID. uint64_t part[2];
// part[0] is the phase id, part[1] is the command index in the phase.
// TODO: Add another field to indicate version-batch round
class CMDUID {
public:
	uint16_t batch;
	uint16_t phase;
	uint64_t cmdID;
	CMDUID() : batch(0), phase(0), cmdID(0) { }
	CMDUID( uint16_t a, uint64_t b ) { batch = 0; phase=a; cmdID=b; }
	CMDUID(const CMDUID &cmd) { batch = cmd.batch; phase = cmd.phase; cmdID = cmd.cmdID; }

	void initPhase(RestoreCommandEnum phase);

	void nextPhase(); // Set to the next phase.

	void nextCmd(); // Increase the command index at the same phase

	RestoreCommandEnum getPhase();
	void setPhase(RestoreCommandEnum newPhase);
	void setBatch(int newBatchIndex);

	uint64_t getIndex();

	std::string toString() const;

	bool operator == ( const CMDUID& r ) const { return batch == r.batch && phase == r.phase && cmdID == r.cmdID; }
	bool operator != ( const CMDUID& r ) const { return batch != r.batch || phase != r.phase || cmdID != r.cmdID; }
	bool operator < ( const CMDUID& r ) const { return batch < r.batch || (batch == r.batch && phase < r.phase) || (batch == r.batch && phase == r.phase && cmdID < r.cmdID); }

	//uint64_t hash() const { return first(); }
	//uint64_t first() const { return part[0]; }
	//uint64_t second() const { return part[1]; }

	template <class Ar>
	void serialize_unversioned(Ar& ar) { // Changing this serialization format will affect key definitions, so can't simply be versioned!
		serializer(ar, batch, phase, cmdID);
	}
};
template <class Ar> void load( Ar& ar, CMDUID& uid ) { uid.serialize_unversioned(ar); }
template <class Ar> void save( Ar& ar, CMDUID const& uid ) { const_cast<CMDUID&>(uid).serialize_unversioned(ar); }

 struct FastRestoreStatus {
	double curWorkloadSize;
	double curRunningTime;
	double curSpeed;

	double totalWorkloadSize;
	double totalRunningTime;
	double totalSpeed;
};

// Common restore request/response interface
// Reply type
struct RestoreCommonReply { 
	UID id; // unique ID of the server who sends the reply
	CMDUID cmdID; // The restore command for the reply
	
	RestoreCommonReply() : id(UID()), cmdID(CMDUID()) {}
	explicit RestoreCommonReply(UID id, CMDUID cmdID) : id(id), cmdID(cmdID) {}
	
	std::string toString() const {
		std::stringstream ss;
		ss << "ServerNodeID:" << id.toString() << " CMDID:" << cmdID.toString();
		return ss.str();
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, id, cmdID);
	}
};

struct RestoreSimpleRequest : TimedRequest {
	CMDUID cmdID;

	ReplyPromise<RestoreCommonReply> reply;

	RestoreSimpleRequest() : cmdID(CMDUID()) {}
	explicit RestoreSimpleRequest(CMDUID cmdID) : cmdID(cmdID) {}

	template <class Ar> 
	void serialize( Ar& ar ) {
		serializer(ar, cmdID, reply);
	}
};

#endif //FDBSERVER_RESTOREUTIL_ACTOR_H