/**
 * ReadVersionBatcher.h
 */

#pragma once

#include "fdbclient/CommitProxyInterface.h"
#include "fdbclient/NativeAPI.actor.h"

class ReadVersionBatcher {
	friend class ReadVersionBatcherImpl;

	struct VersionRequest {
		SpanContext spanContext;
		Promise<GetReadVersionReply> reply;
		TagSet tags;
		Optional<UID> debugID;

		VersionRequest(SpanContext spanContext, TagSet tags = TagSet(), Optional<UID> debugID = {})
		  : spanContext(spanContext), tags(tags), debugID(debugID) {}
	};

	PromiseStream<VersionRequest> stream;
	Future<Void> actor;

public:
	Future<GetReadVersionReply> getReadVersion(SpanContext, TagSet tags, Optional<UID> debugId);

	void startActor(Database, TransactionPriority, uint32_t flags, Optional<TenantGroupName> const&);
};
