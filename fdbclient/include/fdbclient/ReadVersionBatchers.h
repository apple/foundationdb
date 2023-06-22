/**
 * ReadVersionBatchers.h
 */

#pragma once

#include "fdbclient/ReadVersionBatcher.h"

class ReadVersionBatchers {
	using Index = std::pair<uint32_t, Optional<TenantGroupName>>;
	// TODO: Use more efficient data structure:
	std::map<Index, ReadVersionBatcher> batchers;

public:
	Future<GetReadVersionReply> getReadVersion(Database,
	                                           TransactionPriority,
	                                           uint32_t flags,
	                                           Optional<TenantGroupName> const&,
	                                           SpanContext,
	                                           TagSet,
	                                           Optional<UID> debugID);
};
