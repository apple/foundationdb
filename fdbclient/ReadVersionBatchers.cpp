/**
 * ReadVersionBatchers.h
 */

#include "fdbclient/ReadVersionBatchers.h"

Future<GetReadVersionReply> ReadVersionBatchers::getReadVersion(Database cx,
                                                                TransactionPriority priority,
                                                                uint32_t flags,
                                                                Optional<TenantGroupName> const& tenantGroup,
                                                                SpanContext spanContext,
                                                                TagSet tags,
                                                                Optional<UID> debugID) {
	auto& batcher = batchers[std::make_pair(flags, tenantGroup)];
	batcher.startActor(cx, priority, flags, tenantGroup);
	return batcher.getReadVersion(spanContext, tags, debugID);
}
