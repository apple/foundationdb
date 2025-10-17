#include "fdbclient/CommitProxyInterface.h"
#include "fdbclient/CoordinationInterface.h"
#include "fdbclient/GetEncryptCipherKeys_impl.actor.h"

// Instantiate ClientDBInfo related templates
template class ReplyPromise<struct ClientDBInfo>;
template class ReplyPromise<CachedSerialization<ClientDBInfo>>;
template class GetEncryptCipherKeys<ClientDBInfo>;

// Instantiate OpenDatabaseCoordRequest related templates
template struct NetNotifiedQueue<OpenDatabaseCoordRequest, true>;

// Instantiate GetKeyServerLocationsReply related templates
template class ReplyPromise<GetKeyServerLocationsReply>;
template struct NetSAV<GetKeyServerLocationsReply>;

Standalone<StringRef> getBackupKey(BinaryWriter& wr, uint32_t** partBuffer, int part) {
	// Write the last part of the mutation to the serialization, if the buffer is not defined
	if (*partBuffer == nullptr) {
		// Serialize the part to the writer
		wr << bigEndian32(part);

		// Define the last buffer part
		*partBuffer = (uint32_t*)((char*)wr.getData() + wr.getLength() - sizeof(uint32_t));
	} else {
		**partBuffer = bigEndian32(part);
	}
	return wr.toValue();
}

StringRef getBackupValue(Key& content, int part) {
	return content.substr(
	    part * CLIENT_KNOBS->MUTATION_BLOCK_SIZE,
	    std::min(content.size() - part * CLIENT_KNOBS->MUTATION_BLOCK_SIZE, CLIENT_KNOBS->MUTATION_BLOCK_SIZE));
}