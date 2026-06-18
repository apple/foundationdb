#include "fdbclient/CommitProxyInterface.h"
#include "fdbclient/CoordinationInterface.h"
#include "flow/UnitTest.h"

// Instantiate ClientDBInfo related templates
template class ReplyPromise<struct ClientDBInfo>;
template class ReplyPromise<CachedSerialization<ClientDBInfo>>;

// Instantiate OpenDatabaseCoordRequest related templates
template struct NetNotifiedQueue<OpenDatabaseCoordRequest, true>;

// Instantiate GetKeyServerLocationsReply related templates
template class ReplyPromise<GetKeyServerLocationsReply>;
template struct NetSAV<GetKeyServerLocationsReply>;

TEST_CASE("/NativeCDC/ClientDBInfoProtocolGating") {
	ClientDBInfo source;
	source.nativeCdcEnabled = true;
	source.nativeCdcTagCount = 256;
	source.streamToCDCProxyId.emplace(1, UID(2, 3));

	Standalone<StringRef> legacy =
	    BinaryWriter::toValue(source, IncludeVersion(ProtocolVersion::withMutationChecksum()));
	ClientDBInfo legacyDecoded = BinaryReader::fromStringRef<ClientDBInfo>(legacy, IncludeVersion());
	ASSERT(!legacyDecoded.nativeCdcEnabled);
	ASSERT_EQ(legacyDecoded.nativeCdcTagCount, 0);
	ASSERT(legacyDecoded.cdcProxies.empty());
	ASSERT(legacyDecoded.streamToCDCProxyId.empty());

	Standalone<StringRef> nativeCdc =
	    BinaryWriter::toValue(source, IncludeVersion(ProtocolVersion::withNativeCdc()));
	ClientDBInfo nativeCdcDecoded = BinaryReader::fromStringRef<ClientDBInfo>(nativeCdc, IncludeVersion());
	ASSERT(nativeCdcDecoded.nativeCdcEnabled);
	ASSERT_EQ(nativeCdcDecoded.nativeCdcTagCount, source.nativeCdcTagCount);
	ASSERT_EQ(nativeCdcDecoded.streamToCDCProxyId, source.streamToCDCProxyId);
	return Void();
}

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
