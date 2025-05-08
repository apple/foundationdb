#include "fdbclient/StorageCheckpoint.h"

namespace {
// PAYLOAD_ROUND_TO_NEXT: The granularity for rounding up payload sizes.
// Any payload will be padded to the next multiple of this value.
// For example, with PAYLOAD_ROUND_TO_NEXT = 5000:
//   - Payload of 100 bytes → padded to 5000 bytes
//   - Payload of 5001 bytes → padded to 10000 bytes
//   - Payload of 12345 bytes → padded to 15000 bytes
constexpr size_t PAYLOAD_ROUND_TO_NEXT = 5000;

// FOOTER_BYTE_SIZE: Fixed size of the footer section that stores metadata.
// The footer contains the padding size as an ASCII decimal number, followed
// by 'f' characters to fill the remaining space to exactly 100 bytes.
constexpr size_t FOOTER_BYTE_SIZE = 100;
} // namespace

/*
 * CheckpointMetaData Serialization Protocol
 * =========================================
 *
 * This class implements a simple protocol for adding dynamic padding to checkpoint
 * payloads during simulation. The padding ensures consistent sizes
 * for testing purposes and makes the data format more predictable.
 *
 * Protocol Structure:
 *
 * ┌─────────────────────┬─────────────────────┬─────────────────────┐
 * │                     │                     │                     │
 * │    ORIGINAL         │    DYNAMIC          │      FOOTER         │
 * │    PAYLOAD          │    PADDING          │    (100 bytes)      │
 * │                     │                     │                     │
 * │  Variable size      │  'p' characters     │  Padding size as    │
 * │  (actual data)      │  repeated to fill   │  ASCII decimal +    │
 * │                     │  to target size     │  'f' fill chars     │
 * │                     │                     │                     │
 * └─────────────────────┴─────────────────────┴─────────────────────┘
 *
 * Example with a 12345-byte payload:
 * - Target size: 15000 bytes (next multiple of 5000)
 * - Padding needed: 15000 - 12345 = 2655 bytes
 * - Footer content: "2655fffffff..." (2655 followed by 96 'f' characters)
 *
 * Final structure:
 * ┌─────────────────┬─────────────────┬─────────────────────────────┐
 * │ Original Data   │ 2655 'p' chars  │ "2655ffffff...ffffff"       │
 * │ (12345 bytes)   │                 │ (100 bytes total)           │
 * └─────────────────┴─────────────────┴─────────────────────────────┘
 *
 * Total size: 12345 + 2655 + 100 = 15100 bytes
 *
 * Note: This protocol is intentionally optimized for readability over efficiency.
 * It uses ASCII encoding and verbose padding for easier debugging in tests.
 */

/**
 * Sets the serialized checkpoint with optional padding in simulation mode.
 *
 * In production (!isSimulated), the checkpoint is stored as-is.
 * In simulation, dynamic padding is added to round the payload to the next
 * multiple of PAYLOAD_ROUND_TO_NEXT bytes, making sizes predictable for testing.
 *
 * @param checkpoint The original checkpoint data to serialize
 */
void CheckpointMetaData::setSerializedCheckpoint(Standalone<StringRef> checkpoint) {
	const bool addPadding = g_network->isSimulated();
	if (!addPadding) {
		// Production mode: store checkpoint without modification
		serializedCheckpoint = checkpoint;
		return;
	}

	// Simulation mode: add padding for consistent sizing

	// Step 1: Calculate target size and required padding
	// Round up payload size to the next multiple of PAYLOAD_ROUND_TO_NEXT
	const size_t payloadSize = checkpoint.size();
	const size_t targetSize =
	    std::max<size_t>(PAYLOAD_ROUND_TO_NEXT,
	                     ((payloadSize + (PAYLOAD_ROUND_TO_NEXT - 1)) / PAYLOAD_ROUND_TO_NEXT) * PAYLOAD_ROUND_TO_NEXT);
	const size_t paddingBytes = targetSize - payloadSize;

	// Step 2: Build the footer
	// Footer format: ASCII decimal padding size, followed by 'f' fill characters
	// Example: if paddingBytes = 2655, footer = "2655ffffff...ffffff" (100 bytes total)
	std::string footer(FOOTER_BYTE_SIZE, 'f');
	const std::string num = std::to_string(paddingBytes);
	ASSERT(num.size() <= footer.size()); // Ensure padding size fits in footer
	std::memcpy(&footer[0], num.data(), num.size());
	ASSERT(footer.size() == FOOTER_BYTE_SIZE);

	// Step 3: Assemble the final serialized checkpoint
	// Structure: [original payload] + [padding bytes] + [footer]
	std::string payload = checkpoint.toString();
	std::string padding(paddingBytes, 'p'); // Fill padding with 'p' characters
	serializedCheckpoint = std::move(payload) + padding + footer;

	// Debug trace for verification (uncomment if needed for debugging)
	TraceEvent("CheckpointSet")
	    .detail("OriginalCheckpoint", checkpoint)
	    .detail("OriginalCheckpointSize", checkpoint.size())
	    .detail("SerializedCheckpoint", serializedCheckpoint)
	    .detail("SerializedCheckpointSize", serializedCheckpoint.size())
	    .detail("Footer", footer)
	    .detail("FooterSize", FOOTER_BYTE_SIZE)
	    .detail("PaddingSize", paddingBytes);
}

/**
 * Retrieves the original checkpoint data, removing any padding added during serialization.
 *
 * In production (!isSimulated), returns the checkpoint as-is.
 * In simulation, extracts the original payload by parsing the footer to determine
 * padding size and removing both padding and footer.
 *
 * @return The original checkpoint data without padding or footer
 */
Standalone<StringRef> CheckpointMetaData::getSerializedCheckpoint() const {
	const bool addPadding = g_network->isSimulated();
	if (!addPadding) {
		// Production mode: return checkpoint without modification
		return serializedCheckpoint;
	}

	// Simulation mode: extract original payload by removing padding and footer

	// Step 1: Extract footer and parse padding size
	// Footer is the last FOOTER_BYTE_SIZE bytes of the serialized data
	const std::string& str = serializedCheckpoint.toString();
	ASSERT(str.size() >= FOOTER_BYTE_SIZE); // Ensure we have at least a footer

	// Parse ASCII decimal number from the beginning of the footer
	size_t start = str.size() - FOOTER_BYTE_SIZE;
	size_t paddingBytes = 0;
	for (size_t i = start; i < str.size() && std::isdigit(str[i]); ++i) {
		paddingBytes = paddingBytes * 10 + (str[i] - '0');
	}

	// Step 2: Calculate original payload size
	// Total size - padding - footer = original payload size
	size_t payloadSize = str.size() - paddingBytes - FOOTER_BYTE_SIZE;
	ASSERT(payloadSize <= str.size()); // Sanity check

	// Step 3: Extract and return the original payload
	// Create a StringRef pointing to just the payload portion
	auto ptr = reinterpret_cast<const uint8_t*>(str.data());
	StringRef ref(ptr, int(payloadSize));
	auto ret = Standalone<StringRef>(ref);

	// Debug trace for verification
	TraceEvent("CheckpointGet")
	    .detail("ReturnedCheckpoint", ret)
	    .detail("ReturnedCheckpointSize", ret.size())
	    .detail("SerializedCheckpoint", serializedCheckpoint)
	    .detail("SerializedCheckpointSize", serializedCheckpoint.size())
	    .detail("FooterSize", FOOTER_BYTE_SIZE)
	    .detail("PaddingSize", paddingBytes);

	return ret;
}
