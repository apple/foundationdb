#include "fdbclient/StorageCheckpoint.h"

namespace {
// PAYLOAD_ROUND_TO_NEXT: The granularity for rounding up payload sizes.
// Any payload will be padded to the next multiple of this value.
// For example, with PAYLOAD_ROUND_TO_NEXT = 5000:
//   - Payload of 100 bytes -> padded to 5000 bytes
//   - Payload of 5001 bytes -> padded to 10000 bytes
//   - Payload of 12345 bytes -> padded to 15000 bytes
constexpr size_t PAYLOAD_ROUND_TO_NEXT = 5000;

// FOOTER_BYTE_SIZE: Fixed size of the footer section of CheckpointMetaData
constexpr size_t FOOTER_BYTE_SIZE = 16;
} // namespace

/*
 * CheckpointMetaData Serialization Protocol
 * =========================================
 *
 * This comment describes the protocol for adding dynamic padding to checkpoint
 * payloads *only* during simulation. The padding ensures consistent sizes
 * in simulation testing, across runs with the same seed. Not having consistent sizes
 * results in UnseedMismatch which means we've broken determinism and therefore
 * can not reproduce the run.
 *
 * Protocol Structure:
 *
 * ┌─────────────────────┬─────────────────────┬─────────────────────┐
 * │                     │                     │                     │
 * │    ORIGINAL         │    DYNAMIC          │      FOOTER         │
 * │    PAYLOAD          │    PADDING          │    (16 bytes)       │
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
 * - Footer content: "2655ffffffffffff" (2655 followed by 12 'f' characters)
 *
 * Final structure:
 * ┌─────────────────┬─────────────────┬─────────────────────────────┐
 * │ Original Data   │ 2655 'p' chars  │ "2655ffffffffffff"          │
 * │ (12345 bytes)   │                 │ (16 bytes total)            │
 * └─────────────────┴─────────────────┴─────────────────────────────┘
 *
 * Total size: 12345 + 2655 + 100 = 15100 bytes
 *
 * Note: This protocol is intentionally optimized for readability over efficiency.
 * It uses ASCII encoding and verbose padding for easier debugging in tests.
 * This is *only* meant for simulation and for any consumer of checkpoint, it
 * should be a no-op and an internal implementation detail. This is because
 * as a consumer, you can set your checkpoint, and get your checkpoint. Both
 * these checkpoints would not have dynamic padding. Only in the internal
 * representation, we'd have such padding to ensure that byte size of checkpoint
 * data is deterministic across simulation runs.
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
	// TraceEvent("CheckpointSet")
	//     .detail("OriginalCheckpoint", checkpoint)
	//     .detail("OriginalCheckpointSize", checkpoint.size())
	//     .detail("SerializedCheckpoint", serializedCheckpoint)
	//     .detail("SerializedCheckpointSize", serializedCheckpoint.size())
	//     .detail("Footer", footer)
	//     .detail("FooterSize", FOOTER_BYTE_SIZE)
	//     .detail("PaddingSize", paddingBytes);
}

Standalone<StringRef> CheckpointMetaData::getSerializedCheckpoint() const {
	const bool addPadding = g_network->isSimulated();
	if (!addPadding) {
		// Production mode: return checkpoint without modification
		return serializedCheckpoint;
	}

	// Simulation mode: extract original payload by removing padding and footer

	// Step 1: Extract footer and parse padding size
	// Footer is the last FOOTER_BYTE_SIZE bytes of the serialized data
	// Parse ASCII decimal number from the beginning of the footer
	const std::string& str = serializedCheckpoint.toString();
	ASSERT(str.size() >= FOOTER_BYTE_SIZE);
	size_t start = str.size() - FOOTER_BYTE_SIZE;
	size_t paddingBytes = 0;
	for (size_t i = start; i < str.size() && std::isdigit(str[i]); ++i) {
		paddingBytes = (paddingBytes * 10) + (str[i] - '0');
	}

	// Step 2: Calculate original payload size
	// Total size - padding - footer = original payload size
	const size_t payloadSize = str.size() - paddingBytes - FOOTER_BYTE_SIZE;
	ASSERT(payloadSize <= str.size());

	// Step 3: Extract and return the original payload
	// Create a StringRef pointing to just the payload portion
	auto ret =
	    Standalone<StringRef>(StringRef(serializedCheckpoint.begin(), payloadSize), serializedCheckpoint.arena());

	// Debug trace for verification
	// TraceEvent("CheckpointGet")
	//     .detail("ReturnedCheckpoint", ret)
	//     .detail("ReturnedCheckpointSize", ret.size())
	//     .detail("SerializedCheckpoint", serializedCheckpoint)
	//     .detail("SerializedCheckpointSize", serializedCheckpoint.size())
	//     .detail("FooterSize", FOOTER_BYTE_SIZE)
	//     .detail("PaddingSize", paddingBytes);

	return ret;
}
