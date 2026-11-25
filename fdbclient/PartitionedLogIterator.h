#ifndef FDBCLIENT_PARTITIONED_LOG_ITERATOR_H
#define FDBCLIENT_PARTITIONED_LOG_ITERATOR_H

#include "fdbclient/FDBTypes.h"

// Structure to represent each mutation entity
struct VersionedMutation {
	Version version;
	int32_t subsequence;
	MutationRef mutation;
	VersionedMutation(Arena& p, const VersionedMutation& toCopy) : mutation(p, toCopy.mutation) {
		version = toCopy.version;
		subsequence = toCopy.subsequence;
	}
	VersionedMutation() = default;
};

class PartitionedLogIterator : public ReferenceCounted<PartitionedLogIterator> {
	// Returns true when the version might continue in the next block.
	virtual bool consumeBlock(Version version, Standalone<VectorRef<VersionedMutation>>* mutations) = 0;

public:
	// Select the authoritative file and return its next version, or empty at end of stream.
	virtual Future<Optional<Version>> peekNextVersion() = 0;

	// Consume the version returned by peekNextVersion. A null output skips the framed mutation bodies.
	Future<Void> consumeVersion(Version version, Standalone<VectorRef<VersionedMutation>>* mutations);

	virtual ~PartitionedLogIterator() = default;
};
#endif
