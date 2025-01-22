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
	VersionedMutation() {}
};

class PartitionedLogIterator : public ReferenceCounted<PartitionedLogIterator> {
public:
	virtual bool hasNext() = 0;

	virtual Future<Version> peekNextVersion() = 0;

	virtual Future<Standalone<VectorRef<VersionedMutation>>> getNext() = 0;

	virtual ~PartitionedLogIterator() = default;
};
#endif