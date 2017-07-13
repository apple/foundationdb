/*
 * VersionedBTree.actor.cpp
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

#include "flow/flow.h"
#include "IVersionedStore.h"
#include "IPager.h"
#include "fdbclient/Tuple.h"
#include "flow/serialize.h"
#include "flow/genericactors.actor.h"
#include "flow/UnitTest.h"
#include "MemoryPager.h"
#include <map>
#include <vector>

#define INTERNAL_PAGES_HAVE_TUPLES 1

struct SimpleFixedSizeMapRef {
	typedef std::vector<std::pair<std::string, std::string>> KVPairsT;

	SimpleFixedSizeMapRef() : flags(0) {}

	static SimpleFixedSizeMapRef decode(StringRef buf) {
		SimpleFixedSizeMapRef result;
		BinaryReader r(buf, AssumeVersion(currentProtocolVersion));
		r >> result.flags;
		r >> result.entries;
		return result;
	};

	// Returns -1 if key is less than first key, otherwise index into entries
	int findLastLessOrEqual(StringRef key) {
		return std::upper_bound(entries.begin(), entries.end(),
								key,
								[](StringRef const& a, KVPairsT::value_type const& b) { return a<b.first; })
					- entries.begin() - 1;
	}

	template<typename Allocator>
	static Reference<IPage> emptyPage(uint8_t newFlags, Allocator const &newPageFn) {
		Reference<IPage> page = newPageFn();
		BinaryWriter bw(AssumeVersion(currentProtocolVersion));
		bw << newFlags;
		bw << KVPairsT();
		memcpy(page->mutate(), bw.getData(), bw.getLength());
		return page;
	}

	template<typename Allocator>
	static vector<std::pair<int, Reference<IPage>>> buildMany(const KVPairsT &kvPairs, uint8_t newFlags, Allocator const &newPageFn, int page_size_override = -1) {
		vector<std::pair<int, Reference<IPage>>> pages;

		Reference<IPage> page = newPageFn();
		int pageSize = page->size();
		if(page_size_override > 0 && page_size_override < pageSize)
			pageSize = page_size_override;
		BinaryWriter bw(AssumeVersion(currentProtocolVersion));
		bw << newFlags;
		uint32_t i = 0;
		uint32_t start = i;
		int mapSizeOffset = bw.getLength();
		bw << start;  // placeholder for map size

		for(auto const &kv : kvPairs) {
			// If page would overflow, output it and start new one
			if(bw.getLength() + 8 + kv.first.size() + kv.second.size() > pageSize) {
				memcpy(page->mutate(), bw.getData(), bw.getLength());
				*(uint32_t *)(page->mutate() + mapSizeOffset) = i - start;
				//printf("buildmany: writing page start=%d %s\n", start, kvPairs[start].first.c_str());
				pages.push_back({start, page});
				bw = BinaryWriter(AssumeVersion(currentProtocolVersion));
				bw << newFlags;
				page = newPageFn();
				start = i;
				int mapSizeOffset = bw.getLength();
				bw << start; // placeholder for map size;
			}

			bw << kv;
			++i;
		}

		if(bw.getLength() != sizeof(newFlags)) {
			//printf("buildmany: adding last page start=%d %s\n", start, kvPairs[start].first.c_str());
			memcpy(page->mutate(), bw.getData(), bw.getLength());
			*(uint32_t *)(page->mutate() + mapSizeOffset) = i - start;
			pages.push_back({start, page});
		}

		//printf("buildmany: returning pages.size %lu, kvpairs %lu\n", pages.size(), kvPairs.size());
		return pages;
	}

	std::string toString();

	KVPairsT entries;
	uint8_t flags;
};

#define NOT_IMPLEMENTED { UNSTOPPABLE_ASSERT(false); }

class VersionedBTree : public IVersionedStore {
public:
	enum EPageFlags { IS_LEAF = 1};

	typedef SimpleFixedSizeMapRef FixedSizeMap;

	virtual Future<Void> getError() NOT_IMPLEMENTED
	virtual Future<Void> onClosed() NOT_IMPLEMENTED
	virtual void dispose() NOT_IMPLEMENTED
	virtual void close() NOT_IMPLEMENTED

	virtual KeyValueStoreType getType() NOT_IMPLEMENTED
	virtual bool supportsMutation(int op) NOT_IMPLEMENTED
	virtual StorageBytes getStorageBytes() NOT_IMPLEMENTED

	// Writes are provided in an ordered stream.
	// A write is considered part of (a change leading to) the version determined by the previous call to setWriteVersion()
	// A write shall not become durable until the following call to commit() begins, and shall be durable once the following call to commit() returns
	virtual void set(KeyValueRef keyValue) {
		ASSERT(m_writeVersion != invalidVersion);
		m_buffer[keyValue.key.toString()].push_back({m_writeVersion, keyValue.value.toString()});
	}
	virtual void clear(KeyRangeRef range) NOT_IMPLEMENTED
	virtual void mutate(int op, StringRef param1, StringRef param2) NOT_IMPLEMENTED

	// Versions [begin, end) no longer readable
	virtual void forgetVersions(Version begin, Version end) NOT_IMPLEMENTED

	virtual Future<Version> getLatestVersion() {
		if(m_writeVersion != invalidVersion)
			return m_writeVersion;
		return m_pager->getLatestVersion();
	}

	VersionedBTree(IPager *pager, int page_size_override = -1) : m_pager(pager), m_writeVersion(invalidVersion), m_page_size_override(page_size_override) {
	}

	ACTOR static Future<Void> init(VersionedBTree *self) {
		// TODO: don't just create a new root, load the existing one
		Version latest = wait(self->m_pager->getLatestVersion());
		self->m_root = self->m_pager->allocateLogicalPage();
		Version v = latest + 1;
		IPager *pager = self->m_pager;
		self->writePage(self->m_root, FixedSizeMap::emptyPage(EPageFlags::IS_LEAF, [pager](){ return pager->newPageBuffer(); }), v);
		self->m_pager->setLatestVersion(v);
		Void _ = wait(self->m_pager->commit());
		return Void();
	}

	Future<Void> init() { return init(this); }

	virtual ~VersionedBTree() {}

	// readAtVersion() may only be called on a version which has previously been passed to setWriteVersion() and never previously passed
	//   to forgetVersion.  The returned results when violating this precondition are unspecified; the store is not required to be able to detect violations.
	// The returned read cursor provides a consistent snapshot of the versioned store, corresponding to all the writes done with write versions less
	//   than or equal to the given version.
	// If readAtVersion() is called on the *current* write version, the given read cursor MAY reflect subsequent writes at the same
	//   write version, OR it may represent a snapshot as of the call to readAtVersion().
	virtual Reference<IStoreCursor> readAtVersion(Version v) {
		// TODO: Use the buffer to return uncommitted data
		return Reference<IStoreCursor>(new Cursor(v, m_pager, m_root));
	}

	// Must be nondecreasing
	virtual void setWriteVersion(Version v) {
		ASSERT(v >= m_writeVersion);
		m_writeVersion = v;
		//m_pager->setLatestVersion(v);
	}

	virtual Future<Void> commit() {
		return commit_impl(this);
	}

private:
	void writePage(LogicalPageID id, Reference<IPage> page, Version ver) {
		FixedSizeMap map = FixedSizeMap::decode(StringRef(page->begin(), page->size()));
		printf("Writing page: id=%d ver=%lld %s\n", id, ver, map.toString().c_str());
		m_pager->writePage(id, page, ver);
	}

	LogicalPageID m_root;

	typedef std::pair<std::string, LogicalPageID> KeyPagePairT;
	typedef std::pair<Version, std::vector<KeyPagePairT>> VersionedKeyToPageSetT;
	typedef std::vector<VersionedKeyToPageSetT> VersionedChildrenT;
	typedef std::map<std::string, std::vector<std::pair<Version, std::string>>> MutationBufferT;
	struct KeyVersionValue {
		KeyVersionValue(Key k, Version ver, Value val) : key(k), version(ver), value(val) {}
		bool operator< (KeyVersionValue const &rhs) const {
			int64_t cmp = key.compare(rhs.key);
			if(cmp == 0) {
				cmp = version - rhs.version;
				if(cmp == 0)
					return false;
			}
			return cmp < 0;
		}
		Key key;
		Version version;
		Value value;
	};

	void buildNewRoot(Version version, vector<std::pair<int, Reference<IPage>>> &pages, std::vector<LogicalPageID> &logicalPageIDs, FixedSizeMap::KVPairsT &childEntries) {
		// While there are multiple child pages for this version we must write new tree levels.
		while(pages.size() > 1) {
			FixedSizeMap::KVPairsT newChildEntries;
			for(int i=0; i<pages.size(); i++)
				newChildEntries.push_back( {childEntries[pages[i].first].first, std::string((char *)&logicalPageIDs[i], sizeof(uint32_t))});
			childEntries = std::move(newChildEntries);

			pages = FixedSizeMap::buildMany( childEntries, 0, [=](){ return m_pager->newPageBuffer(); }, m_page_size_override);

			printf("Writing a new root level at version %lld with %lu children across %lu pages\n", version, childEntries.size(), pages.size());

			// Allocate logical page ids for the new level
			logicalPageIDs.clear();

			// Only reuse root if there's one replacement page being written or if the subtree root is not the tree root
			if(pages.size() == 1)
				logicalPageIDs.push_back(m_root);

			// Allocate enough pageIDs for all of the pages
			for(int i=logicalPageIDs.size(); i<pages.size(); i++)
				logicalPageIDs.push_back( m_pager->allocateLogicalPage() );

			for(int i=0; i<pages.size(); i++)
				writePage( logicalPageIDs[i], pages[i].second, version );
		}
	}


	// Returns list of (version, list of (lower_bound, list of children) )
	ACTOR static Future<VersionedChildrenT> commitSubtree(VersionedBTree *self, Reference<IPagerSnapshot> snapshot, LogicalPageID root, std::string lowerBoundKey, MutationBufferT::const_iterator bufBegin, MutationBufferT::const_iterator bufEnd) {
		state std::string printPrefix = format("commit subtree(lowerboundkey %s, page %u) ", lowerBoundKey.c_str(), root);
		printf("%s\n", printPrefix.c_str());

		if(bufBegin == bufEnd) {
			printf("%s no changes\n", printPrefix.c_str());
			return VersionedChildrenT({ {0,{{lowerBoundKey,root}}} });
		}

		state FixedSizeMap map;
		Reference<const IPage> rawPage = wait(snapshot->getPhysicalPage(root));
		map = FixedSizeMap::decode(StringRef(rawPage->begin(), rawPage->size()));
		printf("%s Read page %d: %s\n", printPrefix.c_str(), root, map.toString().c_str());

		if(map.flags & EPageFlags::IS_LEAF) {
			VersionedChildrenT results;
			FixedSizeMap::KVPairsT kvpairs;

			// Fill existing with records from the roof page (which is a leaf)
			std::vector<KeyVersionValue> existing;
			for(auto const &kv : map.entries) {
				Tuple t = Tuple::unpack(kv.first);
				existing.push_back(KeyVersionValue(t.getString(0), t.getInt(1), StringRef(kv.second)));
			}

			// Fill mutations with changes begin committed
			std::vector<KeyVersionValue> mutations;
			Version minVersion = std::numeric_limits<Version>::max();
			MutationBufferT::const_iterator iBuf = bufBegin;
			while(iBuf != bufEnd) {
				Key k = StringRef(iBuf->first);
				for(auto const &vv : iBuf->second) {
					printf("Inserting %s %s @%lld\n", k.toString().c_str(), vv.second.c_str(), vv.first);
					mutations.push_back(KeyVersionValue(k, vv.first, StringRef(vv.second)));
					minVersion = std::min(minVersion, vv.first);
				}
				++iBuf;
			}

			std::vector<KeyVersionValue> merged;
			std::merge(existing.cbegin(), existing.cend(), mutations.cbegin(), mutations.cend(), std::back_inserter(merged));

			// TODO: Make version and key splits based on contents of merged list

			FixedSizeMap::KVPairsT leafEntries;
			for(auto const &kvv : merged) {
				Tuple t;
				t.append(kvv.key);
				t.append(kvv.version);
				leafEntries.push_back({t.pack().toString(), kvv.value.toString()});
			}

			IPager *pager = self->m_pager;
			vector< std::pair<int, Reference<IPage>> > pages = FixedSizeMap::buildMany( leafEntries, EPageFlags::IS_LEAF, [pager](){ return pager->newPageBuffer(); }, self->m_page_size_override);

			// If there isn't still just a single page of data then return the previous lower bound and page ID that lead to this page to be used for version 0
			if(pages.size() != 1) {
				results.push_back( {0, {{lowerBoundKey, root}}} );
			}

			// For each IPage of data, assign a logical pageID.
			std::vector<LogicalPageID> logicalPages;

			// Only reuse first page if only one page is being returned or if root is not the btree root.
			if(pages.size() == 1 || root != self->m_root)
				logicalPages.push_back(root);

			// Allocate enough pageIDs for all of the pages
			for(int i=logicalPages.size(); i<pages.size(); i++)
				logicalPages.push_back(self->m_pager->allocateLogicalPage() );

			// Write each page using its assigned page ID
			printf("%s Writing %lu replacement pages for %d at version %lld\n", printPrefix.c_str(), pages.size(), root, minVersion);
			for(int i=0; i<pages.size(); i++)
				self->writePage(logicalPages[i], pages[i].second, minVersion);

			// If this commitSubtree() is operating on the root, write new levels if needed until until we're returning a single page
			if(root == self->m_root)
				self->buildNewRoot(minVersion, pages, logicalPages, leafEntries);

			results.push_back({minVersion, {}});

			for(int i=0; i<pages.size(); i++) {
				// Actorcompiler doesn't like using #if here since there are no lines of code after this loop
				if(INTERNAL_PAGES_HAVE_TUPLES)
					results.back().second.push_back( {leafEntries[pages[i].first].first, logicalPages[i]} );
				else {
					Tuple t = Tuple::unpack(leafEntries[pages[i].first].first);
					results.back().second.push_back( {t.getString(0).toString(), logicalPages[i]} );
				}
			}

			printf("%s DONE.\n", printPrefix.c_str());
			return results;
		}
		else {
			state std::vector<Future<VersionedChildrenT>> m_futureChildren;

			auto childMutBegin = bufBegin;

			for(int i=0; i<map.entries.size(); i++) {
				auto childMutEnd = bufEnd;
				if (i+1 != map.entries.size()) {
					if(INTERNAL_PAGES_HAVE_TUPLES) {
						Tuple t = Tuple::unpack(map.entries[i+1].first);
						childMutEnd = self->m_buffer.lower_bound( t.getString(0).toString() );
					}
					else
						childMutEnd = self->m_buffer.lower_bound( map.entries[i+1].first );
				}

				m_futureChildren.push_back(self->commitSubtree(self, snapshot, *(uint32_t*)map.entries[i].second.data(), map.entries[i].first, childMutBegin, childMutEnd));
				childMutBegin = childMutEnd;
			}

			Void _ = wait(waitForAll(m_futureChildren));

			bool modified = false;
			for( auto &c : m_futureChildren) {
				if(c.get().size() != 1 || c.get()[0].second.size() != 1) {
					modified = true;
					break;
				}
			}

			if(!modified) {
				printf("%s not modified.\n", printPrefix.c_str());
				return VersionedChildrenT({{0, {{lowerBoundKey, root}}}});
			}

			Version version = 0;
			VersionedChildrenT result;

			loop { // over version splits of this page
				Version nextVersion = std::numeric_limits<Version>::max();

				FixedSizeMap::KVPairsT childEntries;  // Logically std::vector<std::pair<std::string, LogicalPageID>> childEntries;

				// For each Future<VersionedChildrenT>
				printf("%s creating replacement pages for id=%d at Version %lld\n", printPrefix.c_str(), root, version);

				// If we're writing version 0, there is a chance that we don't have to write ourselves, if there are no changes
				bool modified = version != 0;

				for(int i = 0; i < m_futureChildren.size(); ++i) {
					const VersionedChildrenT &children = m_futureChildren[i].get();
					LogicalPageID pageID = *(uint32_t*)map.entries[i].second.data();

					printf("  Versioned page set that replaced page %d: %lu versions\n", pageID, children.size());
					for(auto &versionedPageSet : children) {
						printf("    version: %lld\n", versionedPageSet.first);
						for(auto &boundaryPage : versionedPageSet.second) {
							printf("      %s -> %u\n", boundaryPage.first.c_str(), boundaryPage.second);
						}
					}

					// Find the first version greater than the current version we are writing
					auto cv = std::upper_bound( children.begin(), children.end(), version, [](Version a, VersionedChildrenT::value_type const &b) { return a < b.first; } );

					// If there are no versions before the one we found, just update nextVersion and continue.
					if(cv == children.begin()) {
						printf("  First version (%lld) in set is greater than current, setting nextVersion and continuing\n", cv->first);
						nextVersion = std::min(nextVersion, cv->first);
						printf("  curr %lld next %lld\n", version, nextVersion);
						continue;
					}

					// If a version greater than the current version being written was found, update nextVersion
					if(cv != children.end()) {
						nextVersion = std::min(nextVersion, cv->first);
						printf("  curr %lld next %lld\n", version, nextVersion);
					}

					// Go back one to the last version that was valid prior to or at the current version we are writing
					--cv;

					printf("  Using children for version %lld from this set, building version %lld\n", cv->first, version);

					// If page count isn't 1 then the root is definitely modified
					modified = modified || cv->second.size() != 1;

					// Add the children at this version to the child entries list for the current version being built.
					for (auto &childPage : cv->second) {
						printf("  Adding child page '%s'\n", childPage.first.c_str());
						childEntries.push_back( {childPage.first, std::string((char *)&childPage.second, sizeof(uint32_t))});
					}
				}

				printf("Finished pass through futurechildren.  childEntries=%lu  version=%lld  nextVersion=%lld\n", childEntries.size(), version, nextVersion);

				if(modified) {
					// TODO: Track split points across iterations of this loop, so that they don't shift unnecessarily and
					// cause unnecessary path copying

					IPager *pager = self->m_pager;
					vector< std::pair<int, Reference<IPage>> > pages = FixedSizeMap::buildMany( childEntries, 0, [pager](){ return pager->newPageBuffer(); }, self->m_page_size_override);

					// For each IPage of data, assign a logical pageID.
					std::vector<LogicalPageID> logicalPages;

					// Only reuse first page if only one page is being returned or if root is not the btree root.
					if(pages.size() == 1 || root != self->m_root)
						logicalPages.push_back(root);

					// Allocate enough pageIDs for all of the pages
					for(int i=logicalPages.size(); i<pages.size(); i++)
						logicalPages.push_back( self->m_pager->allocateLogicalPage() );

					// Write each page using its assigned page ID
					printf("Writing internal pages, subtreeRoot=%u\n", root);
					for(int i=0; i<pages.size(); i++)
						self->writePage( logicalPages[i], pages[i].second, version );

					// If this commitSubtree() is operating on the root, write new levels if needed until until we're returning a single page
					if(root == self->m_root)
						self->buildNewRoot(version, pages, logicalPages, childEntries);

					result.resize(result.size()+1);
					result.back().first = version;

					for(int i=0; i<pages.size(); i++)
						result.back().second.push_back( {childEntries[pages[i].first].first, logicalPages[i]} );

					if (result.size() > 1 && result.back().second == result.end()[-2].second) {
						printf("Output same as last version, popping it.\n");
						result.pop_back();
					}
				}
				else {
					printf("Version 0 has no changes\n");
					result.push_back({0, {{lowerBoundKey, root}}});
				}

				if (nextVersion == std::numeric_limits<Version>::max())
					break;
				version = nextVersion;
			}

			printf("%s DONE.\n", printPrefix.c_str());
			return result;
		}
	}

	ACTOR static Future<Void> commit_impl(VersionedBTree *self) {
		Version latestVersion = wait(self->m_pager->getLatestVersion());

		VersionedChildrenT _ = wait(commitSubtree(self, self->m_pager->getReadSnapshot(latestVersion), self->m_root, std::string(), self->m_buffer.begin(), self->m_buffer.end()));

		self->m_pager->setLatestVersion(self->m_writeVersion);
		Void _ = wait(self->m_pager->commit());

		self->m_buffer.clear();
		return Void();
	}

	IPager *m_pager;
	MutationBufferT m_buffer;
	Version m_writeVersion;
	int m_page_size_override;

	class Cursor : public IStoreCursor, public ReferenceCounted<Cursor> {
	public:
		Cursor(Version version, IPager *pager, LogicalPageID root)
		  : m_version(version), m_pager(pager->getReadSnapshot(version)), m_root(root) {
		}
		virtual ~Cursor() {}

		virtual Future<Void> findFirstGreaterOrEqual(KeyRef key, int prefetchNextBytes) NOT_IMPLEMENTED
		virtual Future<Void> findLastLessOrEqual(KeyRef key, int prefetchPriorBytes) NOT_IMPLEMENTED
		virtual Future<Void> next(bool needValue) NOT_IMPLEMENTED
		virtual Future<Void> prev(bool needValue) NOT_IMPLEMENTED

		virtual bool isValid() {
			return m_kv.present();
		}

		virtual KeyRef getKey() {
			return m_kv.get().key;
		}
		//virtual StringRef getCompressedKey() = 0;
		virtual ValueRef getValue() {
			return m_kv.get().value;
		}

		virtual void invalidateReturnedStrings() {
			m_pager->invalidateReturnedPages();
		}

		Version m_version;
		Reference<IPagerSnapshot> m_pager;
		Optional<KeyValueRef> m_kv;
		Arena m_arena;
		LogicalPageID m_root;

		void addref() { ReferenceCounted<Cursor>::addref(); }
		void delref() { ReferenceCounted<Cursor>::delref(); }

		ACTOR static Future<Void> findEqual_impl(Reference<Cursor> self, KeyRef key) {
			state LogicalPageID pageNumber = self->m_root;

			state Tuple t;
			t.append(key);
			t.append(self->m_version);
			state KeyRef tupleKey = t.pack();

			loop {
				Reference<const IPage> rawPage = wait(self->m_pager->getPhysicalPage(pageNumber));
				FixedSizeMap map = FixedSizeMap::decode(StringRef(rawPage->begin(), rawPage->size()));
				//printf("Read page %d @%lld: %s\n", pageNumber, self->m_version, map.toString().c_str());

				// Special case of empty page (which should only happen for root)
				if(map.entries.empty()) {
					ASSERT(pageNumber == self->m_root);
					self->m_kv = Optional<KeyValueRef>();
					return Void();
				}

				if(map.flags && EPageFlags::IS_LEAF) {
					int i = map.findLastLessOrEqual(tupleKey);
					if(i >= 0 && Tuple::unpack(map.entries[i].first).getString(0) == key) {
						self->m_kv = Standalone<KeyValueRef>(KeyValueRef(key, map.entries[i].second), self->m_arena);
					}
					else {
						self->m_kv = Optional<KeyValueRef>();
					}
					return Void();
				}
				else {
					int i = map.findLastLessOrEqual(INTERNAL_PAGES_HAVE_TUPLES ? tupleKey : key);
					i = std::max(i, 0);
					pageNumber = *(uint32_t *)map.entries[i].second.data();
				}
			}
		}

		virtual Future<Void> findEqual(KeyRef key) {
			return findEqual_impl(Reference<Cursor>::addRef(this), key);
		}
	};
};

KeyValue randomKV() {
	int kLen = g_random->randomInt(1, 10);
	int vLen = g_random->randomInt(0, 5);
	KeyValue kv;
	kv.key = makeString(kLen, kv.arena());
	kv.value = makeString(vLen, kv.arena());
	for(int i = 0; i < kLen; ++i)
		mutateString(kv.key)[i] = (uint8_t)g_random->randomInt('a', 'm');
	for(int i = 0; i < vLen; ++i)
		mutateString(kv.value)[i] = (uint8_t)g_random->randomInt('n', 'z');
	return kv;
}

TEST_CASE("/redwood/set") {
	state IPager *pager = new MemoryPager();
	state VersionedBTree *btree = new VersionedBTree(pager, g_random->randomInt(50, 200));
	Void _ = wait(btree->init());

	state std::map<std::pair<std::string, Version>, std::string> written;

	Version lastVer = wait(btree->getLatestVersion());
	state Version version = lastVer + 1;
	state int commits = g_random->randomInt(1, 20);
	//printf("Will do %d commits\n", commits);
	while(commits--) {
		int versions = g_random->randomInt(1, 20);
		//printf("  Commit will have %d versions\n", versions);
		while(versions--) {
			btree->setWriteVersion(version);
			int changes = g_random->randomInt(0, 20);
			//printf("    Version %lld will have %d changes\n", version, changes);
			while(changes--) {
				KeyValue kv = randomKV();
				//printf("      Set '%s' -> '%s' @%lld\n", kv.key.toString().c_str(), kv.value.toString().c_str(), version);
				btree->set(kv);
				written[std::make_pair(kv.key.toString(), version)] = kv.value.toString();
			}
			++version;
		}
		Void _ = wait(btree->commit());

		// Check that all writes can be read at their written versions
		state std::map<std::pair<std::string, Version>, std::string>::const_iterator i = written.cbegin();
		state std::map<std::pair<std::string, Version>, std::string>::const_iterator iEnd = written.cend();
		state int errors = 0;

		printf("Checking changes committed thus far.\n");
		while(i != iEnd) {
			state std::string key = i->first.first;
			state Version ver = i->first.second;
			state std::string val = i->second;

			state Reference<IStoreCursor> cur = btree->readAtVersion(ver);

			Void _ = wait(cur->findEqual(i->first.first));

			if(!(cur->isValid() && cur->getKey() == key && cur->getValue() == val)) {
				++errors;
				if(!cur->isValid())
					printf("Verify failed: key_not_found: '%s' -> '%s' @%lld\n", key.c_str(), val.c_str(), ver);
				else if(cur->getKey() != key)
					printf("Verify failed: key_incorrect: found '%s' expected '%s' @%lld\n", cur->getKey().toString().c_str(), key.c_str(), ver);
				else if(cur->getValue() != val)
					printf("Verify failed: value_incorrect: for '%s' found '%s' expected '%s' @%lld\n", cur->getKey().toString().c_str(), cur->getValue().toString().c_str(), val.c_str(), ver);
			}
			++i;
		}

		printf("%d sets, %d errors\n", (int)written.size(), errors);

		if(errors != 0)
			throw internal_error();
	}

	return Void();
}

std::string SimpleFixedSizeMapRef::toString() {
	std::string result;
	result.append(format("flags=0x%x data: ", flags));
	for(auto const &kv : entries) {
		result.append(" ");
		if(INTERNAL_PAGES_HAVE_TUPLES || flags && VersionedBTree::IS_LEAF) {
			Tuple t = Tuple::unpack(kv.first);
			result.append("[");
			for(int i = 0; i < t.size(); ++i) {
				if(i != 0)
					result.append(",");
				if(t.getType(i) == Tuple::ElementType::BYTES)
					result.append(format("%s", t.getString(i).toString().c_str()));
				if(t.getType(i) == Tuple::ElementType::INT)
					result.append(format("%lld", t.getInt(i)));
			}
		}
		else
			result.append(format("'%s'", printable(StringRef(kv.first)).c_str()));
		result.append("->");
		if(flags && VersionedBTree::IS_LEAF)
			result.append(format("'%s'", printable(StringRef(kv.second)).c_str()));
		else
			result.append(format("%u", *(const uint32_t *)kv.second.data()));
		result.append("]");
	}

	return result;
}
