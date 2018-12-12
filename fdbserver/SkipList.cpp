/*
 * SkipList.cpp
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

#include <stdint.h>
#include <memory.h>
#include <stdio.h>
#include <algorithm>
#include <numeric>
#include <string>

/*
#ifdef __GNUG__
#include <smmintrin.h>
#endif
*/

#include "flow/Platform.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbrpc/PerfMetric.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/KeyRangeMap.h"
#include "fdbclient/SystemData.h"
#include "Knobs.h"

#define PARALLEL_THREAD_COUNT 0	// FIXME: When >1, program execution (e.g. random numbers) is/was nondeterministic.  Why?

using std::min;
using std::max;
using std::make_pair;

static vector<PerfDoubleCounter*> skc;

static thread_local uint32_t g_seed = 0;

static inline int skfastrand() {
	g_seed = g_seed * 1664525L + 1013904223L;
	return g_seed;
}

void setAffinity(int proc);

class SlowConflictSet {
public:
	bool is_conflict( const VectorRef<KeyRangeRef>& readRanges, Version read_snapshot );
	void add( const VectorRef<KeyRangeRef>& clearRanges, const VectorRef<KeyValueRef>& setValues, Version now );
	void clear( Version now );

private:
	KeyRangeMap<Version> age;
};

bool SlowConflictSet::is_conflict( const VectorRef<KeyRangeRef>& readRanges, Version read_snapshot ) {
	for(auto range = readRanges.begin(); range != readRanges.end(); ++range) {
		auto intersecting = age.intersectingRanges( *range );
		for(auto it = intersecting.begin(); it != intersecting.end(); ++it)
			if ( it.value() > read_snapshot )
				return true;
	}
	return false;
}

void SlowConflictSet::clear( Version now ) {
	age.insert(allKeys, now);
}

void SlowConflictSet::add( const VectorRef<KeyRangeRef>& clearRanges, const VectorRef<KeyValueRef>& setValues, Version now ) {
	for(auto c = clearRanges.begin(); c != clearRanges.end(); ++c)
		age.insert( *c, now );
	for(auto s = setValues.begin(); s != setValues.end(); ++s)
		age.insert( s->key, now );
}


PerfDoubleCounter 
	g_buildTest("Build", skc),
	g_add("Add", skc),
	g_add_sort("A.Sort", skc),
	g_detectConflicts("Detect", skc),
	g_sort("D.Sort", skc),
	g_combine("D.Combine", skc),
	g_checkRead("D.CheckRead", skc),
	g_checkBatch("D.CheckIntraBatch", skc),
	g_merge("D.MergeWrite", skc),
	g_merge_launch("D.Merge.Launch", skc),
	g_merge_fork("D.Merge.Fork", skc),
	g_merge_start_var("D.Merge.StartVariance", skc),
	g_merge_end_var("D.Merge.EndVariance", skc),
	g_merge_run_var("D.Merge.RunVariance", skc),
	g_merge_run_shortest("D.Merge.ShortestRun", skc),
	g_merge_run_longest("D.Merge.LongestRun", skc),
	g_merge_run_total("D.Merge.TotalRun", skc),
	g_merge_join("D.Merge.Join", skc),
	g_removeBefore("D.RemoveBefore", skc)
	;

static force_inline int compare( const StringRef& a, const StringRef& b ) {
	int c = memcmp( a.begin(), b.begin(), min(a.size(), b.size()) );
	if (c<0) return -1;
	if (c>0) return +1;
	if (a.size() < b.size()) return -1;
	if (a.size() == b.size()) return 0;
	return +1;
}

struct ReadConflictRange {
	StringRef begin, end;
	Version version;
	int transaction;
	ReadConflictRange( StringRef begin, StringRef end, Version version, int transaction )
		: begin(begin), end(end), version(version), transaction(transaction)
	{
	}
	bool operator<(const ReadConflictRange& rhs) const { return compare(begin, rhs.begin)<0; }
};


struct KeyInfo {
	StringRef key;
	int* pIndex;
	bool nextKey;
	bool begin;
	bool write;
	int transaction;

	KeyInfo() {};
	KeyInfo( StringRef key, bool nextKey, bool begin, bool write, int transaction, int* pIndex ) : key(key), nextKey(nextKey), begin(begin), write(write), transaction(transaction), pIndex(pIndex) {}
};

// returns true if done with string
force_inline bool getCharacter(const KeyInfo& ki, int character, int &outputCharacter){
	// normal case
	if (character < ki.key.size()){
		outputCharacter = 5 + ki.key.begin()[character];
		return false;
	}

	// nextKey append a zero
	if (ki.nextKey && character >= ki.key.size()){
		if (character == ki.key.size()){
			outputCharacter =  5; // extra '0' character
			return false;
		}
		character--;
	}

	// termination
	if (character == ki.key.size()){
		outputCharacter = 0;
		return false; 
	}

	if (character == ki.key.size()+1) {
		// end/begin+read/write relative sorting
		outputCharacter = ki.begin*2 + (ki.write ^ ki.begin);
		return false;
	}

	outputCharacter = 0;
	return true;
}

bool operator < ( const KeyInfo& lhs, const KeyInfo& rhs ) {
	int i = min(lhs.key.size(), rhs.key.size());
	int c = memcmp( lhs.key.begin(), rhs.key.begin(), i );
	if (c!=0) return c<0;

	// SOMEDAY: This is probably not very fast.  Slows D.Sort by ~20% relative to previous (incorrect) version.

	bool lDone, rDone;
	int lc, rc;
	while (true) {
		lDone = getCharacter(lhs, i, lc);
		rDone = getCharacter(rhs, i, rc);
		if (lDone && rDone) return false;		// equality
		if (lc < rc) return true;
		if (lc > rc) return false;
		i++;
	}
}

bool operator == (const KeyInfo& lhs, const KeyInfo& rhs ) {
	return !(lhs<rhs || rhs<lhs);
}

void swapSort(vector<KeyInfo>& points, int a, int b){
	if (points[b] < points[a]){
		KeyInfo temp;
		temp = points[a];
		points[a] = points[b];
		points[b] = temp;
	}
}

void smallSort(vector<KeyInfo>& points, int start, int N){
	for (int i=1;i<N;i++)
		for (int j=i;j>0;j-=2)
			swapSort(points, start+j-1, start+j);
	for (int i=N-2;i>0;i--)
		for (int j=i;j>0;j-=2)
			swapSort(points, start+j-1, start+j);
}

struct SortTask {
	int begin;
	int size;
	int character;
	SortTask(int begin, int size, int character) : begin(begin), size(size), character(character) {}
};

void sortPoints(vector<KeyInfo>& points){
	vector<SortTask> tasks;
	vector<KeyInfo> newPoints;
	vector<int> counts;

	tasks.push_back( SortTask(0, points.size(), 0) );

	while (tasks.size()){
		SortTask st = tasks.back();
		tasks.pop_back();

		if (st.size < 10){
			//smallSort(points, st.begin, st.size);
			std::sort(points.begin() + st.begin, points.begin() + st.begin + st.size );
			continue;
		}

		newPoints.resize(st.size);
		counts.assign(256+5, 0);

		// get counts
		int c;
		bool allDone = true;
		for (int i=st.begin; i<st.begin+st.size; i++){
			allDone &= getCharacter(points[i], st.character, c);
			counts[c]++;
		}
		if (allDone)
			continue;

		// calculate offsets from counts and build next level of tasks
		int total=0;
		for(int i=0;i<counts.size();i++){
			int temp = counts[i];
			if (temp > 1)
				tasks.push_back(SortTask(st.begin+total, temp, st.character+1));
			counts[i] = total;
			total += temp;
		}

		// put in their places
		for (int i=st.begin; i<st.begin+st.size; i++){
			getCharacter(points[i], st.character, c);
			newPoints[counts[c]++] = points[i];
		}

		//copy back into original points array
		for (int i=0;i<st.size;i++)
			points[st.begin+i] = newPoints[i];
	}

	//cout << endl << "Radix sort done" << endl;
}

class SkipList : NonCopyable
{
private:
	static const int MaxLevels = 26;

	int randomLevel() {
		/*int l = 0;
		while (g_random->random01() < 0.5 && l < MaxLevels-1) l++;
		return l; */

		//g_random->randomInt(0, 1<<(MaxLevels-1));
		uint32_t i = uint32_t(skfastrand()) >> (32-(MaxLevels-1));
		int level = 0;
		while (i&1) {
			i>>=1;
			level++;
		}
		ASSERT( level < MaxLevels );
		return level;
	}

	/*
	struct Node {
		int nPointers, valueLength;
		Node *pointers[nPointers];
		Version maxVersions[nPointers];
		char value[valueLength];
	};
	*/

	struct Node {
		int level() { return nPointers-1; }
		uint8_t* value() { return end() + nPointers*(sizeof(Node*)+sizeof(Version)); }
		int length() { return valueLength; }
		Node* getNext(int i) { return *((Node**)end() + i); }
		void setNext(int i, Node* n) { 
			*((Node**)end() + i) = n; 
			#if defined(_DEBUG) || 1
			/*if (n && n->level() < i)
				*(volatile int*)0 = 0;*/
			#endif
		}

		Version getMaxVersion(int i) { return ((Version*)(end() + nPointers*sizeof(Node*)))[i]; }
		void setMaxVersion(int i, Version v) { ((Version*)(end() + nPointers*sizeof(Node*)))[i] = v; }

		// Return a node with initialized value but uninitialized pointers
		static Node* create( const StringRef& value, int level ) {
			int nodeSize = sizeof(Node) + value.size() + (level+1)*(sizeof(Node*)+sizeof(Version));

			Node* n;
			if (nodeSize <= 64) {
				n = (Node*)FastAllocator<64>::allocate();
				INSTRUMENT_ALLOCATE("SkipListNode64");
			} else if (nodeSize <= 128) {
				n = (Node*)FastAllocator<128>::allocate();
				INSTRUMENT_ALLOCATE("SkipListNode128");
			} else {
				n = (Node*)new char[ nodeSize ];
				INSTRUMENT_ALLOCATE("SkipListNodeLarge");
			}

			n->nPointers = level+1;

			n->valueLength = value.size();
			memcpy(n->value(), value.begin(), value.size());
			return n;
		}

		// pre: level>0, all lower level nodes between this and getNext(level) have correct maxversions
		void calcVersionForLevel(int level){
			Node *end = getNext(level);
			Version v = getMaxVersion(level-1);
			for(Node *x = getNext(level-1); x != end; x = x->getNext(level-1))
				v = max(v, x->getMaxVersion(level-1));
			setMaxVersion(level, v);
		}

		void destroy() {
			int nodeSize = getNodeSize();
			if (nodeSize <= 64) {
				FastAllocator<64>::release(this);
				INSTRUMENT_RELEASE("SkipListNode64");
			} else if (nodeSize <= 128) {
				FastAllocator<128>::release(this);
				INSTRUMENT_RELEASE("SkipListNode128");
			} else {
				delete[] (char*)this;
				INSTRUMENT_RELEASE("SkipListNodeLarge");
			}
		}
	private:
		int getNodeSize() { return sizeof(Node) + valueLength + nPointers*(sizeof(Node*)+sizeof(Version)); }
		uint8_t* end() { return (uint8_t*)(this+1); }
		int nPointers,
			valueLength;
	};

	static force_inline bool less( const uint8_t* a, int aLen, const uint8_t* b, int bLen ) {
		int len = min(aLen, bLen);
		for(int i=0; i<len; i++)
			if (a[i] < b[i])
				return true;
			else if (a[i] > b[i])
				return false;

		/*int c = memcmp(a,b,min(aLen,bLen));
		if (c<0) return true;
		if (c>0) return false;*/
		return aLen < bLen;
	}

	Node *header;

	void destroy() {
		Node *next, *x;
		for(x = header; x; x = next) {
			next = x->getNext(0);
			x->destroy();
		}
	}
public:
	struct Finger{
		Node *finger[MaxLevels]; // valid for levels >= level
		int level;
		Node* x;
		Node *alreadyChecked;
		StringRef value;

		Finger() : level(MaxLevels), x(NULL), alreadyChecked(NULL) {}

		Finger( Node* header, const StringRef& ptr ) :
			value(ptr), level(MaxLevels),
			alreadyChecked(NULL), x(header)
		{
		}

		void init(const StringRef& value, Node *header){
			this->value = value;
			x = header;
			alreadyChecked = NULL;
			level = MaxLevels;
		}

		// pre: !finished()
		force_inline void prefetch() {
			Node* next = x->getNext(level-1);
			_mm_prefetch( (const char*)next, _MM_HINT_T0 );
			//if ( (((intptr_t)next) & 64) == 0 )
			_mm_prefetch( (const char*)next+64, _MM_HINT_T0 );
			//_mm_prefetch( (const char*)next+128, _MM_HINT_T0 );
			//_mm_prefetch( (const char*)next+192, _MM_HINT_T0 );
			//_mm_prefetch( (const char*)next+256, _MM_HINT_T0 );
			//_mm_prefetch( (const char*)next+320, _MM_HINT_T0 );
		}

		// pre: !finished()
		// Returns true if we have advanced to the next level
		force_inline bool advance() {
			Node* next = x->getNext(level-1);
			
			if (next == alreadyChecked || !less(next->value(), next->length(), value.begin(), value.size())) {
				alreadyChecked = next;
				level--;
				finger[level] = x;
				return true;
			} else {
				x = next;
				return false;
			}
		}

		// pre: !finished()
		force_inline void nextLevel() {
			while (!advance());
		}

		force_inline bool finished(){
			return level == 0;
		}

		force_inline Node* found() const {
			// valid after finished returns true
			Node *n = finger[0]->getNext(0);	// or alreadyChecked, but that is more easily invalidated
			if (n && n->length() == value.size() && !memcmp(n->value(), value.begin(), value.size()))
				return n;
			else 
				return NULL;
		}

		StringRef getValue() const {
			Node* n = finger[0]->getNext(0);
			return n ? StringRef( n->value(), n->length() ) : StringRef();
		}
	};

	int count() {
		int count = 0;
		Node* x = header->getNext(0);
		while (x) { 
			x = x->getNext(0); 
			count++; 
		}
		return count;
	}

	explicit SkipList( Version version = 0 ) {
		header = Node::create(StringRef(), MaxLevels-1);
		for(int l=0; l<MaxLevels; l++) {
			header->setNext(l, NULL);
			header->setMaxVersion(l, version);
		}
	}
	~SkipList() {
		destroy();
	}
	SkipList(SkipList&& other) noexcept(true)
		: header(other.header)
	{
		other.header = NULL;
	}
	void operator=(SkipList&& other) noexcept(true) {
		destroy();
		header = other.header;
		other.header = NULL;
	}
	void swap( SkipList& other ) {
		std::swap(header, other.header);
	}

	void addConflictRanges( const Finger* fingers, int rangeCount, Version version ) {
		for(int r=rangeCount-1; r>=0; r--) {
			const Finger& startF = fingers[r*2];
			const Finger& endF = fingers[r*2+1];

			if (endF.found()==NULL)
				insert(endF, endF.finger[0]->getMaxVersion(0));

			remove( startF, endF );
			insert( startF, version );
		}
	}

	void detectConflicts( ReadConflictRange* ranges, int count, bool* transactionConflictStatus ) {
		const int M = 16;
		int nextJob[M];
		CheckMax inProgress[ M ];
		if (!count) return;

		int started = min(M,count);
		for(int i=0; i<started; i++){
			inProgress[i].init( ranges[i], header, transactionConflictStatus );
			nextJob[i] = i+1;
		}
		nextJob[started-1] = 0;

		int prevJob = started-1;
		int job = 0;
		// vtune: 340 parts
		while (true) {
			if (inProgress[job].advance()) {
				if (started == count){
					if (prevJob == job) break;
					nextJob[prevJob] = nextJob[job];
					job = prevJob;
				}
				else
					inProgress[job].init( ranges[started++], header, transactionConflictStatus );
			}
			prevJob = job;
			job = nextJob[job];
		}
	}

	// Splits the version history represented by this skiplist into separate key ranges
	//   delimited by the given array of keys.  This SkipList is left empty.  this->partition
	//   is intended to be followed by a call to this->concatenate() recombining the same
	//   partitions.  In between, operations on each partition must not touch any keys outside
	//   the partition.  Specifically, the partition to the left of 'key' must not have a range
	//	 [...,key) inserted, since that would insert an entry at 'key'.
	void partition( StringRef* begin, int splitCount, SkipList* output ) {
		for(int i=splitCount-1; i>=0; i--) {
			Finger f( header, begin[i] );
			while (!f.finished()) 
				f.nextLevel();
			split(f, output[i+1]);
		}
		swap(output[0]);
	}

	void concatenate( SkipList* input, int count ) {
		vector<Finger> ends( count-1 );
		for(int i=0; i<ends.size(); i++)
			input[i].getEnd( ends[i] );

		for(int l=0; l<MaxLevels; l++) {
			for(int i=ends.size()-1; i>=0; i--) {
				ends[i].finger[l]->setNext( l, input[i+1].header->getNext(l) );
				if (l && (!i || ends[i].finger[l] != input[i].header))
					ends[i].finger[l]->calcVersionForLevel(l);
				input[i+1].header->setNext( l, NULL );
			}
		}
		swap(input[0]);
	}

	void find( const StringRef* values, Finger* results, int* temp, int count ) {
		// Relying on the ordering of values, descend until the values aren't all in the 
		// same part of the tree

		// vtune: 11 parts
		results[0].init( values[0], header );
		const StringRef& endValue = values[count-1];
		while ( results[0].level > 1 ) {
			results[0].nextLevel();
			Node* ac = results[0].alreadyChecked;
			if (ac && less(ac->value(), ac->length(), endValue.begin(), endValue.size()))
				break;
		}

		// Init all the other fingers to start descending where we stopped
		//   the first one

		// SOMEDAY: this loop showed up on vtune, could be faster?
		// vtune: 8 parts
		int startLevel = results[0].level+1;
		Node *x = startLevel<MaxLevels ? results[0].finger[startLevel] : header;
		for(int i=1; i<count; i++) {
			results[i].level = startLevel;
			results[i].x = x;
			results[i].alreadyChecked = NULL;
			results[i].value = values[i];
			for(int j=startLevel; j<MaxLevels; j++)
				results[i].finger[j] = results[0].finger[j];
		}

		int* nextJob = temp;
		for (int i=0;i<count-1;i++)
			nextJob[i] = i+1;
		nextJob[count-1] = 0;

		int prevJob = count-1;
		int job = 0;

		// vtune: 225 parts
		while (true) {
			Finger* f = &results[job];
			f->advance();
			if (f->finished()) {
				if (prevJob == job) break;
				nextJob[prevJob] = nextJob[job];
			}
			else {
				f->prefetch();
				prevJob = job;
			}
			job = nextJob[job];
		}
	}

	/*Finger randomFinger() {
		// Written, not exactly uniform, not tested
		Finger f( header, StringRef() );
		Node* begin = header, *end = 0;
		for(int lev = MaxLevels-1; lev>=0; lev--) {
			int length = 0;
			for( Node* x = begin; x != end; x=x->getNext(lev) )
				length++;
			if (length == 1) { // forced down
				f.finger[lev] = begin;
			} else {
				int c = g_random->randomInt(0, length);
				for( Node* x = begin; x != end; x=x->getNext(lev) )
					if (!c--) {
						f.finger[lev] = begin = x;
						end = x->getNext(lev);
						break;
					}
			}
		}
		f.level = 0;
		return f;
	}*/

	int removeBefore( Version v, Finger& f, int nodeCount ) {
		/*Finger f( header, StringRef() );
		for(int i=0; i<MaxLevels; i++)
			f.finger[i] = header;
		f.level = 0;*/
		// f.x, f.alreadyChecked?

		int removedCount = 0;
		bool wasAbove = true;
		while (nodeCount--) {
			Node* x = f.finger[0]->getNext(0);
			if (!x) break;
			
			// double prefetch gives +25% speed (single threaded)
			Node* next = x->getNext(0);
			_mm_prefetch( (const char*)next, _MM_HINT_T0 );
			//_mm_prefetch( (const char*)next+64, _MM_HINT_T0 );
			next = x->getNext(1);
			_mm_prefetch( (const char*)next, _MM_HINT_T0 );
			//_mm_prefetch( (const char*)next+64, _MM_HINT_T0 );

			bool isAbove = x->getMaxVersion(0) >= v;
			if (isAbove || wasAbove) {  // f.nextItem
				for(int l=0; l<=x->level(); l++)
					f.finger[l] = x;
			} else {					// f.eraseItem
				removedCount++;
				for(int l=0; l<=x->level(); l++)
					f.finger[l]->setNext(l, x->getNext(l));
				for(int i=1; i<=x->level(); i++)
					f.finger[i]->setMaxVersion( i, max(f.finger[i]->getMaxVersion(i), x->getMaxVersion(i)) );
				x->destroy();
			}
			wasAbove = isAbove;
		}

		return removedCount;
	}

private:
	void remove( const Finger& start, const Finger& end ) {
		if (start.finger[0] == end.finger[0]) 
			return;

		Node *x = start.finger[0]->getNext(0);

		// vtune says: this loop is the expensive parts (6 parts)
		for(int i=0; i<MaxLevels; i++)
			if (start.finger[i] != end.finger[i])
				start.finger[i]->setNext(i, end.finger[i]->getNext(i));

		while (true) {
			Node* next = x->getNext(0);
			x->destroy();
			if (x == end.finger[0]) break;
			x = next;
		}
	}

	//void insert( const std::string& v, Version version ) { insert(StringRef(v), version); }

	void insert( const Finger& f, Version version ) {
		int level = randomLevel();
		//cout << std::string((const char*)value,length) << " level: " << level << endl;
		Node *x = Node::create( f.value, level );
		x->setMaxVersion(0, version);
		for(int i=0; i<=level; i++) {
			x->setNext(i, f.finger[i]->getNext(i));
			f.finger[i]->setNext(i, x);
		}
		// vtune says: this loop is the costly part of this function
		for(int i=1; i<=level; i++) {
			f.finger[i]->calcVersionForLevel(i);
			x->calcVersionForLevel(i);
		}
		for(int i=level+1; i<MaxLevels; i++) {
			Version v = f.finger[i]->getMaxVersion(i);
			if (v >= version) break;
			f.finger[i]->setMaxVersion(i, version);
		}
	}

	void insert( const StringRef& value, Version version ) {
		Finger f(header, value);
		while (!f.finished())
			f.nextLevel();
		// SOMEDAY: equality?
		insert( f, version );
	}

	struct CheckMax {
		Finger start, end;
		Version version;
		bool *result;
		int state;

		void init( const ReadConflictRange& r, Node* header, bool* tCS ) {
			this->start.init( r.begin, header );
			this->end.init( r.end, header );
			this->version = r.version;
			result = &tCS[ r.transaction ];
			this->state = 0;
		}

		bool noConflict() { return true; }
		bool conflict() { *result = true; return true; }

		// Return true if finished
		force_inline bool advance() {
			switch (state) {
			case 0:
				// find where start and end fingers diverge
				while (true) {
					if (!start.advance()) {
						start.prefetch();
						return false;
					}
					end.x = start.x;
					while (!end.advance());

					int l = start.level;
					if (start.finger[l] != end.finger[l])
						break;
					// accept if the range spans the check range, but does not have a greater version
					if (start.finger[l]->getMaxVersion(l) <= version)
						return noConflict();
					if (l==0)
						return conflict();
				}
				state = 1;
			case 1: 
				{
					// check the end side of the pyramid
					Node *e = end.finger[end.level];
					while (e->getMaxVersion(end.level) > version) {
						if (end.finished()) 
							return conflict();
						end.nextLevel();
						Node *f = end.finger[end.level];
						while (e != f){
							if (e->getMaxVersion(end.level) > version) 
								return conflict();
							e = e->getNext(end.level);
						}
					}

					// check the start side of the pyramid
					Node *s = end.finger[start.level];
					while (true){
						Node *nextS = start.finger[start.level]->getNext(start.level);
						Node *p = nextS;
						while (p != s){
							if (p->getMaxVersion(start.level) > version) 
								return conflict();
							p = p->getNext(start.level);
						}
						if (start.finger[start.level]->getMaxVersion(start.level) <= version) 
							return noConflict();
						s = nextS;
						if (start.finished()) {
							if (nextS->length() == start.value.size() && !memcmp(nextS->value(), start.value.begin(), start.value.size()))
								return noConflict();
							else
								return conflict();
						}
						start.nextLevel();
					}
				}
			default:
				__assume(false);
			}
		}
	};

	void split( const Finger& f, SkipList& right ) {
		ASSERT( !right.header->getNext(0) );  // right must be empty
		right.header->setMaxVersion(0, f.finger[0]->getMaxVersion(0));
		for(int l=0; l<MaxLevels; l++) {
			right.header->setNext(l, f.finger[l]->getNext(l));
			f.finger[l]->setNext(l, NULL);
			/*if (l) {
				// SOMEDAY: Do we actually need these?
				right.header->calcVersionForLevel(l);
				f.finger[l]->calcVersionForLevel(l);
			}*/
		}
 	}

	void getEnd( Finger& end ) {
		Node* node = header;
		for(int l=MaxLevels-1; l>=0; l--) {
			Node* next;
			while ( (next=node->getNext(l)) != NULL ) 
				node = next;
			end.finger[l] = node;
		}
		end.level = 0;
		// SOMEDAY: end.x? end.alreadyChecked?
		/*end = Finger(header, (const uint8_t*)"\xff\xff\xff\xff\xff\xff", 6);
		while (!end.finished())
			end.nextLevel();*/
	}
};

struct Action { 
	virtual void operator()() = 0;		// self-destructs
};
typedef Action* PAction;

template <class F>
PAction action( F && f ) {
	struct FAction : Action, F, FastAllocated<FAction> {
		FAction( F&& f ) : F(std::move(f)) {}
		virtual void operator()() { F::operator()(); delete this; }
	};
	return new FAction( std::move(f) );
};

void workerThread( PAction* nextAction, Event* nextActionReady, int index, Event* whenFinished ) {
	ASSERT(false);
	/*
	inThread<Void>( [nextAction,nextActionReady,index,whenFinished]()->Void {
		g_seed = index*123; fastrand();
		setAffinity( index );
		while (true) {
			try {
				nextActionReady->block();   // auto-reset
				Action* action = *nextAction;
				*nextAction = 0;
				if (!action) break;

				(*action)();
			} catch (Error& e) {
				fprintf(stderr, "Error in worker thread: %s\n", e.what());
			} catch (...) {
				fprintf(stderr, "Error in worker thread: %s\n", unknown_error().what());
			}
		}
		//cout << "Worker thread finished" << endl;
		whenFinished->set();
		return Void();
	});*/
}

StringRef setK( Arena& arena, int i ) {
	char t[ sizeof(i) ];
	*(int*)t = i;

	const int keySize = 16;

	char* ss = new (arena) char[ keySize ];
	for(int c=0; c<keySize-sizeof(i); c++)
		ss[c] = '.';
	for(int c=0; c<sizeof(i); c++)
		ss[c+keySize-sizeof(i)] = t[sizeof(i)-1-c];

	return StringRef( (const uint8_t*)ss, keySize );
}

#include "ConflictSet.h"

struct ConflictSet {
	ConflictSet() : oldestVersion(0) {
		static_assert(PARALLEL_THREAD_COUNT == 0, "workerThread() not implemented");
		static_assert(PARALLEL_THREAD_COUNT == 0 || FASTALLOC_THREAD_SAFE, "Thread safe fast allocator required for multithreaded conflict set");
		for (int i = 0; i < PARALLEL_THREAD_COUNT; i++) {
			worker_nextAction.push_back( NULL );
			worker_ready.push_back( new Event );
			worker_finished.push_back( new Event );
		}
		for(int t=0; t<worker_nextAction.size(); t++)
			workerThread( &worker_nextAction[t], worker_ready[t], (t)*2, worker_finished[t] );
	}
	~ConflictSet() {
		for(int i=0; i<worker_nextAction.size(); i++) {
			worker_nextAction[i] = 0;
			worker_ready[i]->set();
		}
		// Wait for workers to terminate; otherwise can get crashes at shutdown time
		for(int i=0; i<worker_finished.size(); i++)
			worker_finished[i]->block();
	}

	SkipList versionHistory;
	Key removalKey;
	Version oldestVersion;
	vector<PAction> worker_nextAction;
	vector<Event*> worker_ready;
	vector<Event*> worker_finished;
};

ConflictSet* newConflictSet() { return new ConflictSet; }
void clearConflictSet( ConflictSet* cs, Version v ) {
	SkipList(v).swap( cs->versionHistory );
}
void destroyConflictSet(ConflictSet* cs) {
	delete cs;
}

ConflictBatch::ConflictBatch( ConflictSet* cs )
	: cs(cs), transactionCount(0)
{
}

ConflictBatch::~ConflictBatch()
{
}

struct TransactionInfo {
	VectorRef< std::pair<int,int> > readRanges;
	VectorRef< std::pair<int,int> > writeRanges;
	bool tooOld;
};

void ConflictBatch::addTransaction( const CommitTransactionRef& tr ) {
	int t = transactionCount++;

	Arena& arena = transactionInfo.arena();
	TransactionInfo* info = new (arena) TransactionInfo;

	if (tr.read_snapshot < cs->oldestVersion && tr.read_conflict_ranges.size()) {
		info->tooOld = true;
	} else {
		info->tooOld = false;
		info->readRanges.resize( arena, tr.read_conflict_ranges.size() );
		info->writeRanges.resize( arena, tr.write_conflict_ranges.size() );

		vector<KeyInfo> &points = this->points;
		for(int r=0; r<tr.read_conflict_ranges.size(); r++) {
			const KeyRangeRef& range = tr.read_conflict_ranges[r];
			points.push_back( KeyInfo( range.begin, false, true, false, t, &info->readRanges[r].first ) );
			//points.back().keyEnd = StringRef(buf,range.second);
			points.push_back( KeyInfo( range.end, false, false, false, t, &info->readRanges[r].second ) );
			combinedReadConflictRanges.push_back( ReadConflictRange( range.begin, range.end, tr.read_snapshot, t ) );
		}
		for(int r=0; r<tr.write_conflict_ranges.size(); r++) {
			const KeyRangeRef& range = tr.write_conflict_ranges[r];
			points.push_back( KeyInfo( range.begin, false, true, true, t, &info->writeRanges[r].first ) );
			points.push_back( KeyInfo( range.end, false, false, true, t, &info->writeRanges[r].second ) );
		}
	}

	this->transactionInfo.push_back( arena, info );
}

class MiniConflictSet2 : NonCopyable {
	vector<bool> values;
public:
	explicit MiniConflictSet2( int size ) {
		values.assign( size, false );
	}
	void set( int begin, int end ) {
		for(int i=begin; i<end; i++)
			values[i] = true;
	}
	bool any( int begin, int end ) {
		for(int i=begin; i<end; i++)
			if (values[i])
				return true;
		return false;
	}
};

class MiniConflictSet : NonCopyable {
	typedef uint64_t wordType;
	enum { bucketShift = 6, bucketMask=sizeof(wordType)*8-1 };
	vector<wordType> values; // undefined when andValues is true for a range of values
	vector<wordType> orValues;
	vector<wordType> andValues;
	MiniConflictSet2 debug;		// SOMEDAY: Test on big ranges, eliminate this

	uint64_t bitMask(unsigned int bit){ // computes results for bit%word
		return (((wordType)1) << ( bit & bucketMask )); // '&' unnecesary?
	}
	void setNthBit(vector<wordType> &v, const unsigned int bit){
		v[bit>>bucketShift] |= bitMask(bit);
	}
	void clearNthBit(vector<wordType> &v, const unsigned int bit){
		v[bit>>bucketShift] &= ~(bitMask(bit));
	}
	bool getNthBit(const vector<wordType> &v, const unsigned int bit){
		return (v[bit>>bucketShift] & bitMask(bit)) != 0;
	}
	int wordsForNBits(unsigned int bits){
		return (bits+((1<<bucketShift)-1))>>bucketShift;
	}
	wordType highBits(int b){ // bits (b&bucketMask) and higher are 1
		#pragma warning(disable: 4146)
		return -(wordType(1) << b);
		#pragma warning(default: 4146)
	}
	wordType lowBits(int b){ // bits lower than b are 1
		return (wordType(1)<<b)-1;
	}
	wordType lowBits2(int b) {
		return (b&bucketMask) ? lowBits(b) : -1;
	}

	void setBits(vector<wordType> &v, int bitBegin, int bitEnd, bool fillMiddle){
		if (bitBegin >= bitEnd)	return;
		int beginWord = bitBegin>>bucketShift;
		int lastWord = ((bitEnd+bucketMask) >> bucketShift) - 1;
		if (beginWord == lastWord){
			v[beginWord] |= highBits(bitBegin) & lowBits2(bitEnd);
		} else {
			v[beginWord] |= highBits(bitBegin);
			if (fillMiddle)
				for(int w=beginWord+1;w<lastWord;w++)
					v[w] = wordType(-1);
			v[lastWord] |= lowBits2(bitEnd);
		}
	}

	bool orBits(vector<wordType> &v, int bitBegin, int bitEnd, bool getMiddle) {
		if (bitBegin >= bitEnd) return false;
		int beginWord = bitBegin >> bucketShift;
		int lastWord = ((bitEnd+bucketMask) >> bucketShift) - 1;
		if (beginWord == lastWord)
			return (v[beginWord] & highBits(bitBegin) & lowBits2(bitEnd)) != 0;
		else {
			if (getMiddle)
				for(int w=beginWord+1; w<lastWord; w++)
					if (v[w])
						return true;
			return ((v[beginWord] & highBits(bitBegin)) | (v[lastWord] & lowBits2(bitEnd))) != 0;
		}
	}

public:
	explicit MiniConflictSet( int size ) : debug(size) {
		static_assert((1<<bucketShift) == sizeof(wordType)*8, "BucketShift incorrect");

		values.assign( wordsForNBits(size), false );
		orValues.assign( wordsForNBits(wordsForNBits(size)), false);
		andValues.assign( wordsForNBits(wordsForNBits(size)), false);
	}

	void set( int begin, int end ) {
		debug.set(begin,end);
		if (begin == end) return;

		int beginWord = begin>>bucketShift;
		int lastWord = ((end+bucketMask) >> bucketShift) - 1;

		setBits(values, begin, end, false);
		setBits(andValues, beginWord+1, lastWord, true);
		setBits(orValues, beginWord, lastWord+1, true);
	}

	bool any(int begin, int end) {
		bool a = orImpl(begin,end);
		bool b = debug.any(begin,end);
		ASSERT( a == b );
		return b;
	}

	bool orImpl( int begin, int end ) {
		if (begin == end) return false;
		int beginWord = begin>>bucketShift;
		int lastWord = ((end+bucketMask) >> bucketShift) - 1;

		return orBits( orValues, beginWord+1, lastWord, true ) ||
			getNthBit( andValues, beginWord ) || getNthBit( andValues, lastWord ) ||
			orBits( values, begin, end, false );
	}
};


void ConflictBatch::checkIntraBatchConflicts() {
	int index = 0;
	for(int p=0; p<points.size(); p++)
		*points[p].pIndex = index++;

	MiniConflictSet mcs( index );
	for(int t=0; t<transactionInfo.size(); t++) {
		const TransactionInfo& tr = *transactionInfo[t];
		if (transactionConflictStatus[t]) continue;
		bool conflict = tr.tooOld;
		for(int i=0; i<tr.readRanges.size(); i++)
			if ( mcs.any( tr.readRanges[i].first, tr.readRanges[i].second ) ) {
				conflict = true;
				break;
			}
		transactionConflictStatus[t] = conflict;
		if (!conflict)
			for(int i=0; i<tr.writeRanges.size(); i++)
				mcs.set( tr.writeRanges[i].first, tr.writeRanges[i].second );
	}
}

void ConflictBatch::GetTooOldTransactions(vector<int>& tooOldTransactions) {
	for (int i = 0; i<transactionInfo.size(); i++) {
		if (transactionInfo[i]->tooOld) {
			tooOldTransactions.push_back(i);
		}
	}
}

void ConflictBatch::detectConflicts(Version now, Version newOldestVersion, vector<int>& nonConflicting, vector<int>* tooOldTransactions) {
	double t = timer();
	sortPoints( points );
	//std::sort( combinedReadConflictRanges.begin(), combinedReadConflictRanges.end() );
	g_sort += timer()-t;

	transactionConflictStatus = new bool[ transactionCount ];
	memset(transactionConflictStatus, 0, transactionCount*sizeof(bool));

	t = timer();
	checkReadConflictRanges();
	g_checkRead += timer()-t;

	t = timer();
	checkIntraBatchConflicts();
	g_checkBatch += timer()-t;

	t = timer();
	combineWriteConflictRanges();
	g_combine += timer()-t;

	t = timer();
	mergeWriteConflictRanges(now);
	g_merge += timer()-t;
	
	for (int i = 0; i < transactionCount; i++)
	{
		if (!transactionConflictStatus[i])
			nonConflicting.push_back( i );
		if (tooOldTransactions && transactionInfo[i]->tooOld)
			tooOldTransactions->push_back(i);
	}

	delete[] transactionConflictStatus;

	t = timer();
	if (newOldestVersion > cs->oldestVersion) {
		cs->oldestVersion = newOldestVersion;
		SkipList::Finger finger; 
		int temp;
		cs->versionHistory.find( &cs->removalKey, &finger, &temp, 1 );
		cs->versionHistory.removeBefore( cs->oldestVersion, finger, combinedWriteConflictRanges.size()*3 + 10 );
		cs->removalKey = finger.getValue();
	}
	g_removeBefore += timer()-t;
}

void ConflictBatch::checkReadConflictRanges() {
	if (!combinedReadConflictRanges.size()) 
		return;

	if (PARALLEL_THREAD_COUNT) {
		Event done[PARALLEL_THREAD_COUNT?PARALLEL_THREAD_COUNT:1];
		for(int t=0; t<PARALLEL_THREAD_COUNT; t++) {
			cs->worker_nextAction[t] = action( [&,t] {
#pragma GCC diagnostic push
DISABLE_ZERO_DIVISION_FLAG
				auto begin = &combinedReadConflictRanges[0] + t*combinedReadConflictRanges.size()/PARALLEL_THREAD_COUNT;
				auto end = &combinedReadConflictRanges[0] + (t+1)*combinedReadConflictRanges.size()/PARALLEL_THREAD_COUNT;
#pragma GCC diagnostic pop
				cs->versionHistory.detectConflicts( begin, end-begin, transactionConflictStatus );
				done[t].set();
			});
			cs->worker_ready[t]->set();
		}
		for(int i=0; i<PARALLEL_THREAD_COUNT; i++)
			done[i].block();
	} else {
		cs->versionHistory.detectConflicts( &combinedReadConflictRanges[0], combinedReadConflictRanges.size(), transactionConflictStatus );
	}
}

void ConflictBatch::addConflictRanges(Version now, vector< pair<StringRef,StringRef> >::iterator begin, vector< pair<StringRef,StringRef> >::iterator end,SkipList* part) {
	int count = end-begin;
#if 0
	//for(auto w = begin; w != end; ++w)
	for(auto w = end-1; w != begin-1; --w)
		part->addConflictRange( w->first, w->second, now );
#else
	static_assert( sizeof( begin[0] ) == sizeof(StringRef)*2, "Write Conflict Range type not convertible to two StringPtrs" );
	const StringRef* strings = reinterpret_cast<const StringRef*>( &*begin );
	int stringCount = count*2;

	static const int stripeSize = 16;
	SkipList::Finger fingers[ stripeSize ];
	int temp[ stripeSize ];
	int stripes = (stringCount+stripeSize-1)/stripeSize;

	int ss = stringCount - (stripes-1)*stripeSize;
	for(int s=stripes-1; s>=0; s--) {
		part->find( &strings[s * stripeSize], &fingers[0], temp, ss );
		part->addConflictRanges( &fingers[0], ss/2, now );
		ss = stripeSize;
	}
#endif
}

void ConflictBatch::mergeWriteConflictRanges(Version now) {
	if (!combinedWriteConflictRanges.size()) 
		return;

	if (PARALLEL_THREAD_COUNT) {
		vector<SkipList> parts;
		for (int i = 0; i < PARALLEL_THREAD_COUNT; i++)
			parts.push_back(SkipList());

		vector<StringRef> splits( parts.size()-1 );
		for(int s=0; s<splits.size(); s++)
			splits[s] = combinedWriteConflictRanges[ (s+1)*combinedWriteConflictRanges.size()/parts.size() ].first;

		cs->versionHistory.partition( splits.size() ? &splits[0] : NULL, splits.size(), &parts[0] );
		vector<double> tstart(PARALLEL_THREAD_COUNT), tend(PARALLEL_THREAD_COUNT);
		Event done[PARALLEL_THREAD_COUNT ? PARALLEL_THREAD_COUNT : 1];
		double before = timer();
		for(int t=0; t<parts.size(); t++) {
			cs->worker_nextAction[t] = action( [&,t] {
				tstart[t] = timer();
				auto begin = combinedWriteConflictRanges.begin() + (t*combinedWriteConflictRanges.size()/parts.size());
				auto end = combinedWriteConflictRanges.begin() + ((t+1)*combinedWriteConflictRanges.size()/parts.size());

				addConflictRanges(now, begin, end, &parts[t]);

				tend[t] = timer();
				done[t].set();
			});
			cs->worker_ready[t]->set();
		}
		double launch = timer();
		for(int i=0; i<PARALLEL_THREAD_COUNT; i++)
			done[i].block();
		double after = timer();

		g_merge_launch += launch-before;
		//g_merge_start_var += *std::max_element(tstart.begin(), tstart.end()) - before;
		g_merge_fork += *std::min_element(tstart.begin(), tstart.end()) - before;
		g_merge_start_var += *std::max_element(tstart.begin(), tstart.end()) - *std::min_element(tstart.begin(), tstart.end());
		g_merge_end_var += *std::max_element(tend.begin(), tend.end()) - *std::min_element(tend.begin(), tend.end());
		g_merge_join += after - *std::max_element(tend.begin(), tend.end());
		double run_max = 0, run_min = 1e9;
		for(int i=0; i<tend.size(); i++) {
			run_max = max(run_max, tend[i]-tstart[i]);
			run_min = min(run_min, tend[i]-tstart[i]);
		}
		g_merge_run_var += run_max-run_min;
		g_merge_run_shortest += run_min;
		g_merge_run_longest += run_max;
		g_merge_run_total += std::accumulate(tend.begin(),tend.end(),0.0)-std::accumulate(tstart.begin(),tstart.end(),0.0);

		cs->versionHistory.concatenate( &parts[0], parts.size() );
	} else {
		addConflictRanges( now, combinedWriteConflictRanges.begin(), combinedWriteConflictRanges.end(), &cs->versionHistory );
	}

	//for(auto w = combinedWriteConflictRanges.begin(); w != combinedWriteConflictRanges.end(); ++w)
	//	versionHistory.addConflictRange( w->first.begin(), w->first.size(), w->second.begin(), w->second.size(), now );
}

void ConflictBatch::combineWriteConflictRanges()
{
	int activeWriteCount = 0;
	for(int i=0; i<points.size(); i++) {
		KeyInfo& point = points[i];
		if (point.write && !transactionConflictStatus[ point.transaction ]) {
			if (point.begin) {
 				activeWriteCount++;
  				if (activeWriteCount == 1)
  					combinedWriteConflictRanges.push_back( make_pair( point.key, KeyRef() ) );
			} else /*if (point.end)*/ {
				activeWriteCount--;
				if (activeWriteCount == 0)
					combinedWriteConflictRanges.back().second = point.key;
			}
		}
	}
}

//void showNumaStatus();

/*
bool sse4Less( const uint8_t* a, int aLen, const uint8_t* b, int bLen ) {
	while (true) {
		int res = _mm_cmpestri(*(__m128i*)a, aLen, *(__m128i*)b, bLen, _SIDD_UBYTE_OPS | _SIDD_CMP_EQUAL_EACH | _SIDD_NEGATIVE_POLARITY | _SIDD_LEAST_SIGNIFICANT );
		printf("%d ", res);
		if (res == 16) {
			if (bLen < 16) return false;
			a += 16; b += 16; aLen -= 16; bLen -= 16;
		}
		if (res == bLen) return false;
		if (res == aLen) return true;

		return a[res] < b[res];
	}
}

void tless( const char* a, const char* b ) {
	bool x = sse4Less( (const uint8_t*)a, strlen(a), (const uint8_t*)b, strlen(b) );
	if (x)
		printf("%s < %s\n", a, b);
	else
		printf("%s >= %s\n", a, b);
}

void sse4Test(){

	tless("hello", "world");
	tless("a", "a");
	tless("world", "hello");
	tless("world", "worry");
	tless("worry", "world");
	tless("hello", "hello1");
	tless("hello1", "hello");

	tless("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaahello", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaworld");
	tless("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaworld", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaahello");
	tless("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaworld", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaworry");
	tless("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaworry", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaworld");
	tless("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaahello", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaahello1");
	tless("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaahello1", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaahello");

	char *a = "hello1worldthisisalonglonglongstring";
	char *b = "hello";
	__m128i aa = *(__m128i*)a;
	__m128i bb = *(__m128i*)a;

	int res = _mm_cmpestri(aa, 2, bb, 2, _SIDD_UBYTE_OPS | _SIDD_CMP_EQUAL_EACH | _SIDD_NEGATIVE_POLARITY | _SIDD_LEAST_SIGNIFICANT );

	cout << res << endl;

}
*/

void miniConflictSetTest() {
	for(int i=0; i<2000000; i++) {
		int size = 64*5;		// Also run 64*64*5 to test multiple words of andValues and orValues
		MiniConflictSet mini(size);
		for(int j=0; j<2; j++) {
			int a = g_random->randomInt(0, size);
			int b = g_random->randomInt(a, size);
			mini.set( a, b );
		}
		for(int j=0; j<4; j++) {
			int a = g_random->randomInt(0, size);
			int b = g_random->randomInt(a, size);
			mini.any( a, b );	// Tests correctness internally
		}
	}
	printf("miniConflictSetTest complete\n");
}

void skipListTest() {
	printf("Skip list test\n");

	//sse4Test();

	//A test case that breaks the old operator<
	//KeyInfo a( LiteralStringRef("hello"), true, false, true, -1 );
	//KeyInfo b( LiteralStringRef("hello\0"), false, false, false, 0 );

	miniConflictSetTest();


	setAffinity(0);
	//showNumaStatus();

	double start;

	ConflictSet* cs = newConflictSet();

	Arena testDataArena;
	VectorRef< VectorRef<KeyRangeRef> > testData;
	testData.resize(testDataArena, 500);
	vector<vector<uint8_t>> success( testData.size() );
	vector<vector<uint8_t>> success2( testData.size() );
	for(int i=0; i<testData.size(); i++) {
		testData[i].resize(testDataArena, 5000);
		success[i].assign( testData[i].size(), false );
		success2[i].assign( testData[i].size(), false );
		for(int j=0; j<testData[i].size(); j++) {
			int key = g_random->randomInt(0, 20000000);
			int key2 = key + 1 + g_random->randomInt(0, 10);
			testData[i][j] = KeyRangeRef(
				setK( testDataArena, key ),
				setK( testDataArena, key2 ) );
		}
	}
	printf("Test data generated (%d)\n", g_random->randomInt(0,100000));
	printf("  %d threads, %d batches, %d/batch\n", PARALLEL_THREAD_COUNT, testData.size(), testData[0].size());

	printf("Running\n");

	int readCount = 1, writeCount = 1;
	int cranges = 0, tcount = 0;

	start = timer();
	vector<vector<int>> nonConflict( testData.size() );
	for(int i=0; i<testData.size(); i++) {
		Arena buf;
		vector<CommitTransactionRef> trs;
		double t = timer();
		for(int j=0; j+readCount+writeCount<=testData[i].size(); j+=readCount+writeCount) {
			CommitTransactionRef tr;
			for(int k=0; k<readCount; k++) {
				KeyRangeRef r( buf, testData[i][j+k] );
				tr.read_conflict_ranges.push_back( buf, r );
			}
			for(int k=0; k<writeCount; k++) {
				KeyRangeRef r( buf, testData[i][j+readCount+k] );
				tr.write_conflict_ranges.push_back( buf, r );
			}
			cranges += tr.read_conflict_ranges.size() + tr.write_conflict_ranges.size();
			tr.read_snapshot = i;
			trs.push_back(tr);
		}
		tcount += trs.size();
		g_buildTest += timer()-t;

		t = timer();
		ConflictBatch batch( cs );
		for(int j=0; j<trs.size(); j++)
			batch.addTransaction( trs[j] );
		g_add += timer()-t;

		t = timer();
		batch.detectConflicts( i+50, i, nonConflict[i] );
		g_detectConflicts += timer()-t;
	}
	double elapsed = timer()-start;
	printf("New conflict set: %0.3f sec\n", elapsed);
	printf("                  %0.3f Mtransactions/sec\n", tcount/elapsed/1e6);
	printf("                  %0.3f Mkeys/sec\n", cranges*2/elapsed/1e6);

	elapsed = g_detectConflicts.getValue();
	printf("Detect only:      %0.3f sec\n", elapsed);
	printf("                  %0.3f Mtransactions/sec\n", tcount/elapsed/1e6);
	printf("                  %0.3f Mkeys/sec\n", cranges*2/elapsed/1e6);

	elapsed = g_checkRead.getValue() + g_merge.getValue();
	printf("Skiplist only:    %0.3f sec\n", elapsed);
	printf("                  %0.3f Mtransactions/sec\n", tcount/elapsed/1e6);
	printf("                  %0.3f Mkeys/sec\n", cranges*2/elapsed/1e6);

	printf("Performance counters:\n");
	for(int c=0; c<skc.size(); c++) {
		printf("%20s: %s\n", skc[c]->getMetric().name().c_str(), skc[c]->getMetric().formatted().c_str());
	}

	//showNumaStatus();

	printf("%d entries in version history\n", cs->versionHistory.count());

	/*start = timer();
	vector<vector<int>> nonConflict2( testData.size() );
	SlowConflictSet scs;
	Standalone<VectorRef<KeyRangeRef>> ranges;
	ranges.resize( ranges.arena(), 1 );

	for(int i=0; i<testData.size(); i++) {
		for(int j=0; j<testData[i].size(); j++) {
			ranges[0] = testData[i][j];
			if (!scs.is_conflict( ranges, i )) {
				nonConflict2[i].push_back( j );
				scs.add( ranges, VectorRef<KeyValueRef>(), i + 50 );
			}
		}
	}
	printf("Old conflict set: %0.3f sec\n", timer()-start);

	int aminusb=0, bminusa=0, atotal=0;
	for(int i=0; i<testData.size(); i++) {
		vector<bool> a( testData[i].size() ), b( testData[i].size() );
		for(int j=0; j<nonConflict[i].size(); j++)
			a[ nonConflict[i][j] ] = true;
		for(int j=0; j<nonConflict2[i].size(); j++)
			b[ nonConflict2[i][j] ] = true;
		for(int j=0; j<a.size(); j++) {
			if (a[j]) atotal++;
			if (a[j] && !b[j]) aminusb++;
			else if (b[j] && !a[j]) bminusa++;
		}
	}
	printf("%d transactions accepted\n", atotal);
	if (bminusa)
		printf("ERROR: %d transactions unnecessarily rejected!\n", bminusa);
	if (aminusb)
		printf("ERROR: %d transactions incorrectly accepted!\n", aminusb);
		*/
	//for(int i=0; i<testData.size(); i++)
	//	printf("%d %d %d %d\n", i, nonConflict[i].size(), nonConflict2[i].size()-nonConflict[i].size(), nonConflict[i] != nonConflict2[i]);
}
