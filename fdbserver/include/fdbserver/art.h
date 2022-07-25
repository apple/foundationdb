/*
 * art.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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
#include "fdbclient/FDBTypes.h"
#include "flow/Arena.h"
#include "flow/Platform.h"

#ifndef ART_H
#define ART_H

struct art_iterator;

struct art_tree {
	enum ART_NODE_TYPE : uint8_t {
		ART_LEAF = 0,
		ART_NODE4 = 1,
		ART_NODE16 = 2,
		ART_NODE48 = 3,
		ART_NODE256 = 4,
		ART_NODE4_KV = 5,
		ART_NODE16_KV = 6,
		ART_NODE48_KV = 7,
		ART_NODE256_KV = 8
	};
#define ART_MIN_LEAF_UNSET 1UL
#define ART_LEAF_MATCH_KEY 1
#define ART_LEAF_SMALLER_KEY 2
#define ART_LEAF_LARGER_KEY 3
#define ART_I_FOUND 2
#define ART_I_DEPTH 3
#define ART_I_BACKTRACK 4

#define ART_NEXT 1
#define ART_PREV -1
#define ART_NEITHER 0

	//#define ART_IS_LEAF(x) ( (*((ART_NODE_TYPE*)x) == ART_LEAF))
	template <class T>
	static inline bool ART_IS_LEAF(T const& x) {
		return *((ART_NODE_TYPE*)x) == ART_LEAF;
	}

#define ART_LEAF_RAW(x) ((art_leaf*)(x))

#define ART_LEAF_DISPL(x) fat_leaf_offset[(x)->type]
#define ART_FAT_NODE_LEAF(node_ptr) (*((art_leaf**)(((char*)(node_ptr)) + ART_LEAF_DISPL(node_ptr))))

// In Bytes
// This is needed so that we can pre-allocate a static stack to perform efficient backtracking in iterative_bound
#define ART_MAX_KEY_LEN 10000

#define _mm_cmpge_epu8(a, b) _mm_cmpeq_epi8(_mm_max_epu8(a, b), a)

#define _mm_cmple_epu8(a, b) _mm_cmpge_epu8(b, a)

#define _mm_cmpgt_epu8(a, b) _mm_xor_si128(_mm_cmple_epu8(a, b), _mm_set1_epi8(-1))

#define _mm_cmplt_epu8(a, b) _mm_cmpgt_epu8(b, a)

#define ART_MAX_PREFIX_LEN 10

	/**
	 * This struct is included as part
	 * of all the various node sizes
	 */
	struct art_node {
		ART_NODE_TYPE type;
		uint8_t num_children;
		uint32_t partial_len;
		unsigned char partial[ART_MAX_PREFIX_LEN];
	};

	/**
	 * Represents a leaf. These are
	 * of arbitrary size, as they include the key.
	 * Note that the first two fields must be the same as in art_node
	 */
	struct art_leaf {
		ART_NODE_TYPE type;
		void* value;
		art_leaf* prev = nullptr;
		art_leaf* next = nullptr;
		KeyRef key;

		art_leaf(const KeyRef& k, void* v) : type(ART_LEAF), value(v), key(k.begin(), k.size()) {}

		art_leaf(const uint8_t* ptr, int len, void* v) : type(ART_LEAF), value(v), key(ptr, len) {}
	};

	/**
	 * Small node with only 4 children
	 */
	struct art_node4 {
		art_node n;
		unsigned char keys[4];
		art_node* children[4];
	};

	/**
	 * Node with 16 children
	 */
	struct art_node16 {
		art_node n;
		unsigned char keys[16];
		art_node* children[16];
	};

	/**
	 * Node with 48 children, but
	 * a full 256 byte field.
	 */
	struct art_node48 {
		art_node n;
		unsigned char keys[256];
		art_node* children[48];
	};

	/**
	 * Full node with 256 children
	 */
	struct art_node256 {
		art_node n;
		art_node* children[256];
	};

	/**
	 * Small node with only 4 children
	 */
	struct art_node4_kv {
		art_node4 n;
		art_leaf* leaf;
	};

	/**
	 * Node with 16 children
	 */
	struct art_node16_kv {
		art_node16 n;
		art_leaf* leaf;
	};

	/**
	 * Node with 48 children, but
	 * a full 256 byte field.
	 */
	struct art_node48_kv {
		art_node48 n;
		art_leaf* leaf;
	};

	/**
	 * Full node with 256 children
	 */
	struct art_node256_kv {
		art_node256 n;
		art_leaf* leaf;
	};

private:
	art_node* root;
	uint64_t size;
	Arena* arena;
	static int fat_leaf_offset[9];
	static int node_sizes[9];

	static int check_bound_node(art_node* n, const KeyRef& k, int* depth_p, art_leaf** result, bool strict);

	static int art_bound_leaf(art_node* n, const KeyRef& k, int depth, art_leaf** result);

	static art_leaf* minimum(art_node* n);

	static art_leaf* maximum(art_node* n);

	static int leaf_matches_signed(art_leaf* n, const KeyRef& k, int depth);

	static int leaf_matches(const art_leaf* n, const KeyRef& k, int depth);

	static int signed_prefix_mismatch(art_node* n, const KeyRef& k, int depth, art_leaf** min_leaf, bool find_min);

	static int prefix_mismatch(art_node* n, KeyRef& k, int depth, art_leaf** minout);

	static art_leaf* minimum_kv(art_node* n);

	static art_node** find_child(art_node* n, unsigned char c);

	static void find_next(art_node* n, unsigned char c, art_node** out);

	static void find_prev(art_node* n, unsigned char c, art_node** out);

	static int art_next_prev(art_node* n, unsigned char c, art_node** out);

	void art_bound_iterative(art_node* n, const KeyRef& k, int depth, art_leaf** result, bool strict);

	static inline int min(int a, int b) { return (a < b) ? a : b; }

	static inline void* SET_LEAF(void* x) {
		(*((uint8_t*)x) = 0);
		return x;
	}

	static int check_prefix(const art_node* n, const KeyRef& k, int depth);

	void recursive_delete_binary(art_node* n, art_node** ref, const KeyRef& k, int depth);

	void remove_child(art_node* n, art_node** ref, unsigned char c, art_node** l, int depth);

	// needs this pointer to do allocation
	art_node* alloc_node(ART_NODE_TYPE type);

	art_node* alloc_kv_node(ART_NODE_TYPE type);

	art_leaf* make_leaf(const KeyRef& k, void* value);

	static void remove_fat_child(art_node* n, art_node** ref, int depth);

	static void remove_fat_child4(art_node4_kv* n, art_node** ref, int depth);

	static void remove_child4(art_node4* n, art_node** ref, art_node** l, int depth);

	static void remove_fat_child16(art_node16_kv* n, art_node** ref);

	void remove_child16(art_node16* n, art_node** ref, art_node** l);

	static void remove_fat_child48(art_node48_kv* n, art_node** ref);

	void remove_child48(art_node48* n, art_node** ref, unsigned char c);

	static void remove_fat_child256(art_node256_kv* n, art_node** ref);

	void remove_child256(art_node256* n, art_node** ref, unsigned char c);

	static void copy_header(art_node* dest, art_node* src);

	art_leaf* iterative_insert(art_node* root,
	                           art_node** root_ptr,
	                           KeyRef& k,
	                           void* value,
	                           int depth,
	                           int* old,
	                           int replace_existing);

	art_leaf*
	insert_leaf(art_node* n, art_node** ref, const KeyRef& k, void* value, int depth, int* old, int replace_existing);

	art_leaf* insert_fat_node(art_node* n,
	                          art_node** ref,
	                          const KeyRef& k,
	                          void* value,
	                          int depth,
	                          int* old,
	                          art_leaf* min_of_n,
	                          int replace_existing);

	art_leaf* insert_internal_node(art_node* n,
	                               art_node** ref,
	                               const KeyRef& k,
	                               void* value,
	                               int depth,
	                               int* old,
	                               art_leaf* min_of_n,
	                               int prefix_diff,
	                               int replace_existing);

	art_leaf*
	insert_child(art_node* n, art_node** ref, const KeyRef& k, void* value, int depth, int* old, int replace_existing);

	static inline int longest_common_prefix(const KeyRef& k1, const KeyRef& k2, int depth);

	static void insert_after(art_leaf* l_new, art_leaf* l_existing);

	static void insert_before(art_leaf* l_new, art_leaf* l_existing);

	void add_child(art_node* n, art_node** ref, unsigned char c, void* child);

	void add_child256(art_node256* n, art_node** ref, unsigned char c, void* child);

	void add_child48(art_node48* n, art_node** ref, unsigned char c, void* child);

	void add_child16(art_node16* n, art_node** ref, unsigned char c, void* child);

	void add_child4(art_node4* n, art_node** ref, unsigned char c, void* child);

public:
	art_tree(Arena& a) {
		this->arena = &a;
		this->root = alloc_node(ART_NODE4); // Sentinel node. Also needed for empty key
		this->size = 0;
	}

	art_iterator lower_bound(const KeyRef& k);

	art_iterator upper_bound(const KeyRef& k);

	art_iterator insert(KeyRef& key, void* value);

	art_iterator insert_if_absent(KeyRef& key, void* value, int* replaced);

	void erase(const art_iterator& it);

	uint64_t count() { return size; }

}; // art_tree

struct art_iterator {

private:
	art_tree::art_leaf* leaf;

public:
	art_iterator(art_tree::art_leaf* l) { leaf = l; }

	art_iterator(const art_iterator& i) { leaf = i.leaf; }

	art_iterator() { leaf = nullptr; }

	art_iterator& operator++() {
		leaf = leaf->next;
		return (*this);
	}

	art_iterator& operator--() {
		leaf = leaf->prev;
		return (*this);
	}

	bool operator==(const art_iterator& other) const { return leaf == other.leaf; }

	bool operator!=(const art_iterator& other) const { return leaf != other.leaf; }

	const KeyRef& key() const { return leaf->key; }

	void* value() const { return leaf->value; }

	void** value_ptr() { return &(leaf->value); }
};

#endif // ART_H
