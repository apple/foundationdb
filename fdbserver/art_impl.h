/*
 * art_impl.h
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

#ifndef ART_IMPL_H
#define ART_IMPL_H

using art_tree = VersionedBTree::art_tree;
using art_leaf = art_tree::art_leaf;
#define art_node art_tree::art_node

int art_tree::fat_leaf_offset[] = {
	0, 0, 0, 0, 0, sizeof(art_node4), sizeof(art_node16), sizeof(art_node48), sizeof(art_node256)
};
int art_tree::node_sizes[] = { 0,
	                           sizeof(art_node4),
	                           sizeof(art_node16),
	                           sizeof(art_node48),
	                           sizeof(art_node256),
	                           sizeof(art_node4_kv),
	                           sizeof(art_node16_kv),
	                           sizeof(art_node48_kv),
	                           sizeof(art_node256_kv) };

VersionedBTree::art_iterator art_tree::insert(KeyRef& k, void* value) {
#define INIT_DEPTH 0
#define REPLACE 1
	int old_val = 0;
	art_leaf* l = iterative_insert(this->root, &this->root, k, value, INIT_DEPTH, &old_val, REPLACE);

	if (!old_val)
		this->size++;
	return VersionedBTree::art_iterator(l);
}

VersionedBTree::art_iterator art_tree::insert_if_absent(KeyRef& k, void* value, int* existing) {
#define INIT_DEPTH 0
#define DONTREPLACE 0
	art_leaf* l = iterative_insert(this->root, &this->root, k, value, INIT_DEPTH, existing, DONTREPLACE);
	if (!existing)
		this->size++;
	return VersionedBTree::art_iterator(l);
}

VersionedBTree::art_iterator art_tree::lower_bound(const KeyRef& key) {
	if (!size)
		return art_iterator(nullptr);
	art_node* n = root;
	art_leaf* res = nullptr;
	int depth = 0;

	art_bound_iterative(n, key, depth, &res, false);

	return art_iterator(res);
}

VersionedBTree::art_iterator art_tree::upper_bound(const KeyRef& key) {
	if (!size)
		return art_iterator(nullptr);
	art_node* n = root;
	art_leaf* res = nullptr;
	int depth = 0;

	art_bound_iterative(n, key, depth, &res, true);

	return art_iterator(res);
}

struct stack_entry {
	art_node* node;
	unsigned char key;
	stack_entry* prev;

	stack_entry(art_node* n, unsigned char k, stack_entry* s) : node(n), key(k), prev(s) {}

	stack_entry() {}
};

art_leaf* art_tree::minimum(art_node* n) {
	// Handle base cases
	if (!n) {
		return nullptr;
	}
	if (ART_IS_LEAF(n)) {
		return ART_LEAF_RAW(n);
	}

	int idx;
	switch (n->type) {
	case ART_NODE4:
		return minimum(((art_node4*)n)->children[0]);
	case ART_NODE16:
		return minimum(((art_node16*)n)->children[0]);
	case ART_NODE48:
		idx = 0;
		while (!((art_node48*)n)->keys[idx])
			idx++;
		idx = ((art_node48*)n)->keys[idx] - 1;
		return minimum(((art_node48*)n)->children[idx]);
	case ART_NODE256:
		idx = 0;
		while (!((art_node256*)n)->children[idx])
			idx++;
		return minimum(((art_node256*)n)->children[idx]);
	case ART_NODE256_KV:
	case ART_NODE48_KV:
	case ART_NODE16_KV:
	case ART_NODE4_KV:
		return ART_FAT_NODE_LEAF(n);
	default:
		printf("%d\n", n->type);
		UNSTOPPABLE_ASSERT(false);
	}
}

art_leaf* art_tree::maximum(art_node* n) {
	// Handle base cases
	if (!n)
		return nullptr;
	if (ART_IS_LEAF(n))
		return ART_LEAF_RAW(n);

	int idx;
	switch (n->type) {
	case ART_NODE4:
	case ART_NODE4_KV:
		return maximum(((art_node4*)n)->children[n->num_children - 1]);
	case ART_NODE16:
	case ART_NODE16_KV:
		return maximum(((art_node16*)n)->children[n->num_children - 1]);
	case ART_NODE48:
	case ART_NODE48_KV:
		idx = 255;
		while (!((art_node48*)n)->keys[idx])
			idx--;
		idx = ((art_node48*)n)->keys[idx] - 1;
		return maximum(((art_node48*)n)->children[idx]);
	case ART_NODE256:
	case ART_NODE256_KV:
		idx = 255;
		while (!((art_node256*)n)->children[idx])
			idx--;
		return maximum(((art_node256*)n)->children[idx]);
	default:
		UNSTOPPABLE_ASSERT(false);
	}
}

void art_tree::art_bound_iterative(art_node* n, const KeyRef& k, int depth, art_leaf** result, bool strict) {

	static stack_entry arena[ART_MAX_KEY_LEN]; // Single threaded implementation.

	stack_entry *head = nullptr, *curr_arena = arena;
	int ret;
	art_node** child;
	unsigned char* key = (unsigned char*)k.begin();
	while (n) {
		ret = check_bound_node(n, k, &depth, result, strict);
		if (ret == ART_I_FOUND) {
			return;
		}
		if (ret == ART_I_BACKTRACK)
			break;
		if (ret == ART_I_DEPTH) {
			head = new (curr_arena++) stack_entry(n, key[depth], head);
			// if the child is nullptr, then we have to look right; i.e., we start backtracking
			child = find_child(n, key[depth]);
			if (!child)
				break;
			n = *child;
			depth = depth + 1;
			// else go on
		} else {
			UNSTOPPABLE_ASSERT(false);
		}
	}
	art_node* next;
	while (head) {
		n = head->node;
		find_next(n, head->key, &next);
		if (next) {
			*result = minimum(next);
			break;
		} else {
			head = head->prev;
		}
	}
}

int art_tree::check_bound_node(art_node* n, const KeyRef& k, int* depth_p, art_leaf** result, bool strict) {
	if (ART_IS_LEAF(n)) {
		int ret = art_bound_leaf(n, k, *depth_p, result);
		*result = ART_LEAF_RAW(n);
		if (ret == ART_LEAF_SMALLER_KEY || (ret == ART_LEAF_MATCH_KEY && strict)) {
			*result = (*result)->next;
		}
		return ART_I_FOUND;
	}
	int depth = *depth_p;
	const int key_len = k.size();
	if (n->type < ART_NODE4_KV) {
		const uint32_t n_partial_len = n->partial_len;
		// Case 1: the search ends on this node
		if (key_len <= (depth + n_partial_len)) {
			// Easy case w/o checking prefix:
			if (0 == n_partial_len) {
				*result = minimum(n);
				return ART_I_FOUND;
			} else {
				// The Whole prefix is the set of bytes after the first depth bytes and up to partial_len
				art_leaf* min_leaf;
				int prefix_differs = signed_prefix_mismatch(n, k, depth, &min_leaf, true);

				if (prefix_differs < 0) { // our key is greater than or equal to the partial prefix. I.e., this subtree
					// only stores smaller keys.
					return ART_I_BACKTRACK;
				} else { // our key is less than or equal to the partial prefix. So this subtree stores our bound
					     // (modulo null leaf)
					if (min_leaf == (art_leaf*)ART_MIN_LEAF_UNSET) {
						min_leaf = minimum(n);
					}
					*result = min_leaf;
					return ART_I_FOUND;
				}
			}
		}
		// Case 2: the search has to go deeper
		// First, look if you have any prefix. If you do, you have to check this prefix before checking your children
		if (n_partial_len) {
			art_leaf* min_leaf;
			int prefix_differs = signed_prefix_mismatch(n, k, depth, &min_leaf, true);

			if (prefix_differs == 0) {
				// The partial prefix matches (0 ==> match)
				// The mismatch happens after the prefix covered by the node, so it is fine to check its children
				*depth_p += n->partial_len;
				return ART_I_DEPTH;
			} else {
				// The mismatch happens in the prefix of this node. Now there are two options
				// 1. The subtree stores smaller keys. Have to backtrack
				if (prefix_differs < 0) {
					return ART_I_BACKTRACK;
				} else {
					if (min_leaf == (art_leaf*)ART_MIN_LEAF_UNSET) {
						min_leaf = minimum(n);
					}
					*result = min_leaf;
					return ART_I_FOUND;
				}
			}
		}
		return ART_I_DEPTH;
	} else {
		if (key_len <= depth + n->partial_len) { // Case 1: the key ends here
			art_leaf* l = ART_FAT_NODE_LEAF(n);
			// Check if the expanded path matches
			int m = leaf_matches_signed(l, k, depth);
			if (!m) {
				// We have a match. If the bound is not strict, we save the result
				if (!strict) {
					*result = l;
					return ART_I_FOUND;
				} else { // Assuming --as we do-- that we have a child, the smallest child in the subtree is the bound
					*result = minimum_kv(n);
					return ART_I_FOUND; // We have found the bound. We use this return value so that the value
					// We set as result is propagated directly
				}
			}
			if (m < 0) {
				// The key in the fat node is smaller than the desired key
				// This means that the target key does not exist.
				// Also, it means that this subtree as a whole is smaller than the target key
				// So the bound has to be searched by my daddy
				return ART_I_BACKTRACK;
			} else {
				*result = l;
				return ART_I_FOUND;
			}
		}
		if (n->partial_len) {
			art_leaf* l = ART_FAT_NODE_LEAF(n);
			// Note we know target key *does not* end here, so we just need to check the prefix
			// To avoid distinguishing whether the prefix is larger or smaller than min, we compare with the leaf
			// That is readily available w/o issuing a "min"
			// The fat leaf key includes the prefix for sure
			int cmp =
			    memcmp(((unsigned char*)k.begin()) + depth, ((unsigned char*)l->key.begin()) + depth, n->partial_len);
			if (cmp < 0) { // Target key is smaller, fat key is then the smallest key larger than current key
				*result = l;
				return ART_I_FOUND;
			} else if (cmp > 0) { // Target key is larger than key and hence than subtree. Ask daddy to go right
				return ART_I_BACKTRACK;
			}
			// If prefix is the same, then go in depth with the updated depth value
			*depth_p += n->partial_len;
			return ART_I_DEPTH;
		}
		return ART_I_DEPTH;
	}
}

int art_tree::art_bound_leaf(art_node* n, const KeyRef& k, int depth, art_leaf** result) {
	n = (art_node*)ART_LEAF_RAW(n);
	// Check if the expanded path matches
	int m = leaf_matches_signed((art_leaf*)n, k, depth);
	if (0 == m)
		return ART_LEAF_MATCH_KEY;
	if (m < 0)
		return ART_LEAF_SMALLER_KEY;
	return ART_LEAF_LARGER_KEY;
}

int art_tree::leaf_matches_signed(art_leaf* n, const KeyRef& k, int depth) {

	const int key_len = k.size();
	const unsigned char* key = k.begin();
	int common_length = min(n->key.size() - depth, key_len - depth);

	// Compare the keys starting at the depth
	int cmp = memcmp((n->key.begin()) + depth, key + depth, common_length);
	if (cmp)
		return cmp;
	return n->key.size() - key_len;
}

int art_tree::leaf_matches(const art_leaf* n, const KeyRef& k, int depth) {
	const int key_len = k.size();
	// Fail if the key lengths are different
	if (n->key.size() != (uint32_t)key_len)
		return 1;

	const unsigned char* key = k.begin();

	// Compare the keys starting at the depth
	return memcmp(((unsigned char*)(n->key).begin()) + depth, key + depth, key_len - depth);
}

int art_tree::signed_prefix_mismatch(art_node* n, const KeyRef& k, int depth, art_leaf** min_leaf, bool find_min) {
	int all_to_check = min(n->partial_len, k.size() - depth);
	int max_cmp = min(ART_MAX_PREFIX_LEN, all_to_check);
	// Mark the leaf as unset right away, so that if you don't return in the first loop AND you don't enter the
	// Second loop, the leaf is marked correctly as unset
	*min_leaf = (art_leaf*)ART_MIN_LEAF_UNSET;
	int idx;
	for (idx = 0; idx < max_cmp; idx++) {
		if (n->partial[idx] != k.begin()[depth + idx]) {
			return ((int)n->partial[idx]) - ((int)(k.begin()[depth + idx]));
		}
	}
	// So far they are the same.
	// There are two cases now. They are the same BUT we have to check after ART_MAX_PREFIX_LEN, or they are the same
	// and there's nothing more to check.
	if (all_to_check <= ART_MAX_PREFIX_LEN)
		return 0;
	// Else, we need to go deeper and check the bytes after ART_MAX_PREFIX_LEN

	int remaining = all_to_check - ART_MAX_PREFIX_LEN;
	// If the prefix is short we can avoid finding a leaf
	if (remaining > 0) {
		// Prefix is longer than what we've checked, find a leaf
		art_leaf* l = find_min ? minimum(n) : maximum(n);
		*min_leaf = l;
		// We have to compare the last partial_len - ART_MAX_PREFIX_LEN bytes
		// If the minimum is below me, then it has at least my same prefix, so I can safely check
		// all its bytes from depth to depth+remaining
		for (; idx < ART_MAX_PREFIX_LEN + remaining; idx++) {
			if (l->key.begin()[idx + depth] != k.begin()[depth + idx]) {
				return ((int)l->key.begin()[idx + depth]) - ((int)k.begin()[depth + idx]);
			}
		}
	}
	return 0;
}

art_leaf* art_tree::minimum_kv(art_node* n) {
	// Handle base cases
	if (!n) {
		return nullptr;
	}
	if (ART_IS_LEAF(n)) {
		return ART_LEAF_RAW(n);
	}

	int idx;
	switch (n->type) {
	case ART_NODE4_KV:
		return minimum(((art_node4*)n)->children[0]);
	case ART_NODE16_KV:
		return minimum(((art_node16*)n)->children[0]);
	case ART_NODE48_KV:
		idx = 0;
		while (!((art_node48*)n)->keys[idx])
			idx++;
		idx = ((art_node48*)n)->keys[idx] - 1;
		return minimum(((art_node48*)n)->children[idx]);
	case ART_NODE256_KV:
		idx = 0;
		while (!((art_node256*)n)->children[idx])
			idx++;
		return minimum(((art_node256*)n)->children[idx]);
	default:
		UNSTOPPABLE_ASSERT(false);
	}
}

art_node** art_tree::find_child(art_node* n, unsigned char c) {
	int i, mask, bitfield;
	union {
		art_node4* p1;
		art_node16* p2;
		art_node48* p3;
		art_node256* p4;
	} p;
	switch (n->type) {
	case ART_NODE4:
	case ART_NODE4_KV:
		p.p1 = (art_node4*)n;
		switch (n->num_children) {
		case 4:
			if (p.p1->keys[3] == c) {
				return &p.p1->children[3];
			}
		case 3:
			if (p.p1->keys[2] == c) {
				return &p.p1->children[2];
			}
		case 2:
			if (p.p1->keys[1] == c) {
				return &p.p1->children[1];
			}
			/*
			 * We need a case 1. Otherwise this could happen: we could be looking for char '0'
			 * On a node with 0 children (root node). Since the node has been zeroed on allocation,
			 * we get that key[0] is exactly '0', and we think we have found a child. Duh.
			 */
		case 1:
			if (p.p1->keys[0] == c) {
				return &p.p1->children[0];
			}
		default:
			break;
		}
		break;

		{
		case ART_NODE16:
		case ART_NODE16_KV:
			__m128i cmp;
			p.p2 = (art_node16*)n;

			// Compare the key to all 16 stored keys
			cmp = _mm_cmpeq_epi8(_mm_set1_epi8(c), _mm_loadu_si128((__m128i*)p.p2->keys));

			// Use a mask to ignore children that don't exist
			mask = (1 << n->num_children) - 1;
			bitfield = _mm_movemask_epi8(cmp) & mask;

			/*
			 * If we have a match (any bit set) then we can
			 * return the pointer match using ctz to get
			 * the index.
			 */
			if (bitfield)
				return &p.p2->children[ctz(bitfield)];
			break;
		}

	case ART_NODE48:
	case ART_NODE48_KV:
		p.p3 = (art_node48*)n;
		i = p.p3->keys[c];
		if (i)
			return &p.p3->children[i - 1];
		break;

	case ART_NODE256:
	case ART_NODE256_KV:
		p.p4 = (art_node256*)n;
		if (p.p4->children[c])
			return &p.p4->children[c];
		break;

	default:
		UNSTOPPABLE_ASSERT(false);
	}
	return nullptr;
}

void art_tree::find_next(art_node* n, unsigned char c, art_node** out) {
	int i, mask, bitfield;
	union {
		art_node4* p1;
		art_node16* p2;
		art_node48* p3;
		art_node256* p4;
	} p;
	*out = nullptr;
	switch (n->type) {
	case ART_NODE4:
	case ART_NODE4_KV:
		p.p1 = (art_node4*)n;
		switch (n->num_children) { // unrolling loop
		case 4:
			if (p.p1->keys[0] > c) {
				*out = p.p1->children[0];
				break;
			}
			if (p.p1->keys[1] > c) {
				*out = p.p1->children[1];
				break;
			}
			if (p.p1->keys[2] > c) {
				*out = p.p1->children[2];
				break;
			}
			if (p.p1->keys[3] > c) {
				*out = p.p1->children[3];
				break;
			}
			break;
		case 3:
			if (p.p1->keys[0] > c) {
				*out = p.p1->children[0];
				break;
			}
			if (p.p1->keys[1] > c) {
				*out = p.p1->children[1];
				break;
			}
			if (p.p1->keys[2] > c) {
				*out = p.p1->children[2];
				break;
			}
			break;
		case 2:
			if (p.p1->keys[0] > c) {
				*out = p.p1->children[0];
				break;
			}
			if (p.p1->keys[1] > c) {
				*out = p.p1->children[1];
				break;
			}
			break;
			// We add a case 1 just in case... (see previous similar comment)
		case 1:
			if (p.p1->keys[0] > c) {
				*out = p.p1->children[0];
				break;
			}
		default:
			break;
		}
		break;

		{
		case ART_NODE16:
		case ART_NODE16_KV:
			__m128i cmp;
			p.p2 = (art_node16*)n;
			// We did not find the child corresponding to the key. Let's see if we have a next at least
			// Compare the key to all 16 stored keys for Greater than
			cmp = _mm_cmplt_epu8(_mm_set1_epi8(c), _mm_loadu_si128((__m128i*)p.p2->keys));

			// Use a mask to ignore children that don't exist
			mask = (1 << n->num_children) - 1;
			bitfield = _mm_movemask_epi8(cmp) & mask;
			if (bitfield) {
				// ALL children greater than char have their bit set
				// We need the smallest one
				// let's get the least significant bit that is set in the bitfield
				int one = ctz(bitfield);
				*out = p.p2->children[one];
			}
			break;
		}
	case ART_NODE48:
	case ART_NODE48_KV: {
		p.p3 = (art_node48*)n;

		if (c == 255)
			break;
		unsigned char cc = c + 1;
		do {
			i = p.p3->keys[cc];
			if (i) {
				*out = p.p3->children[i - 1];
				break;
			}
			++cc;
		} while (cc > 0); // cc wraps around at 256
		break;
	}

	case ART_NODE256:
	case ART_NODE256_KV: {
		p.p4 = (art_node256*)n;
		unsigned char cc = c + 1;
		if (c == 255)
			break;
		do {
			if (p.p4->children[cc]) {
				*out = p.p4->children[cc];
				break;
			}
			++cc;
		} while (cc > 0); // cc wraps around at 256
		break;
	}

	default:
		UNSTOPPABLE_ASSERT(false);
	}
}

void art_tree::recursive_delete_binary(art_node* n, art_node** ref, const KeyRef& k, int depth) {
	// Bail if the prefix does not match
	unsigned char* key = (unsigned char*)k.begin();
	unsigned int key_len = (unsigned int)k.size();
	if (n->partial_len) {
		int prefix_len = check_prefix(n, k, depth);
		if (prefix_len != min(ART_MAX_PREFIX_LEN, n->partial_len)) {
			return;
		}
		depth = depth + n->partial_len;
	}

	// Find child node
	art_node** child = find_child(n, key[depth]);
	if (!child)
		return;

	// The child contains the key to be deleted in two cases
	// 1. The child is a leaf (and the key matches)
	// 2. THe child is a fat node (and the key ands in the node and the key matches)

	// If the child is leaf, delete from this node
	if (ART_IS_LEAF(*child)) {
		art_leaf* l = ART_LEAF_RAW(*child);
		if (!leaf_matches(l, k, depth)) {
			remove_child(n, ref, key[depth], child, depth);
			if (l->prev) {
				l->prev->next = l->next;
			}
			if (l->next) {
				l->next->prev = l->prev;
			}
			return;
		}
		return;
	} else {
		// Fat node and key ends within the child
		// Note that the predicate is on the child node, so at depth+1
		if ((*child)->type >= ART_NODE4_KV && (key_len == depth + 1 + (*child)->partial_len)) {
			// Check if key matches
			art_leaf* l = ART_FAT_NODE_LEAF((*child));
			// Return nullptr if key does not match
			if (leaf_matches(l, k, depth + 1)) {
				return;
			}

			// Set prev and next, since the leaf is going away
			if (l->prev) {
				l->prev->next = l->next;
			}
			if (l->next) {
				l->next->prev = l->prev;
			}
			// Remove the fat child. This also triggers compaction
			// NOTE: we are removing the fat leaf on our child, which is at depth+1!!!
			remove_fat_child(*child, child, depth + 1);
			return;
		}
		// Recurse if key is not supposed to be in the child. The child is going to be an internal node
		recursive_delete_binary(*child, child, k, depth + 1);
	}
}

int art_tree::check_prefix(const art_node* n, const KeyRef& k, int depth) {
	int max_cmp = min(min(n->partial_len, ART_MAX_PREFIX_LEN), k.size() - depth);
	int idx;
	for (idx = 0; idx < max_cmp; idx++) {
		if (n->partial[idx] != ((unsigned char*)k.begin())[depth + idx])
			return idx;
	}
	return idx;
}

// Remove a LEAF from a node256/node256kv
void art_tree::remove_child256(art_node256* n, art_node** ref, unsigned char c) {
	n->children[c] = nullptr;
	n->n.num_children--;

	// Resize to a node48 on underflow, not immediately to prevent
	// trashing if we sit on the 48/49 boundary
	if (n->n.num_children == 37) {
		art_node48* new_node;

		if (n->n.type == ART_NODE256_KV) {
			new_node = (art_node48*)alloc_node(ART_NODE48_KV);
			ART_FAT_NODE_LEAF(&new_node->n) = ART_FAT_NODE_LEAF(&n->n);
		} else {
			new_node = (art_node48*)alloc_node(ART_NODE48);
		}
		*ref = (art_node*)new_node;
		copy_header((art_node*)new_node, (art_node*)n);

		int pos = 0;
		for (int i = 0; i < 256; i++) {
			if (n->children[i]) {
				new_node->children[pos] = n->children[i];
				new_node->keys[i] = pos + 1;
				pos++;
			}
		}
	}
}

void art_tree::remove_fat_child256(art_node256_kv* n, art_node** ref) {
	// Delete the child by turning the fat node to a normal internal node
	n->n.n.type = ART_NODE256;
	*ref = (art_node*)n;
}

// Remove a LEAF from a node48/node48kv
void art_tree::remove_child48(art_node48* n, art_node** ref, unsigned char c) {
	int pos = n->keys[c];
	n->keys[c] = 0;
	n->children[pos - 1] = nullptr;
	n->n.num_children--;

	if (n->n.num_children == 12) {
		art_node16* new_node;
		if (n->n.type == ART_NODE48_KV) {
			new_node = (art_node16*)alloc_node(ART_NODE16_KV);
			ART_FAT_NODE_LEAF(&new_node->n) = ART_FAT_NODE_LEAF(&n->n);
		} else {
			new_node = (art_node16*)alloc_node(ART_NODE16);
		}
		*ref = (art_node*)new_node;
		copy_header((art_node*)new_node, (art_node*)n);

		int child = 0;
		for (int i = 0; i < 256; i++) {
			pos = n->keys[i];
			if (pos) {
				new_node->keys[child] = i;
				new_node->children[child] = n->children[pos - 1];
				child++;
			}
		}
	}
}

void art_tree::remove_fat_child48(art_node48_kv* n, art_node** ref) {
	// Delete the child by turning the fat node to a normal internal node
	n->n.n.type = ART_NODE48;
	*ref = (art_node*)n;
}

// Remove a LEAF from a node16/node16kv
void art_tree::remove_child16(art_node16* n, art_node** ref, art_node** l) {
	int pos = l - n->children;
	memmove(n->keys + pos, n->keys + pos + 1, n->n.num_children - 1 - pos);
	memmove(n->children + pos, n->children + pos + 1, (n->n.num_children - 1 - pos) * sizeof(void*));
	n->n.num_children--;

	if (n->n.num_children == 3) {
		art_node4* new_node;
		if (n->n.type == ART_NODE16_KV) {
			new_node = (art_node4*)alloc_node(ART_NODE4_KV);
			ART_FAT_NODE_LEAF(&new_node->n) = ART_FAT_NODE_LEAF(&n->n);
		} else {
			new_node = (art_node4*)alloc_node(ART_NODE4);
		}

		*ref = (art_node*)new_node;
		copy_header((art_node*)new_node, (art_node*)n);
		memcpy(new_node->keys, n->keys, 4);
		memcpy(new_node->children, n->children, 4 * sizeof(void*));
	}
}

void art_tree::remove_fat_child16(art_node16_kv* n, art_node** ref) {
	// Delete the child by turning the fat node to a normal internal node
	n->n.n.type = ART_NODE16;
	*ref = (art_node*)n;
}

// Remove a LEAF from a node4/node4kv
void art_tree::remove_child4(art_node4* n, art_node** ref, art_node** l, int depth) {
	int pos = l - n->children;
	// We should do this only if then we do not remove the node altogether
	memmove(n->keys + pos, n->keys + pos + 1, n->n.num_children - 1 - pos);
	memmove(n->children + pos, n->children + pos + 1, (n->n.num_children - 1 - pos) * sizeof(void*));
	n->n.num_children--;

	// Remove nodes with only a single child
	// This can only be done if the node is not a fat node.
	// In that case, the key in the fat node is a prefix to the key in the only child
	// And hence has to be preserved in the fat node
	if (n->n.num_children == 1 && (n->n.type < ART_NODE4_KV) && depth) {
		art_node* child = n->children[0];
		if (!ART_IS_LEAF(child)) {
			// Concatenate the prefixes
			int prefix = n->n.partial_len;
			if (prefix < ART_MAX_PREFIX_LEN) {
				n->n.partial[prefix] = n->keys[0];
				prefix++;
			}
			if (prefix < ART_MAX_PREFIX_LEN) {
				int sub_prefix = min(child->partial_len, ART_MAX_PREFIX_LEN - prefix);
				memcpy(n->n.partial + prefix, child->partial, sub_prefix);
				prefix += sub_prefix;
			}

			// Store the prefix in the child
			memcpy(child->partial, n->n.partial, min(prefix, ART_MAX_PREFIX_LEN));
			child->partial_len += n->n.partial_len + 1;
		}
		// This is done also for the leaf. If the only son is a leaf, just delete the internal node
		// And have the father of the deleted internal node point directly to the leaf.
		*ref = child;
	}

	// What can happen now is that the node has ZERO children.
	// If you have a fat node, you cannot compress when you get to one child
	// So it can happen that the one child get removed
	// At that point, you transform the fat node into a leaf: the fat key is not prefix of any subtree!
	// This should only happen to a node4kv: if the node is normal, then already when there there is 1 child
	// the node gets compressed.
	// We still check for depth b/c we want to avoid that root becomes empty and becomes a leaf
	else if (n->n.num_children == 0 && depth) {
		*ref = (art_node*)SET_LEAF((art_node*)ART_FAT_NODE_LEAF(&n->n));
	} else if (!depth && n->n.num_children == 0) {
		n->children[0] = nullptr;
	}
}

void art_tree::remove_fat_child4(art_node4_kv* n, art_node** ref, int depth) {

	// Delete the child by turning the fat node to a normal internal node
	n->n.n.type = ART_NODE4;

	*ref = (art_node*)n; // Tell daddy that my address has changed
	art_node4* node = (art_node4*)n;

	// Remove nodes with only a single child
	if (node->n.num_children == 1 && depth) {
		art_node* child = node->children[0];
		if (!ART_IS_LEAF(child)) {
			// Concatenate the prefixes
			int prefix = node->n.partial_len;
			if (prefix < ART_MAX_PREFIX_LEN) {
				node->n.partial[prefix] = node->keys[0];
				prefix++;
			}
			if (prefix < ART_MAX_PREFIX_LEN) {
				int sub_prefix = min(child->partial_len, ART_MAX_PREFIX_LEN - prefix);
				memcpy(node->n.partial + prefix, child->partial, sub_prefix);
				prefix += sub_prefix;
			}

			// Store the prefix in the child
			memcpy(child->partial, node->n.partial, min(prefix, ART_MAX_PREFIX_LEN));
			child->partial_len += node->n.partial_len + 1;
		}
		*ref = child;

	} else if (!depth && node->n.num_children == 0) {
		// This happens if we have a fat root with the empty key.
		// If only the empty key remains, then the fat root has zero children
		node->children[0] = nullptr;
	}
}

void art_tree::remove_child(art_node* n, art_node** ref, unsigned char c, art_node** l, int depth) {
	switch (n->type) {
	case ART_NODE4_KV:
	case ART_NODE4:
		return remove_child4((art_node4*)n, ref, l, depth);
	case ART_NODE16_KV:
	case ART_NODE16:
		return remove_child16((art_node16*)n, ref, l);
	case ART_NODE48_KV:
	case ART_NODE48:
		return remove_child48((art_node48*)n, ref, c);
	case ART_NODE256_KV:
	case ART_NODE256:
		return remove_child256((art_node256*)n, ref, c);
	default:
		UNSTOPPABLE_ASSERT(false);
	}
}

// NB: Here, n is the fat node from which we remove the kv/pair
void art_tree::remove_fat_child(art_node* n, art_node** ref, int depth) {
	switch (n->type) {
	case ART_NODE4_KV:
		return remove_fat_child4((art_node4_kv*)n, ref, depth);
	case ART_NODE16_KV:
		return remove_fat_child16((art_node16_kv*)n, ref);
	case ART_NODE48_KV:
		return remove_fat_child48((art_node48_kv*)n, ref);
	case ART_NODE256_KV:
		return remove_fat_child256((art_node256_kv*)n, ref);
	default:
		UNSTOPPABLE_ASSERT(false);
	}
}

void art_tree::copy_header(art_node* dest, art_node* src) {
	dest->num_children = src->num_children;
	dest->partial_len = src->partial_len;
	memcpy(dest->partial, src->partial, min(ART_MAX_PREFIX_LEN, src->partial_len));
}

void art_tree::erase(const art_iterator& it) {
	recursive_delete_binary(this->root, &this->root, it.key(), 0);
}

art_leaf* art_tree::iterative_insert(art_node* root,
                                     art_node** root_ptr,
                                     KeyRef& k,
                                     void* value,
                                     int depth,
                                     int* old,
                                     int replace_existing) {

	art_node* n = root;
	art_node** ref = root_ptr;
	// Ref is the memory location of the father that stores the pointer to the child in which we add the new item
	// Of course, the root is not the child of anybody. We need the ptr to root bc it is possible that the root itself
	// Grows by adding nodes, and hence gets relocated. Then, we need to update the ptr to the root
	while (n) {
		if (ART_IS_LEAF(n)) {
			return insert_leaf(n, ref, k, value, depth, old, replace_existing);
		}
		art_leaf* min_of_n = nullptr;
		if (n->partial_len) {
			// Determine if the prefixes differ, since we need to split
			int prefix_diff = prefix_mismatch(n, k, depth, &min_of_n);
			if ((uint32_t)prefix_diff >= n->partial_len) {
				depth += n->partial_len;
				// handle fat key  or go in depth
			} else {
				return insert_internal_node(n, ref, k, value, depth, old, min_of_n, prefix_diff, replace_existing);
			}
		}
		if (k.size() == (depth)) {
			return insert_fat_node(n, ref, k, value, depth, old, min_of_n, replace_existing);
		}

		// Find a child to recurse to
		// Child is the pointer to the memory location within the node that contains the ptr to the child
		art_node** child = find_child(n, ((unsigned char*)k.begin())[depth]);
		if (!child) {
			break;
		}
		// Else go on. Update depth, the current node and its reference in the list of children in the parent
		depth++;
		ref = child;
		n = *ref;
	}
	return insert_child(n, ref, k, value, depth, old, replace_existing);
}

art_leaf* art_tree::insert_child(art_node* n,
                                 art_node** ref,
                                 const KeyRef& k,
                                 void* value,
                                 int depth,
                                 int* old,
                                 int replace_existing) {
	// No child, node goes within us
	art_leaf* l = make_leaf(k, value);
	const unsigned char* key = (const unsigned char*)k.begin();
	add_child(n, ref, key[depth], SET_LEAF(l));
	// After adding a child, the node might have grown (and old pointer n becomes invalid)
	art_node* new_node = *ref;

	art_node* pn = nullptr;
	art_leaf* lm = nullptr;
	int prev_next = art_next_prev(new_node, key[depth], &pn);
	if (prev_next == ART_NEXT) {
		// If the char of this key is NOT the maximum in the node, take the key's next node from curr_node
		// If the next node exists, then the MIN of the next node is the new_key's next
		// NB: The fat leaf cannot be the next, so we search for the min in the subtree
		lm = minimum(pn);
		// new key is LOWER than min
		insert_before(l, lm);
	} else if (prev_next == ART_PREV) {
		// If the next does not exist, it means there is a prev (the curr_node has at least one child).
		// The curr key is the next of the MAX of the prev node
		// Check if the prev is the fat_leaf
		if (pn == new_node) {
			lm = ART_FAT_NODE_LEAF(new_node);
		} else {
			lm = maximum(pn);
		}
		// new key is BIGGER than lm
		insert_after(l, lm);
	} // if it is ART_NEITHER, then there's no prev nor next. They have been set to nullptr already

	return l;
}

int art_tree::art_next_prev(art_node* n, unsigned char c, art_node** out) {
	find_next(n, c, out);
	if (*out) {
		return ART_NEXT;
	}
	find_prev(n, c, out);
	if (*out) {
		return ART_PREV;
	}
	return ART_NEITHER;
}

art_leaf* art_tree::insert_internal_node(art_node* n,
                                         art_node** ref,
                                         const KeyRef& k,
                                         void* value,
                                         int depth,
                                         int* old,
                                         art_leaf* min_of_n,
                                         int prefix_diff,
                                         int replace_existing) {

	art_node4* new_node;
	bool kv_creat = (prefix_diff == (k.size() - depth));
	if (!kv_creat) {
		new_node = (art_node4*)alloc_node(ART_NODE4);
		*ref = (art_node*)new_node;
		new_node->n.partial_len = prefix_diff;
		memcpy(new_node->n.partial, n->partial, min(ART_MAX_PREFIX_LEN, prefix_diff));

		// Adjust the prefix of the old node
		if (n->partial_len <= ART_MAX_PREFIX_LEN) {
			add_child4(new_node, ref, n->partial[prefix_diff], n);
			n->partial_len -= (prefix_diff + 1);
			memmove(n->partial, n->partial + prefix_diff + 1, min(ART_MAX_PREFIX_LEN, n->partial_len));
		} else {
			n->partial_len -= (prefix_diff + 1);
			art_leaf* l = min_of_n == nullptr ? minimum(n) : min_of_n;
			min_of_n = l;
			unsigned char* lkey = (unsigned char*)l->key.begin();
			add_child4(new_node, ref, lkey[depth + prefix_diff], n);
			memcpy(n->partial, lkey + depth + prefix_diff + 1, min(ART_MAX_PREFIX_LEN, n->partial_len));
		}

		// Insert the new leaf
		art_leaf* l = make_leaf(k, value);
		add_child4(new_node, ref, ((unsigned char*)k.begin())[depth + prefix_diff], SET_LEAF(l));

		art_node* pn;
		art_leaf* lm = nullptr;
		int prev_next = art_next_prev((art_node*)new_node, l->key[depth + prefix_diff], &pn);
		if (prev_next == ART_NEXT) {
			// If the char of this key is NOT the maximum in the node, take the key's next node from curr_node
			// If the next node exists, then the MIN of the next node is the new_key's next
			lm = minimum(pn);
			insert_before(l, lm);
		} else if (prev_next == ART_PREV) {
			// If the next does not exist, it means there is a prev (the curr_node has at least one child).
			// The curr key is the next of the MAX of the prev node
			// We do not check for fat node
			lm = maximum(pn);
			// new key is BIGGER than lm
			insert_after(l, lm);
		}
		return l;
	} else {
		// The new key becomes a fat node that has as child the current internal node.
		// The current internal node has to be indexed by the first char in the prefix, whose size has to be reduced
		// By one accordingly
		new_node = (art_node4*)alloc_kv_node(ART_NODE4_KV);
		ART_FAT_NODE_LEAF(&new_node->n) = make_leaf(k, value);
		// This is legacy code. I think it is valid, but probably it can be simplified in our case
		*ref = (art_node*)new_node;
		new_node->n.partial_len = prefix_diff;
		memcpy(new_node->n.partial, n->partial, min(ART_MAX_PREFIX_LEN, prefix_diff));

		// Adjust the prefix of the old node

		if (n->partial_len <= ART_MAX_PREFIX_LEN) {
			add_child4(new_node, ref, n->partial[prefix_diff], n);
			n->partial_len -= (prefix_diff + 1);
			memmove(n->partial, n->partial + prefix_diff + 1, min(ART_MAX_PREFIX_LEN, n->partial_len));
		} else {
			n->partial_len -= (prefix_diff + 1);
			art_leaf* l = min_of_n == nullptr ? minimum(n) : min_of_n; // minimum(n);
			min_of_n = l;
			add_child4(new_node, ref, ((unsigned char*)l->key.begin())[depth + prefix_diff], n);
			memcpy(n->partial, l->key.begin() + depth + prefix_diff + 1, min(ART_MAX_PREFIX_LEN, n->partial_len));
		}
		// Fat leaf is the smallest in this subtree. So the minimum in the subtree is its next
		art_leaf* lm =
		    min_of_n == nullptr ? minimum(n) : min_of_n; // minimum(n); //FIXME: reuse from before if already taken
		min_of_n = lm;
		// new key is LOWER than min
		art_leaf* l_new = ART_FAT_NODE_LEAF(&new_node->n);
		insert_before(l_new, lm);
		return l_new;
	}
}

art_leaf* art_tree::insert_fat_node(art_node* n,
                                    art_node** ref,
                                    const KeyRef& k,
                                    void* value,
                                    int depth,
                                    int* old,
                                    art_leaf* min_of_n,
                                    int replace_existing) {
	if (n->type >= ART_NODE4_KV) {
		// If the node is already fat, it means you already have the key
		*old = 1;
		art_leaf* l = ART_FAT_NODE_LEAF(n);
		if (replace_existing) {
			l->value = value;
		}
		return l;
	} else {
		// We have to transform a node into a fat node
		// The minimum function considers the leaf to be the minimum.
		// Let's grab the minimum BEFORE we create the fat node and insert the new leaf :)
		art_leaf* lm = min_of_n == nullptr ? minimum(n) : min_of_n;
		art_node* nkv = n;
		// change the type before deferencing the leaf
		nkv->type = static_cast<ART_NODE_TYPE>(n->type + 4);
		art_leaf* l_new = make_leaf(k, value);
		ART_FAT_NODE_LEAF(nkv) = l_new;
		*ref = (art_node*)nkv;

		// Fat leaf is the smallest in this subtree. So the minimum in the subtree is its next
		// new key is LOWER than min
		if (lm)
			insert_before(l_new, lm);
		return l_new;
	}
}

art_leaf* art_tree::insert_leaf(art_node* n,
                                art_node** ref,
                                const KeyRef& k,
                                void* value,
                                int depth,
                                int* old,
                                int replace_existing) {
	art_leaf* l = ART_LEAF_RAW(n);

	// Check if we are updating an existing value
	// Original case
	if (!leaf_matches(l, k, depth)) {
		*old = 1;
		if (replace_existing) {
			l->value = value;
		}
		return l;
	}
	const unsigned char* key = (const unsigned char*)k.begin();
	const int key_len = k.size();

	int longest_prefix = longest_common_prefix(k, l->key, depth);
	int kv_creat = (longest_prefix == (min(key_len, l->key.size()) - depth));
	if (!kv_creat) { // Common case, old code
		// Recompute longest prefix from depth. Probably can be optimized

		// New value, we must split the leaf into a node4
		art_node4* new_node = (art_node4*)alloc_node(ART_NODE4);
		// Create a new leaf
		art_leaf* l2 = make_leaf(k, value);
		// Determine longest prefix
		new_node->n.partial_len = longest_prefix;
		memcpy(new_node->n.partial, key + depth, min(ART_MAX_PREFIX_LEN, longest_prefix));
		// Add the leaves to the new node4
		*ref = (art_node*)new_node;
		add_child4(new_node, ref, l->key[depth + longest_prefix], SET_LEAF(l));
		add_child4(new_node, ref, l2->key[depth + longest_prefix], SET_LEAF(l2));

		// Set next and prev. Check if new leaf goes before or after existing leaf
		if (l2->key[depth + longest_prefix] < l->key[depth + longest_prefix]) {
			// New key comes BEFORE existing key
			insert_before(l2, l);
		} else {
			// New key comes AFTER existing key
			insert_after(l2, l);
		}
		return l2;
	}

	// The new key  is a superset of the key in the leaf.
	// So the current leaf becomes the value of the kv node
	if (key_len > l->key.size()) {
		art_node4_kv* new_node = (art_node4_kv*)alloc_kv_node(ART_NODE4_KV);
		// Create a new leaf with the new key
		art_leaf* l2 = make_leaf(k, value);

		// Determine longest prefix
		new_node->n.n.partial_len = longest_prefix;
		memcpy(new_node->n.n.partial, key + depth, min(ART_MAX_PREFIX_LEN, longest_prefix));

		// Add the leaf to the new node4
		*ref = (art_node*)new_node;
		add_child4((art_node4*)&new_node->n, ref, l2->key[depth + longest_prefix], SET_LEAF(l2));

		ART_FAT_NODE_LEAF(&new_node->n.n) = l;
		art_leaf* fat_leaf = ART_FAT_NODE_LEAF(&new_node->n.n);

		// The new key goes in the leaf and its predecessor is the key in the new fat node
		insert_after(l2, fat_leaf);

		return l2;
	} else {
		// The key in the leaf is a supertset of the new key
		// So the leaf stays a leaf and the new key goes in the kv_node
		art_node4_kv* new_node = (art_node4_kv*)alloc_kv_node(ART_NODE4_KV);
		art_leaf* fat_leaf = make_leaf(k, value);
		ART_FAT_NODE_LEAF(&new_node->n.n) = fat_leaf;
		new_node->n.n.partial_len = longest_prefix;

		memcpy(new_node->n.n.partial, key + depth, min(ART_MAX_PREFIX_LEN, longest_prefix));
		*ref = (art_node*)new_node;
		add_child4((art_node4*)&new_node->n, ref, l->key[depth + longest_prefix], SET_LEAF(l));

		// The fat node is the prev of the current leaf
		insert_before(fat_leaf, l);
		return fat_leaf;
	}
}

int art_tree::longest_common_prefix(const KeyRef& k1, const KeyRef& k2, int depth) {
	int max_cmp = min(k1.size(), k2.size()) - depth;
	int idx;
	for (idx = 0; idx < max_cmp; idx++) {
		if (((unsigned char*)k1.begin())[depth + idx] != ((unsigned char*)k2.begin())[depth + idx])
			return idx;
	}
	return idx;
}

void art_tree::insert_before(art_leaf* l_new, art_leaf* l_existing) {
	l_new->next = l_existing;
	if (l_existing) {
		l_new->prev = l_existing->prev;
		if (l_existing->prev) {
			l_existing->prev->next = l_new;
		}
		l_existing->prev = l_new;
	} else {
		l_new->prev = nullptr;
	}
}

void art_tree::insert_after(art_leaf* l_new, art_leaf* l_existing) {
	l_new->prev = l_existing;
	if (l_existing) {
		l_new->next = l_existing->next;
		if (l_existing->next) {
			l_existing->next->prev = l_new;
		}
		l_existing->next = l_new;
	} else {
		l_new->next = nullptr;
	}
}

void art_tree::add_child(art_node* n, art_node** ref, unsigned char c, void* child) {
	switch (n->type) {
	case ART_NODE4_KV:
	case ART_NODE4:
		return add_child4((art_node4*)n, ref, c, child);
	case ART_NODE16_KV:
	case ART_NODE16:
		return add_child16((art_node16*)n, ref, c, child);
	case ART_NODE48_KV:
	case ART_NODE48:
		return add_child48((art_node48*)n, ref, c, child);
	case ART_NODE256_KV:
	case ART_NODE256:
		return add_child256((art_node256*)n, ref, c, child);
	default:
		UNSTOPPABLE_ASSERT(false);
	}
}

void art_tree::add_child256(art_node256* n, art_node** ref, unsigned char c, void* child) {
	(void)ref;
	n->n.num_children++;
	n->children[c] = (art_node*)child;
}

void art_tree::add_child48(art_node48* n, art_node** ref, unsigned char c, void* child) {
	if (n->n.num_children < 48) {
		int pos = 0;
		while (n->children[pos])
			pos++;
		n->children[pos] = (art_node*)child;
		n->keys[c] = pos + 1;
		n->n.num_children++;
	} else {
		art_node256* new_node;
		// Copy the fat leaf pointer if needed
		if (n->n.type == ART_NODE48_KV) {
			new_node = (art_node256*)alloc_node(ART_NODE256_KV);
			ART_FAT_NODE_LEAF(&new_node->n) = ART_FAT_NODE_LEAF(&n->n);
		} else {
			new_node = (art_node256*)alloc_node(ART_NODE256);
		}
		for (int i = 0; i < 256; i++) {
			if (n->keys[i]) {
				new_node->children[i] = n->children[n->keys[i] - 1];
			}
		}
		copy_header((art_node*)new_node, (art_node*)n);
		*ref = (art_node*)new_node;
		add_child256(new_node, ref, c, child);
	}
}

void art_tree::add_child16(art_node16* n, art_node** ref, unsigned char c, void* child) {
	if (n->n.num_children < 16) {
		__m128i cmp;

		// Compare the key to all 16 stored keys
		cmp = _mm_cmplt_epu8(_mm_set1_epi8(c), _mm_loadu_si128((__m128i*)n->keys));

		// Use a mask to ignore children that don't exist
		unsigned mask = (1 << n->n.num_children) - 1;
		unsigned bitfield = _mm_movemask_epi8(cmp) & mask;

		// Check if less than any
		unsigned idx;
		if (bitfield) {
			idx = ctz(bitfield);
			memmove(n->keys + idx + 1, n->keys + idx, n->n.num_children - idx);
			memmove(n->children + idx + 1, n->children + idx, (n->n.num_children - idx) * sizeof(void*));
		} else
			idx = n->n.num_children;

		// Set the child
		n->keys[idx] = c;
		n->children[idx] = (art_node*)child;
		n->n.num_children++;

	} else {
		art_node48* new_node;
		// Check whether this is a fat node
		if (n->n.type == ART_NODE16_KV) {
			new_node = (art_node48*)alloc_node(ART_NODE48_KV);
			ART_FAT_NODE_LEAF(&new_node->n) = ART_FAT_NODE_LEAF(&n->n);
		} else {
			new_node = (art_node48*)alloc_node(ART_NODE48);
		}
		// Copy the child pointers and populate the key map
		memcpy(new_node->children, n->children, sizeof(void*) * n->n.num_children);
		for (int i = 0; i < n->n.num_children; i++) {
			new_node->keys[n->keys[i]] = i + 1;
		}
		copy_header((art_node*)new_node, (art_node*)n);
		*ref = (art_node*)new_node;
		add_child48(new_node, ref, c, child);
	}
}

void art_tree::add_child4(art_node4* n, art_node** ref, unsigned char c, void* child) {
	if (n->n.num_children < 4) {
		int idx;
		for (idx = 0; idx < n->n.num_children; idx++) {
			if (c < n->keys[idx])
				break;
		}

		// Shift to make room
		memmove(n->keys + idx + 1, n->keys + idx, n->n.num_children - idx);
		memmove(n->children + idx + 1, n->children + idx, (n->n.num_children - idx) * sizeof(void*));

		// Insert element
		n->keys[idx] = c;
		n->children[idx] = (art_node*)child;
		n->n.num_children++;

	} else {
		art_node16* new_node;
		// Check whether this is a fat node
		if (n->n.type == ART_NODE4_KV) {
			new_node = (art_node16*)alloc_node(ART_NODE16_KV);
			ART_FAT_NODE_LEAF(&new_node->n) = ART_FAT_NODE_LEAF(&n->n);
		} else {
			new_node = (art_node16*)alloc_node(ART_NODE16);
		}

		// Copy the child pointers and the key map
		memcpy(new_node->children, n->children, sizeof(void*) * n->n.num_children);
		memcpy(new_node->keys, n->keys, sizeof(unsigned char) * n->n.num_children);
		copy_header((art_node*)new_node, (art_node*)n);
		*ref = (art_node*)new_node;
		add_child16(new_node, ref, c, child);
	}
}

// Every node is actually a kv node, but the type is <= NODE256
art_node* art_tree::alloc_node(ART_NODE_TYPE type) {
	const int offset = type > ART_NODE256 ? 0 : ART_NODE256;
	art_node* n = (art_node*)new ((Arena&)*this->arena) uint8_t[node_sizes[offset + type]]();
	n->type = type;
	return n;
}

art_node* art_tree::alloc_kv_node(ART_NODE_TYPE type) {
	art_node* n = (art_node*)new ((Arena&)*this->arena) uint8_t[node_sizes[type]]();
	n->type = type;
	return n;
}

art_leaf* art_tree::make_leaf(const KeyRef& k, void* value) {
	const int key_len = k.size();
	// Allocate contiguous buffer to hold the leaf and the key pointed by the KeyRef
	art_leaf* v = (art_leaf*)new ((Arena&)*this->arena) uint8_t[sizeof(art_leaf) + key_len];
	// copy the key to the proper offset in the buffer
	memcpy(v + 1, k.begin(), key_len);
	KeyRef nkr = KeyRef((const uint8_t*)(v + 1), key_len);
	// create the art_leaf, allocating it on the buffer
	// The KeyRef& passed as argument is the new one, allocated in the buffer
	art_leaf* l = new (v) art_leaf(nkr, value);
	l->prev = nullptr;
	l->next = nullptr;
	l->type = ART_LEAF;
	return l;
}

int art_tree::prefix_mismatch(art_node* n, KeyRef& k, int depth, art_leaf** minout) {
	const int key_len = k.size();
	const unsigned char* key = (const unsigned char*)k.begin();
	int max_cmp = min(min(ART_MAX_PREFIX_LEN, n->partial_len), key_len - depth);
	int idx;
	for (idx = 0; idx < max_cmp; idx++) {
		if (n->partial[idx] != key[depth + idx])
			return idx;
	}

	// If the prefix is short we can avoid finding a leaf
	if (n->partial_len > ART_MAX_PREFIX_LEN) {
		// Prefix is longer than what we've checked, find a leaf
		art_leaf* l = minimum(n);
		*minout = l;
		max_cmp = min(l->key.size(), key_len) - depth;
		for (; idx < max_cmp; idx++) {
			if (l->key.begin()[idx + depth] != k.begin()[depth + idx])
				return idx;
		}
	}
	return idx;
}

// Find the child corresponding to c, if present, and the largest child smaller than c
void art_tree::find_prev(art_node* n, unsigned char c, art_node** out) {
	int i, mask, bitfield;
	*out = nullptr;
	union {
		art_node4* p1;
		art_node16* p2;
		art_node48* p3;
		art_node256* p4;
	} p;
	switch (n->type) {
	case ART_NODE4_KV:
	case ART_NODE4:
		p.p1 = (art_node4*)n;
		for (i = 0; i < n->num_children; i++) {
			if (p.p1->keys[i] < c) {
				*out = p.p1->children[i];
			} else {
				break;
			}
		}
		if (i == 0 && n->type == ART_NODE4_KV) {
			*out = n;
		}

		break;

		{
			__m128i cmp;
		case ART_NODE16_KV:
		case ART_NODE16:

			p.p2 = (art_node16*)n;

			// Compare the key to all 16 stored keys for less than
			cmp = _mm_cmpgt_epu8(_mm_set1_epi8(c), _mm_loadu_si128((__m128i*)p.p2->keys));

			// Use a mask to ignore children that don't exist
			mask = (1 << n->num_children) - 1;
			bitfield = _mm_movemask_epi8(cmp) & mask;
			if (bitfield) {
				// We have at least one bit set to one, so the largest smaller exists
				int one = clz(bitfield);
				one = (sizeof(bitfield) * 8) - one;
				*out = p.p2->children[one - 1];
			} else {
				if (n->type == ART_NODE16_KV) {
					*out = n;
				}
			}

			break;
		}
	case ART_NODE48_KV:
	case ART_NODE48: {
		p.p3 = (art_node48*)n;

		unsigned char cc = c - 1;
		while (cc < 255) {
			i = p.p3->keys[cc];
			if (i) {
				*out = p.p3->children[i - 1];
				break;
			}
			--cc;
		}
		if (cc == 255 && n->type == ART_NODE48_KV) {
			*out = n;
		}
		break;
	}
	case ART_NODE256_KV:
	case ART_NODE256: {
		p.p4 = (art_node256*)n;
		unsigned char cc = c - 1;
		if (c == 0 && n->type == ART_NODE256_KV) {
			*out = n;
		}
		while (cc < 255) {
			if (p.p4->children[cc]) {
				*out = p.p4->children[cc];
				break;
			}
			--cc;
		}
		break;
	}

	default:
		UNSTOPPABLE_ASSERT(false);
	}
}

#endif // ART_IMPL_H
