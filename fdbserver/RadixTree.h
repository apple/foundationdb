/*
 * RadixTree.h
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

#ifndef FLOW__RADIXTREE_H
#define FLOW__RADIXTREE_H
#pragma once

#include <cassert>
#include <string>
#include <utility>
#include <vector>
#include <iostream>
#include <functional>
#include <map>
#include <stdexcept>

#include "fdbserver/IKeyValueContainer.h"
#include "flow/Arena.h"

// forward declaration
const int LEAF_BYTE = -1;
const int INLINE_KEY_SIZE = sizeof(StringRef);

StringRef radix_substr(const StringRef& key, int begin, int num) {
	int size = key.size();
	if (begin > size) {
		throw std::out_of_range("out of range in radix_substr<StringRef>");
	}
	if ((begin + num) > size) {
		num = size - begin;
	}
	return key.substr(begin, num);
}

StringRef radix_join(const StringRef& key1, const StringRef& key2, Arena& arena) {
	int rsize = key1.size() + key2.size();
	uint8_t* s = new (arena) uint8_t[rsize];

	memcpy(s, key1.begin(), key1.size());
	if (key2.size() > 0) {
		memcpy(s + key1.size(), key2.begin(), key2.size());
	}

	return StringRef(s, rsize);
}

StringRef radix_constructStr(const StringRef& key, int begin, int num, Arena& arena) {
	int size = key.size();
	if (begin > size) {
		throw std::out_of_range("out of range in radix_substr<StringRef>");
	}
	if ((begin + num) > size) {
		num = size - begin;
	}
	return StringRef(arena, key.substr(begin, num));
}

class radix_tree {
public:
	typedef std::size_t size_type;

private:
	// union used inside both base node and leaf node
	union inlineUnion {
		inlineUnion() : data() {} // constructor

		uint8_t inlineData[INLINE_KEY_SIZE];
		StringRef data;
	};

	struct node {
		// constructor for all kinds of node (root/internal/leaf)
		node() : m_is_leaf(0), m_is_inline(0), m_inline_length(0), m_depth(0), key(), arena(), m_parent(nullptr) {}

		node(const node&) = delete; // delete
		node& operator=(const node& other) {
			m_is_leaf = other.m_is_leaf;
			m_is_inline = other.m_is_inline;
			m_inline_length = other.m_inline_length;
			m_depth = other.m_depth;
			memcpy(key.inlineData, other.key.inlineData, INLINE_KEY_SIZE);
			arena = other.arena;
			m_parent = other.m_parent;

			return *this;
		}

		void setKey(const StringRef& content, int start, int num) {
			bool isInline = num <= INLINE_KEY_SIZE;
			if (isInline) {
				memcpy(key.inlineData, content.begin() + start, num);
				m_inline_length = num;
				if (!m_is_inline)
					arena = Arena();
			} else {
				Arena new_arena(num);
				key.data = radix_constructStr(content, start, num, new_arena);
				arena = new_arena;
			}
			m_is_inline = isInline;
		}

		StringRef getKey() const {
			if (m_is_inline) {
				return StringRef(&key.inlineData[0], m_inline_length);
			} else {
				return key.data;
			}
		}

		inline int getKeySize() const { return m_is_inline ? m_inline_length : key.data.size(); }

		inline int16_t getFirstByte() const {
			if (m_is_inline) {
				return m_inline_length == 0 ? LEAF_BYTE : key.inlineData[0];
			} else {
				return key.data.size() == 0 ? LEAF_BYTE : key.data[0];
			}
		}

		inline size_type getArenaSize() const { return m_is_inline ? 0 : arena.getSize(); }

		uint32_t m_is_leaf : 1;
		uint32_t m_is_fixed : 1; // if true, then we have fixed number of children (3)
		uint32_t m_is_inline : 1;
		uint32_t m_inline_length : 4;
		// m_depth can be seen as common prefix length with your ancestors
		uint32_t m_depth : 25;
		// key is the prefix, a substring that shared by your children
		inlineUnion key;
		// arena assign memory for key
		Arena arena;
		node* m_parent;
	};

	struct leafNode : FastAllocated<leafNode> {
		leafNode(const StringRef& content) : base(), is_inline(0), inline_length(0), arena() {
			base.m_is_leaf = 1;
			setValue(content);
		}

		~leafNode() = default;

		void setValue(const StringRef& content) {
			bool isInline = content.size() <= INLINE_KEY_SIZE;
			if (isInline) {
				memcpy(value.inlineData, content.begin(), content.size());
				inline_length = content.size();
				if (!is_inline)
					arena = Arena();
			} else {
				Arena new_arena(content.size());
				value.data = StringRef(new_arena, content);
				arena = new_arena;
			}
			is_inline = isInline;
		}

		StringRef getValue() {
			if (is_inline) {
				return StringRef(&value.inlineData[0], inline_length);
			} else {
				return value.data;
			}
		}

		inline size_type getLeafArenaSize() { return is_inline ? 0 : arena.getSize(); }

		node base; // 32 bytes

		uint32_t is_inline : 1;
		uint32_t inline_length : 31;
		inlineUnion value; // using the same data structure to store value
		Arena arena;
	};

	struct internalNode : FastAllocated<internalNode> {
		internalNode() : base(), m_children(std::vector<std::pair<int16_t, node*>>()) {
			m_children.reserve(4);
			base.m_is_fixed = 0;
		}

		~internalNode() {
			for (auto it = 0; it < m_children.size(); ++it) {
				node* current = m_children[it].second;
				if (current->m_is_leaf) {
					delete (leafNode*)current;
				} else {
					current->m_is_fixed ? delete (internalNode4*)current : delete (internalNode*)current;
				}
			}
			m_children.clear();
		}

		node base;
		// ordered map by char, m_children.begin() return the smallest value
		std::vector<std::pair<int16_t, node*>> m_children;
	};

	struct internalNode4 : FastAllocated<internalNode4> {
		internalNode4() : base(), num_children(0) {
			base.m_is_fixed = 1;
			memset(keys, 0, sizeof(keys));
			memset(m_children, 0, sizeof(m_children));
		}

		~internalNode4() { num_children = 0; }

		node base;
		int16_t num_children;
		int16_t keys[3];
		node* m_children[3];
	};

public:
	class iterator : public std::iterator<std::forward_iterator_tag, std::pair<StringRef, StringRef>> {
	public:
		node* m_pointee;

		iterator() : m_pointee(nullptr) {}
		iterator(const iterator& r) : m_pointee(r.m_pointee) {}
		iterator(node* p) : m_pointee(p) {}
		iterator& operator=(const iterator& r) {
			m_pointee = r.m_pointee;
			return *this;
		}
		~iterator() = default;

		const iterator& operator++();
		const iterator& operator--();
		bool operator!=(const iterator& lhs) const;
		bool operator==(const iterator& lhs) const;
		StringRef getKey(uint8_t* content) const;
		StringRef getValue() const {
			ASSERT(m_pointee->m_is_leaf);
			return ((leafNode*)m_pointee)->getValue();
		}

	private:
		node* increment(node* target) const;
		node* decrement(node* target) const;
	};

	explicit radix_tree() : m_size(0), m_node(0), inline_keys(0), total_bytes(0), m_root(nullptr) {}

	~radix_tree() {}

	radix_tree(const radix_tree& other) = delete; // delete
	radix_tree& operator=(const radix_tree other) = delete; // delete

	inline std::tuple<size_type, size_type, size_type> size() const {
		return std::make_tuple(m_size, m_node, inline_keys);
	}

	// Return the amount of memory used by an entry in the RadixTree
	static int getElementBytes(node* node) {
		int result = 0;
		if (node->m_is_leaf) {
			result = sizeof(leafNode) + ((leafNode*)node)->getLeafArenaSize();
		} else if (node->m_is_fixed) {
			result = sizeof(internalNode4);
		} else {
			ASSERT(!node->m_is_fixed);
			result = sizeof(internalNode);
		}
		return result;
	}

	// dummy method interface(to keep every interface same as IndexedSet )
	static int getElementBytes() {
		ASSERT(false);
		return 0;
	}

	bool empty() const { return m_size == 0; }

	void clear() {
		if (m_root != nullptr) {
			delete (internalNode*)m_root;
			m_root = nullptr;
		}
		m_size = 0;
		m_node = 0;
		inline_keys = 0;
		total_bytes = 0;
	}
	// iterators
	iterator find(const StringRef& key);
	iterator begin();
	iterator end() const;
	iterator previous(iterator i);
	// modifications
	std::pair<iterator, bool> insert(const StringRef& key, const StringRef& val, bool replaceExisting = true);
	int insert(const std::vector<std::pair<KeyValueMapPair, uint64_t>>& pairs, bool replaceExisting = true) {
		// dummy method interface(to keep every interface same as IndexedSet )
		ASSERT(false);
		return 0;
	}
	void erase(iterator it);
	void erase(iterator begin, iterator end);
	// lookups
	iterator lower_bound(const StringRef& key);
	iterator upper_bound(const StringRef& key);
	// access
	uint64_t sumTo(iterator to) const;

private:
	size_type m_size;
	// number of nodes that has been created
	size_type m_node;
	// number of nodes with key.size() <= 12
	size_type inline_keys;
	uint64_t total_bytes;
	node* m_root;

	// modification
	void add_child(node* parent, node* child);
	void add_child_vector(node* parent, node* child);
	void add_child4(node* parent, node* child);
	void delete_child(node* parent, node* child);
	void delete_child_vector(node* parent, node* child);
	void delete_child4(node* parent, node* child);
	// access
	static int find_child(node* parent, int16_t ch); // return index
	static int child_size(node* parent); // how many children does parent node have
	static node* get_child(node* parent, int index); // return node pointer

	// direction 0 = left, 1 = right
	template <int reverse>
	static node* descend(node* i) {
		while (!i->m_is_leaf) {
			ASSERT(child_size(i) != 0);
			if (reverse) {
				i = get_child(i, child_size(i) - 1);
			} else {
				i = get_child(i, 0);
			}
		}
		return i;
	}

	node* find_node(const StringRef& key, node* node, int depth);
	node* append(node* parent, const StringRef& key, const StringRef& val);
	node* prepend(node* node, const StringRef& key, const StringRef& val);
	bool erase(node* child);
	iterator lower_bound(const StringRef& key, node* node);
	iterator upper_bound(const StringRef& key, node* node);
};
/////////////////////// iterator //////////////////////////
void radix_tree::add_child(node* parent, node* child) {
	if (parent->m_is_fixed) {
		add_child4(parent, child);
	} else {
		add_child_vector(parent, child);
	}
}

void radix_tree::add_child4(node* parent, node* child) {
	int16_t ch = child->getFirstByte();
	internalNode4* parent_ref = (internalNode4*)parent;
	int i = 0;

	for (; i < parent_ref->num_children; ++i) {
		if (parent_ref->keys[i] >= ch)
			break;
	}

	if (!parent_ref->num_children) {
		// empty
		parent_ref->num_children++;
		parent_ref->keys[0] = ch;
		parent_ref->m_children[0] = child;
		// DEBUG
		total_bytes += getElementBytes(child) + child->getArenaSize();
	} else if (i >= 0 && i < parent_ref->num_children && parent_ref->keys[i] == ch) {
		// replace
		node* original = parent_ref->m_children[i];
		total_bytes -= (getElementBytes(original) + original->getArenaSize());
		parent_ref->m_children[i] = child;
		total_bytes += getElementBytes(child) + child->getArenaSize();
	} else if (parent_ref->num_children < 3) {
		// Shift to make room
		memmove(parent_ref->keys + i + 1, parent_ref->keys + i, (parent_ref->num_children - i) * sizeof(int16_t));
		memmove(
		    parent_ref->m_children + i + 1, parent_ref->m_children + i, (parent_ref->num_children - i) * sizeof(void*));

		// Insert element
		parent_ref->keys[i] = ch;
		parent_ref->m_children[i] = child;
		parent_ref->num_children++;
		// DEBUG
		total_bytes += getElementBytes(child) + child->getArenaSize();
	} else {
		ASSERT(parent_ref->num_children >= 3);

		internalNode* new_node = new radix_tree::internalNode();
		new_node->base = parent_ref->base; // equal operator
		for (int index = 0; index < parent_ref->num_children; index++) {
			new_node->m_children.emplace_back(parent_ref->keys[index], parent_ref->m_children[index]);
			parent_ref->m_children[index]->m_parent = (node*)new_node;
		}
		// Insert new element
		new_node->m_children.insert(new_node->m_children.begin() + i, std::make_pair(ch, child));
		child->m_parent = (node*)new_node;
		// update parent info
		add_child(new_node->base.m_parent, (node*)new_node);
		// DEBUG
		total_bytes += new_node->m_children.size() * sizeof(std::pair<int16_t, void*>) + getElementBytes(child) +
		               child->getArenaSize();
		delete parent_ref;
	}
}

void radix_tree::add_child_vector(node* parent, node* child) {
	int16_t ch = child->getFirstByte();
	internalNode* parent_ref = (internalNode*)parent;
	int i = 0;

	for (; i < parent_ref->m_children.size(); ++i) {
		if (parent_ref->m_children[i].first >= ch)
			break;
	}

	if (parent_ref->m_children.empty() || i == parent_ref->m_children.size() || parent_ref->m_children[i].first > ch) {
		parent_ref->m_children.insert(parent_ref->m_children.begin() + i, std::make_pair(ch, child));
		// DEBUG
		total_bytes += getElementBytes(child) + child->getArenaSize() + sizeof(std::pair<int16_t, void*>);
	} else {
		ASSERT(parent_ref->m_children[i].first == ch);
		// replace with the new child
		node* original = parent_ref->m_children[i].second;
		total_bytes -= (getElementBytes(original) + original->getArenaSize());
		parent_ref->m_children[i] = std::make_pair(ch, child); // replace with the new child
		total_bytes += getElementBytes(child) + child->getArenaSize();
	}
}

void radix_tree::delete_child(radix_tree::node* parent, radix_tree::node* child) {
	if (parent->m_is_fixed) {
		delete_child4(parent, child);
	} else {
		delete_child_vector(parent, child);
	}
}

void radix_tree::delete_child4(radix_tree::node* parent, radix_tree::node* child) {
	int16_t ch = child->getFirstByte();
	internalNode4* parent_ref = (internalNode4*)parent;
	int i = 0;

	for (; i < parent_ref->num_children; i++) {
		if (parent_ref->keys[i] == ch)
			break;
	}
	ASSERT(i != parent_ref->num_children);
	memmove(parent_ref->keys + i, parent_ref->keys + i + 1, (parent_ref->num_children - 1 - i) * sizeof(int16_t));
	memmove(
	    parent_ref->m_children + i, parent_ref->m_children + i + 1, (parent_ref->num_children - 1 - i) * sizeof(void*));
	parent_ref->num_children--;
	total_bytes -= (getElementBytes(child) + child->getArenaSize());
}

void radix_tree::delete_child_vector(radix_tree::node* parent, radix_tree::node* child) {
	int16_t ch = child->getFirstByte();
	internalNode* parent_ref = (internalNode*)parent;
	int i = 0;

	for (; i < parent_ref->m_children.size(); i++) {
		if (parent_ref->m_children[i].first == ch)
			break;
	}
	ASSERT(i != parent_ref->m_children.size());
	parent_ref->m_children.erase(parent_ref->m_children.begin() + i);
	total_bytes -= (getElementBytes(child) + child->getArenaSize() + sizeof(std::pair<int16_t, void*>));
	if (parent_ref->m_children.size() && parent_ref->m_children.size() <= parent_ref->m_children.capacity() / 4)
		parent_ref->m_children.shrink_to_fit();
}

int radix_tree::find_child(radix_tree::node* parent, int16_t ch) {
	int i = 0;
	if (parent->m_is_fixed) {
		internalNode4* parent_ref = (internalNode4*)parent;
		for (; i < parent_ref->num_children; ++i) {
			if (parent_ref->keys[i] == ch)
				return i;
		}
	} else {
		internalNode* parent_ref = (internalNode*)parent;
		for (; i != parent_ref->m_children.size(); ++i) {
			if (parent_ref->m_children[i].first == ch)
				return i;
		}
	}
	return i;
}

int radix_tree::child_size(radix_tree::node* parent) {
	if (parent->m_is_fixed) {
		return ((internalNode4*)parent)->num_children;
	} else {
		return ((internalNode*)parent)->m_children.size();
	}
}

radix_tree::node* radix_tree::get_child(node* parent, int index) {
	if (parent->m_is_fixed) {
		ASSERT(index < ((internalNode4*)parent)->num_children);
		return ((internalNode4*)parent)->m_children[index];
	} else {
		return ((internalNode*)parent)->m_children[index].second;
	}
}

radix_tree::node* radix_tree::iterator::increment(node* target) const {
	radix_tree::node* parent = target->m_parent;
	if (parent == nullptr)
		return nullptr;

	int index = find_child(parent, target->getFirstByte());
	ASSERT(index != child_size(parent));
	++index;

	if (index == child_size(parent))
		return increment(target->m_parent);
	else
		return descend<0>(get_child(parent, index));
}

radix_tree::node* radix_tree::iterator::decrement(radix_tree::node* target) const {
	radix_tree::node* parent = target->m_parent;
	if (parent == nullptr)
		return nullptr;

	int index = find_child(parent, target->getFirstByte());
	ASSERT(index != child_size(parent));

	if (index == 0)
		return decrement(target->m_parent);
	else {
		--index;
		return descend<1>(get_child(parent, index));
	}
}

bool radix_tree::iterator::operator!=(const radix_tree::iterator& lhs) const {
	return m_pointee != lhs.m_pointee;
}

bool radix_tree::iterator::operator==(const radix_tree::iterator& lhs) const {
	return m_pointee == lhs.m_pointee;
}

const radix_tree::iterator& radix_tree::iterator::operator++() {
	if (m_pointee != nullptr) // it is undefined behaviour to dereference iterator that is out of bounds...
		m_pointee = increment(m_pointee);
	return *this;
}

const radix_tree::iterator& radix_tree::iterator::operator--() {
	if (m_pointee != nullptr && m_pointee->m_is_leaf) {
		m_pointee = decrement(m_pointee);
	}
	return *this;
}

/*
 * reconstruct the key
 */
StringRef radix_tree::iterator::getKey(uint8_t* content) const {
	if (m_pointee == nullptr)
		return StringRef();

	ASSERT(m_pointee->m_is_leaf);
	//  memset(content, 0, len);

	auto node = m_pointee;
	uint32_t pos = m_pointee->m_depth;
	while (true) {
		if (node->getKeySize() > 0) {
			memcpy(content + pos, node->getKey().begin(), node->getKeySize());
		}
		node = node->m_parent;
		if (node == nullptr || pos <= 0)
			break;
		pos -= node->getKeySize();
	}
	return StringRef(content, (m_pointee->m_depth + m_pointee->getKeySize()));
}

radix_tree::iterator radix_tree::end() const {
	return iterator(nullptr);
}

radix_tree::iterator radix_tree::begin() {
	if (m_root == nullptr || m_size == 0)
		return iterator(nullptr);
	else {
		return descend<0>(m_root);
	}
}

/////////////////////// lookup //////////////////////////
radix_tree::iterator radix_tree::find(const StringRef& key) {
	if (m_root == nullptr)
		return iterator(nullptr);

	auto node = find_node(key, m_root, 0);
	StringRef key_sub = radix_substr(key, node->m_depth, (key.size() - node->m_depth));

	if (node->m_is_leaf && key_sub == node->getKey())
		return node;
	else
		return nullptr;
}

/*
 * corner case : insert "apache, append", then search for "appends". find_node() will return leaf node with m_key ==
 * "pend"; if search for "ap", find_node() will return internal node with m_key = ap
 */
radix_tree::node* radix_tree::find_node(const StringRef& key, node* node, int depth) {
	if (node->m_is_leaf)
		return node;

	int size = child_size(node);
	//	printf("try to find key %s on node %s [%d]\n", printable(key).c_str(), printable(node->getKey()).c_str(), size);
	for (int it = 0; it < size; ++it) {
		auto current = get_child(node, it);
		// for leaf node with empty key, exact match
		if (depth == key.size() && current->getKeySize() == 0) {
			ASSERT(current->m_is_leaf); // find the exact match
			return current;
		}
		// they have at least one byte in common
		if (depth < key.size() && key[depth] == current->getFirstByte()) {
			int len_node = current->getKeySize();
			StringRef key_sub = radix_substr(key, depth, len_node);

			if (key_sub == current->getKey()) {
				if (current->m_is_leaf)
					return current;
				else
					return find_node(key, current, depth + len_node);
			} else {
				// return the current match (which is the smallest match)
				// radix tree won't have siblings that share the same prefix
				return current;
			}
		}
	}

	return node;
}

/*
 * Returns the smallest node x such that *x>=key, or end()
 */
radix_tree::iterator radix_tree::lower_bound(const StringRef& key) {
	if (m_root == nullptr || m_size == 0)
		return iterator(nullptr);
	return lower_bound(key, m_root);
}

radix_tree::iterator radix_tree::lower_bound(const StringRef& key, node* node) {
	iterator result(nullptr);
	int size = child_size(node);

	for (int it = 0; it < size; ++it) {
		auto current = get_child(node, it);
		// short cut as find_node
		if (key.size() == current->m_depth && current->getKeySize() == 0) {
			return iterator(current);
		}

		StringRef key_sub = radix_substr(key, current->m_depth, current->getKeySize());
		StringRef node_data = current->getKey();

		if (key_sub == node_data) {
			if (current->m_is_leaf && ((key.size() - current->m_depth) == current->getKeySize()))
				return iterator(current); // exact match
			else if (!current->m_is_leaf)
				result = lower_bound(key, current);
		} else if (node_data > key_sub) {
			return descend<0>(current);
		}
		if (result != end())
			return result;
	}

	return result;
}

/*
 * Returns the smallest x such that *x>key, or end()
 */
radix_tree::iterator radix_tree::upper_bound(const StringRef& key) {
	if (m_root == nullptr || m_size == 0)
		return iterator(nullptr);
	return upper_bound(key, m_root);
}

radix_tree::iterator radix_tree::upper_bound(const StringRef& key, node* node) {
	if (node == nullptr || node->m_is_leaf)
		return iterator(node);

	iterator result(nullptr);
	int size = child_size(node);

	for (int it = 0; it < size; ++it) {
		auto current = get_child(node, it);
		StringRef key_sub = radix_substr(key, current->m_depth, current->getKeySize());
		StringRef node_data = current->getKey();

		if (!current->m_is_leaf && node_data == key_sub) {
			result = upper_bound(key, current);
		} else if (node_data > key_sub) {
			return descend<0>(current);
		}
		if (result != end())
			return result;
	}
	return result;
}

// Return the sum of getT(x) for begin()<=x<to
uint64_t radix_tree::sumTo(iterator to) const {
	if (to == end()) {
		return m_root ? total_bytes : 0;
	} else {
		throw std::invalid_argument("sumTo method only support end() input");
	}
}

radix_tree::iterator radix_tree::previous(radix_tree::iterator i) {
	if (i == end()) {
		// for iterator == end(), find the largest element
		return descend<1>(m_root);
	} else if (i == begin()) {
		return iterator(nullptr);
	} else {
		--i;
		return i;
	}
}

/////////////////////// modification //////////////////////////
/*
 * @param parent : direct parent of this newly inserted node
 * @param val : using val to create a newly inserted node
 */
radix_tree::node* radix_tree::append(node* parent, const StringRef& key, const StringRef& val) {
	int depth = parent->m_depth + parent->getKeySize();
	int len = key.size() - depth;

	radix_tree::node* node_c = (node*)new radix_tree::leafNode(val);
	node_c->m_depth = depth;
	node_c->m_parent = parent;

	if (len == 0) {
		// node_c->key is empty (len = 0);
		inline_keys++;
	} else {
		node_c->setKey(key, depth, len);
		// DEBUG
		if (len <= INLINE_KEY_SIZE)
			inline_keys++;
	}
	// printf("node_c key is %s value is %s %p\n", printable(node_c->getKey()).c_str(),
	// printable(((leafNode*)node_c)->getValue()).c_str(), node_c);
	add_child(parent, node_c);
	m_node++;
	return node_c;
}

/*
 * step one : find common substring of node->m_key and val(findnode() method has already guaranteed that they have
 * something in common) step two : split the existing node into two based on the common substring step three : append
 * newly inserted node to node_a
 *
 * @param node : split node
 * @param val : using val to create a newly inserted node
 */
radix_tree::node* radix_tree::prepend(node* split, const StringRef& key, const StringRef& val) {
	int len1 = split->getKeySize();
	int len2 = key.size() - split->m_depth;
	int count = 0;
	// deep copy original data using a temp_arena(becomes invalid once out)
	Arena temp_arena(split->getKeySize());
	StringRef original_data(temp_arena, split->getKey());

	for (; count < len1 && count < len2; count++) {
		if (!(original_data[count] == key[count + split->m_depth]))
			break;
	}
	ASSERT(count != 0);

	// create a new internal node
	node* node_a = (node*)new radix_tree::internalNode4();
	m_node++;

	node_a->m_parent = split->m_parent;
	node_a->setKey(original_data, 0, count);
	node_a->m_depth = split->m_depth;
	add_child(node_a->m_parent, node_a); // replace original node* with node_a*

	// DEBUG
	if (count <= INLINE_KEY_SIZE)
		inline_keys++;
	if (split->getKeySize() > INLINE_KEY_SIZE && (len1 - count) <= INLINE_KEY_SIZE)
		inline_keys++;

	// modify original internal node
	split->m_depth += count;
	split->m_parent = node_a;
	split->setKey(original_data, count, len1 - count);
	add_child(node_a, split);
	return append(node_a, key, val);
}

std::pair<radix_tree::iterator, bool> radix_tree::insert(const StringRef& key,
                                                         const StringRef& val,
                                                         bool replaceExisting) {
	if (m_root == nullptr) {
		m_root = (node*)new radix_tree::internalNode();
		total_bytes += getElementBytes(m_root);
	}

	auto node = find_node(key, m_root, 0);
	// short cut for root node
	if (node == m_root) {
		m_size++;
		return std::pair<iterator, bool>(append(m_root, key, val), true);
	}

	StringRef key_sub = radix_substr(key, node->m_depth, node->getKeySize());
	if (key_sub == node->getKey()) {
		if (node->m_is_leaf) {
			if (node->m_depth + node->getKeySize() == key.size()) {
				// case one : exact match, replace with new value
				bool inserted = false;
				if (replaceExisting) {
					size_type original_value = ((leafNode*)node)->getLeafArenaSize();
					((leafNode*)node)->setValue(val);
					total_bytes += ((leafNode*)node)->getLeafArenaSize() - original_value;
					inserted = true;
				}
				return std::pair<iterator, bool>(node, inserted);
			} else {
				// case two : prepend (e.g leaf is "a", inserted key is "ab");
				m_size++;
				return std::pair<iterator, bool>(prepend(node, key, val), true);
			}
		} else {
			m_size++;
			return std::pair<iterator, bool>(append(node, key, val), true);
		}
	} else {
		m_size++;
		return std::pair<iterator, bool>(prepend(node, key, val), true);
	}
}

void radix_tree::erase(iterator it) {
	erase(it.m_pointee);
}

bool radix_tree::erase(radix_tree::node* child) {
	if (m_root == nullptr)
		return false;
	ASSERT(child != nullptr);

	if (!child->m_is_leaf)
		return false;

	radix_tree::node* parent;

	parent = child->m_parent;
	delete_child(parent, child);
	// DEBUG
	if (child->getKeySize() <= INLINE_KEY_SIZE)
		inline_keys--;
	delete (leafNode*)child;
	m_size--;
	m_node--;

	// can't do the merge if parent is root node
	if (parent == m_root)
		return true;

	if (child_size(parent) > 1)
		return true;
	ASSERT(child_size(parent) == 1);

	// parent has only one child left, merge parent with the sibling
	node* brother = get_child(parent, 0);

	// DEBUG
	if (brother->getKeySize() <= INLINE_KEY_SIZE)
		inline_keys--;
	delete_child(parent, brother);

	Arena temp_arena;
	StringRef new_data = radix_join(parent->getKey(), brother->getKey(), temp_arena);
	brother->setKey(new_data, 0, new_data.size());
	brother->m_depth = parent->m_depth;
	brother->m_parent = parent->m_parent;
	// delete parent and replace with brother
	add_child(parent->m_parent, brother);
	// DEBUG
	if (brother->getKeySize() <= INLINE_KEY_SIZE)
		inline_keys++;
	if (parent->getKeySize() <= INLINE_KEY_SIZE)
		inline_keys--;

	parent->m_is_fixed ? delete (internalNode4*)parent : delete (internalNode*)parent;
	m_node--;

	return true;
}

// Erase the items in the indicated range.
void radix_tree::erase(radix_tree::iterator begin, radix_tree::iterator end) {
	std::vector<radix_tree::node*> node_set;
	for (auto it = begin; it != end; ++it) {
		node_set.push_back(it.m_pointee);
	}

	for (int i = 0; i < node_set.size(); ++i) {
		erase(node_set[i]);
	}
}

#endif
