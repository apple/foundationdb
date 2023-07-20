#
# micro_doc.rb
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

require 'fdb'
require 'ostruct'
require 'json'

FDB.api_version 300
@db = FDB.open()

@doc_space = FDB::Subspace.new(['D'])
EMPTY_OBJECT = -2
EMPTY_ARRAY = -1

def to_tuples(item)
  if item.is_a?(Hash)
    return [[EMPTY_OBJECT,nil]] if item.empty?
    final = []
    item.each do |k,v|
      to_tuples(v).each do |sub|
        tup = [*k,*sub]
        final.push(tup)
      end
    end
    return final

  elsif item.is_a?(Array)
    return [[EMPTY_ARRAY,nil]] if item.empty?
    final = []
    item.each_with_index do |k,v|
      to_tuples(v).each do |sub|
        tup = [*sub,*k]
        final.push(tup)
      end
    end
    return final

  else
    return [[item]]

  end
end

def from_tuples(tuples)
  return {} if tuples.nil?
  first = tuples.first # Determine kind of object from first tuple
  return first.first if first.size == 1 # Primitive value
  return {} if first == [EMPTY_OBJECT,nil]
  return [] if first == [EMPTY_ARRAY,nil]
  # For an object or array, we need to group the tuples by their first element
  groups = tuples.group_by { |t| t.first }.map { |k,g| g }
  if first.first == 0 # array
    return groups.map { |g| from_tuples(g.map { |t| t[1..-1] }) }
  else # object
    obj = {}
    groups.each do |g|
      obj[g[0][0]] = from_tuples(g.map { |t| t[1..-1] })
    end
    return obj
  end
end

def insert_doc(db,doc)
  db.transact do |tr|
    if doc.is_a?(String)
      doc = json.loads(doc)
    end
    if !doc.include?('doc_id')
      new_id = get_new_id(tr)
      doc['doc_id'] = new_id
    end
    to_tuples(doc).each do |tup|
      tr[@doc_space.pack([doc['doc_id']] + tup[0..-2])] = FDB::Tuple.pack([tup[-1]])
    end
  end
  doc['doc_id']
end

def get_new_id(db)
  db.transact do |tr|
    found = false
    while !found
      new_id = rand(100000000)+1
      found = true
      tr.get_range(*@doc_space[new_id].range) do |k|
        found = false
        break
      end
    end
    new_id
  end
end

def get_doc(db,doc_id,prefix=[])
  db.transact do |tr|
    v = tr[@doc_space.pack([doc_id] + prefix)]
    if !v.nil?
      # next is safer than return because it lets transact finish
      next from_tuples([prefix + FDB::Tuple.unpack(v)])
    end
    tuples = tr.get_range(*@doc_space.range([doc_id]+prefix)).map do |kv|
      @doc_space.unpack(kv.key)[1..-1] + FDB::Tuple.unpack(kv.value)
    end
    from_tuples tuples
  end
end

def clear_subspace(db,subspace)
  db.transact do |tr|
    tr.clear_range_start_with(subspace.key)
  end
end

clear_subspace(@db,@doc_space)
h1 = {'user' => {'jones' => {'friend_of' => 'smith', 'group' => ['sales','service']}, 'smith' => {'friend_of' => 'jones','group'=>['dev','research']}}}
id = insert_doc(@db,h1)

puts get_doc(@db,id,['user','smith','group'])