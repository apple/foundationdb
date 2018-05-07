#
# micro_graph.rb
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
FDB.api_version 300
@db = FDB.open

@graph = FDB::Subspace.new(['G'])
@edge = @graph['E']
@inverse = @graph['I']

def set_edge(db,node,neighbor)
  db.transact do |tr|
    tr[@edge[node][neighbor]] = ''
    tr[@inverse[neighbor][node]] = ''
  end
end

def del_edge(db,node,neighbor)
  db.transact do |tr|
    tr.clear(@edge[node][neighbor])
    tr.clear(@inverse[neighbor][node])
  end
end

def get_out_neighbors(db,node)
  db.transact do |tr|
    tr.get_range(*@edge[node].range()).map { |kv| @edge.unpack(kv.key)[1] }
  end
end

def get_in_neighbors(db,node)
  db.transact do |tr|
    tr.get_range(*@inverse[node].range()).map { |kv| @inverse.unpack(kv.key)[1] }
  end
end

def clear_subspace(db,subspace)
  db.transact do |tr|
    tr.clear_range_start_with(subspace.key)
  end
end

clear_subspace(@db,@graph)

set_edge(@db, "a", "b")
set_edge(@db, "a", "c")
set_edge(@db, "a", "d")
set_edge(@db, "b", "d")
puts "a's out neighbors:"
puts get_out_neighbors(@db, "a")
puts "d's in neighbors:"
puts get_in_neighbors(@db, "d")
puts "b's out neighbors:"
puts get_out_neighbors(@db, "b")
del_edge(@db, "a", "d")
puts "a's out neighbors after removing a->d:"
puts get_out_neighbors(@db, "a")