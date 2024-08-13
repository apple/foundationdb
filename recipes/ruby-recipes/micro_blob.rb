#
# micro_blob.rb
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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
@db = FDB.open()

def clear_subspace(db,subspace)
  db.transact do |tr|
    tr.clear_range_start_with(subspace.key)
  end
end

CHUNK_SIZE = 5
@blob = FDB::Subspace.new(['B'])

def write_blob(db,data)
  return if data.length == 0
  db.transact do |tr|
    chunks = (0...data.length).step(CHUNK_SIZE).map { |x| [x, x + CHUNK_SIZE] }
    chunks.each do |chunk|
      start, stop = *chunk
      tr[@blob[start]] = data[start...stop]
    end
  end
end

def read_blob(db)
  db.transact do |tr|
    value = ''
    tr.get_range( *@blob.range() ) do |kv|
      puts kv.value
      value += kv.value
    end
    value
  end
end

clear_subspace(@db,@blob)
write_blob(@db,'ABCDEFGHIJKLMNOPQRSTUVWXYZ')
puts read_blob(@db)