#
# micro_queue.rb
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
require 'securerandom'

FDB.api_version 300
@db = FDB.open

def clear_subspace(db,subspace)
  db.transact do |tr|
    tr.clear_range_start_with(subspace.key)
  end
end

@queue = FDB::Subspace.new(['Q'])
clear_subspace(@db,@queue)

def dequeue(db)
  db.transact do |tr|
    item = first_item(tr)
    next nil if item.nil?
    tr.clear(item.key)
    item.value
  end
end

def enqueue(db,value)
  db.transact do |tr|
    tr[@queue[last_index(tr) + 1][SecureRandom.hex(20)]] = value
  end
end

def last_index(db)
  value = 0
  db.transact do |tr|
    tr.snapshot.get_range(*@queue.range(), :limit => 1, :reverse => true) do |kv|
      value = @queue.unpack(kv.key)[0]
    end
  end
  value
end

def first_item(db)
  pair = nil
  db.transact do |tr|
    tr.get_range(*@queue.range(), :limit=>1) do |kv|
      pair = kv
    end
  end
  pair
end

enqueue(@db, "a")
enqueue(@db, "b")
enqueue(@db, "c")
enqueue(@db, "d")
enqueue(@db, "e")
puts dequeue(@db)
puts dequeue(@db)
puts dequeue(@db)
puts dequeue(@db)
puts dequeue(@db)
puts dequeue(@db)