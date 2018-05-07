#
# micro_priority.rb
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

@pq = FDB::Subspace.new(["P"])
def clear_subspace(db,subspace)
  db.transact do |tr|
    tr.clear_range_start_with(subspace.key)
  end
end

def push(db,value, priority)
  db.transact do |tr|
    tr[@pq[priority][next_count(tr,priority)][SecureRandom.hex(20)]] = value
  end
end

def next_count(db,priority)
  value = 0
  db.transact do |tr|
    tr.snapshot.get_range(*@pq[priority].range(), :limit => 1, :reverse => true) do |kv|
      value = @pq[priority].unpack(kv.key)[0] + 1
    end
  end
  value
end

def pop(db,max=false)
  db.transact do |tr|
    tr.get_range(*@pq.range(), :limit => 1, :reverse => max) do |kv|
      tr.clear(kv.key)
      puts kv.value
    end
  end
end

def peek(db,max=false)
  value = nil
  db.transact do |tr|
    tr.get_range(*@pq.range(), :limit => 1, :reverse => max) do |kv|
      value = kv.value
    end
  end
  value
end

def smoke_test()
  clear_subspace(@db,@pq)
  puts "peek none: " + peek(@db).to_s
  push(@db,'a',1)
  push(@db,'b',5)
  push(@db,'c',2)
  push(@db,'d',4)
  push(@db,'e',3)
  puts "peak in min order"
  puts peek(@db)
  puts peek(@db)
  puts "pop in min order"
  pop(@db)
  pop(@db)
  pop(@db)
  pop(@db)
  pop(@db)
  puts "peek none: "
  peek(@db,max=true)
  push(@db,'a1',1)
  push(@db,'a2',1)
  push(@db,'a3',1)
  push(@db,'a4',1)
  push(@db,'b',5)
  push(@db,'c',2)
  push(@db,'d',4)
  push(@db,'e',3)
  puts "peek in max  "
  puts peek(@db,max=true)
  puts peek(@db,max=true)
  puts "pop in max  "
  pop(@db,max=true)
  pop(@db,max=true)
  pop(@db,max=true)
  pop(@db,max=true)
  pop(@db,max=true)
  pop(@db,max=true)
end
smoke_test()
