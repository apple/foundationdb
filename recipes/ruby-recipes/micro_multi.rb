#
# micro_multi.rb
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
@db = FDB.open

def clear_subspace(db,subspace)
  db.transact do |tr|
    tr.clear_range_start_with(subspace.key)
  end
end

@multi = FDB::Subspace.new(['M'])
clear_subspace(@db,@multi)

def multi_add(db,index,value)
  db.transact do |tr|
    tr.add(@multi[index][value],[1].pack('<q'))
  end
end

def multi_sub(db,index,value)
  db.transact do |tr|
    v = tr[@multi[index][value]]
    if(!v.nil? and v.unpack('<q')[0] > 1)
      tr.add(@multi[index][value],[-1].pack('<q'))
    else
      tr.clear(v)
    end
  end
end

def multi_get(db,index)
  db.transact do |tr|
    tr.get_range(*@multi[index].range()).map do |kv|
      @multi.unpack(kv.key)
    end
  end
end

def multi_get_counts(db,index)
  db.transact do |tr|
    tr.get_range(*@multi[index].range).map do |kv|
      [(@multi.unpack(kv.key)[1]..v.unpack('<q')[0])]
    end
  end
end

def multi_is_element(db,index,value)
  db.transact do |tr|
    !tr[@multi[index][value]].nil?
  end
end

TIMES = 100
def time_atomic_add(db)
  db.transact do |tr|
    TIMES.times do
      multi_add(@db,'foo','bar')
    end
  end
end

def time_atomic_subtract(db)
  db.transact do |tr|
    start =  Time.now
    TIMES.times do
      multi_sub(tr,'foo','bar')
    end
    done = Time.now
    puts "#{(done - start)} seconds for atomic subtract"
  end
end

time_atomic_add(@db)
time_atomic_subtract(@db)