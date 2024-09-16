#
# micro_table.rb
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

@table = FDB::Subspace.new(["T"])
@row_index = @table['R']
@col_index = @table['C']

def _pack(value)
  FDB::Tuple.pack([value])
end

def _unpack(value)
  FDB::Tuple.unpack(value)[0]
end

def table_set_cell(db,row,col,value)
  db.transact do |tr|
    tr[@row_index[row][col]] = _pack(value)
    tr[@col_index[col][row]] = _pack(value)
  end
end

def table_get_cell(db,row,col)
  db.transact do |tr|
    _unpack(tr[@row_index[row][col]])
  end
end

def table_set_row(db,row,cols)
  db.transact do |tr|
    tr.clear_range_start_with(@table[row])
    cols.each do |c,v|
      table_set_cell(tr,row,c,v)
    end
  end
end

def table_get_row(db,row)
  cols = {}
  db.transact do |tr|
    tr.get_range(*@row_index[row].range()) do |kv|
      c = @row_index.unpack(kv.key)
      cols[c] = _unpack(kv.value)
    end
  end
  cols
end

def table_get_col(db,col)
  rows = {}
  db.transact do |tr|
    tr.get_range(*@col_index[col].range()) do |kv|
      r = @col_index.unpack(kv.value)
      rows[r] = _unpack(kv.value)
    end
  end
  rows
end

def clear_subspace(db,subspace)
  db.transact do |tr|
    tr.clear_range_start_with(subspace.key)
  end
end

clear_subspace(@db,@table)
table_set_cell(@db, 0, 0, "a")
table_set_cell(@db, 0, 1, "b")
puts table_get_row(@db, 0)
puts table_get_cell(@db, 0, 0)
puts table_get_cell(@db, 0, 1)
table_set_row(@db, 1, { 0 => "c", 1 => "d" })
puts table_get_row(@db, 1)