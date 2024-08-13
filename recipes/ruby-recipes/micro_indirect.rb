#
# micro_indirect.rb
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

class Workspace
  def initialize(directory,db)
    @dir = directory
    @db = db
  end

  def enter()
    @dir.create_or_open(@db,["new".encode('utf-8')])
  end

  def exit()
    update(@db)
  end

  def update(db)
    db.transact do |tr|
      @dir.remove(tr,["current".encode('utf-8')])
      @dir.move(tr,["new".encode('utf-8')],["current".encode('utf-8')])
    end
  end

  def current()
    @dir.create_or_open(@db,["current".encode('utf-8')])
  end
end

def clear_subspace(db,subspace)
  db.transact do |tr|
    tr.clear_range_start_with(subspace.key)
  end
end

def print_subspace(db,subspace)
  db.transact do |tr|
    tr.get_range(*subspace.range()) do |kv|
      print subspace.unpack(kv.key)
      puts kv.value
    end
  end
end

def smoke_test()
  db = FDB.open()
  working_dir = FDB::directory.create_or_open(db,["working".encode('utf-8')])
  workspace = Workspace.new(working_dir,db)
  current = workspace.current()
  clear_subspace(db,current)
  db[current[1]] = 'a'
  db[current[2]] = 'b'
  puts "contents:"
  print_subspace(db,current)
  newspace = workspace.enter()
  clear_subspace(db,newspace)
  db[newspace[3]] = 'c'
  db[newspace[4]] = 'd'
  workspace.exit()
  puts "contents"
  print_subspace(db,workspace.current())
end
smoke_test()
