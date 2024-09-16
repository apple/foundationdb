#encoding:BINARY

#
# directory_extension.rb
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

module DirectoryExtension
  class DirectoryTester
    @@ops_that_create_dirs = [
      'DIRECTORY_CREATE_SUBSPACE',
      'DIRECTORY_CREATE_LAYER',
      'DIRECTORY_CREATE_OR_OPEN',
      'DIRECTORY_CREATE',
      'DIRECTORY_OPEN',
      'DIRECTORY_MOVE',
      'DIRECTORY_MOVE_TO',
      'DIRECTORY_OPEN_SUBSPACE',
    ]

    def initialize
      @dir_list = [FDB.directory]
      @dir_index = 0
      @error_index = 0
    end

    def pop_tuples(inst, num=nil)
      actual_num = num
      if actual_num.nil?
        actual_num = 1
      end

      tuples = (0...actual_num).map do 
        (0...inst.wait_and_pop).map do 
          inst.wait_and_pop 
        end 
      end

      if num.nil?
        return tuples[0]
      end

      return tuples
    end

    def process_instruction(inst)
      begin
        #puts "#{inst.index} #{inst.op}"
        directory = @dir_list[@dir_index]
        if inst.op == 'DIRECTORY_CREATE_SUBSPACE'
          path = pop_tuples(inst)
          raw_prefix = inst.wait_and_pop
          @dir_list << FDB::Subspace.new(path, raw_prefix)
        elsif inst.op == 'DIRECTORY_CREATE_LAYER'
          index1 = inst.wait_and_pop
          index2 = inst.wait_and_pop
          allow_manual_prefixes = inst.wait_and_pop
          if @dir_list[index1].nil? or @dir_list[index2].nil?
            @dir_list << nil
          else
            @dir_list << FDB::DirectoryLayer.new(:node_subspace => @dir_list[index1], 
                                                 :content_subspace => @dir_list[index2], 
                                                 :allow_manual_prefixes => allow_manual_prefixes == 1)
          end
        elsif inst.op == 'DIRECTORY_CHANGE'
          @dir_index = inst.wait_and_pop
          if @dir_list[@dir_index].nil?
            @dir_index = @error_index
          end
        elsif inst.op == 'DIRECTORY_SET_ERROR_INDEX'
          @error_index = inst.wait_and_pop
        elsif inst.op == 'DIRECTORY_CREATE_OR_OPEN'
          @dir_list << directory.create_or_open(inst.tr, pop_tuples(inst), :layer=>inst.wait_and_pop || '')
        elsif inst.op == 'DIRECTORY_CREATE'
          @dir_list << directory.create(inst.tr, pop_tuples(inst), :layer=>inst.wait_and_pop || '', :prefix=>inst.wait_and_pop)
        elsif inst.op == 'DIRECTORY_OPEN'
          @dir_list << directory.open(inst.tr, pop_tuples(inst), :layer=>inst.wait_and_pop || '')
        elsif inst.op == 'DIRECTORY_MOVE'
          @dir_list << directory.move(inst.tr, pop_tuples(inst), pop_tuples(inst))
        elsif inst.op == 'DIRECTORY_MOVE_TO'
          @dir_list << directory.move_to(inst.tr, pop_tuples(inst))
        elsif inst.op == 'DIRECTORY_REMOVE'
          count = inst.wait_and_pop
          if count == 0
            directory.remove(inst.tr)
          else
            directory.remove(inst.tr, pop_tuples(inst))
          end
        elsif inst.op == 'DIRECTORY_REMOVE_IF_EXISTS'
          count = inst.wait_and_pop
          if count == 0
            directory.remove_if_exists(inst.tr)
          else
            directory.remove_if_exists(inst.tr, pop_tuples(inst))
          end
        elsif inst.op == 'DIRECTORY_LIST'
          count = inst.wait_and_pop
          results = 
            if count == 0
              directory.list(inst.tr)
            else
              directory.list(inst.tr, pop_tuples(inst))
            end

          inst.push(FDB::Tuple.pack(results))
        elsif inst.op == 'DIRECTORY_EXISTS'
          count = inst.wait_and_pop
          result = 
            if count == 0
              directory.exists?(inst.tr)
            else
              directory.exists?(inst.tr, pop_tuples(inst))
            end

          if result
            inst.push(1)
          else
            inst.push(0)
          end
        elsif inst.op == 'DIRECTORY_PACK_KEY'
          inst.push(directory.pack(pop_tuples(inst)))
        elsif inst.op == 'DIRECTORY_UNPACK_KEY'
          tup = directory.unpack(inst.wait_and_pop)
          tup.each do |t| inst.push(t) end
        elsif inst.op == 'DIRECTORY_RANGE'
          rng = directory.range(pop_tuples(inst))
          inst.push(rng[0])
          inst.push(rng[1])
        elsif inst.op == 'DIRECTORY_CONTAINS'
          if directory.contains?(inst.wait_and_pop)
            inst.push(1)
          else
            inst.push(0)
          end
        elsif inst.op == 'DIRECTORY_OPEN_SUBSPACE'
          @dir_list << directory.subspace(pop_tuples(inst))
        elsif inst.op == 'DIRECTORY_LOG_SUBSPACE'
          inst.tr[inst.wait_and_pop + FDB::Tuple.pack([@dir_index])] = directory.key
        elsif inst.op == 'DIRECTORY_LOG_DIRECTORY'
          exists = directory.exists?(inst.tr)
          children = exists ? directory.list(inst.tr) : []
          log_subspace = FDB::Subspace.new([@dir_index], inst.wait_and_pop)
          inst.tr[log_subspace['path'.encode('utf-8')]] = FDB::Tuple.pack(directory.path)
          inst.tr[log_subspace['layer'.encode('utf-8')]] = FDB::Tuple.pack([directory.layer])
          inst.tr[log_subspace['exists'.encode('utf-8')]] = FDB::Tuple.pack([exists ? 1 : 0])
          inst.tr[log_subspace['children'.encode('utf-8')]] = FDB::Tuple.pack(children)
        elsif inst.op == 'DIRECTORY_STRIP_PREFIX'
          str = inst.wait_and_pop
          throw "String #{str} does not start with raw prefix #{directory.key}" if !str.start_with?(directory.key)
          inst.push(str[directory.key.length..-1])
        else
          raise "Unknown op #{inst.op}"
        end
      rescue Exception => e
        #puts "#{e}"
        #puts e.backtrace
        #raise
        if @@ops_that_create_dirs.include?(inst.op)
          @dir_list << nil
        end

        inst.push('DIRECTORY_ERROR')
      end
    end
  end
end

