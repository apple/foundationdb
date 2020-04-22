#!/usr/bin/env ruby
#encoding:BINARY

#
# tester.rb
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

require 'thread'

$:.unshift( File.join( File.dirname(__FILE__), "../lib" ) )

require 'fdb'
if FDB.is_api_version_selected?()
  raise 'FDB API version already selected'
end
FDB.api_version(ARGV[1].to_i)
if FDB.get_api_version() != ARGV[1].to_i
  raise 'FDB API version did not match'
end

require_relative 'directory_extension'

if ARGV.length == 2
  db = FDB.open
else
  db = FDB.open( ARGV[2] )
end

class Stack
  def initialize
    @stack = []
  end
  attr_reader :stack

  def length
    @stack.length
  end

  def [](idx)
    @stack[idx]
  end

  def []=(idx, val)
    @stack[idx] = val
  end

  def last
    @stack.last
  end

  def push(entry)
    @stack.push(entry)
  end

  def wait_and_pop(with_idx=false)
    p = @stack.pop

    begin
      # This works because the only futures we push currently are
      # strings and nils. If we do other types, we will need to add
      # them here or implement something more generic
      if p[0].instance_of? String
        p[0] = p[0].to_s
      elsif p[0].instance_of? FDB::FutureNil
        p[0].wait
        p[0] = "RESULT_NOT_PRESENT"
      end

    rescue FDB::Error => e
      p[0] = FDB::Tuple.pack(["ERROR", e.code.to_s])
    end

    if with_idx
      p
    else
      p[0]
    end
  end
end

class Instruction
  def initialize(tr, stack, op, index, is_database=false, is_snapshot=false)
    @tr = tr
    @stack = stack
    @op = op
    @index = index
    @is_database = is_database
    @is_snapshot = is_snapshot
  end

  attr_reader :tr
  attr_reader :stack
  attr_reader :op
  attr_reader :index
  attr_reader :is_database
  attr_reader :is_snapshot

  def push(val)
    @stack.push([val, @index])
  end

  def wait_and_pop(with_idx=false)
    @stack.wait_and_pop(with_idx)
  end
end

class Tester
  class << self
    attr_accessor :tr_map
    attr_reader :tr_map_lock
  end

  @tr_map = {}
  @tr_map_lock = Monitor.new

  def initialize(db, prefix)
    @db = db
    @prefix = prefix

    @instructions = @db.get_range(*(FDB::Tuple.range([prefix])))

    @stack = Stack.new
    @tr_name = prefix
    @last_version = nil

    @threads = []
    @directory_extension = DirectoryExtension::DirectoryTester.new
  end

  def push_range(r, inst, prefix_filter=nil)
    inst.push(FDB::Tuple.pack(
      r.select do |kv| prefix_filter.nil? or kv.key.start_with? prefix_filter end
      .flat_map do |kv| [kv.key, kv.value] end
    ))
  end

  def check_watches(watches, expected)
      watches.each_with_index do |w,i|
        if w.ready? or expected
          begin
            w.wait
            raise "Watch #{i} unexpectedly triggered" if not expected
          rescue FDB::Error => e
            tr = @db.create_transaction
            tr.on_error(e).wait
            return false
          end
        end
      end

      true
  end

  def test_watches
    while true do
      @db['w0'] = '0'
      @db['w3'] = '3'

      watches = Array.new(4)
      watches[0] = @db.transact do |tr|
        w = tr.watch('w0')
        raise "Watch unexpectedly triggered" if w.ready?
        tr['w0'] = '0'
        w
      end

      watches[1] = @db.clear_and_watch('w1')
      watches[2] = @db.set_and_watch('w2', '2')
      watches[3] = @db.get_and_watch('w3')

      raise "get_and_watch has wrong value" if watches[3][0] != '3'
      watches[3] = watches[3][1]

      sleep 1

      if not check_watches(watches, false)
        next
      end

      @db.clear 'w1'

      sleep 5

      if not check_watches(watches, false)
        next
      end

      @db['w0'] = 'a'
      @db['w1'] = 'b'
      @db.clear 'w2'
      @db.xor('w3', "\xff\xff")

      if check_watches(watches, true)
        return
      end
    end
  end

  def test_locality
    @db.transact do |tr|
      tr.options.set_timeout(60*1000)
      tr.options.set_read_system_keys
      boundary_keys = FDB::Locality.get_boundary_keys(tr, "", "\xFF\xFF").to_a
      for i in 0..boundary_keys.length-2
        bkey = boundary_keys[i]
        ekey = tr.get_key FDB::KeySelector.last_less_than(boundary_keys[i+1])
        bkey_addrs = FDB::Locality.get_addresses_for_key(tr, bkey)
        ekey_addrs = FDB::Locality.get_addresses_for_key(tr, ekey)
        if bkey_addrs.sort != ekey_addrs.sort
          raise "Locality not internally consistent"
        end
      end
    end
  end

  def current_transaction()
    Tester.tr_map_lock.synchronize {
      Tester.tr_map[@tr_name]
    }
  end

  def new_transaction()
    Tester.tr_map_lock.synchronize {
      Tester.tr_map[@tr_name] = @db.create_transaction()
    }
  end

  def switch_transaction(name)
    @tr_name = name
    Tester.tr_map_lock.synchronize {
      if !Tester.tr_map.has_key?(@tr_name)
        new_transaction
      end
    }
  end

  def log_stack(entries, prefix)
    @db.transact do |tr|
      for i, (el, idx) in entries
        pk = prefix + FDB::Tuple.pack([i, idx])
        pv = FDB::Tuple.pack([el])

        tr.set(pk, pv.byteslice(0, 40000))
      end
    end
  end

  def run
    @instructions.each_with_index do |inst_str, index|
      inst_tuple = FDB::Tuple.unpack inst_str.value
      op = inst_tuple[0]

      # puts "#{@prefix} Instruction is #{inst_tuple}"
      # puts "#{@prefix} Stack from #{@stack.stack.to_s}"

      is_database = op.end_with?("_DATABASE")
      is_snapshot = op.end_with?("_SNAPSHOT")

      if is_database
        op = op.slice( 0, op.length - 9 )
        obj = @db
      elsif is_snapshot
        op = op.slice( 0, op.length - 9 )
        obj = current_transaction.snapshot
      else
        obj = current_transaction
      end

      inst = Instruction.new(obj, @stack, op, index, is_database, is_snapshot)

      begin
        case op
        when "PUSH"
          inst.push(inst_tuple[1])
        when "DUP"
          inst.stack.push inst.stack.last
        when "EMPTY_STACK"
          @stack = Stack.new
        when "SWAP"
          idx = inst.wait_and_pop
          inst.stack[@stack.length-1], inst.stack[inst.stack.length-idx-1] = inst.stack[inst.stack.length-idx-1], inst.stack[inst.stack.length-1]
        when "POP"
          inst.wait_and_pop
        when "SUB"
          inst.push(inst.wait_and_pop - inst.wait_and_pop)
        when "CONCAT"
          inst.push(inst.wait_and_pop + inst.wait_and_pop)
        when "WAIT_FUTURE"
          inst.stack.push(inst.wait_and_pop(true))
          # @stack.push [wait_and_pop, index]
        when "NEW_TRANSACTION"
          new_transaction
        when "USE_TRANSACTION"
          switch_transaction(inst.wait_and_pop)
        when "ON_ERROR"
          inst.push(inst.tr.on_error( FDB::Error.new(inst.wait_and_pop) ))
        when "GET"
          res = obj[inst.wait_and_pop]
          if res.nil?
            inst.push("RESULT_NOT_PRESENT")
          else
            inst.push(res)
          end
        when "GET_ESTIMATED_RANGE_SIZE"
          inst.tr.get_estimated_range_size_bytes(inst.wait_and_pop, inst.wait_and_pop).to_i
          inst.push("GOT_ESTIMATED_RANGE_SIZE")
        when "GET_KEY"
          selector = FDB::KeySelector.new(inst.wait_and_pop, inst.wait_and_pop, inst.wait_and_pop)
          prefix = inst.wait_and_pop
          result = obj.get_key(selector)
          if result.start_with? prefix
            inst.push result
          elsif result < prefix
            inst.push prefix
          else
            inst.push FDB.strinc(prefix)
          end
        when "GET_RANGE"
          push_range(obj.get_range( inst.wait_and_pop, inst.wait_and_pop, { :limit => inst.wait_and_pop, :reverse => !inst.wait_and_pop.zero?, :streaming_mode => inst.wait_and_pop } ), inst)
        when "GET_RANGE_STARTS_WITH"
          push_range(obj.get_range_start_with( inst.wait_and_pop, { :limit => inst.wait_and_pop, :reverse => !inst.wait_and_pop.zero?, :streaming_mode => inst.wait_and_pop } ), inst)
        when "GET_RANGE_SELECTOR"
          beginKey = FDB::KeySelector.new(inst.wait_and_pop, inst.wait_and_pop, inst.wait_and_pop)
          endKey = FDB::KeySelector.new(inst.wait_and_pop, inst.wait_and_pop, inst.wait_and_pop)
          params = { :limit => inst.wait_and_pop, :reverse => !inst.wait_and_pop.zero?, :streaming_mode => inst.wait_and_pop }
          prefix = inst.wait_and_pop
          push_range(obj.get_range(beginKey, endKey, params), inst, prefix)
        when "GET_READ_VERSION"
          @last_version = inst.tr.get_read_version.to_i
          inst.push("GOT_READ_VERSION")
        when "SET"
          obj[inst.wait_and_pop] = inst.wait_and_pop
          inst.push("RESULT_NOT_PRESENT") if obj == @db
        when "ATOMIC_OP"
          obj.send(inst.wait_and_pop.downcase, inst.wait_and_pop, inst.wait_and_pop)
          inst.push("RESULT_NOT_PRESENT") if obj == @db
        when "SET_READ_VERSION"
          inst.tr.set_read_version @last_version
        when "CLEAR"
          obj.clear(inst.wait_and_pop)
          inst.push("RESULT_NOT_PRESENT") if obj == @db
        when "CLEAR_RANGE"
          obj.clear_range( inst.wait_and_pop, inst.wait_and_pop )
          inst.push("RESULT_NOT_PRESENT") if obj == @db
        when "CLEAR_RANGE_STARTS_WITH"
          obj.clear_range_start_with( inst.wait_and_pop )
          inst.push("RESULT_NOT_PRESENT") if obj == @db
        when "READ_CONFLICT_RANGE"
          inst.tr.add_read_conflict_range( inst.wait_and_pop, inst.wait_and_pop )
          inst.push("SET_CONFLICT_RANGE")
        when "WRITE_CONFLICT_RANGE"
          inst.tr.add_write_conflict_range( inst.wait_and_pop, inst.wait_and_pop )
          inst.push("SET_CONFLICT_RANGE")
        when "READ_CONFLICT_KEY"
          inst.tr.add_read_conflict_key( inst.wait_and_pop )
          inst.push("SET_CONFLICT_KEY")
        when "WRITE_CONFLICT_KEY"
          inst.tr.add_write_conflict_key( inst.wait_and_pop )
          inst.push("SET_CONFLICT_KEY")
        when "DISABLE_WRITE_CONFLICT"
          inst.tr.options.set_next_write_no_write_conflict_range
        when "COMMIT"
          inst.push(inst.tr.commit)
        when "RESET"
          inst.tr.reset
        when "CANCEL"
          inst.tr.cancel
        when "GET_COMMITTED_VERSION"
          @last_version = inst.tr.get_committed_version
          inst.push("GOT_COMMITTED_VERSION")
        when "GET_APPROXIMATE_SIZE"
          size = inst.tr.get_approximate_size.to_i
          inst.push("GOT_APPROXIMATE_SIZE")
        when "GET_VERSIONSTAMP"
          inst.push(inst.tr.get_versionstamp)
        when "TUPLE_PACK"
          arr = []
          inst.wait_and_pop.times do |i|
            arr.push(inst.wait_and_pop)
          end
          inst.push(FDB::Tuple.pack(arr))
        when "TUPLE_UNPACK"
          FDB::Tuple.unpack( inst.wait_and_pop ).each do |i| inst.push(FDB::Tuple.pack([i])) end
        when "TUPLE_RANGE"
          arr = []
          inst.wait_and_pop.times do |i|
            arr.push(inst.wait_and_pop)
          end
          (FDB::Tuple.range arr).each do |x|
            inst.push(x)
          end
        when "TUPLE_SORT"
            arr = []
            inst.wait_and_pop.times do |i|
                arr.push(FDB::Tuple.unpack inst.wait_and_pop)
            end
            arr.sort! { |t1, t2| FDB::Tuple.compare(t1, t2) }
            arr.each do |x|
                inst.push( FDB::Tuple.pack x)
            end
        when "ENCODE_FLOAT"
            bytes = inst.wait_and_pop
            inst.push(FDB::Tuple::SingleFloat.new(bytes.unpack("g")[0]))
        when "ENCODE_DOUBLE"
            bytes = inst.wait_and_pop
            inst.push(bytes.unpack("G")[0])
        when "DECODE_FLOAT"
            f_val = inst.wait_and_pop
            inst.push([f_val.value].pack("g"))
        when "DECODE_DOUBLE"
            d_val = inst.wait_and_pop
            inst.push([d_val].pack("G"))
        when "START_THREAD"
          t = Tester.new( @db, inst.wait_and_pop )
          thr = Thread.new do
            t.run
          end
          @threads.push thr
        when "WAIT_EMPTY"
          prefix = inst.wait_and_pop
          @db.transact do |tr|
            raise FDB::Error.new(1020) if tr.get_range_start_with(prefix).any?
          end
          inst.push("WAITED_FOR_EMPTY")
        when "UNIT_TESTS"
          api_version = FDB::get_api_version()
          begin
            FDB::api_version(api_version + 1)
            raise "Was not stopped from selecting two API versions"
          rescue RuntimeError => e
            if e.message != "FDB API already loaded at version #{api_version}."
                raise
            end
          end
          begin
            FDB::api_version(api_version - 1)
            raise "Was not stopped from selecting two API versions"
          rescue RuntimeError => e
            if e.message != "FDB API already loaded at version #{api_version}."
                raise
            end
          end
          FDB::api_version(api_version)
          begin
            @db.options.set_location_cache_size(100001)
            @db.options.set_max_watches(10001)
            @db.options.set_datacenter_id("dc_id")
            @db.options.set_machine_id("machine_id")
            @db.options.set_snapshot_ryw_enable()
            @db.options.set_snapshot_ryw_disable()
            @db.options.set_transaction_logging_max_field_length(1000)
            @db.options.set_transaction_timeout(100000)
            @db.options.set_transaction_timeout(0)
            @db.options.set_transaction_max_retry_delay(100)
            @db.options.set_transaction_size_limit(100000)
            @db.options.set_transaction_retry_limit(10)
            @db.options.set_transaction_retry_limit(-1)
            @db.options.set_transaction_causal_read_risky()
            @db.options.set_transaction_include_port_in_address()

            @db.transact do |tr|
              tr.options.set_priority_system_immediate
              tr.options.set_priority_batch
              tr.options.set_causal_read_risky
              tr.options.set_causal_write_risky
              tr.options.set_read_your_writes_disable
              tr.options.set_read_system_keys
              tr.options.set_access_system_keys
              tr.options.set_transaction_logging_max_field_length(1000)
              tr.options.set_timeout(60*1000)
              tr.options.set_retry_limit(50)
              tr.options.set_max_retry_delay(100)
              tr.options.set_used_during_commit_protection_disable
              tr.options.set_debug_transaction_identifier('my_transaction')
              tr.options.set_log_transaction()
              tr.options.set_read_lock_aware()
              tr.options.set_lock_aware()
              tr.options.set_include_port_in_address()

              tr.get("\xff").to_s
            end

            test_watches
            test_locality
          rescue FDB::Error => e
            raise "Unit tests failed: #{e.description.to_s}"
          end
        when "LOG_STACK"
          prefix = inst.wait_and_pop
          entries = {}
          while inst.stack.length > 0 do
            entries[inst.stack.length-1] = inst.wait_and_pop(true)
            if entries.length == 100
               log_stack(entries, prefix)
         entries = {}
            end
          end

          log_stack(entries, prefix)
        else
          if op.start_with?('DIRECTORY_')
            @directory_extension.process_instruction(inst)
          else
            raise "Unknown op #{op}"
          end
        end

      rescue FDB::Error => e
        inst.push(FDB::Tuple.pack(["ERROR", e.code.to_s]))
      end

      # puts "#{@prefix}         to #{@stack}"
      # puts ""
    end

    @threads.each do |thr| thr.join end
  end
end

t = Tester.new(db, ARGV[0].dup.force_encoding("BINARY"))
t.run
