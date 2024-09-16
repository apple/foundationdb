#encoding: BINARY

#
# fdbimpl.rb
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

# FoundationDB Ruby API

# Documentation for this API can be found at
# https://apple.github.io/foundationdb/api-ruby.html

require 'ffi'

require 'thread'
require 'monitor'

require_relative 'fdboptions'

module FDB
  module FDBC
    require 'rbconfig'

    unless ["x86_64", "arm64"].include? RbConfig::CONFIG['host_cpu']
      raise LoadError, "FoundationDB API only supported on x86_64 and arm64 (not #{RbConfig::CONFIG['host_cpu']})"
    end

    case RbConfig::CONFIG['host_os']
    when /linux/
      dlobj = 'libfdb_c.so'
    when /darwin/
      dlobj = 'libfdb_c.dylib'
    when /mswin|mingw|cygwin/
      dlobj = 'fdb_c.dll'
      if Gem.loaded_specs['ffi'].version < Gem::Version.create('1.7.0.dev')
        raise LoadError, "You must install ffi >= 1.7.0.dev on 64-bit Windows (see https://github.com/ffi/ffi/issues/259)"
      end
    else
      raise LoadError, "FoundationDB API is not supported on #{RbConfig::CONFIG['host_os']}"
    end

    extend FFI::Library
    begin
      ffi_lib [File.join( File.dirname(__FILE__), dlobj ), dlobj]
    rescue LoadError => e
      raise $!, "#{$!} (is the FoundationDB client library installed?)"
    end

    typedef :int, :fdb_error
    typedef :int, :fdb_bool

    attach_function :fdb_select_api_version_impl, [ :int, :int ], :fdb_error
    attach_function :fdb_get_max_api_version, [ ], :int

    def self.init_c_api
      attach_function :fdb_get_error, [ :fdb_error ], :string

      attach_function :fdb_network_set_option, [ :int, :pointer, :int ], :fdb_error
      attach_function :fdb_setup_network, [ ], :fdb_error
      attach_function :fdb_run_network, [ ], :fdb_error, :blocking => true
      attach_function :fdb_stop_network, [ ], :fdb_error

      attach_function :fdb_future_cancel, [ :pointer ], :void
      attach_function :fdb_future_release_memory, [ :pointer ], :void
      attach_function :fdb_future_destroy, [ :pointer ], :void
      attach_function :fdb_future_block_until_ready, [ :pointer ], :fdb_error, :blocking => true
      attach_function :fdb_future_is_ready, [ :pointer ], :fdb_bool

      callback :fdb_future_callback, [ :pointer, :pointer ], :void
      attach_function :fdb_future_set_callback, [ :pointer, :fdb_future_callback, :pointer ], :fdb_error

      attach_function :fdb_future_get_error, [ :pointer ], :fdb_error
      attach_function :fdb_future_get_int64, [ :pointer, :pointer ], :fdb_error
      attach_function :fdb_future_get_key, [ :pointer, :pointer, :pointer ], :fdb_error
      attach_function :fdb_future_get_value, [ :pointer, :pointer, :pointer, :pointer ], :fdb_error
      attach_function :fdb_future_get_keyvalue_array, [ :pointer, :pointer, :pointer, :pointer ], :fdb_error
      attach_function :fdb_future_get_string_array, [ :pointer, :pointer, :pointer ], :fdb_error

      attach_function :fdb_create_database, [ :string, :pointer ], :fdb_error

      attach_function :fdb_database_destroy, [ :pointer ], :void
      attach_function :fdb_database_set_option, [ :pointer, :int, :pointer, :int ], :fdb_error

      attach_function :fdb_database_create_transaction, [ :pointer, :pointer ], :fdb_error
      attach_function :fdb_transaction_destroy, [ :pointer ], :void
      attach_function :fdb_transaction_cancel, [ :pointer ], :void
      attach_function :fdb_transaction_atomic_op, [ :pointer, :pointer, :int, :pointer, :int, :int ], :void
      attach_function :fdb_transaction_add_conflict_range, [ :pointer, :pointer, :int, :pointer, :int, :int ], :int
      attach_function :fdb_transaction_get_addresses_for_key, [ :pointer, :pointer, :int ], :pointer
      attach_function :fdb_transaction_set_option, [ :pointer, :int, :pointer, :int ], :fdb_error
      attach_function :fdb_transaction_set_read_version, [ :pointer, :int64 ], :void
      attach_function :fdb_transaction_get_read_version, [ :pointer ], :pointer
      attach_function :fdb_transaction_get, [ :pointer, :pointer, :int, :int ], :pointer
      attach_function :fdb_transaction_get_key, [ :pointer, :pointer, :int, :int, :int, :int ], :pointer
      attach_function :fdb_transaction_get_range, [ :pointer, :pointer, :int, :int, :int, :pointer, :int, :int, :int, :int, :int, :int, :int, :int, :int ], :pointer
      attach_function :fdb_transaction_get_estimated_range_size_bytes, [ :pointer, :pointer, :int, :pointer, :int ], :pointer
      attach_function :fdb_transaction_get_range_split_points, [ :pointer, :pointer, :int, :pointer, :int, :int64 ], :pointer
      attach_function :fdb_transaction_set, [ :pointer, :pointer, :int, :pointer, :int ], :void
      attach_function :fdb_transaction_clear, [ :pointer, :pointer, :int ], :void
      attach_function :fdb_transaction_clear_range, [ :pointer, :pointer, :int, :pointer, :int ], :void
      attach_function :fdb_transaction_watch, [ :pointer, :pointer, :int ], :pointer
      attach_function :fdb_transaction_commit, [ :pointer ], :pointer
      attach_function :fdb_transaction_get_committed_version, [ :pointer, :pointer ], :fdb_error
      attach_function :fdb_transaction_get_approximate_size, [ :pointer ], :pointer
      attach_function :fdb_transaction_get_versionstamp, [ :pointer ], :pointer
      attach_function :fdb_transaction_on_error, [ :pointer, :fdb_error ], :pointer
      attach_function :fdb_transaction_reset, [ :pointer ], :void
    end

    class KeyValueStruct < FFI::Struct
      pack 4
      layout :key, :pointer,
             :key_length, :int,
             :value, :pointer,
             :value_length, :int
    end

    class KeyStruct < FFI::Struct
      pack 4
      layout :key, :pointer,
             :key_length, :int
    end

    def self.check_error(code)
      raise Error.new(code) if code.nonzero?
      nil
    end
  end

  @@cb_mutex = Mutex.new
  def self.cb_mutex
    @@cb_mutex
  end

  class CallbackEntry
    attr_accessor :callback
    attr_accessor :index

    def initialize
      @callback = nil
      @index = nil
    end
  end

  @@ffi_callbacks = []
  def self.ffi_callbacks
    @@ffi_callbacks
  end

  [ "Network", "Database", "Transaction" ].each do |scope|
    klass = FDB.const_set("#{scope}Options", Class.new)
    klass.class_eval do
      define_method(:initialize) do |setfunc|
        instance_variable_set("@setfunc", setfunc)
      end
    end
    class_variable_get("@@#{scope}Option").each_pair do |k,v|
      p =
        case v[2]
        when NilClass then
          Proc.new do || @setfunc.call(v[0], nil) end
        when String then
          Proc.new do |opt=nil| @setfunc.call(v[0], (opt.nil? ? opt : opt.encode('UTF-8')) ) end
        when Integer then
          Proc.new do |opt| @setfunc.call(v[0], [opt].pack("q<")) end
        else
          raise ArgumentError, "Don't know how to set options of type #{v[2].class}"
        end
      klass.send :define_method, "set_#{k.downcase}", p
    end
  end

  def self.key_to_bytes(k)
    if k.respond_to? 'as_foundationdb_key'
      k.as_foundationdb_key
    else
      k
    end
  end

  def self.value_to_bytes(v)
    if v.respond_to? 'as_foundationdb_value'
      v.as_foundationdb_value
    else
      v
    end
  end

	def self.strinc(key)
		key = key.gsub(/\xff*\z/, '')
		raise ArgumentError, 'Key must contain at least one byte not equal to 0xFF.' if key.length == 0

		key[0..-2] + (key[-1].ord + 1).chr
	end

  @@options = NetworkOptions.new lambda { |code, param|
    FDBC.check_error FDBC.fdb_network_set_option(code, param, param.nil? ? 0 : param.bytesize)
  }
  def FDB.options
    @@options
  end

  @@network_thread = nil
  @@network_thread_monitor = Monitor.new

  def self.init()
    @@network_thread_monitor.synchronize do
      if !@@network_thread.nil?
        raise Error.new(2000)
      end

      begin
        @@network_thread = Thread.new do
          @@network_thread_monitor.synchronize do
            # Don't start running until init releases this
          end
          # puts "Starting FDB network"
          begin
            FDBC.check_error FDBC.fdb_run_network
          rescue Error => e
            $stderr.puts "Unhandled error in FoundationDB network thread: #{e.to_s}"
          end
        end

        FDBC.check_error FDBC.fdb_setup_network
      rescue
        @@network_thread.kill
        @@network_thread = nil
        raise
      end
    end

    nil
  end

  class << self
    private :init
  end

  def self.stop()
    FDBC.check_error FDBC.fdb_stop_network
  end

  at_exit do
    if !@@network_thread.nil?
      # puts "Stopping FDB network"
      stop
      @@network_thread.join
    end
  end

  @@open_databases = {}
  @@cache_lock = Mutex.new

  def self.open( cluster_file = nil )
    @@network_thread_monitor.synchronize do
      if ! @@network_thread
        init
      end
    end

    @@cache_lock.synchronize do
      if ! @@open_databases.has_key? [cluster_file]
        dpointer = FFI::MemoryPointer.new :pointer
        FDBC.check_error FDBC.fdb_create_database(cluster_file, dpointer)
        @@open_databases[cluster_file] = Database.new dpointer.get_pointer(0)
      end

      @@open_databases[cluster_file]
    end
  end

  class Error < StandardError
    attr_reader :code

    def initialize(code)
      @code = code
      @description = nil
    end

    def description
      if !@description
        @description = FDBC.fdb_get_error(@code)
      end
      @description
    end

    def to_s
      "#{description} (#{@code})"
    end
  end

  class Future
    def self.finalize(ptr)
      proc do
        #puts "Destroying future #{ptr}"
        FDBC.fdb_future_destroy(ptr)
      end
    end

    def initialize(fpointer)
      @fpointer = fpointer
      ObjectSpace.define_finalizer(self, FDB::Future.finalize(@fpointer))
    end

    def cancel
      FDBC.fdb_future_cancel(@fpointer)
    end

    def ready?
      return !FDBC.fdb_future_is_ready(@fpointer).zero?
    end

    def block_until_ready
      FDBC.check_error FDBC.fdb_future_block_until_ready(@fpointer)
    end

    def on_ready(&block)
      def callback_wrapper(f, &block)
        begin
          yield f
        rescue Exception
          begin
            $stderr.puts "Discarding uncaught exception from user callback:"
            $stderr.puts "#{$@.first}: #{$!.message} (#{$!.class})", $@.drop(1).map{|s| "\t#{s}"}
          rescue Exception
          end
        end
      end

      entry = CallbackEntry.new

      FDB.cb_mutex.synchronize {
        pos = FDB.ffi_callbacks.length
        entry.index = pos
        FDB.ffi_callbacks << entry
      }

      entry.callback = FFI::Function.new(:void, [:pointer, :pointer]) do |ign1, ign2|
        FDB.cb_mutex.synchronize {
          FDB.ffi_callbacks[-1].index = entry.index
          FDB.ffi_callbacks[entry.index] = FDB.ffi_callbacks[-1]
          FDB.ffi_callbacks.pop
        }
        callback_wrapper(self, &block)
      end
      FDBC.fdb_future_set_callback(@fpointer, entry.callback, nil)
    end

    def self.wait_for_any(*futures)
      if futures.empty?
        raise ArgumentError, "wait_for_any requires at least one future"
      end

      mx = Mutex.new
      cv = ConditionVariable.new

      ready_idx = -1

      futures.each_with_index do |f, i|
        f.on_ready do |f|
          mx.synchronize {
            if ready_idx < 0
              ready_idx = i
              cv.signal
            end
          }
        end
      end

      mx.synchronize {
        if ready_idx < 0
          cv.wait mx
        end
      }

      ready_idx
    end

    private

    def release_memory
      FDBC.fdb_future_release_memory(@fpointer)
    end
  end

  class FutureNil < Future
    def wait
      block_until_ready
      FDBC.check_error FDBC.fdb_future_get_error(@fpointer)
    end
  end

  class LazyFuture < Future
    instance_methods.each { |m| undef_method m unless (m =~ /^__/ or (LazyFuture.instance_methods - Object.instance_methods).include? m or m == :object_id)}

    def respond_to?(message)
      message = message.to_sym
      message == :__result__ or
        message == :to_ptr or
        value.respond_to? message
    end

    def method_missing( *args, &block )
      value.__send__( *args, &block )
    end

    def initialize(fpointer)
      super(fpointer)
      @set = false
      @value = nil
    end

    def value
      if !@set
        block_until_ready

        begin
          getter
          release_memory
        rescue Error => e
          if e.code != 1102 # future_released
            raise
          end
        end

        @set = true
      end

      @value
    end
  end

  class LazyString < LazyFuture
    def to_ptr
      FFI::MemoryPointer.from_string(value)
    end
  end

  class Int64Future < LazyFuture
    def getter
      val = FFI::MemoryPointer.new :int64
      FDBC.check_error FDBC.fdb_future_get_int64(@fpointer, val)
      @value = val.read_long_long
    end
    private :getter
  end

  class FutureKeyValueArray < Future
    def wait
      block_until_ready

      kvs = FFI::MemoryPointer.new :pointer
      count = FFI::MemoryPointer.new :int
      more = FFI::MemoryPointer.new :int
      FDBC.check_error FDBC.fdb_future_get_keyvalue_array(@fpointer, kvs, count, more)
      kvs = kvs.read_pointer

      [(0..count.read_int-1).map{|i|
        x = FDBC::KeyValueStruct.new(kvs + (i * FDBC::KeyValueStruct.size))
        KeyValue.new(x[:key].read_bytes(x[:key_length]),
                     x[:value].read_bytes(x[:value_length]))
       }, count.read_int, more.read_int]
    end
  end

  class FutureKeyArray < Future
    def wait
      block_until_ready

      ks = FFI::MemoryPointer.new :pointer
      count = FFI::MemoryPointer.new :int
      FDBC.check_error FDBC.fdb_future_get_key_array(@fpointer, kvs, count)
      ks = ks.read_pointer

      (0..count.read_int-1).map{|i|
        x = FDBC::KeyStruct.new(ks + (i * FDBC::KeyStruct.size))
        x[:key].read_bytes(x[:key_length])
       }
    end
  end

  class FutureStringArray < LazyFuture
    def getter
      strings = FFI::MemoryPointer.new :pointer
      count = FFI::MemoryPointer.new :int
      FDBC.check_error FDBC.fdb_future_get_string_array(@fpointer, strings, count)

      @value = strings.read_pointer.get_array_of_string(0, count.read_int).compact
    end
  end

  class FormerFuture
    def ready?
      true
    end

    def block_until_ready
    end

    def on_ready(&block)
      begin
        yield self
      rescue Exception
        begin
          $stderr.puts "Discarding uncaught exception from user callback:"
          $stderr.puts "#{$@.first}: #{$!.message} (#{$!.class})", $@.drop(1).map{|s| "\t#{s}"}
        rescue Exception
        end
      end
    end
  end

  class Database < FormerFuture
    attr_reader :options

    def self.finalize(ptr)
      proc do
        # puts "Destroying database #{ptr}"
        FDBC.fdb_database_destroy(ptr)
      end
    end

    def initialize(dpointer)
      @dpointer = dpointer
      @options = DatabaseOptions.new lambda { |code, param|
        FDBC.check_error FDBC.fdb_database_set_option(dpointer, code, param, param.nil? ? 0 : param.bytesize)
      }
      ObjectSpace.define_finalizer(self, self.class.finalize(@dpointer))
    end

    def create_transaction
      tr = FFI::MemoryPointer.new :pointer
      FDBC.check_error FDBC.fdb_database_create_transaction(@dpointer, tr)
      Transaction.new(tr.read_pointer, self)
    end

    def transact
      tr = create_transaction
      committed = false
      begin
        ret = yield tr
        # puts ret
        tr.commit.wait
        committed = true
      rescue Error => e
        # puts "Rescued #{e}"
        tr.on_error(e).wait
      end until committed
      ret
    end

    def set(key, value)
      transact do |tr|
        tr[key] = value
      end
    end
    alias []= set

    def get(key)
      transact do |tr|
        tr[key].value
      end
    end
    alias [] get

    def get_range(bkeysel, ekeysel, options={}, &block)
      transact do |tr|
        a = tr.get_range(bkeysel, ekeysel, options).to_a
        if block_given?
          a.each &block
        else
          a
        end
      end
    end

    def clear(key)
      transact do |tr|
        tr.clear(key)
      end
    end

    def clear_range(bkey, ekey)
      transact do |tr|
        tr.clear_range(bkey, ekey)
      end
    end

    def get_key(keysel)
      transact do |tr|
        tr.get_key(keysel).value
      end
    end

    def get_range_start_with(prefix, options={}, &block)
      transact do |tr|
        a = tr.get_range_start_with(prefix, options).to_a
        if block_given?
          a.each &block
        else
          a
        end
      end
    end

    def clear_range_start_with(prefix)
      transact do |tr|
        tr.clear_range_start_with(prefix)
      end
    end

    def get_and_watch(key)
      transact do |tr|
        value = tr.get(key)
        watch = tr.watch(key)
        [value.value, watch]
      end
    end

    def set_and_watch(key, value)
      transact do |tr|
        tr.set(key, value)
        tr.watch(key)
      end
    end

    def clear_and_watch(key)
      transact do |tr|
        tr.clear(key)
        tr.watch(key)
      end
    end

    def watch(key)
      transact do |tr|
        tr.watch(key)
      end
    end
  end

  class KeySelector
    attr_reader :key, :or_equal, :offset

    def initialize(key, or_equal, offset)
      @key = key
      @or_equal = or_equal
      @offset = offset
    end

    def self.last_less_than(key)
      self.new(key, 0, 0)
    end

    def self.last_less_or_equal(key)
      self.new(key, 1, 0)
    end

    def self.first_greater_than(key)
      self.new(key, 1, 1)
    end

    def self.first_greater_or_equal(key)
      self.new(key, 0, 1)
    end

    def +(offset)
      KeySelector.new(@key, @or_equal, @offset + offset)
    end

    def -(offset)
      KeySelector.new(@key, @or_equal, @offset - offset)
    end
  end

  class TransactionRead
    attr_reader :db
    attr_reader :tpointer

    def self.finalize(ptr)
      proc do
        #puts "Destroying transaction #{ptr}"
        FDBC.fdb_transaction_destroy(ptr)
      end
    end

    def initialize(tpointer, db, is_snapshot)
      @tpointer = tpointer
      @db = db
      @is_snapshot = is_snapshot

      ObjectSpace.define_finalizer(self, self.class.finalize(@tpointer))
    end

    def transact
      yield self
    end

    def get_read_version
      Int64Future.new(FDBC.fdb_transaction_get_read_version @tpointer)
    end

    def get(key)
      key = FDB.key_to_bytes(key)
      Value.new(FDBC.fdb_transaction_get(@tpointer, key, key.bytesize, @is_snapshot))
    end
    alias [] get

    def get_key(keysel)
      key = FDB.key_to_bytes(keysel.key)
      Key.new(FDBC.fdb_transaction_get_key(@tpointer, key, key.bytesize, keysel.or_equal, keysel.offset, @is_snapshot))
    end

    def to_selector(key_or_selector)
      if key_or_selector.kind_of? KeySelector
        key_or_selector
      else
        KeySelector.first_greater_or_equal key_or_selector
      end
    end
    private :to_selector

    @@RangeEnum = Class.new do
      include Enumerable

      def initialize(get_range, bsel, esel, limit, reverse, streaming_mode)
        @get_range = get_range

        @bsel = bsel
        @esel = esel

        @limit = limit
        @reverse = reverse
        @mode = streaming_mode

        @future = @get_range.call(@bsel, @esel, @limit, @mode, 1, @reverse)
      end

      def to_a
        o = self.dup
        o.instance_eval do
          if @mode == @@StreamingMode["ITERATOR"][0]
            if @limit.zero?
              @mode = @@StreamingMode["WANT_ALL"][0]
            else
              @mode = @@StreamingMode["EXACT"][0]
            end
          end
        end
        Enumerable.instance_method(:to_a).bind(o).call
      end

      def each
        bsel = @bsel
        esel = @esel
        limit = @limit

        iteration = 1 # the first read was fired off when the RangeEnum was initialized
        future = @future

        done = false

        while !done
          if future
            kvs, count, more = future.wait
            index = 0
            future = nil

            return if count.zero?
          end

          result = kvs[index]
          index += 1

          if index == count
            if more.zero? || limit == count
              done = true
            else
              iteration += 1
              if limit.nonzero?
                limit -= count
              end
              if @reverse.nonzero?
                esel = KeySelector.first_greater_or_equal(kvs.last.key)
              else
                bsel = KeySelector.first_greater_than(kvs.last.key)
              end
              future = @get_range.call(bsel, esel, limit, @mode, iteration, @reverse)
            end
          end

          yield result
        end
      end
    end

    def get_range(bkeysel, ekeysel, options={}, &block)
      defaults = { :limit => 0, :reverse => false, :streaming_mode => :iterator }
      options = defaults.merge options
      bsel = to_selector bkeysel
      esel = to_selector ekeysel

      if options[:streaming_mode].kind_of? Symbol
        streaming_mode = @@StreamingMode[options[:streaming_mode].to_s.upcase]
        raise ArgumentError, "#{options[:streaming_mode]} is not a valid streaming mode" if !streaming_mode
        streaming_mode = streaming_mode[0]
      else
        streaming_mode = options[:streaming_mode]
      end

      r = @@RangeEnum.new(lambda {|bsel, esel, limit, streaming_mode, iteration, reverse|
                            begin_key = FDB.key_to_bytes(bsel.key)
                            end_key = FDB.key_to_bytes(esel.key)
                            FDB::FutureKeyValueArray.new(FDBC.fdb_transaction_get_range(@tpointer, begin_key, begin_key.bytesize, bsel.or_equal, bsel.offset, end_key, end_key.bytesize, esel.or_equal, esel.offset, limit, 0, streaming_mode, iteration, @is_snapshot, reverse))
                          }, bsel, esel, options[:limit], options[:reverse] ? 1 : 0, streaming_mode)

      if !block_given?
        r
      else
        r.each &block
      end
    end

    def get_range_start_with(prefix, options={}, &block)
      prefix = FDB.key_to_bytes(prefix)
      prefix = prefix.dup.force_encoding "BINARY"
      get_range(prefix, FDB.strinc(prefix), options, &block)
    end

    def get_estimated_range_size_bytes(begin_key, end_key)
      bkey = FDB.key_to_bytes(begin_key)
      ekey = FDB.key_to_bytes(end_key)
      Int64Future.new(FDBC.fdb_transaction_get_estimated_range_size_bytes(@tpointer, bkey, bkey.bytesize, ekey, ekey.bytesize))
    end

    def get_range_split_points(begin_key, end_key, chunk_size)
      if chunk_size <=0
        raise ArgumentError, "Invalid chunk size"
      end
      bkey = FDB.key_to_bytes(begin_key)
      ekey = FDB.key_to_bytes(end_key)
      FutureKeyArray.new(FDBC.fdb_transaction_get_range_split_points(@tpointer, bkey, bkey.bytesize, ekey, ekey.bytesize, chunk_size))
    end

  end

  TransactionRead.class_variable_set("@@StreamingMode", @@StreamingMode)

  class Transaction < TransactionRead
    attr_reader :snapshot, :options

    def initialize(tpointer, db)
      super(tpointer, db, 0)
      @snapshot = TransactionRead.new(tpointer, db, 1)

      @options = TransactionOptions.new lambda { |code, param|
        FDBC.check_error FDBC.fdb_transaction_set_option(@tpointer, code, param, param.nil? ? 0 : param.bytesize)
      }

      ObjectSpace.undefine_finalizer self
    end

    def set(key, value)
      key = FDB.key_to_bytes(key)
      value = FDB.value_to_bytes(value)
      FDBC.fdb_transaction_set(@tpointer, key, key.bytesize, value, value.bytesize)
    end
    alias []= set

    def add_read_conflict_range(bkey, ekey)
      bkey = FDB.key_to_bytes(bkey)
      ekey = FDB.key_to_bytes(ekey)
      FDBC.check_error FDBC.fdb_transaction_add_conflict_range(@tpointer, bkey, bkey.bytesize, ekey, ekey.bytesize, @@ConflictRangeType["READ"][0])
    end

    def add_read_conflict_key(key)
      key = FDB.key_to_bytes(key)
      add_read_conflict_range(key, key + "\x00")
    end

    def add_write_conflict_range(bkey, ekey)
      bkey = FDB.key_to_bytes(bkey)
      ekey = FDB.key_to_bytes(ekey)
      FDBC.check_error FDBC.fdb_transaction_add_conflict_range(@tpointer, bkey, bkey.bytesize, ekey, ekey.bytesize, @@ConflictRangeType["WRITE"][0])
    end

    def add_write_conflict_key(key)
      key = FDB.key_to_bytes(key)
      add_write_conflict_range(key, key + "\x00")
    end

    def commit
      FutureNil.new(FDBC.fdb_transaction_commit(@tpointer))
    end

    def watch(key)
      key = FDB.key_to_bytes(key)
      FutureNil.new(FDBC.fdb_transaction_watch(@tpointer, key, key.bytesize))
    end

    def on_error(e)
      raise e if !e.kind_of? Error
      FutureNil.new(FDBC.fdb_transaction_on_error(@tpointer, e.code))
    end

    def clear(key)
      key = FDB.key_to_bytes(key)
      FDBC.fdb_transaction_clear(@tpointer, key, key.bytesize)
    end

    def clear_range(bkey, ekey)
      bkey = FDB.key_to_bytes(bkey)
      ekey = FDB.key_to_bytes(ekey)
      FDBC.fdb_transaction_clear_range(@tpointer, bkey || "", bkey && bkey.bytesize || 0, ekey || "", ekey && ekey.bytesize || 0)
    end

    def clear_range_start_with(prefix)
      prefix = FDB.key_to_bytes(prefix)
      prefix = prefix.dup.force_encoding "BINARY"
      clear_range(prefix, FDB.strinc(prefix))
    end

    def set_read_version(version)
      FDBC.fdb_transaction_set_read_version(@tpointer, version)
    end

    def get_committed_version
      version = FFI::MemoryPointer.new :int64
      FDBC.check_error FDBC.fdb_transaction_get_committed_version(@tpointer, version)
      version.read_long_long
    end

    def get_approximate_size
      Int64Future.new(FDBC.fdb_transaction_get_approximate_size @tpointer)
    end

    def get_versionstamp
      Key.new(FDBC.fdb_transaction_get_versionstamp(@tpointer))
    end

    def reset
      FDBC.fdb_transaction_reset @tpointer
    end

    def cancel
      FDBC.fdb_transaction_cancel @tpointer
    end

    def atomic_op(code, key, param)
      key = FDB.key_to_bytes(key)
      param = FDB.value_to_bytes(param)
      FDBC.fdb_transaction_atomic_op(@tpointer, key, key.bytesize, param, param.bytesize, code)
    end
    private :atomic_op
  end

  @@MutationType.each_pair do |k,v|
    p = Proc.new do |key, param| atomic_op(v[0], key, param) end
    Transaction.class_eval do
      define_method("#{k.downcase}") do |key, param|
        atomic_op(v[0], key, param)
      end
    end
    Database.class_eval do
      define_method("#{k.downcase}") do |key, param|
        transact do |tr|
          tr.send("#{k.downcase}", key, param)
        end
      end
    end
  end

  Transaction.class_variable_set("@@ConflictRangeType", @@ConflictRangeType)

  class Value < LazyString
    def getter
      present = FFI::MemoryPointer.new :int
      value = FFI::MemoryPointer.new :pointer
      length = FFI::MemoryPointer.new :int
      FDBC.check_error FDBC.fdb_future_get_value(@fpointer, present, value, length)
      if present.read_int > 0
        @value = value.read_pointer.read_bytes(length.read_int)
      end
    end
    private :getter
  end

  class Key < LazyString
    def getter
      key = FFI::MemoryPointer.new :pointer
      key_length = FFI::MemoryPointer.new :int
      FDBC.check_error FDBC.fdb_future_get_key(@fpointer, key, key_length)
      if key_length.read_int.zero?
        @value = ''
      else
        @value = key.read_pointer.read_bytes(key_length.read_int)
      end
    end
    private :getter
  end

  class KeyValue
    attr_reader :key, :value

    def initialize(key, value)
      @key = key
      @value = value
    end
  end
end
