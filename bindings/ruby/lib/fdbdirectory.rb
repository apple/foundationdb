#encoding: BINARY

#
# fdbdirectory.rb
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

# FoundationDB Ruby API

# Documentation for this API can be found at
# https://apple.github.io/foundationdb/api-ruby.html

require 'thread'

require_relative 'fdbimpl'
require_relative 'fdbsubspace'

module FDB
  class AllocatorTransactionState
    def initialize()
      @lock = Mutex.new
    end

    attr_reader :lock
  end

  class HighContentionAllocator
    def initialize(subspace)
      @counters = subspace[0]
      @recent = subspace[1]
      @lock = Mutex.new
    end

    def allocate(db_or_tr)
      db_or_tr.transact do |tr|
        if !tr.instance_variable_defined?(:@__fdb_directory_layer_hca_state__)
          @lock.synchronize do
            if !tr.instance_variable_defined?(:@__fdb_directory_layer_hca_state__)
              tr.instance_variable_set(:@__fdb_directory_layer_hca_state__, AllocatorTransactionState.new)
            end
          end
        end

        tr_state = tr.instance_variable_get(:@__fdb_directory_layer_hca_state__)

        loop do
          start, count = 
            tr.snapshot.get_range(@counters.range[0], 
                                  @counters.range[1], 
                                  {:limit => 1, :reverse => true}) 
            .map { |kv| 
              [ @counters.unpack(kv.key)[0], kv.value.unpack('q<')[0] ]
            }.first || [0,0]

          window = 0
          window_advanced = false
          loop do
            tr_state.lock.synchronize do
              if window_advanced
                tr.clear_range(@counters, @counters[start])
                tr.options.set_next_write_no_write_conflict_range()
                tr.clear_range(@recent, @recent[start])
              end

              tr.add(@counters[start], [1].pack('q<'))
              count = tr.snapshot[@counters[start]]
            end
            
            count = count.nil? ? 0 : count.unpack('q<')[0]
            window = window_size(start)
            if count * 2 < window
              break
            end

            start += window
            window_advanced = true
          end

          candidate = 0
          found = false
          loop do
            candidate = rand(start...start+window)
            latest_counter = nil
            candidate_value = nil

            tr_state.lock.synchronize do
              latest_counter = tr.snapshot.get_range(@counters.range[0],
                                                    @counters.range[1],
                                                    {:limit => 1, :reverse => true})
              candidate_value = tr[@recent[candidate]]
              tr.options.set_next_write_no_write_conflict_range()
              tr[@recent[candidate]] = ''
            end
            
            latest_counter = latest_counter.map{ |kv| [ @counters.unpack(kv.key)[0] ] }.first || [0]
            if latest_counter.length > 0 and latest_counter[0] > start
              break
            end

            if candidate_value.nil?
              found = true
              tr.add_write_conflict_key(@recent[candidate])
              break
            end
          end

          if found
            break FDB::Tuple.pack([candidate])
          end
        end
      end
    end

    def window_size(start)
      if start < 255
        64
      elsif start < 65535
        1024
      else
        8192
      end
    end

    private :window_size
  end

  class DirectoryLayer
    @@SUBDIRS = 0
    @@VERSION = [1,0,0]

    def initialize(options={})
      defaults = { :node_subspace => Subspace.new([], "\xfe"), 
                   :content_subspace =>Subspace.new, 
                   :allow_manual_prefixes => false }

      options = defaults.merge(options)

      @content_subspace = options[:content_subspace]
      @node_subspace = options[:node_subspace]
      @allow_manual_prefixes = options[:allow_manual_prefixes]

      @root_node = @node_subspace[@node_subspace.key]
      @allocator = HighContentionAllocator.new(@root_node['hca'])

      @path = []
      @layer = ''
    end

    def path
      return @path.dup
    end

    def layer
      return @layer.dup
    end

    attr_writer :path
    private :path=

    def create_or_open(db_or_tr, path, options={})
      create_or_open_internal(db_or_tr, path, true, true, options)
    end

    def open(db_or_tr, path, options={})
      create_or_open_internal(db_or_tr, path, false, true, options)
    end

    def create(db_or_tr, path, options={})
      create_or_open_internal(db_or_tr, path, true, false, options)
    end

    def move_to(db_or_tr, new_absolute_path)
      raise 'The root directory cannot be moved.'
    end

    def move(db_or_tr, old_path, new_path)
      db_or_tr.transact do |tr|
        check_version(tr, true)

        old_path = to_unicode_path(old_path)
        new_path = to_unicode_path(new_path)

        if old_path == new_path[0...old_path.length]
          raise ArgumentError, 
            'The destination directory cannot be a subdirectory of the source directory.'
        end

        old_node = find(tr, old_path).prefetch_metadata(tr)
        new_node = find(tr, new_path).prefetch_metadata(tr)

        raise ArgumentError, 'The source directory does not exist.' unless old_node.exists?

        if old_node.is_in_partition? || new_node.is_in_partition?
          if !old_node.is_in_partition? || 
            !new_node.is_in_partition? || 
            old_node.path != new_node.path
          then
            raise ArgumentError, 'Cannot move between partitions'
          end

          next new_node
            .get_contents(self)
            .move(tr, old_node.get_partition_subpath, new_node.get_partition_subpath)
        end

        if new_node.exists?
          raise ArgumentError, 'The destination directory already exists. Remove it first.'
        end

        parent_node = find(tr, new_path[0...-1])
        if !parent_node.exists?
          raise ArgumentError, 
            'The parent directory of the destination directory does not exist. Create it first.'
        end
        
        tr[parent_node.subspace[@@SUBDIRS][new_path[-1]]] = 
          @node_subspace.unpack(old_node.subspace.key)[0]
        remove_from_parent(tr, old_path)

        contents_of_node(old_node.subspace, new_path, old_node.layer)
      end
    end

    def remove(db_or_tr, path=[])
      remove_internal(db_or_tr, path, true)
    end

    def remove_if_exists(db_or_tr, path=[])
      remove_internal(db_or_tr, path, false)
    end

    def list(db_or_tr, path=[])
      db_or_tr.transact do |tr|
        check_version(tr, false)

        path = to_unicode_path(path)
        node = find(tr, path).prefetch_metadata(tr)

        raise ArgumentError, 'The directory does not exist.' unless node.exists?

        if node.is_in_partition?(nil, true)
          next node.get_contents(self).list(tr, node.get_partition_subpath)
        end

        subdir_names_and_nodes(tr, node.subspace).map { |name, node| name }
      end
    end

    def exists?(db_or_tr, path=[])
      db_or_tr.transact do |tr|
        check_version(tr, false)

        path = to_unicode_path(path)
        node = find(tr, path).prefetch_metadata(tr)

        next false if !node.exists?

        if node.is_in_partition?
          next node.get_contents(self).exists?(tr, node.get_partition_subpath)
        end

        true
      end
    end

    protected

    def create_or_open_internal(db_or_tr, path, allow_create, allow_open, options={})
      defaults = { :layer => '', :prefix => nil }
      options = defaults.merge(options)

      if !options[:prefix].nil? and allow_open and allow_create
        raise ArgumentError, 'Cannot specify a prefix when calling create_or_open.'
      end

      if !options[:prefix].nil? and !@allow_manual_prefixes
        if @path.length == 0
          raise ArgumentError, 'Cannot specify a prefix unless manual prefixes are enabled.'
        else
          raise ArgumentError, 'Cannot specify a prefix in a partition.'
        end
      end

      db_or_tr.transact do |tr|
        check_version(tr, false)
        path = to_unicode_path(path)

        raise ArgumentError, 'The root directory cannot be opened.' if path.length == 0

        existing_node = find(tr, path).prefetch_metadata(tr)
        
        if existing_node.exists?
          if existing_node.is_in_partition?
            subpath = existing_node.get_partition_subpath
            existing_node.get_contents(self).directory_layer.create_or_open_internal(tr, subpath, allow_create, allow_open, options)
          else
            raise ArgumentError, 'The directory already exists.' unless allow_open
            open_directory(path, options, existing_node)
          end
        else
          raise ArgumentError, 'The directory does not exist.' unless allow_create
          create_directory(tr, path, options)
        end
      end
    end
    def open_directory(path, options, existing_node)
      if options[:layer] and !options[:layer].empty? and options[:layer] != existing_node.layer
        raise 'The directory was created with an incompatible layer.'
      end
      existing_node.get_contents(self)
    end

    def create_directory(tr, path, options)
      check_version(tr, true)

      prefix = options[:prefix]
      if prefix.nil?
        prefix = @content_subspace.key + @allocator.allocate(tr)
        if !tr.get_range_start_with(prefix, { :limit => 1 }).to_a.empty?
          raise "The database has keys stored at the prefix chosen by the automatic prefix allocator: #{prefix.dump}."
        end

        if !is_prefix_free?(tr.snapshot, prefix)
          raise 'The directory layer has manually allocated prefixes that conflict with the automatic prefix allocator.'
        end
      elsif !is_prefix_free?(tr, prefix)
        raise ArgumentError, 'The given prefix is already in use.'
      end

      parent_node = if path[0...-1].length > 0
                      node_with_prefix(create_or_open(tr, path[0...-1]).key)
                    else
                      @root_node
                    end

      raise 'The parent directory does not exist.' unless parent_node

      node = node_with_prefix(prefix)
      tr[parent_node[@@SUBDIRS][path[-1]]] = prefix
      tr[node['layer']] = options[:layer]

      contents_of_node(node, path, options[:layer])
    end

    def remove_internal(db_or_tr, path, fail_on_nonexistent)
      db_or_tr.transact do |tr|
        check_version(tr, true)

        path = to_unicode_path(path)

        if path.empty?
          raise ArgumentError, 'The root directory cannot be removed.'
        end

        node = find(tr, path).prefetch_metadata(tr)

        if !node.exists?
          raise ArgumentError, 'The directory does not exist.' if fail_on_nonexistent
          next false
        end

        if node.is_in_partition?
          next node.get_contents(self).directory_layer
                      .remove_internal(tr, node.get_partition_subpath, fail_on_nonexistent)
        end

        remove_recursive(tr, node.subspace)
        remove_from_parent(tr, path)
        true
      end
    end

    private

    def check_version(tr, write_access)
      version = tr[@root_node['version']]

      initialize_directory(tr) if !version && write_access
      return if !version

      version = version.to_s.unpack('III<')
      
      dir_ver = "#{version[0]}.#{version[1]}.#{version[2]}"
      layer_ver = "#{@@VERSION[0]}.#{@@VERSION[1]}.#{@@VERSION[2]}"

      if version[0] != @@VERSION[0]
        raise "Cannot load directory with version #{dir_ver} using directory layer #{layer_ver}"
      elsif version[1] != @@VERSION[1] && write_access
        raise "Directory with version #{dir_ver} is read-only 
              when opened using directory layer #{layer_ver}"
      end
    end

    def initialize_directory(tr)
      tr[@root_node['version']] = @@VERSION.pack('III<')
    end

    def node_containing_key(tr, key)
      return @root_node if key.start_with?(@node_subspace.key)

      tr.get_range(@node_subspace.range[0], 
                   @node_subspace.pack([key]) + "\x00", 
                   { :reverse => true, :limit => 1})
      .map { |kv|
        prev_prefix = @node_subspace.unpack(kv.key)[0]
        node_with_prefix(prev_prefix) if key.start_with?(prev_prefix)
      }[0]
    end

    def node_with_prefix(prefix)
      @node_subspace[prefix] if !prefix.nil?
    end

    def contents_of_node(node, path, layer='')
      prefix = @node_subspace.unpack(node.key)[0]
      if layer == 'partition'
        DirectoryPartition.new(@path + path, prefix, self)
      else
        DirectorySubspace.new(@path + path, prefix, self, layer)
      end
    end

    def find(tr, path)
      node = Internal::Node.new(@root_node, [], path)
      path.each_with_index do |name, index|
        node = Internal::Node.new(node_with_prefix(tr[node.subspace[@@SUBDIRS][name]]), 
                                  path[0..index], path)

        return node unless node.exists? and node.layer(tr) != 'partition'
      end

      node
    end

    def subdir_names_and_nodes(tr, node)
      subdir = node[@@SUBDIRS]
      tr.get_range(subdir.range[0], subdir.range[1]).map { |kv|
        [subdir.unpack(kv.key)[0], node_with_prefix(kv.value)]
      }
    end

    def remove_from_parent(tr, path)
      parent = find(tr, path[0...-1])
      tr.clear(parent.subspace[@@SUBDIRS][path[-1]])
    end

    def remove_recursive(tr, node)
      subdir_names_and_nodes(tr, node).each do |name, subnode|
        remove_recursive(tr, subnode)
      end

      tr.clear_range_start_with(@node_subspace.unpack(node.key)[0])
      tr.clear_range(node.range[0], node.range[1])
    end

    def is_prefix_free?(tr, prefix)
      prefix && 
        prefix.length > 0 &&
        !node_containing_key(tr, prefix) && 
        tr.get_range(@node_subspace.pack([prefix]), @node_subspace.pack([FDB.strinc(prefix)]), 
                     { :limit => 1 }).to_a.empty?
    end

    def convert_path_element(name)
      if !name.kind_of? String
        raise TypeError, 'Invalid path: must be a unicode string or an array of unicode strings'
      end
      name.dup.force_encoding('UTF-8')
    end

    def to_unicode_path(path)
      if path.respond_to? 'each_with_index'
        path.each_with_index { |name, index| path[index] = convert_path_element(name) }
      else
        [convert_path_element(path)]
      end
    end
  end

  @@directory = DirectoryLayer.new
  def self.directory 
    @@directory
  end

  class DirectorySubspace < Subspace
    def initialize(path, prefix, directory_layer=FDB::directory, layer='')
      super([], prefix)
      @path = path
      @layer = layer
      @directory_layer = directory_layer
    end

    def path
      return @path.dup
    end

    def layer
      return @layer.dup
    end

    attr_reader :directory_layer

    def create_or_open(db_or_tr, name_or_path, options={})
      path = tuplify_path(name_or_path)
      @directory_layer.create_or_open(db_or_tr, partition_subpath(path), options)
    end

    def open(db_or_tr, name_or_path, options={})
      path = tuplify_path(name_or_path)
      @directory_layer.open(db_or_tr, partition_subpath(path), options)
    end

    def create(db_or_tr, name_or_path, options={})
      path = tuplify_path(name_or_path)
      @directory_layer.create(db_or_tr, partition_subpath(path), options)
    end

    def list(db_or_tr, name_or_path=[])
      path = tuplify_path(name_or_path)
      @directory_layer.list(db_or_tr, partition_subpath(path))
    end

    def move(db_or_tr, old_name_or_path, new_name_or_path)
      old_path = tuplify_path(old_name_or_path)
      new_path = tuplify_path(new_name_or_path)
      @directory_layer.move(db_or_tr, partition_subpath(old_path), partition_subpath(new_path))
    end

    def move_to(db_or_tr, new_absolute_name_or_path)
      directory_layer = get_layer_for_path([])
      new_absolute_path = directory_layer.send(:to_unicode_path, new_absolute_name_or_path)
      partition_len = directory_layer.path.length
      partition_path = new_absolute_path[0...partition_len]
      raise ArgumentError, 'Cannot move between partitions.' if partition_path != directory_layer.path
      directory_layer.move(db_or_tr, @path[partition_len..-1], 
                            new_absolute_path[partition_len..-1])
    end

    def remove(db_or_tr, name_or_path=[])
      path = tuplify_path(name_or_path)
      directory_layer = get_layer_for_path(path)
      directory_layer.remove(db_or_tr, partition_subpath(path, directory_layer))
    end

    def remove_if_exists(db_or_tr, name_or_path=[])
      path = tuplify_path(name_or_path)
      directory_layer = get_layer_for_path(path)
      directory_layer.remove_if_exists(db_or_tr, partition_subpath(path, directory_layer))
    end

    def exists?(db_or_tr, name_or_path=[])
      path = tuplify_path(name_or_path)
      directory_layer = get_layer_for_path(path)
      directory_layer.exists?(db_or_tr, partition_subpath(path, directory_layer))
    end

    def tuplify_path(path)
      if path.is_a? String
        [path]
      else
        path
      end
    end
    private :tuplify_path

    def partition_subpath(path, directory_layer = @directory_layer)
      self.path[directory_layer.path.length..-1] + path
    end
    private :partition_subpath

    def get_layer_for_path(path)
      @directory_layer
    end
    private :get_layer_for_path
  end

  class DirectoryPartition < DirectorySubspace
    def initialize(path, prefix, parent_directory_layer)
      directory_layer = DirectoryLayer.new(:node_subspace => Subspace.new([], prefix + "\xfe"), 
                                           :content_subspace => Subspace.new([], prefix))
      directory_layer.send(:path=, path)
      super(path, prefix, directory_layer, 'partition')
      @parent_directory_layer = parent_directory_layer
    end

    def [](name)
      raise 'Cannot open subspace in the root of a directory partition.'
    end

    def key
      raise 'Cannot get key for the root of a directory partition.'
    end

    def pack(tuple)
      raise 'Cannot pack keys using the root of a directory partition.'
    end

    def unpack(key)
      raise 'Cannot unpack keys using the root of a directory partition.'
    end

    def range(tuple=[])
      raise 'Cannot get range for the root of a directory partition.'
    end

    def contains?(key)
      raise 'Cannot check whether a key belongs to the root of a directory partition.'
    end
    
    def as_foundationdb_key
      raise 'Cannot use the root of a directory partition as a key.'
    end

    def subspace(tuple)
      raise 'Cannot open subspace in the root of a directory partition.'
    end

    def get_layer_for_path(path)
      if path.length == 0
        @parent_directory_layer
      else
        @directory_layer
      end
    end
    private :get_layer_for_path
  end

  module Internal
    class Node
      def initialize(subspace, path, target_path)
        @subspace = subspace
        @path = path
        @target_path = target_path
        @layer = nil
      end

      attr_reader :subspace
      attr_reader :path

      def exists?
        !@subspace.nil?
      end

      def prefetch_metadata(tr)
        layer(tr) if exists?
        self
      end

      def layer(tr=nil)
        if tr
          @layer = tr[@subspace['layer']]
        else
          raise 'Layer has not been read' unless @layer
        end

        @layer
      end

      def is_in_partition?(tr=nil, include_empty_subpath=false)
        exists? && 
          @layer == 'partition' && 
          (include_empty_subpath || @path.length < @target_path.length)
      end

      def get_partition_subpath(tr=nil)
        @target_path[@path.length..-1]
      end

      def get_contents(directory_layer, tr=nil)
        directory_layer.send(:contents_of_node, @subspace, @path, layer(tr))
      end
    end
  end
end
