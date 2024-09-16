#encoding: BINARY

#
# fdbsubspace.rb
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

require_relative 'fdbtuple'

module FDB
  class Subspace
    def initialize(prefix_tuple=[], raw_prefix='')
      @raw_prefix = raw_prefix.dup.force_encoding('BINARY') + FDB::Tuple.pack(prefix_tuple)
    end
    attr_reader :raw_prefix

    def [](name)
      Subspace.new([name], @raw_prefix)
    end

    def key
      @raw_prefix
    end

    def pack(tuple)
      @raw_prefix + FDB::Tuple.pack(tuple)
    end

    def unpack(key)
      raise ArgumentError, 'Cannot unpack key that is not in subspace.' if not contains? key
      FDB::Tuple.unpack(key[@raw_prefix.length..-1])
    end

    def range(tuple=[])
      rng = FDB::Tuple.range(tuple)
      [@raw_prefix + rng[0], @raw_prefix + rng[1]]
    end

    def contains?(key)
      key.start_with? @raw_prefix
    end

    def as_foundationdb_key
      key
    end

    def subspace(tuple)
      Subspace.new(tuple, @raw_prefix)
    end
  end
end
