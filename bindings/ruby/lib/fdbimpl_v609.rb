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

module FDB
  class << self
    alias_method :open_impl, :open
    def open( cluster_file = nil, database_name = "DB" )
      if database_name != "DB"
        raise Error.new(2013) # invalid_database_name
      end

      open_impl(cluster_file)
    end

    def create_cluster(cluster_file_path=nil)
      Cluster.new cluster_file_path
    end

    public :init
  end

  class ClusterOptions
  end

  class Cluster < FormerFuture
    attr_reader :options

    def initialize(cluster_file_path)
      @cluster_file_path = cluster_file_path
      @options = ClusterOptions.new
    end

    def open_database(name="DB")
      FDB.open(@cluster_file_path, name)
    end
  end

end
