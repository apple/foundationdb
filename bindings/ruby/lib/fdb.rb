#
# fdb.rb
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

module FDB
  @@chosen_version = -1
  def self.is_api_version_selected?()
    @@chosen_version >= 0
  end
  def self.get_api_version()
    if self.is_api_version_selected?()
      return @@chosen_version
    else
      raise "FDB API version not selected"
    end
  end
  def self.api_version(version)
    header_version = 720
    if self.is_api_version_selected?()
      if @@chosen_version != version
        raise "FDB API already loaded at version #{@@chosen_version}."
      end
      return
    end

    if version < 14
      raise "FDB API versions before 14 are not supported"
    end

    if version > header_version
      raise "Latest known FDB API version is #{header_version}"
    end

    @@chosen_version = version

    require_relative 'fdbimpl'

    err = FDBC.fdb_select_api_version_impl(version, header_version)
    if err.nonzero?
      if err == 2203
        max_supported_version = FDBC.fdb_get_max_api_version()
        if header_version > max_supported_version
          raise "This version of the FoundationDB Ruby binding is not supported by the installed FoundationDB C library. The binding requires a library that supports API version #{header_version}, but the installed library supports a maximum version of #{max_supported_version}."

        else
          raise "API version #{version} is not supported by the installed FoundationDB C library."
        end
      end
      raise "FoundationDB API version error"
    end

    FDBC.init_c_api()

    require_relative 'fdbtuple'
    require_relative 'fdbdirectory'

    if version < 610
      require_relative 'fdbimpl_v609'
    end

    if version > 22
      require_relative 'fdblocality'
    end

    return
  end
end
