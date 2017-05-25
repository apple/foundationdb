#
# fdbtuple.rb
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

#encoding: BINARY

# FoundationDB Ruby API

# Documentation for this API can be found at
# https://foundationdb.org/documentation/api-ruby.html

module FDB
  module Tuple
    @@size_limits = (0..8).map {|x| (1 << (x*8)) - 1}

    def self.find_terminator(v, pos)
      while true
        pos = v.index("\x00", pos)
        if !pos
          return v.length
        elsif pos+1 == v.length || v[pos+1] != "\xff"
          return pos
        end
        pos += 2
      end
    end
    private_class_method :find_terminator

    def self.decode(v, pos)
      code = v.getbyte(pos)
      if code == 0
        [nil, pos+1]
      elsif code == 1
        epos = find_terminator(v, pos+1)
        [v.slice(pos+1, epos-pos-1).gsub("\x00\xFF", "\x00"), epos+1]
      elsif code == 2
        epos = find_terminator(v, pos+1)
        [v.slice(pos+1, epos-pos-1).gsub("\x00\xFF", "\x00").force_encoding("UTF-8"), epos+1]
      elsif code >= 20 && code <= 28
        n = code - 20
        [("\x00" * (8-n) + v.slice(pos+1, n)).unpack("Q>")[0], pos+n+1]
      elsif code >= 12 and code < 20
        n = 20 - code
        [("\x00" * (8-n) + v.slice(pos+1, n)).unpack("Q>")[0]-@@size_limits[n], pos+n+1]
      else
        raise "Unknown data type in DB: " + v
      end
    end
    private_class_method :decode

    def self.bisect_left(list, item)
      count = 0
      list.each{|i|
        return count if i >= item
        count += 1
      }
      nil
    end
    private_class_method :bisect_left

    def self.encode(v)
      if !v
        "\x00"
      elsif v.kind_of? String
        if v.encoding == Encoding::BINARY || v.encoding == Encoding::ASCII
          1.chr + v.gsub("\x00", "\x00\xFF") + 0.chr
        elsif v.encoding == Encoding::UTF_8
          2.chr + v.dup.force_encoding("BINARY").gsub("\x00", "\x00\xFF") + 0.chr
        else
          raise ArgumentError, "unsupported encoding #{v.encoding.name}"
        end
      elsif v.kind_of? Integer
        raise RangeError, "value outside inclusive range -2**64+1 to 2**64-1" if v < -2**64+1 || v > 2**64-1
        if v == 0
          20.chr
        elsif v > 0
          n = bisect_left( @@size_limits, v )
          (20+n).chr + [v].pack("Q>").slice(8-n, n)
        else
          n = bisect_left( @@size_limits, -v )
          (20-n).chr + [@@size_limits[n]+v].pack("Q>").slice(8-n, n)
        end
      else
        raise ArgumentError, "unsupported type #{v.class}"
      end
    end
    private_class_method :encode

    def self.pack(t)
      (t.each_with_index.map {|el, i|
         begin
           (encode el).force_encoding("BINARY")
         rescue
           raise $!, "#{$!} at index #{i}", $!.backtrace
         end
       }).join
    end

    def self.unpack(key)
      key = key.dup.force_encoding("BINARY")
      pos = 0
      res = []
      while pos < key.length
        r, pos = decode(key, pos)
        res << r
      end
      res
    end

    def self.range(tuple=[])
      p = pack(tuple)
      [p+"\x00", p+"\xFF"]
    end
  end
end
