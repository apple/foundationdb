#encoding: BINARY

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

# FoundationDB Ruby API

# Documentation for this API can be found at
# https://apple.github.io/foundationdb/api-ruby.html

module FDB
  module Tuple
    @@size_limits = (0..8).map {|x| (1 << (x*8)) - 1}

    # Type codes 
    @@NULL_CODE       = 0x00
    @@BYTES_CODE      = 0x01
    @@STRING_CODE     = 0x02
    @@NESTED_CODE     = 0x05
    @@INT_ZERO_CODE   = 0x14
    @@POS_INT_END     = 0x1d
    @@NEG_INT_START   = 0x0b
    @@FLOAT_CODE      = 0x20
    @@DOUBLE_CODE     = 0x21
    @@FALSE_CODE      = 0x26
    @@TRUE_CODE       = 0x27
    @@UUID_CODE       = 0x30

    class UUID
      def initialize(data)
        if data.length != 16
          raise Error.new(2268) # invalid_uuid_size
        end
        @data=data.slice(0,16)
      end
      def data
        @data
      end
      def <=> (other)
        self.data <=> other.data
      end
      def to_s
        self.data.each_byte.map { |b| b.to_s(16) } .join
      end
    end

    class SingleFloat
      def initialize(value)
        if value.kind_of? Float
            @value=value
        elsif value.kind_of? Integer
            @value=value.to_f
        else
          raise ArgumentError, "Invalid value type for SingleFloat: " + value.class.name
        end
        @value=value
      end
      def value
        @value
      end
      def <=> (other)
        Tuple._compare_floats(self.value, other.value, false)
      end
      def to_s
        self.value.to_s
      end
    end

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

    def self.float_adjust(v, pos, length, encode)
      if (encode and v[pos].ord & 0x80 != 0x00) or (not encode and v[pos].ord & 0x80 == 0x00)
        v.slice(pos, length).chars.map { |b| (b.ord ^ 0xff).chr } .join
      else
        ret = v.slice(pos, length)
        ret[0] = (ret[0].ord ^ 0x80).chr
        ret
      end
    end
    private_class_method :float_adjust

    def self.decode(v, pos)
      code = v.getbyte(pos)
      if code == @@NULL_CODE
        [nil, pos+1]
      elsif code == @@BYTES_CODE
        epos = find_terminator(v, pos+1)
        [v.slice(pos+1, epos-pos-1).gsub("\x00\xFF", "\x00"), epos+1]
      elsif code == @@STRING_CODE
        epos = find_terminator(v, pos+1)
        [v.slice(pos+1, epos-pos-1).gsub("\x00\xFF", "\x00").force_encoding("UTF-8"), epos+1]
      elsif code >= @@INT_ZERO_CODE && code < @@POS_INT_END
        n = code - @@INT_ZERO_CODE
        [("\x00" * (8-n) + v.slice(pos+1, n)).unpack("Q>")[0], pos+n+1]
      elsif code > @@NEG_INT_START and code < @@INT_ZERO_CODE
        n = @@INT_ZERO_CODE - code
        [("\x00" * (8-n) + v.slice(pos+1, n)).unpack("Q>")[0]-@@size_limits[n], pos+n+1]
      elsif code == @@POS_INT_END
        length = v.getbyte(pos+1)
        val = 0
        length.times do |i|
          val = val << 8
          val += v.getbyte(pos+2+i)
        end
        [val, pos+length+2]
      elsif code == @@NEG_INT_START
        length = v.getbyte(pos+1) ^ 0xff
        val = 0
        length.times do |i|
          val = val << 8
          val += v.getbyte(pos+2+i)
        end
        [val - (1 << (length*8)) + 1, pos+length+2]
      elsif code == @@FALSE_CODE
        [false, pos+1]
      elsif code == @@TRUE_CODE
        [true, pos+1]
      elsif code == @@FLOAT_CODE
        [SingleFloat.new(float_adjust(v, pos+1, 4, false).unpack("g")[0]), pos+5]
      elsif code == @@DOUBLE_CODE
        [float_adjust(v, pos+1, 8, false).unpack("G")[0], pos+9]
      elsif code == @@UUID_CODE
        [UUID.new(v.slice(pos+1, 16)), pos+17]
      elsif code == @@NESTED_CODE
        epos = pos+1
        nested = []
        while epos < v.length
          if v.getbyte(epos) == @@NULL_CODE 
            if epos+1 < v.length and v.getbyte(epos+1) == 0xFF
              nested << nil
              epos += 2
            else
              break
            end
          else
            r, epos = decode(v, epos)
            nested << r
          end
        end
        [nested, epos+1]
      else
        raise "Unknown data type in DB: " + code.ord.to_s
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

    def self.encode(v, nested=false)
      if v.nil?
        if nested
          "\x00\xFF"
        else
          @@NULL_CODE.chr
        end
      elsif v.kind_of? String
        if v.encoding == Encoding::BINARY || v.encoding == Encoding::ASCII
          @@BYTES_CODE.chr + v.gsub("\x00", "\x00\xFF") + 0.chr
        elsif v.encoding == Encoding::UTF_8
          @@STRING_CODE.chr + v.dup.force_encoding("BINARY").gsub("\x00", "\x00\xFF") + 0.chr
        else
          raise ArgumentError, "unsupported encoding #{v.encoding.name}"
        end
      elsif v.kind_of? Integer
        raise RangeError, "Integer magnitude is too large (more than 255 bytes)" if v < -2**2040+1 || v > 2**2040-1
        if v == 0
          @@INT_ZERO_CODE.chr
        elsif v > 0
          if v > @@size_limits[-1]
            length = (v.bit_length + 7) / 8
            result = @@POS_INT_END.chr + length.chr
            length.times do |i|
              result << ((v >> (8 * (length-i-1))) & 0xff)
            end
            result
          else
            n = bisect_left( @@size_limits, v )
            (@@INT_ZERO_CODE+n).chr + [v].pack("Q>").slice(8-n, n)
          end
        else
          if -v > @@size_limits[-1]
            length = ((-v).bit_length + 7) / 8
            v += (1 << (length * 8)) - 1
            result = @@NEG_INT_START.chr + (length ^ 0xff).chr
            length.times do |i|
              result << ((v >> (8 * (length-i-1))) & 0xff)
            end
            result
          else
            n = bisect_left( @@size_limits, -v )
            (@@INT_ZERO_CODE-n).chr + [@@size_limits[n]+v].pack("Q>").slice(8-n, n)
          end
        end
      elsif v.kind_of? TrueClass
        @@TRUE_CODE.chr
      elsif v.kind_of? FalseClass
        @@FALSE_CODE.chr
      elsif v.kind_of? SingleFloat
        @@FLOAT_CODE.chr + float_adjust([v.value].pack("g"), 0, 4, true)
      elsif v.kind_of? Float
        @@DOUBLE_CODE.chr + float_adjust([v].pack("G"), 0, 8, true)
      elsif v.kind_of? UUID
        @@UUID_CODE.chr + v.data
      elsif v.kind_of? Array
        @@NESTED_CODE.chr + (v.map { |el| encode(el, true).force_encoding("BINARY") }).join + 0.chr
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

    def self._code_for(v)
      if v.nil?
        @@NULL_CODE
      elsif v.kind_of? String
        if v.encoding == Encoding::BINARY || v.encoding == Encoding::ASCII
          @@BYTES_CODE
        elsif v.encoding == Encoding::UTF_8
          @@STRING_CODE
        else
          raise ArgumentError, "unsupported encoding #{v.encoding.name}"
        end
      elsif v.kind_of? Integer
         @@INT_ZERO_CODE
      elsif v.kind_of? TrueClass
        @@TRUE_CODE
      elsif v.kind_of? FalseClass
        @@FALSE_CODE
      elsif v.kind_of? SingleFloat
        @@FLOAT_CODE
      elsif v.kind_of? Float
        @@DOUBLE_CODE
      elsif v.kind_of? UUID
        @@UUID_CODE
      elsif v.kind_of? Array
        @@NESTED_CODE
      else
        raise ArgumentError, "unsupported type #{v.class}"
      end
    end

    def self._compare_floats(f1, f2, is_double)
      # This converts the floats to their byte representation and then
      # does the comparison. Why?
      #   1) NaN comparison - Ruby doesn't really do this
      #   2) -0.0 == 0.0 in Ruby but not in our representation
      # It would be better to just take the floats and compare them, but
      # this way handles the edge cases correctly.
      b1 = float_adjust([f1].pack(is_double ? ">G" : ">g"), 0, (is_double ? 8 : 4), true)
      b2 = float_adjust([f2].pack(is_double ? ">G" : ">g"), 0, (is_double ? 8 : 4), true)
      b1 <=> b2
    end

    def self._compare_elems(v1, v2)
      c1 = _code_for(v1)
      c2 = _code_for(v2)
      return c1 <=> c2 unless c1 == c2

      if c1 == @@NULL_CODE
        0
      elsif c1 == @@DOUBLE_CODE
        _compare_floats(v1, v2, true)
      elsif c1 == @@NESTED_CODE
        compare(v1, v2) # recurse
      else
        v1 <=> v2
      end
    end

    def self.compare(tuple1, tuple2)
      i = 0
      while i < tuple1.length && i < tuple2.length
        c = self._compare_elems(tuple1[i], tuple2[i])
        return c unless c == 0
        i += 1
      end
      tuple1.length <=> tuple2.length
    end
  end
end
