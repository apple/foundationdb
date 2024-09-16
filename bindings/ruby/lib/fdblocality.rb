#encoding: BINARY

#
# fdblocality.rb
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
  module Locality
    def self.get_addresses_for_key(db_or_tr, key)
      key = FDB.key_to_bytes(key)
      db_or_tr.transact do |tr|
        FutureStringArray.new(FDBC.fdb_transaction_get_addresses_for_key(tr.tpointer, key, key.bytesize))
      end
    end

    def self.get_boundary_keys(db_or_tr, bkey, ekey)
      bkey = FDB.key_to_bytes(bkey).dup.force_encoding('BINARY')
      ekey = FDB.key_to_bytes(ekey).dup.force_encoding('BINARY')

      if db_or_tr.is_a? Transaction
        tr = db_or_tr.db.create_transaction
        tr.set_read_version db_or_tr.get_read_version
      else
        tr = db_or_tr.create_transaction
      end

      tr.options.set_read_system_keys
      tr.options.set_lock_aware
      lastbkey = bkey
      kvs = tr.snapshot.get_range("\xff/keyServers/"+bkey, "\xff/keyServers/"+ekey)

      y = Enumerator.new do |yielder|
        _tr = tr
        _bkey = bkey
        _ekey = ekey
        _lastbkey = lastbkey
        _kvs = kvs
        while _bkey < _ekey
          begin
            _kvs.each do |kv|
              yielder.yield kv.key.byteslice(13..-1)
              _bkey = kv.key.byteslice(13..-1) + "\x00"
            end
            _bkey = _ekey
          rescue FDB::Error => e
            if e.code == 1007 and _bkey != _lastbkey # if we get a transaction_too_old and *something* has happened, then we are no longer transactional
              _tr = _tr.db.create_transaction
            else
              _tr.on_error(e).wait
            end
            # we either created a new transaction or (implicitly) reset the one we had...
            _tr.options.set_read_system_keys
            _lastbkey = _bkey
            _kvs = _tr.snapshot.get_range("\xff/keyServers/" + _bkey, "\xff/keyServers/" + _ekey)
          end
        end
      end

      return y
    end
  end
end
