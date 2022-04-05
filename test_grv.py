#!/usr/bin/env python3

import fdb
import sys

# As environment variable
# export FDB_NETWORK_OPTION_CLIENT_THREADS_PER_VERSION=2

fdb.api_version(710)
fdb.options.set_trace_enable()
fdb.options.set_trace_format("json")
fdb.options.set_trace_max_logs_size(2**20)
fdb.options.set_external_client_library("libfdb_c_old.so")
fdb.options.set_external_client_library("libfdb_c_new.so")

db1=fdb.open()
db2=fdb.open()

db1.options.set_transaction_timeout(2000)
db2.options.set_transaction_timeout(2000)

i = 0
key = b'foo'
while True:
    i += 1
    if i % 2 == 0:
        while True:
            tr1 = db1.create_transaction()
            try:
                tr1.options.set_use_grv_cache()
                tr1.get_read_version()
                tr1[key] = i.to_bytes(10, byteorder='big')
                tr1.commit().wait()
                break
            except fdb.FDBError as e:
                tr1.on_error(e).wait()
                print("Error in tr1 occurred. Retrying.")
    else:
        while True:
            tr2 = db2.create_transaction()
            try:
                tr2.options.set_use_grv_cache()
                tr2.get_read_version()
                tr2[key] = i.to_bytes(10, byteorder='big')
                tr2.commit().wait()
                break
            except fdb.FDBError as e:
                tr2.on_error(e).wait()
                print("Error in tr2 occurred. Retrying.")