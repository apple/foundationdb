#!/usr/bin/python
#
# admin_server.py
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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
import fdb
from multiprocessing import Pipe, Process
from typing import Union, List
from util import to_str, to_bytes, cleanup_tenant

fdb.api_version(720)

class _admin_request(object):
    def __init__(self, op: str, args: List[Union[str, bytes]]=[]):
        self.op = op
        self.args = args

    def __str__(self):
        return f"admin_request({self.op}, {self.args})"

    def __repr__(self):
        return f"admin_request({self.op}, {self.args})"

def main_loop(main_pipe, pipe, external_lib_path):
    main_pipe.close()
    db = None
    while True:
        try:
            req = pipe.recv()
        except EOFError:
            return
        if not isinstance(req, _admin_request):
            pipe.send(TypeError("unexpected type {}".format(type(req))))
            continue
        op = req.op
        args = req.args
        resp = True
        try:
            if op == "connect":
                db = fdb.open(req.args[0])
            elif op == "configure_client":
                keyfile, certfile, cafile = req.args[:3]
                fdb.options.set_tls_key_path(keyfile)
                fdb.options.set_tls_cert_path(certfile)
                fdb.options.set_tls_ca_path(cafile)
                if external_lib_path:
                    fdb.options.set_external_client_library(external_lib_path)
            elif op == "create_tenant":
                if db is None:
                    resp = Exception("db not open")
                else:
                    for tenant in req.args:
                        tenant_str = to_str(tenant)
                        tenant_bytes = to_bytes(tenant)
                        fdb.tenant_management.create_tenant(db, tenant_bytes)
            elif op == "delete_tenant":
                if db is None:
                    resp = Exception("db not open")
                else:
                    for tenant in req.args:
                        tenant_str = to_str(tenant)
                        tenant_bytes = to_bytes(tenant)
                        cleanup_tenant(db, tenant_bytes)
            elif op == "cleanup_database":
                if db is None:
                    resp = Exception("db not open")
                else:
                    tr = db.create_transaction()
                    del tr[b'':b'\xff']
                    tr.commit().wait()
                    tenants = list(map(lambda x: x.key, list(fdb.tenant_management.list_tenants(db, b'', b'\xff', 0).to_list())))
                    for tenant in tenants:
                        fdb.tenant_management.delete_tenant(db, tenant)
            elif op == "terminate":
                pipe.send(True)
                return
            else:
                resp = ValueError("unknown operation: {}".format(req))
        except Exception as e:
            resp = e
        pipe.send(resp)

_admin_server = None

def get():
    return _admin_server

# server needs to be a singleton running in subprocess, because FDB network layer (including active TLS config) is a global var
class Server(object):
    def __init__(self, external_lib_path):
        global _admin_server
        assert _admin_server is None, "admin server may be setup once per process"
        _admin_server = self
        self._main_pipe, self._admin_pipe = Pipe(duplex=True)
        self._admin_proc = Process(target=main_loop, args=(self._main_pipe, self._admin_pipe, external_lib_path))

    def start(self):
        self._admin_proc.start()

    def join(self):
        self._main_pipe.close()
        self._admin_pipe.close()
        self._admin_proc.join()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.join()

    def request(self, op, args=[]):
        req = _admin_request(op, args)
        try:
            self._main_pipe.send(req)
            resp = self._main_pipe.recv()
            if resp != True:
                print("{} failed: {}".format(req, resp))
                raise resp
            else:
                print("{} succeeded".format(req))
        except Exception as e:
            print("{} failed by exception: {}".format(req, e))
            raise
