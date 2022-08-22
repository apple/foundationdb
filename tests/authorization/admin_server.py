from multiprocessing import Pipe, Process
from typing import Union, List
from util import to_str, to_bytes, cleanup_tenant

class _admin_request(object):
    def __init__(self, op: str, args: List[Union[str, bytes]]=[]):
        self.op = op
        self.args = args

    def __str__(self):
        return f"admin_request({self.op}, {self.args})"

    def __repr__(self):
        return f"admin_request({self.op}, {self.args})"

def _admin_loop(pipe):
    db = None
    while True:
        req = pipe.recv()
        if not isinstance(req, request):
            pipe.send(TypeError("unexpected type {}".format(type(req))))
            continue
        op = req.op
        args = req.args
        resp = True
        try:
            if op == "connect":
                fdb.options.set_trace_enable("admin_server_as_fdb_client")
                db = fdb.open(req.args[0])
            elif op == "configure_tls":
                keyfile, certfile, cafile = req.args[:3]
                fdb.options.set_tls_key_path(keyfile)
                fdb.options.set_tls_cert_path(certfile)
                fdb.options.set_tls_ca_path(cafile)
            elif op == "create_tenant":
                if db is None:
                    resp = Exception("db not open")
                else:
                    for tenant in req.args:
                        tenant_str = to_str(tenant)
                        tenant_bytes = to_bytes(tenant)
                        fdb.tenant_management.create_tenant(db, tenant_bytes)
                        print("created tenant: {}".format(tenant_str))
            elif op == "delete_tenant":
                if db is None:
                    resp = Exception("db not open")
                else:
                    for tenant in req.args:
                        tenant_str = to_str(tenant)
                        tenant_bytes = to_bytes(tenant)
                        cleanup_tenant(tenant_bytes)
                        print("deleted tenant: {}".format(tenant_str))
            elif op == "cleanup_database":
                if db is None:
                    resp Exception("db not open")
                else:
                    print("initiating database cleanup")
                    tr = db.create_transaction()
                    del tr[b"\\x00":b"\\xff"]
                    tr.commit().wait()
                    print("global keyspace cleared, deleting tenants")
                    tenants = fdb.tenant_management.list_tenants(db)
                    print("active tenants: {}".format(map(to_str, tenants)))
                    for tenant in tenants:
                        fdb.tenant_management.delete_tenant(db, tenant)
                        print("tenant {} deleted".format(to_str(tenant)))
            else:
                resp = ValueError("unknown operation: {}".format(req))
        except EOFError:
            print("test process has closed pipe to admin. exiting.")
            break
        except Exception as e:
            resp = e
        pipe.send(resp)

_admin_server = None

def get():
    return _admin_server

class AdminServer(object):
    def __init__(self):
        assert __name__ == "__main__"
        assert _admin_server is None, "admin server may be setup once per process"
        self._main_pipe, self._admin_pipe = Pipe()

    def start(self):
        self._admin_proc = Process(target=_admin_loop, args=(admin_pipe,))

    def join(self):
        self._main_pipe.close()
        self._admin_proc.join()

    def __enter__(self):
        self.start()

    def __exit__(self):
        self.join()

    def request(op, args=[]):
        req = _admin_request(op, args)
        try:
            self._main_pipe.send(req)
            resp = self._main_pipe.recv()
            if resp != True:
                print("Request {} failed: {}".format(req, resp))
                raise resp
            else:
                print("Request {} succeeded".format(req))
        except ExceptionBase as e:
            print("admin request {} failed by exception: {}".format(req, e))
            raise
