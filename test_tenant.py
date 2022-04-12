#!/usr/bin/env python3

import fdb
import sys

fdb.api_version(710)
db=fdb.open()

db.options.set_transaction_timeout(2000)

#tenant = b'tenant'
#tenant2 = b'tenant2'
#tenant3 = b'tenant3'

tenant = (u"tenant",)
tenant2 = (u"tenant2",)
tenant3 = (u"tenant3",)

fdb.tenant_management.create_tenant(db, tenant)
fdb.tenant_management.create_tenant(db, tenant2)
fdb.tenant_management.create_tenant(db, tenant3)

res = fdb.tenant_management.list_tenants(db, (u"a",), (u"z",), 10)
#res = fdb.tenant_management.list_tenants(db, b'a', b'z', 10)
for t in res:
    print(t.key.decode())
    print(t.value.decode())

fdb.tenant_management.delete_tenant(db, tenant)
fdb.tenant_management.delete_tenant(db, tenant2)
fdb.tenant_management.delete_tenant(db, tenant3)

sys.exit(0)
