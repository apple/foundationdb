Overview
--------

Tenant testing is an optional extension to the core binding tester that enables
testing of the tenant API. This testing is enabled by adding some additional 
instructions and modifying the behavior of some existing instructions.

Additional State and Initialization
-----------------------------------

Your tester should store an additional piece of state tracking the active tenant
that is to be used to create transactions. This tenant must support an unset 
state, in which case transactions will be created directly on the database.

New Instructions
----------------

The tenant API introduces some new operations:

#### TENANT_CREATE

    Pops the top item off of the stack as TENANT_NAME. Creates a new tenant
    in the database with the name TENANT_NAME. May optionally push a future
    onto the stack.

#### TENANT_DELETE

    Pops the top item off of the stack as TENANT_NAME. Deletes the tenant with
    the name TENANT_NAME from the database. May optionally push a future onto 
    the stack.

#### TENANT_SET_ACTIVE

    Pops the top item off of the stack as TENANT_NAME. Opens the tenant with
    name TENANT_NAME and stores it as the active tenant.

#### TENANT_CLEAR_ACTIVE

    Unsets the active tenant.

Updates to Existing Instructions
--------------------------------

Some existing operations in the binding tester will have slightly modified
behavior when tenants are enabled.

#### NEW_TRANSACTION

    When creating a new transaction, the active tenant should be used. If no active
    tenant is set, then the transaction should be created as normal using the
    database.

#### _TENANT suffix

    Similar to the _DATABASE suffix, an operation with the _TENANT suffix indicates 
    that the operation should be performed on the current active tenant object. If 
    there is no active tenant, then the operation should be performed on the database 
    as if _DATABASE was specified. In any case where the operation suffixed with
    _DATABASE is allowed to push a future onto the stack, the same operation suffixed
    with _TENANT is also allowed to push a future onto the stack.

    If your binding does not support operations directly on a tenant object, you should
    simulate it using an anonymous transaction. Remember that set and clear operations
    must immediately commit (with appropriate retry behavior!).

    Operations that can include the _TENANT prefix are:

        GET_TENANT
        GET_KEY_TENANT
        GET_RANGE_TENANT
        GET_RANGE_STARTS_WITH_TENANT
        GET_RANGE_SELECTOR_TENANT
        SET_TENANT
        CLEAR_TENANT
        CLEAR_RANGE_TENANT
        CLEAR_RANGE_STARTS_WITH_TENANT
        ATOMIC_OP_TENANT
