#######
Tenants
#######

.. _multi-tenancy:

.. warning :: Tenants are currently experimental and are not recommended for use in production.

FoundationDB provides a feature called tenants that allow you to configure one or more named transaction domains in your cluster. A transaction domain is a key-space in which a transaction is allowed to operate, and no tenant operations are allowed to use keys outside the tenant key-space. Tenants can be useful for managing separate, unrelated use-cases and preventing them from interfering with each other. They can also be helpful for defining safe boundaries when moving a subset of data between clusters.

By default, FoundationDB has a single transaction domain that contains both the normal key-space (``['', '\xff')``) as well as the system keys (``['\xff', '\xff\xff')``) and the :doc:`special-keys` (``['\xff\xff', '\xff\xff\xff')``).

Overview
========

A tenant in a FoundationDB cluster maps a byte-string name to a key-space that can be used to store data associated with that tenant. This key-space is stored in the clusters global key-space under a prefix assigned to that tenant, with each tenant being assigned a separate non-intersecting prefix.

Tenant operations are implicitly confined to the key-space associated with the tenant. It is not necessary for client applications to use or be aware of the prefix assigned to the tenant.

Enabling tenants
================

In order to use tenants, the cluster must be configured with an appropriate tenant mode using ``fdbcli``::

    fdb> configure tenant_mode=<MODE> 

FoundationDB clusters support the following tenant modes:

* ``disabled`` - Tenants cannot be created or used. Disabled is the default tenant mode.
* ``optional_experimental`` - Tenants can be created. Each transaction can choose whether or not to use a tenant. This mode is primarily intended for migration and testing purposes, and care should be taken to avoid conflicts between tenant and non-tenant data.
* ``required_experimental`` - Tenants can be created. Each normal transaction must use a tenant. To support special access needs, transactions will be permitted to access the raw key-space using the ``RAW_ACCESS`` transaction option.

Creating and deleting tenants
=============================

Tenants can be created and deleted using the ``\xff\xff/management/tenant/map/<tenant_name>`` :doc:`special key <special-keys>` range as well as by using APIs provided in some language bindings. 

Tenants can be created with any byte-string name that does not begin with the ``\xff`` character. Once created, a tenant will be assigned an ID and a prefix where its data will reside.

In order to delete a tenant, it must first be empty. If a tenant contains any keys, they must be cleared prior to deleting the tenant.

Using tenants
=============

In order to use the key-space associated with an existing tenant, you must open the tenant using the ``Database`` object provided by your language binding. The resulting ``Tenant`` object can be used to create transactions much like with a ``Database``, and the resulting transactions will be restricted to the tenant's key-space.

All operations performed within a tenant transaction will occur within the tenant key-space. It is not necessary to use or even be aware of the prefix assigned to a tenant in the global key-space. Operations that could resolve outside of the tenant key-space (e.g. resolving key selectors) will be clamped to the tenant.

.. note :: Tenant transactions are not permitted to access system keys.

Raw access
----------

When operating in the tenant mode ``required_experimental`` or using a metacluster, transactions are not ordinarily permitted to run without using a tenant. In order to access the system keys or perform maintenance operations that span multiple tenants, it is required to use the ``RAW_ACCESS`` transaction option to access the global key-space. It is an error to specify ``RAW_ACCESS`` on a transaction that is configured to use a tenant.

.. note :: Setting the ``READ_SYSTEM_KEYS`` or ``ACCESS_SYSTEM_KEYS`` options implies ``RAW_ACCESS`` for your transaction.

.. note :: Many :doc:`special keys <special-keys>` operations access parts of the system keys and will implictly enable raw access on the transactions in which they are used.

.. warning :: Care should be taken when using raw access to run transactions spanning multiple tenants if the tenant feature is being utilized to aid in moving data between clusters. In such scenarios, it may not be guaranteed that all of the data you intend to access is on a single cluster.
