.. _perpetual-storage-wiggle:

############################
Perpetual Storage Wiggle
############################

.. include:: guide-common.rst.inc

This document covers the concept and usage of perpetual storage wiggle.

.. _perpetual-storage-wiggle-introduction:

Summary
============
Perpetual storage wiggle is a feature that forces the data distributor to constantly build new storage teams when the cluster is healthy. On a high-level note, the process is like this:

Order storage servers by their created time, from oldest to newest. For each storage server n:

a. Exclude storage server n.

b. Wait until all data has been moved off the storage server.

c. Include storage n

Goto step a to wiggle the next storage server.

With a perpetual wiggle, storage migrations will be much less impactful. The wiggler will detect the healthy status based on healthy teams, available disk space and the number of unhealthy relocations. It will pause the wiggle until the cluster is healthy again.

Configuration
=============

You can configure the Perpetual Storage Wiggle via ``fdbcli`` :ref:`command line interface <command-line-interface>`.

To enable the Perpetual Storage Wiggle feature, you must configure ``storage_migration_type=gradual perpetual_storage_wiggle=1``.

If you want to migrate a subset of storage servers to the new storage engine, you should combine the command above with ``perpetual_storage_wiggle_engine=<NEW_STORAGE_ENGINE> perpetual_storage_wiggle_locality=<LOCALITY_KEY>:<LOCALITY_VALUE>`` to ensure that only the targeted storage servers make use of the new storage engine.

Example commands
----------------

Enable perpetual storage wiggle: ``configure storage_migration_type=gradual perpetual_storage_wiggle=1``.

Disable perpetual storage wiggle on the cluster: ``configure storage_migration_type=disabled perpetual_storage_wiggle=0``.

Enable perpetual storage wiggle only for processes matching the given locality key and value: ``configure perpetual_storage_wiggle=1 perpetual_storage_wiggle_locality=<LOCALITY_KEY>:<LOCALITY_VALUE>``.

Disable perpetual storage wiggle locality matching filter, which wiggles all the processes: ``configure perpetual_storage_wiggle_locality=0``.

Enable perpetual storage wiggle with new storage engine in the remote DC: ``configure perpetual_storage_wiggle_engine=<NEW_STORAGE_ENGINE> storage_migration_type=gradual perpetual_storage_wiggle=1 perpetual_storage_wiggle_locality=dcid:remote``.

Monitor
=======

* The ``status`` command will report the IP address of the Storage Server under wiggling.
* The ``status json`` command in the FDB :ref:`command line interface <command-line-interface>` will show the current ``perpetual_storage_wiggle`` value. Plus, the ``cluster.storage_wiggler`` field reports storage wiggle details.

Trace Events
----------------------
``PerpetualStorageWiggleOpen`` shows up when you switch on perpetual storage wiggle, while ``PerpetualStorageWiggleClose`` appears when you turn it off;

``PerpetualStorageWiggleStart`` event means the wiggler start wiggling 1 process, it also contains the process id of the wiggling process how many healthy teams we has now. It's worthy to note ``ExtraHealthyTeamCount`` that indicates how many healthy team we need to restart the paused wiggler and ``HealthyTeamCount``. If ``ExtraHealthyTeamCount`` keep being larger than team count, then you may need to add more storage server.

``PerpetualStorageWigglePause`` event shows up when the wiggler pause because it detect the cluster is unhealthy;

``PerpetualStorageWiggleFinish`` event indicates the wiggle is done on current process.

In ``MovingData`` event, the field ``PriorityStorageWiggle`` shows how many relocations are in the queue because storage wiggle.
