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

Order storage servers by process id. For each storage server n:

a. Exclude storage server n.

b. Wait until all data has been moved off the storage server.

c. Include storage n

Goto a to wiggle the next storage process with different process id.

With a perpetual wiggle, storage migrations will be much less impactful. The wiggler will detect the healthy status based on healthy teams, available disk space and the number of unhealthy relocations. It will pause the wiggle until the cluster is healthy again.

Configuration
=============

You can configure the Perpetual Storage Wiggle via the FDB :ref:`command line interface <command-line-interface>`.

Note that to have the Perpetual Storage Wiggle change the storage engine type, you must configure ``storage_migration_type=gradual``.

Example commands
----------------

Open perpetual storage wiggle: ``configure perpetual_storage_wiggle=1``.

Disable perpetual storage wiggle on the cluster: ``configure perpetual_storage_wiggle=0``.

Monitor
=======

The ``status`` command in the FDB :ref:`command line interface <command-line-interface>` will show the current perpetual_storage_wiggle value.

Trace Events
----------------------
``PerpetualStorageWiggleOpen`` shows up when you switch on perpetual storage wiggle, while ``PerpetualStorageWiggleClose`` appears when you turn it off;

``PerpetualStorageWiggleStart`` event means the wiggler start wiggling 1 process, it also contains the process id of the wiggling process how many healthy teams we has now. It's worthy to note ``ExtraHealthyTeamCount`` that indicates how many healthy team we need to restart the paused wiggler and ``HealthyTeamCount``. If ``ExtraHealthyTeamCount`` keep being larger than team count, then you may need to add more storage server.

``PerpetualStorageWigglePause`` event shows up when the wiggler pause because it detect the cluster is unhealthy;

``PerpetualStorageWiggleFinish`` event indicates the wiggle is done on current process.

In ``MovingData`` event, the field ``PriorityStorageWiggle`` shows how many relocations are in the queue because storage wiggle.
