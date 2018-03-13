
###############
Platform Issues
###############

.. include:: guide-common.rst.inc

This document describes issues on particular platforms that affect the operation of FoundationDB. See :doc:`known-limitations` for broader limitations relating to FoundationDB's design or its current version.

.. _platform-centos-gce:

CentOS 6.2 on GCE
=================

In a newly created instance, the CentOS 6.2 image on Google Compute Engine fails to mount the /dev/shm filesystem (used to implement POSIX shared memory) but still reports it as mounted. This bug causes any application that requires access to shared memory, including FoundationDB, to fail to function.

To resolve this issue, you may do one of the following:

* Reboot the newly created instance.
* Attempt to unmount the /dev/shm filesystem with ``sudo umount /dev/shm``, which will report that the filesystem is not mounted, and then mount the /dev/shm filesystem with ``sudo mount /dev/shm``, which will succeed.
* Specify a machine ID in the :ref:`foundationdb.conf <foundationdb-conf-fdbserver>` file, at which point FoundationDB will not attempt to use shared memory.

.. _platform-ubuntu-12:

Ubuntu 12.x
===========

Because of a `bug in the Linux kernel <https://bugzilla.kernel.org/show_bug.cgi?id=43260>`_, **FoundationDB might deadlock when running on Ubuntu 12.04 or 12.10** using the default ext4 filesystem. This was fixed in the 3.7 kernel (released 12/10/2012) thanks to the `hard work of Dmitry Monakhov <http://lkml.indiana.edu/hypermail/linux/kernel/1210.0/03434.html>`_. Versions of Ubuntu 12.04 starting at 12.04.3 use the fixed kernel and are safe to use.

.. _platform-virtual-box:

VirtualBox 
==========

Running FoundationDB on VirtualBox may result in high idle CPU usage.
