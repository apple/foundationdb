.. -*- mode: rst; -*-

.. |platform-not-supported-for-production| replace::
    This version of FoundationDB server is intended only for locally available operation for development purposes. Externally available operation, performance testing, and production use are supported by the :doc:`Linux<getting-started-linux>` version.

.. |cluster-file-rule1| replace::
    The ``description`` is a logical description of the database using alphanumeric characters (a-z, A-Z, 0-9) and underscores.

.. |cluster-file-rule2| replace::
    The ``ID`` is an arbitrary value containing alphanumeric characters (a-z, A-Z, 0-9). We recommend using a random eight-character identifier (such as the output of ``mktemp -u XXXXXXXX``). Note that the ``ID`` will change when coordinators change.

.. |cluster-file-rule3| replace::
    The list of ``IP:PORT`` pairs specify the set of coordination servers. A majority of these servers must be available for the database to be operational so they should be chosen carefully. The number of coordination servers should therefore be odd and must be more than one to support fault-tolerance. We recommend using five coordination servers when using ``triple`` mode to maintain the ability to tolerate two simultaneous machine failures.

.. |simple-installation-mode-warnings| replace::
    FoundationDB installs in a single-server mode appropriate for a development workstation. In this mode data is not replicated, and therefore the database is **not failure tolerant**. This default database is also configured to use the memory storage engine which, while using the disk for durability, **requires data to fit in RAM**.

.. |networking-clarification| replace::
    By default FoundationDB uses the loopback IP (``127.0.0.1``). In this configuration all parts of FoundationDB, **including client applications**, must run on the same machine and communicate via ``127.0.0.1``, not via external IPs.

.. |development-use-only-warning| replace::
    The macOS version of the FoundationDB server is intended for single-machine development use only; its use in multi-machine clusters is not supported. In the present release, the Linux version is the best-tested and most performant platform for multi-machine clusters.

.. |upgrade-client-server-warning| replace::
    Unless using the :ref:`multi-version-client-api`, the installed client and server packages must have the same version. When upgrading a FoundationDB cluster, be sure also to upgrade all clients to the same version.

.. |optimize-configuration| replace::
	The default installation runs only one FoundationDB server process per machine (which will use only one CPU core). Most users of multi-machine configurations will want to maximize performance by running one FoundationDB server process per core. This is accomplished by modifying the :ref:`configuration file <foundationdb-conf>` (located at ``/etc/foundationdb/foundationdb.conf``) to have ``[fdbserver.<ID>]`` sections for each core. Note that 4GiB ECC RAM are required per FoundationDB server process (see :ref:`system-requirements`).

.. |coordinators-auto| replace::
    ``coordinators auto`` selects processes based on IP address. If your cluster has processes on the same machine with different IP addresses, ``coordinators auto`` may select a set of coordinators that are not fault tolerant. To ensure maximal fault tolerance, we recommend selecting coordinators according to the criteria in :ref:`configuration-choosing-coordination-servers` and setting them manually.

.. |conf-file-change-detection| replace::
    Whenever the ``foundationdb.conf`` file changes, the ``fdbmonitor`` daemon automatically detects the changes and starts, stops, or restarts child processes as necessary.  Note that changes to the configuration file contents must be made *atomically*.  It is recommended to save the modified file to a temporary filename and then move/rename it into place, replacing the original.  Some text editors do this automatically when saving.

.. |package-deb-clients| replace::
    foundationdb-clients\_\ |release|\ -1\_amd64.deb

.. |package-deb-server| replace::
    foundationdb-server\_\ |release|\ -1\_amd64.deb

.. |package-rpm-clients| replace::
    foundationdb-clients-|release|\ -1.el9.x86_64.rpm

.. |package-rpm-server| replace::
    foundationdb-server-|release|\ -1.el9.x86_64.rpm

.. |package-mac| replace::
    FoundationDB-|release|.pkg

.. |package-win| replace::
    foundationdb-|release|\ -x64.msi
