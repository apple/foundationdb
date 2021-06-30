################################
Moving a Cluster to New Machines
################################

Goal
====

Move a FoundationDB cluster to new machines.

Challenge
=========

You need to move an existing cluster to new machines, and you don't want to risk data loss or downtime during the process.

Explanation
===========

Use basic administrative commands to add new machines to your cluster, migrate the data, and remove the old machines.
Recipe

To move your cluster to new machines, perform the following steps:

1. Provision your new machines.

2. Install the FoundationDB packages on each of the new machines.

3. Stop the FoundationDB service on the new machines:

$ sudo service foundationdb stop

4. Copy the fdb.cluster file from any one of the old machines to each of the new machines. If you need to modify the default configuration (e.g. changing the data storage location), you should do so now.

5. Restart the FoundationDB service on each new machine::

    $ sudo service foundationdb start 

6. Start ``fdbcli`` and run ``status details``. This should show a number of processes equal to both old and new machines, with processes on the original machines serving as the cluster coordinators.

7. Exclude the original machines from the cluster using ``exclude`` in ``fdbcli``. This command will not return until all database state has been moved off of the original machines and fully replicated to the new machines. For example::

    fdb> exclude 192.168.1.1:4500 192.168.1.2:4500 192.168.1.3:4500 locality_dcid:primary-satellite locality_zoneid:primary-satellite-log-2 locality_machineid:primary-stateless-1 locality_processid:223be2da244ca0182375364e4d122c30 or any locality

8. Run ``coordinators auto`` in ``fdbcli`` to move coordination state to the new machines. Please note that this will cause the fdb.cluster file to be updated with the addresses of the new machines. Any currently connected clients will be notified and (assuming they have appropriate file system :ref:`permissions <cluster_file_permissions>`) will update their own copy of the cluster file. As long as the original machines are still running, any clients that connect to them will be automatically forwarded to the new cluster coordinators. However, if you have a client that has not yet connected or only connects intermittently, you will need to copy the new cluster file from one of the new machines to the client machine.

9. The ``status details`` command in the fdbcli will now show only the new processes (both as workers and coordinators), and you can safely shut down the older machines.
