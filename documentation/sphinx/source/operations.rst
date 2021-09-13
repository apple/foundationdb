:orphan:

##########
Operations
##########

Ready to operate an externally accessible FoundationDB cluster? You'll find what you need to know here.

* :doc:`building-cluster` walks you through installation of an externally accessible cluster on one or more machines. Using FoundationDB in this way is not supported on macOS.

* :doc:`configuration` contains *reference* information for configuring a new cluster. You should read this document before setting up a cluster for performance testing or production use.

* :doc:`administration` covers administration of an *existing* externally accessible cluster.

* :doc:`command-line-interface` covers use of the ``fdbcli`` tool.

* :doc:`mr-status` describes the JSON encoding of a cluster's status information.

* :doc:`tls` describes the Transport Layer Security (TLS) capabilities of FoundationDB, which enable security and authentication through a public/private key infrastructure.

* :doc:`backups` covers the FoundationDB backup tool, which provides an additional level of protection by supporting recovery from disasters or unintentional modification of the database.

* :doc:`disk-snapshot-backup` covers disk snapshot based FoundationDB backup tool, which is an alternate backup solution.

* :doc:`platforms` describes issues on particular platforms that affect the operation of FoundationDB.

* :doc:`transaction-tagging` gives an overview of transaction tagging, including details about throttling particular tags.

* :doc:`tss` gives an overview of the Testing Storage Server feature of FoundationDB, which allows you to safely run an untrusted storage engine in a production cluster.

* :doc:`perpetual-storage-wiggle` gives an overview of Perpetual Storage Wiggle feature about how to use it.

.. toctree::
 :maxdepth: 2
 :titlesonly:
 :hidden:

 building-cluster
 configuration
 administration
 command-line-interface
 mr-status
 tls
 backups
 disk-snapshot-backup
 platforms
 transaction-tagging
 tss
 perpetual-storage-wiggle
