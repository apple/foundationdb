.. _consistency-scan:

############################
Consistency Scan
############################

.. include:: guide-common.rst.inc

This document covers the concept and usage of the consistency scan feature.

.. _consistency-scan-introduction:

Summary
============
 The consistency scan reads all replicas of each shard to verify data consistency at a configured rate.  It is useful for finding corrupt cold data by ensuring that all data is read periodically.  Any errors found will be logged as TraceEvents with Severity = 40.

Configuration
=============

You can configure the Consistency Scan via the FDB :ref:`command line interface <command-line-interface>` using the ``consistencyscan`` command.


Example commands
----------------

Enable consistency scan to scan the cluster once every 28 days with a maximum speed of 5MB/s: ``consistencyscan on maxRate 5000000 targetInterval 2419200``.

Disable consistency scan on the cluster: ``consistencyscan off``.

View the current stats for the consistency scan: ``consistencyscan stats``.

Monitor
=======

The consistency scan role publishes its configuration and metrics in Status JSON under the path ``.cluster.consistency_scan``.

Trace Events
----------------------
``ConsistencyScanMetrics`` show the over-time stats of the consistency scan's behavior, including bytes scanned, error counts, inconsistencies found, etc...
