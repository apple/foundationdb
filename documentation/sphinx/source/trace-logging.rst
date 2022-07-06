#############
Trace Logging
#############

Overview
========

FoundationDB processes generate log files known as trace logs that include details about the sequence of events that a process performed and record a variety of metrics about that process. These log files are useful for observing the state of a cluster over time and for debugging issues that occur in the cluster, a client, or various tools.

Each file contains a sequence of events for a single process, ordered by time. Clients that use the multi-version or multi-threaded client features will generate :ref:`multiple log files simultaneously <mvc-logging>`. Each entry in the log file will be either an XML event or a JSON object that containts several mandatory fields and zero or more arbitrary fields. For example, the following is an event generated in the XML format::

<Event Severity="10" Time="1579736072.656689" Type="Net2Starting" ID="0000000000000000" Machine="1.1.1.1:4000" LogGroup="default"/>

Most FoundationDB processes generate trace logs in the format described in this document. This includes server and client processes, fdbcli, and backup and DR tooling.

Configuration
=============

File Format
-----------

FoundationDB trace logging

* Parameters
* Format

Trace Files
===========

* Filenames
* Rolling

Mandatory Fields
================

* Severity
* Time
* Type
* Machine
* LogGroup
* ID

Common Fields
=============

* Roles

Event Suppression
-----------------

* SuppressedEventCount

Errors
------

* Error
* ErrorDescription
* ErrorCode

Clients
-------

*

Rolled Events
=============

Counters
========

PeriodicEvents
==============

.. mvc-logging::

Multi-Version and Multi-Threaded Client Logging
===============================================
