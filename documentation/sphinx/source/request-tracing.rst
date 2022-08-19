.. _request-tracing:

###############
Request Tracing
###############

The request tracing framework adds the ability to monitor transactions as they
move through FoundationDB. Tracing provides a detailed view into where
transactions spend time with data exported in near real-time, enabling fast
performance debugging. The FoundationDB tracing framework is based off the
`OpenTracing <https://opentracing.io/>`_ specification.

*Disambiguation:* :ref:`Trace files <administration-managing-trace-files>` are
local log files containing debug and error output from a local ``fdbserver``
binary. Request tracing produces similarly named *traces* which record the
amount of time a transaction spent in a part of the system. This document uses
the term tracing (or trace) to refer to these request traces, not local debug
information, unless otherwise specified.

*Note*: Full request tracing capability requires at least ``TLogVersion::V6``.

==============
Recording data
==============

The request tracing framework produces no data by default. To enable collection
of traces, specify the collection type using the ``--tracer`` command line
option for ``fdbserver`` and the ``DISTRIBUTED_CLIENT_TRACER`` :ref:`network
option <network-options-using-environment-variables>` for clients. Both client
and server must have the same trace value set to perform correctly.

========================= ===============
**Option**                **Description**
------------------------- ---------------
none                      No tracing data is collected.
file, logfile, log_file   Write tracing data to FDB trace files, specified with ``--logdir``.
network_lossy             Send tracing data as UDP packets. Data is sent to ``localhost:8889``, but the default port can be changed by setting the ``TRACING_UDP_LISTENER_PORT`` knob. This option is useful if you have a log aggregation program to collect trace data.
========================= ===============

-----------
Data format
-----------

Spans are the building blocks of traces. A span represents an operation in the
life of a transaction, including the start and end timestamp and an operation.
A collection of spans make up a trace, representing a single transaction. The
tracing framework outputs individual spans, which can be reconstructed into
traces through their parent relationships.

Trace data sent as UDP packets when using the ``network_lossy`` option is
serialized using `MessagePack <https://msgpack.org>`_. To save on the amount of
data sent, spans are serialized as an array of length 8 (if the span has one or
more parents), or length 7 (if the span has no parents).

The fields of a span are specified below. The index at which the field appears
in the serialized msgpack array is also specified, for those using the UDP
collection format.

================== ========= ======== ===============
**Field**          **Index** **Type** **Description**
------------------ --------- -------- ---------------
Source IP:port     0         string   The IP and port of the machine where the span originated.
Trace ID           1         uint64   The 64-bit identifier of the trace. All spans in a trace share the same trace ID.
Span ID            2         uint64   The 64-bit identifier of the span. All spans have a unique identifier.
Start timestamp    3         double   The timestamp when the operation represented by the span began.
Duration           4         double   The duration in seconds of the operation represented by the span.
Operation name     5         string   The name of the operation the span represents.
Tags               6         map      User defined tags, added manually to specify additional information.
Parent span IDs    7         vector   (Optional) A list of span IDs representing parents of this span.
================== ========= ======== ===============

^^^^^^^^^^^^^^^^^^^^^
Multiple parent spans
^^^^^^^^^^^^^^^^^^^^^

Unlike traditional distributed tracing frameworks, FoundationDB spans can have
multiple parents. Because many FDB transactions are batched into a single
transaction, to continue tracing the request, the batched transaction must
treat all its component transactions as parents.

---------------
Control options
---------------

In addition to the command line parameter described above, tracing can be set
at a database and transaction level.

Tracing can be controlled on a global level by setting the
``TRACING_SAMPLE_RATE`` knob. Set the knob to 0.0 to record no traces, to 1.0
to record all traces, or somewhere in the middle. Traces are sampled as a unit.
All individual spans in the trace will be included in the sample.

Tracing can be enabled or disabled for individual transactions. The special key
space exposes an API to set a custom trace ID for a transaction, or to disable
tracing for the transaction. See the special key space :ref:`tracing module
documentation <special-key-space-tracing-module>` to learn more.
