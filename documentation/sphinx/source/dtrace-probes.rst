#############
DTrace Probes
#############

FoundationDB contains many dtrace probes that can be inspected during
runtime with tools like bcc and SystemTap. All of them are in the
``foundationdb`` provider namespace.

``FDB_TRACE_PROBE`` is simply an alias to the varias ``DTRACE_PROBE``
macros.

Probes
======

Main-Loop
---------

.. code-block:: c

   FDB_TRACE_PROBE(run_loop_begin)

Is called whenever the main network loop starts over.

.. code-block:: c

   FDB_TRACE_PROBE(run_loop_ready_timers, numTimers)

On each iteration of the run-loop, this indicates how many timers (created through ``delay`` or ``yield``) are
ready. Its argument is of type ``int``.

.. code-block:: c

   FDB_TRACE_PROBE(run_loop_thread_ready, numReady)

On each loop-iteration. The second argument is of type ``int`` and it is the number of thread ready processes.

.. code-block:: c

   FDB_TRACE_PROBE(run_loop_yield)

Run loop yields.

.. code-block:: c

   FDB_TRACE_PROBE(run_loop_tasks_start, queueSize)

.. code-block:: c

   FDB_TRACE_PROBE(run_loop_done, queueSize)

One iteration of the run-loop is done. The argument is of type ``int`` and is the remaining number of tasks on the
ready queue.
