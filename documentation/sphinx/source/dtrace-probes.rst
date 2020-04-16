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


Actors
------

.. code-block:: c

   FDB_TRACE_PROBE(actor_create, "actorname")
   FDB_TRACE_PROBE(actor_destroy, "actorname")

Get's called whenever an actor is created or gets destroyed. It provides one argument which is a
string and it is the name of the actor.

.. code-block:: c

   FDB_TRACE_PROBE(actor_enter, "name", index)
   FDB_TRACE_PROBE(actor_exit, "name", index)

Whenever we call into an actor (either directly through a function call or indirectly through a callback)
we call ``actor_enter``. Whenever we leave an actor (either because it returns or because it calls into
wait) we call ``actor_exit``. The first argument is a string of the name of the actor and the second is an
index. ``-1`` means that we entered/exited through in a main function call, otherwise it is a generated index.

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
