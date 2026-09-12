# AcAC -- Actor Active Context (retired)

AcAC recorded actor lineage for the former Flow actor compiler. The compiler
generated helpers that assigned IDs to active actors, tracked their spawners and
execution contexts, and mapped UUIDs to actor names in build-generated
`.uid` files. On a `SevError` trace, AcAC encoded the current actor lineage
in a base64 `ActorStack` field. Its decoder used the `.uid` files to print
actor names alongside the IDs. It could also dump all active actors or the
current execution stack on demand.

The feature helped explain call stacks produced when a resolved promise
recursively resumed waiting actors. A historical decoded trace looked like:

```text
     5813420 /root/src/fdbserver/storageserver.actor.cpp:getValueQ  <ACTIVE>
      447613 /root/src/fdbserver/storageserver.actor.cpp:serveGetValueRequests
      447600 /root/src/fdbserver/storageserver.actor.cpp:storageServerCore
      446989 /root/src/fdbserver/storageserver.actor.cpp:storageServer
```

The actor compiler, its context instrumentation, and its `.uid` mappings are
gone in the C++ coroutine codebase. AcAC's optional `WITH_ACAC` build mode,
`acac` decoder, context API, and `ActorStack` trace field have been removed.
Historical traces may still contain `ActorStack`; the example above describes
the former compiler-generated actors, not current coroutine source. Ordinary
`SevError` backtraces remain available.
