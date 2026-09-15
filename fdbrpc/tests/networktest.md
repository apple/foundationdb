# Network diagnostics

`fdbrpc_network_test` runs RPC and socket diagnostics using Flow and fdbrpc,
without linking the database client or server. Build it with:

```sh
cmake --build build --target fdbrpc_network_test
```

The executable is `build/bin/fdbrpc_network_test`. It is a development tool;
copy it to each participating host when testing across machines.

## RPC request/reply

Start a server, then run a client in another terminal:

```sh
build/bin/fdbrpc_network_test --mode server --public-address 127.0.0.1:4500
build/bin/fdbrpc_network_test --mode client --testservers 127.0.0.1:4500 \
  --knob_network_test_script_mode=true
```

The client supports comma-separated server addresses. Flow knobs retain their
existing names, including `network_test_request_size`, `network_test_reply_size`,
`network_test_client_count`, and `network_test_request_count`. Script mode reports
one measurement after a warmup interval and then finishes. Without a request
limit or script mode, the client runs until stopped. The server runs until stopped.

`--listen-address` overrides the bind address while `--public-address` specifies
the advertised endpoint. Use `--help` for TLS and tracing options.

## Raw connections and TLS

P2P mode can listen, connect, or do both. For example, a bounded loopback run:

```sh
build/bin/fdbrpc_network_test --mode p2p \
  --test_listenerAddresses=127.0.0.1:4501 \
  --test_remoteAddresses=127.0.0.1:4501 \
  --test_connectionsOut=2 --test_targetDuration=5
```

The existing `--test_*` parameters control payload size ranges, requests per
connection, and delays before reads, writes, and connection close. A duration
of zero means run until stopped. `--mode p2p-oneshot` tests one connection and
handshake without sending the traffic workload; run its listener and connector
as separate processes.

Append `:tls` to addresses to enable TLS and supply `--tls_certificate_file`,
`--tls_key_file`, `--tls_ca_file`, and `--tls_verify_peers` as appropriate. See
[`contrib/mtlsbenchmark`](../../contrib/mtlsbenchmark/readme.md) for a two-process
TLS example with handshake knobs.

## Migration from fdbserver

| Previous invocation | Standalone invocation |
| --- | --- |
| `fdbserver -r networktestserver` | `fdbrpc_network_test --mode server` |
| `fdbserver -r networktestclient` | `fdbrpc_network_test --mode client` |
| `fdbserver -r unittests -f :/network/p2ptest` | `fdbrpc_network_test --mode p2p` |
| `fdbserver -r unittests -f :/network/p2poneshottest` | `fdbrpc_network_test --mode p2p-oneshot` |

The networking options above retain their names. The old server roles and P2P
unit-test registrations are removed. The standalone target's CTest entry runs a
bounded loopback smoke test; it does not start an indefinite benchmark.
