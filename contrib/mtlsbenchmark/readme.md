# Testing TLS in Microbenchmarking

A testing framework for benchmarking TLS performance in peer-to-peer network scenarios

## Prerequisites

- OpenSSL or compatible tool for certificate generation
- Build `fdbrpc_network_test` (`cmake --build build --target fdbrpc_network_test`).
- Set `FDBRPC_NETWORK_TEST` to the executable path if it differs from `/root/build_output/bin/fdbrpc_network_test`.

## Quick Start

### Step 1: Generate TLS Certificates

Generate the required certificate files:
- `ca_file.crt` - Certificate Authority file
- `certificate_file.crt` - Server/Client certificate
- `key_file.key` - Private key file

**References:**
- [Certificate Generation Guide](https://scriptcrunch.com/create-ca-tls-ssl-certificates-keys/)
- [FoundationDB TLS Documentation](https://apple.github.io/foundationdb/tls.html)

### Step 2: Configure Test in Scripts

The standalone network diagnostic supports two P2P modes:

| Test Mode | Purpose | Configuration (set by --mode) |
|-----------|---------|---------------|
| **Long Running** | Testing with connections and messages | `p2p` |
| **One Shot** | One-time connection only and no message | `p2p-oneshot` |

Set the desired test mode in your script before running. The `--test_*`,
`--knob_*`, and `--tls_*` options retain their meanings. These modes were
previously run through `fdbserver -r unittests`; they now use the standalone
[`fdbrpc_network_test`](../../fdbrpc/tests/networktest.md) executable.

## Folder Structure

```
.
├── server.sh           # Server startup script
├── client.sh           # Client startup script
└── keys
    ├── ca_file.crt         # Certificate Authority (generated)
    ├── certificate_file.crt # TLS Certificate (generated)
    └── key_file.key        # Private Key (generated)
```

### Step 3: Start the Server and Client(s)

#### Start the Server
```bash
bash server.sh
```

#### Start Client(s)
```bash
bash client.sh
```

> **Note:** You can start multiple clients for load testing scenarios.



