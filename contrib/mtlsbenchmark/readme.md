# Testing TLS in Microbenchmarking

A testing framework for benchmarking TLS performance in peer-to-peer network scenarios

## Prerequisites

- OpenSSL or compatible tool for certificate generation
- Environment for FoundationDB unit test execution

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

The test scripts support two unit tests that can be configured:

| Test Mode | Purpose | Configuration (set by -f) |
|-----------|---------|---------------|
| **Long Running** | Testing with connections and messages | `:/network/p2ptest` |
| **One Shot** | One-time connection only and no message | `:/network/p2poneshottest` |

Set the desired test mode in your script before running.

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



