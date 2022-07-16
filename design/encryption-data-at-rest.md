# FDB Encryption **data at-rest**

## Threat Model

The proposed solution is `able to handle` the following attacks:

* An attacker, if able to get access to any FDB cluster host or attached disk, would not be able to read the persisted data. Further, for cloud deployments, returning a cloud instance back to the cloud provider will prevent the cloud provider from reading the contents of data stored on the disk.

* Data stored on a lost or stolen FDB host persistent disk storage device can’t be recovered.

The proposed solution `will not be able` to handle the following attacks:

* Encryption is enabled for data at-rest only, generating a memory dump of FDB processes could enable an attacker to read in-memory data contents.
* An FDB cluster host access, if compromised, would allow an attacker to read/write data managed by the FDB cluster.

## Goals

FoundationDB being a multi-model, easily scalable and fault-tolerant, with an ability to provide great performance even with commodity hardware, plays a critical role enabling enterprises to deploy, manage and run mission critical applications.

Data encryption support is a table-stake feature for modern day enterprise service offerings in the cloud. Customers expect, and at times warrant, that their data and metadata be fully encrypted using the latest security standards. The goal of this document includes:

* Discuss detailed design to support data at-rest encryption support for data stored in FDB clusters. Encrypting data in-transit and/or in-memory caches at various layers in the query execution pipeline (inside and external to FDB) is out of the scope of this feature.

* Isolation guarantees: the encryption domain matches with `tenant` partition semantics supported by FDB clusters. Tenants are discrete namespaces in FDB that serve as transaction domains. A tenant is a `identifier` that maps to a `prefix` within the data-FDB cluster, and all operations within a tenant are implicitly bound within a `tenant-prefix`. Refer to `Multi-Tenant FoundationDB API` documentation more details. However, it is possible to use a single encryption key for the whole cluster, in case `tenant partitioning` isn’t available.

* Ease of integration with external Key Management Services enabling persisting, caching, and lookup of encryption keys. 

## Config Knobs

* `ServerKnob::ENABLE_ENCRYPION` allows enable/disable encryption feature.
* `ServerKnob::ENCRYPTION_MODE` controls the encryption mode supported. The current scheme supports `AES-256-CTR` encryption mode.

## Encryption Mode

The proposal is to use strong AES-256 CTR encryption mode. Salient properties are:

* HMAC_SHA256 key hashing technique is used to derive encryption keys using a base encryption key and locally generated random number. The formula used is as follows:

```
                    DEK = HMAC SHA256(BEK || UID)

Where
DEK = Derived Encryption Key
BEK = Base Encryption key
UID  = Host local random generated number
```                    

UID is an 8 byte host-local random number. Another option would have been a simple host-local incrementing counter, however, the scheme runs the risk of repeated encryption-key generation on cluster/process restarts.

* An encryption key derived using the above formula will be cached (in-memory) for a short time interval (10 mins, for instance). The encryption-key is immutable, but, the TTL approach allows refreshing encryption key by reaching out to External Encryption KeyManagement solutions, hence, supporting “restricting lifetime of an encryption” feature if implemented by Encryption Key Management solution.

* Initialization Vector (IV) selection would be random.

## Architecture

The encryption responsibilities are split across multiple modules to ensure data and metadata stored in the cluster is never persisted in plain text on any durable storages (temporary and/or long-term durable storage).

## Encryption Request Workflow

### **Write Request**

* An FDB client initiates a write transaction providing {key, value} in plaintext format.
* An FDB cluster host as part of processing a write transaction would do the following:
    1. Obtain required encryption key based on the transaction request tenant information.
    2. Encrypt mutations before persisting them on Transaction Logs (TLogs). As a background process, the mutations are moved to a long-term durable storage by the Storage Server processes.

Refer to the sections below for more details.

### **Read Request**

* An FDB client initiates a read transaction request. 
* An FDB cluster host as part of processing request would do the following:
    1. StorageServer would read desired data blocks from the persistent storage.
    2. Regenerate the encryption key required to decrypt the data.
    3. Decrypt data and pass results as plaintext to the caller.


Below diagram depicts the end-to-end encryption workflow detailing various modules involved and their interactions. The following section discusses detailed design for involved components.

```
                                          _______________________________________________________
                                         |                FDB CLUSER HOST                        |
                                         |                                                       |
       _____________________             |   ________________________      _________________     |
      |                     | (proprietary) |                        |    |                 |    |
      |                     |<---------- |--|      KMS CONNECTOR     |    |  COMMIT PROXIES |    |
      |  ENCRYPTION KEY     |            |  |                        |    |                 |    |
      | MANAGEMENT SOLUTION |            |  |(non FDB - proprietary) |    |                 |    |
      |                     |            |  |________________________|    |_________________|    |
      |                     |            |              ^                        |               |
      |_____________________|            |              | (REST API)             | (Encrypt      |
                                         |              |                        V  Mutation)    |
                                         |      _________________________________________        |            __________________
                                         |     |                                         |       |           |                  |
                                         |     |           ENCRYPT KEYPROXY SERVER       |<------|-----------|                  |
                                         |     |_________________________________________|       |           |                  |
                                         |                          |                            |           |    BACKUP FILES  |
                                         |                          |  (Encrypt Node)            |           |                  |
                                         |                          V                            |           |                  |
                                         |      _________________________________________        |           |  (Encrypt file)  |
                                         |     |                                         |<------|-----------|                  |
                                         |     |      REDWOOD STORAGE SERVER             |       |           |__________________|
                                         |     |_________________________________________|       |
                                         |_______________________________________________________|
```

## FDB Encryption

An FDB client would insert data i.e. plaintext {key, value} in a FDB cluster for persistence. 

### KMS-Connector

A non-FDB process running on FDB cluster hosts enables an FDB cluster to interact with external Encryption Key Managements services. Salient features includes:

* An external (non-FDB) standalone process implementing a REST server.

* Abstracts organization specific KeyManagementService integration details. The proposed design ensures ease of integration given limited infrastructure needed to implement a local/remote REST server. 

* Ensure organization specific code is implemented outside the FDB codebase.

* The KMS-Connector process is launched and maintained by the FDBMonitor. The process needs to handle the following REST endpoint:
    1. GET - http://localhost/getEncryptionKey 

	Define a single interface returning “encryption key string in plaintext” and accepting an 
    JSON input which can be customized as needed:

```json
    json_input_payload
    {
        “Version”     : int     // version
        “KeyId”       : keyId   // string
    }
```

Few benefits of the above proposed schemes are:
* JSON input format is extensible (adding new fields is backward compatible).

* Popular Cloud KMS “getPublicKey” API accepts “keyId” as a string, hence, API should be easy to integrate.

    1. AWS: https://docs.aws.amazon.com/cli/latest/reference/kms/get-public-key.html
    2. GCP: https://cloud.google.com/kms/docs/retrieve-public-key   

`Future improvements`: FDBMonitor at present will launch one KMS-Connector process per FDB cluster host. Though multiple KMS-Connector processes are launched, only one process (collocated with EncryptKeyServer) would consume cluster resources. In future, possible enhancements could be: 

* Enable FDBMonitor to launch “N” (configurable) processes per cluster.
* Enable the FDB cluster to manage external processes as well.

### Encrypt KeyServer

Salient features include:

* New FDB role/process to allow fetching of encryption keys from external KeyManagementService interfaces. The process connects to the KMS-Connector REST interface to fetch desired encryption keys.

* On an encryption-key fetch from KMS-Connector, it applies HMAC derivative function to generate a new encryption key and cache it in-memory. The in-memory cache is used to serve encryption key fetch requests from other FDB processes. 


Given encryption keys will be needed as part of cluster-recovery, this process/role needs to be recruited at the start of the cluster-recovery process (just after the “master/sequencer” process/role recruitment). All other FDB processes will interact with this process to obtain encryption keys needed to encrypt and/or decrypt the data payload. 

`Note`: An alternative would be to incorporate the functionality into the ClusterController process itself, however, having clear responsibility separation would make design more flexible and extensible in future if needed. 

### Commit Proxies (CPs)

When a FDB client initiates a write transaction to insert/update data stored in a FDB cluster, the transaction is received by a CP, which then resolves the transaction by checking if the transaction is allowed. If allowed, it commits the transaction to TLogs. The proposal is to extend CP responsibilities by encrypting mutations using the desired encryption key before mutations get persisted into TLogs (durable storage). The encryption key derivation is achieved using the following formula:

```
                        DEK = HMAC SHA256(BEK || UID)

Where:

DEK    = Derived Encryption Key
BEK    = Base Encryption Key
UID    = Host local random generated number
```

The Transaction State Store (commonly referred as TxnStateStore) is a Key-Value datastore used by FDB to store metadata about the database itself for bootstrap purposes. The data stored in this store plays a critical role in: guiding the transaction system to persist writes (storage tags to mutations at CPs), and managing FDB internal data movement. The TxnStateStore data gets encrypted with the desired encryption key before getting persisted on the disk queues. 

As part of encryption, every Mutation would be appended by a plaintext `BlobCipherEncryptHeader` to assist decrypting the information for reads.

CPs would cache (in-memory) recently used encryption-keys to optimize network traffic due to encryption related operations. Further, the caching would improve overall performance, avoiding frequent RPC calls to EncryptKeyServer which may eventually become a scalability bottleneck. Each encryption-key in the cache has a short Time-To-Live (10 mins) and on expiry the process will interact with the EncryptKeyServer to fetch the required encryption-keys. The same caching policy is followed by the Redwood Storage Server and the Backup File processes too. 

### **Caveats**

The encryption is done inline in the transaction path, which will increase the total commit latencies. Few possible ways to minimize this impact are:

* Overlap encryption operations with the CP::resolution phase, which would minimize the latency penalty per transaction at the cost of spending more CPU cycles. If needed, for production deployments, we may need to increase the number of CPs per FDB cluster.
* Implement an external process to offload encryption. If done, encryption would appear no different than the CP::resolution phase, where the process would invoke RPC calls to encrypt the buffer and wait for operation completion.

### Storage Servers

The encryption design only supports Redwood Storage Server integration, support for other storage engines is yet to be planned.

### Redwood Storage Nodes

Redwood at heart is a B+ tree and stores data in two types of nodes:

* `Non-leaf` nodes: Nodes will only store keys and not values(prefix compression is applied). 
* `Leaf` Nodes: Will store `{key, value}` tuples for a given key-range.
 
Both above-mentioned nodes will be converted into one or more fixed size pages (likely 4K or 8K) before being persisted on a durable storage. The encryption will be performed at the node level instead of “page level”, i.e. all pages constituting a given Redwood node will be encrypted using the same encryption key generated using the following formula:

```
                        DEK = HMAC SHA256(BEK || UID)

Where:

DEK    = Derived Encryption Key
BEK    = Base Encryption Key
UID    = Host local random generated number
```

### Backup Files

Backup Files are designed to pull committed mutations from StorageServers and persist them as “files” stored on cloud backed BlobStorage such as Amazon S3. Each persisted file stores mutations for a given key-range and will be encrypted by generating an encryption key using below formula:

```
                        DEK = HMAC SHA256(BEK || FID)

Where:

DEK    = Derived Encryption Key
BEK    = Base Encryption Key
FID    = File Identifier (unique)
```

## Decryption on Reads

To assist reads, FDB processes (StorageServers, Backup Files workers) will be modified to read/parse the encryption header. The data decryption will be done as follows:

* The FDB process will interact with Encrypt KeyServer to fetch the desired base encryption key corresponding to the key-id persisted in the encryption header.
* Reconstruct the encryption key and decrypt the data block.

## Future Work

* Extend the TLog API to allow clients to read “plaintext mutations” directly from a TLogServer. In current implementations there are two consumers of TLogs:

    1. Storage Server: At present the plan is for StorageServer to decrypt the mutations.
    2. BackupWorker (Apple implementation) which is currently not used in the code.
