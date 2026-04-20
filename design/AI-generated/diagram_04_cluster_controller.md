# Cluster Controller & Coordination — Internal Architecture

```mermaid
graph TB
    subgraph Election["Leader Election"]
        Coord1["Coordinator 1"]
        Coord2["Coordinator 2"]
        Coord3["Coordinator 3"]
        GenReg["Generation Register\n(monotonic gen numbers)"]
        Cand["CC Candidates\n(CandidacyRequest)"]
    end

    subgraph CC["Cluster Controller"]
        WorkerPool["Worker Registry\n(ProcessClass + Locality)"]
        Recruit["Role Recruitment"]
        Monitor["Health Monitoring"]
        ServerDBInfo["ServerDBInfo\n(cluster-wide broadcast)"]
    end

    subgraph Roles["Recruited Roles"]
        Master["Master / Sequencer"]
        CPr["Commit Proxies"]
        GRVPr["GRV Proxies"]
        Resolvers["Resolvers"]
        TLogs["TLogs"]
        DDist["Data Distributor"]
        RK["Ratekeeper"]
        CS["Consistency Scan"]
    end

    subgraph Workers["Worker Processes"]
        W1["Worker 1\n(transaction class)"]
        W2["Worker 2\n(storage class)"]
        W3["Worker 3\n(stateless class)"]
        Wn["Worker N"]
    end

    Cand --> Coord1
    Cand --> Coord2
    Cand --> Coord3
    Coord1 --> GenReg
    Coord2 --> GenReg
    Coord3 --> GenReg
    GenReg -->|majority agrees| CC

    W1 -->|registrationClient()| WorkerPool
    W2 -->|registrationClient()| WorkerPool
    W3 -->|registrationClient()| WorkerPool
    Wn -->|registrationClient()| WorkerPool
    WorkerPool --> Recruit
    Recruit -->|fitness + locality| Roles

    Monitor -->|process failure| Recruit
    CC --> ServerDBInfo -->|broadcast| Workers

    style Election fill:#f3e5f5,stroke:#9c27b0
    style CC fill:#e1f0ff,stroke:#4a90d9
    style Roles fill:#fff3e0,stroke:#f5a623
    style Workers fill:#e8f5e9,stroke:#4caf50
```

## Leader Election Protocol

```mermaid
sequenceDiagram
    participant C1 as CC Candidate 1 (better fitness)
    participant C2 as CC Candidate 2
    participant Coord as Coordinators (majority)

    C1->>Coord: CandidacyRequest (fitness in changeID)
    C2->>Coord: CandidacyRequest (fitness in changeID)
    Coord->>Coord: Compare fitness (top bits of changeID)
    Coord-->>C1: Nominated as leader
    Coord-->>C2: Not nominated
    C1->>C1: Become CC
    C2->>C2: monitorLeader() — defer to C1
    Note over C1: Begins recruiting roles
```

## Role Recruitment Decision

```mermaid
graph TD
    Need["Need to fill role\n(e.g., CommitProxy)"]
    Need --> Filter["Filter workers by\nProcessClass fitness"]
    Filter --> Locality["Apply locality constraints\n(DC, zone, machine)"]
    Locality --> Exclude["Exclude failed /\nexcluded processes"]
    Exclude --> Best["Select best candidate"]
    Best --> Recruit["Send recruitment request"]
    Recruit --> Running["Role running on worker"]
```

## ServerDBInfo Broadcast

```mermaid
graph LR
    CC["Cluster Controller"] -->|update| Info["ServerDBInfo"]

    Info --> |contains| M["Master interface"]
    Info --> |contains| PL["Proxy list"]
    Info --> |contains| LSC["Log system config"]
    Info --> |contains| RS["Recovery state"]
    Info --> |contains| LB["Latency band config"]

    Info -->|broadcast to all| W["All Workers"]
    W -->|react to changes| Roles["Update connections\nto new proxies, etc."]
```
