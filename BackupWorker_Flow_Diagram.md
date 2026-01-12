# BackupWorker.actor.cpp Flow Diagram

## Overview
The BackupWorker is responsible for pulling mutation logs from TLog servers and uploading them to backup storage containers. It operates in different modes (pulling vs NOOP) and manages multiple concurrent backup jobs.

## Main Flow Diagram

```mermaid
graph TD
    A[backupWorker Entry Point] --> B[Initialize BackupData]
    B --> C[Start Actor Collection]
    C --> D[Check if Backup Key Present]
    
    D --> E{Backup Key Present?}
    E -->|Yes| F[Start Pull Mode]
    E -->|No| G[Start NOOP Mode]
    
    F --> H[monitorBackupKeyOrPullData]
    G --> H
    
    H --> I[pullAsyncData Actor]
    H --> J[uploadData Actor]
    
    I --> K[Pull from TLog Servers]
    K --> L[Store in Message Queue]
    L --> M{Pull Finished?}
    M -->|No| K
    M -->|Yes| N[Trigger Done]
    
    J --> O[Process Message Queue]
    O --> P[Save to Backup Files]
    P --> Q[Update Progress]
    Q --> R{All Messages Saved?}
    R -->|No| O
    R -->|Yes| S[Complete]
    
    N --> S
    
    %% Parallel Actors
    C --> T[checkRemoved Actor]
    C --> U[monitorBackupProgress Actor]
    C --> V[monitorWorkerPause Actor]
    
    T --> W[Monitor DB Recovery]
    U --> X[Track Backup Progress]
    V --> Y[Handle Pause/Resume]
    
    %% Error Handling
    W --> Z{Worker Displaced?}
    Z -->|Yes| AA[Throw worker_removed]
    Z -->|No| W
    
    AA --> BB[Stop Pulling]
    BB --> CC[Complete Upload]
    CC --> S
```

## Detailed Component Breakdown

### 1. BackupData Structure
```mermaid
graph LR
    A[BackupData] --> B[Message Queue]
    A --> C[Per-Backup Info Map]
    A --> D[Version Tracking]
    A --> E[Flow Control Lock]
    
    B --> B1[VersionedMessage List]
    C --> C1[Container References]
    C --> C2[Key Ranges]
    C --> C3[Progress Tracking]
    D --> D1[savedVersion]
    D --> D2[pulledVersion]
    D --> D3[popVersion]
    E --> E1[Memory Management]
```

### 2. Message Processing Flow
```mermaid
graph TD
    A[TLog Messages] --> B[pullAsyncData]
    B --> C[Peek from LogRouter]
    C --> D[Create VersionedMessage]
    D --> E[Check Memory Lock]
    E --> F[Add to Message Queue]
    
    F --> G[uploadData]
    G --> H[Filter Candidate Messages]
    H --> I[Group by Backup]
    I --> J[Create Log Files]
    J --> K[Write Mutations]
    K --> L[Finish Files]
    L --> M[Update Progress]
    M --> N[Pop TLog Data]
```

### 3. Backup Management Flow
```mermaid
graph TD
    A[Monitor Backup Started Key] --> B{Key Changes?}
    B -->|Yes| C[Parse Backup UIDs]
    C --> D[onBackupChanges]
    D --> E[Start New Backups]
    D --> F[Stop Removed Backups]
    
    E --> G[Create PerBackupInfo]
    G --> H[Open Container]
    G --> I[Get Key Ranges]
    G --> J[Update Worker Count]
    
    F --> K[Mark as Stopped]
    K --> L[Cancel Actors]
```

### 4. Version Management and Progress Tracking
```mermaid
graph TD
    A[Version Tracking] --> B[savedVersion]
    A --> C[pulledVersion]
    A --> D[popVersion]
    A --> E[minKnownCommittedVersion]
    
    B --> F[Last Saved to Storage]
    C --> G[Last Pulled from TLog]
    D --> H[Last Popped from TLog]
    E --> I[Min Committed by Cluster]
    
    F --> J[saveProgress]
    J --> K[Update BackupProgress Key]
    K --> L[Enable TLog Pop]
```

### 5. NOOP Mode Flow
```mermaid
graph TD
    A[No Active Backups] --> B[Enter NOOP Mode]
    B --> C[Get Min Committed Version]
    C --> D[Calculate Pop Version]
    D --> E[Save NOOP Version]
    E --> F[Pop TLog Data]
    F --> G[Wait for Backup Key]
    G --> H{Backup Started?}
    H -->|Yes| I[Exit NOOP Mode]
    H -->|No| C
```

### 6. Error Handling and Recovery
```mermaid
graph TD
    A[Error Conditions] --> B[Worker Displaced]
    A --> C[TLog Data Missing]
    A --> D[Container Failures]
    
    B --> E[checkRemoved Actor]
    E --> F[Monitor Recovery Count]
    F --> G[Throw worker_removed]
    
    C --> H[Check NOOP Version]
    H --> I[Validate Pop State]
    
    D --> J[Retry Operations]
    J --> K[Transaction Retry Logic]
```

## Key Actor Interactions

### Main Actors:
1. **[`backupWorker()`](fdbserver/BackupWorker.actor.cpp:1232)** - Entry point and coordinator
2. **[`pullAsyncData()`](fdbserver/BackupWorker.actor.cpp:1030)** - Pulls data from TLog servers
3. **[`uploadData()`](fdbserver/BackupWorker.actor.cpp:931)** - Processes and uploads mutations
4. **[`monitorBackupKeyOrPullData()`](fdbserver/BackupWorker.actor.cpp:1133)** - Switches between pull/NOOP modes
5. **[`monitorBackupProgress()`](fdbserver/BackupWorker.actor.cpp:669)** - Tracks overall backup progress
6. **[`checkRemoved()`](fdbserver/BackupWorker.actor.cpp:1188)** - Monitors for worker displacement

### Data Structures:
1. **[`BackupData`](fdbserver/BackupWorker.actor.cpp:124)** - Main state container
2. **[`VersionedMessage`](fdbserver/BackupWorker.actor.cpp:45)** - Individual mutation message
3. **[`PerBackupInfo`](fdbserver/BackupWorker.actor.cpp:147)** - Per-backup state and containers

## State Transitions

```mermaid
stateDiagram-v2
    [*] --> Initializing
    Initializing --> CheckingBackupKey
    CheckingBackupKey --> PullMode : Backup Key Present
    CheckingBackupKey --> NOOPMode : No Backup Key
    
    PullMode --> Pulling : Start Data Pull
    Pulling --> Uploading : Messages Available
    Uploading --> Pulling : Continue
    Uploading --> Completed : All Data Processed
    
    NOOPMode --> Popping : Pop Old Data
    Popping --> Waiting : Wait for Backup
    Waiting --> PullMode : Backup Key Appears
    
    PullMode --> NOOPMode : Backup Key Removed
    
    Pulling --> Displaced : Worker Removed
    Uploading --> Displaced : Worker Removed
    NOOPMode --> Displaced : Worker Removed
    
    Displaced --> [*]
    Completed --> [*]
```

## Memory Management

The BackupWorker uses a [`FlowLock`](fdbserver/BackupWorker.actor.cpp:145) to manage memory usage:
- Messages are held in memory until uploaded
- Lock prevents excessive memory consumption
- Memory is released after successful upload
- Backpressure applied when memory limit reached

## Concurrency Model

Multiple actors run concurrently:
- **Data Flow**: Pull → Queue → Upload pipeline
- **Monitoring**: Progress tracking, pause/resume, displacement detection
- **Management**: Backup lifecycle, version coordination
- **Error Handling**: Recovery, retry logic, cleanup

This architecture ensures reliable, efficient backup operations while handling various failure scenarios and operational requirements.