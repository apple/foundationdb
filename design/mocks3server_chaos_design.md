# MockS3ServerChaos: S3 Error Injection for Testing

## Overview

Create a MockS3ServerChaos implementation, modeled after the existing [`AsyncFileChaos`](https://github.com/apple/foundationdb/tree/main/fdbrpc/include/fdbrpc/AsyncFileChaos.h#L32) pattern. This enables comprehensive testing of S3 client error handling, retry logic, and resilience against realistic failure scenarios.

Philosophy: mocks3 should be more intolerant/strict than real s3

## Problem

FoundationDB's S3BlobStore client needs thorough testing against realistic S3 failure scenarios, but the existing [`MockS3Server`](https://github.com/apple/foundationdb/tree/main/fdbserver/MockS3Server.actor.cpp#L43) only provides deterministic "happy path" responses. Real S3 services exhibit various error conditions that clients must handle gracefully.

## Design

**MockS3ServerChaos** is a new class that acts as a chaos-enabled wrapper around the base MockS3Server, following the established chaos injection pattern:

### Chaos Control System

MockS3ServerChaos will follow the [`AsyncFileChaos`](https://github.com/apple/foundationdb/tree/main/fdbrpc/include/fdbrpc/AsyncFileChaos.h#L54) pattern with **fault injector-driven chaos**:

#### **S3FaultInjector** - Configurable Rates (Primary Control)

```
// S3FaultInjector provides configurable rates (0.0-1.0) via g_network->global()
auto injector = g_network->global(enS3FaultInjector);
if (injector) {
    double errorRate = injector->getErrorRate();        // 0.0-1.0 probability
    double throttleRate = injector->getThrottleRate();  // 0.0-1.0 probability  
    double delayRate = injector->getDelayRate();        // 0.0-1.0 probability
    double maxDelay = injector->getMaxDelay();          // seconds
    
    // Runtime decision using deterministic random
    if (deterministicRandom()->random01() < errorRate) {
        // Inject error based on configured probability
    }
}
```

#### **BUGGIFY** - Occasional Extra Chaos

`BUGGIFY` is a **probabilistic macro** (not boolean) that evaluates to true with low probability:

```
if (BUGGIFY) {
    // Occasionally inject extra chaos - executes randomly, not always
    // Used for additional chaos beyond configured rates
}
```

### Key Features to Implement

#### 1. **Realistic S3 Error Simulation**

* **HTTP errors**: 429 (throttling), 503 (service unavailable), 500/502 (server errors)
* **Auth errors**: 401 (unauthorized), 406 (not acceptable) 
* **S3-specific errors**: InvalidToken, ExpiredToken (matching [`S3BlobStore.actor.cpp`](https://github.com/apple/foundationdb/tree/main/fdbclient/S3BlobStore.actor.cpp#L1241) patterns)
* **Connection issues**: Connection drops, timeouts
* **Data corruption**: Malformed responses, bit flips

#### 2. **Fault Injector-Driven Rates**

* **No master switch** - chaos is controlled by fault injector presence and rates
* **Configurable probabilities**: Each fault type has independent rate (0.0-1.0)
* **Deterministic randomness**: Reproducible chaos using `deterministicRandom()->random01()`
* **Rate-based decisions**: `if (random < rate)` pattern like [`AsyncFileChaos`](https://github.com/apple/foundationdb/tree/main/fdbrpc/include/fdbrpc/AsyncFileChaos.h#L96)

#### 3. **Operation-Specific Targeting**

Supports targeted chaos injection with configurable multipliers:

* **Read operations** (GET/HEAD objects)
* **Write operations** (PUT objects)
* **Delete operations** (DELETE objects)
* **Multipart upload operations** (initiate/upload/complete/abort)
* **List operations** (bucket listing)

#### 4. **Advanced Chaos Patterns** TODO

* **BUGGIFY extras**: Occasional additional chaos beyond configured rates
* **Delay jitter**: Variable delays with configurable patterns
* **Retry-After headers**: Realistic throttling responses with retry guidance
* **Burst patterns**: Periodic error rate spikes

#### 5. **Framework Integration** 

* **Fault injector pattern**: Uses `g_network->global()` like [`DiskFailureInjector`](https://github.com/apple/foundationdb/tree/main/fdbrpc/include/fdbrpc/AsyncFileChaos.h#L56) and [`BitFlipper`](https://github.com/apple/foundationdb/tree/main/fdbrpc/include/fdbrpc/AsyncFileChaos.h#L93)
* **No boolean master switch**: Always potentially active when fault injectors present
* **Metrics integration**: Tracks chaos events via existing [`ChaosMetrics`](https://github.com/apple/foundationdb/tree/main/fdbrpc/include/fdbrpc/AsyncFileChaos.h#L28) system
* **Trace events**: Comprehensive logging for debugging and analysis

## Architecture

```
MockS3ServerChaos Configuration:
├── S3FaultInjector (primary control) - Configurable fault rates:
│   ├── errorRate: 0.0-1.0 (0% to 100% error probability)
│   ├── throttleRate: 0.0-1.0 (throttling probability) 
│   ├── delayRate: 0.0-1.0 (delay probability)
│   └── maxDelay: seconds (maximum delay time)
├── BUGGIFY (occasional extras) - Probabilistic additional chaos
└── Runtime Decision Logic:
    ├── Operation classification (GET/PUT/DELETE/multipart/list)
    ├── Deterministic random + configured rates  
    ├── Chaos injection actors (delay/error/corruption)
    ├── S3-compatible error response generation 
    └── Base MockS3RequestHandler delegation
```

## Usage

Replace [`startMockS3Server()`](https://github.com/apple/foundationdb/tree/main/fdbserver/include/fdbserver/MockS3Server.h#L47) calls with `startMockS3ServerChaos()` in simulation tests:

```
// Before: 
wait(startMockS3Server(listenAddress));

// After:  
wait(startMockS3ServerChaos(listenAddress));
```

Chaos behavior is controlled by **S3FaultInjector rates** (0.0-1.0), with **BUGGIFY providing occasional extra chaos** - no master boolean switch.

