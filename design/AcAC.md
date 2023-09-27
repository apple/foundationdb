# AcAC -- Actor Active Context

## Introduction

FoundationDB developers often face multiple difficulties debugging the code, and one of the significant challenges is that the call stack is usually hard to interpret. Because of the underlying "chain reaction" mechanism, when a single-assigned value (SAV) is fired,  actors waiting for the `Promise`s will be called recursively, creating a huge stack and overwhelming the reader. Providing an actor-wise call stack for the current running actor would be beneficial. For this purpose, AcAC is developed.

## Usage

### Build FoundationDB with AcAC enabled

AcAC must be enabled when FoundationDB is being built. To do so, either pass

```bash
-DWITH_ACAC
```

to `cmake`, or enable it via

```bash
ccmake ${PATH_TO_FOUNDATIONDB}
```

and turn on the flag `WITH_ACAC`.

To verify if the current FoundationDB is built with or without AcAC, simply run

```bash
bin/acac
```

with no parameters. `acac` would report an error if AcAC is disabled.

### Actor call stack when `SevError` occurred

If AcAC is enabled, `TraceEvent` will have an additional field, `ActorStack`, which includes the call stack of the current actor. One example is:

```xml
<Event Severity="40" ErrorKind="Unset" Time="116.033278" DateTime="2023-09-19T23:13:34Z" Type="GetValueQ" Machine="[abcd::2:0:1:0]:1" ID="0000000000000000" ThreadID="8270072431916078549" Backtrace="addr2line -e fdbserver.debug -p -C -f -i 0x9030301 0x90994be 0x9099798 0x909672f 0x2eea755 0x59fd212 0x59d6b4f 0x59d69c0 0x5c119cf 0x5b0ac8e 0x5b0abb3 0x5b0b25f 0x5b0a808 0x2f325b3 0x2f2fc6d 0x8c86f1c 0x8c8678a 0x8c86310 0x8c84ec0 0x8c84d7d 0x8c832ba 0x8c82eaa 0x8c81aa2 0x8c8179a 0x8c8170b 0x8c8146a 0x8c8130b 0x8c812bb 0x8c80fcb 0x8c83b70 0x8c83b33 0x8c89032 0x8c80f3e 0x2f320a5 0x2f31470 0x8e85ba6 0x8e857c2 0x8e4e995 0x58ec230 0x7f78976c4555" ActorStack="Aqy0WAAAAAAACgAAAKy0WAAAAAAAAMSnmgeH5A8AM7xFqXxiyn3UBgAAAAAAfdQGAAAAAAAA\x0a33tAouRphQB0jvDCp9xVcNQGAAAAAABw1AYAAAAAAABrOYScWM6NACJ87BsiNawN0gYAAAAA\x0aAA3SBgAAAAAAAKX+hucu6gYAEcSkqfQdtgg9AAAAAAAACD0AAAAAAAAAdzyLHjsZVAAscBZA\x0a7qvuHxUAAAAAAAAfFQAAAAAAAABnQ52wLR0zAEOE5LCoMdQUFAAAAAAAABQUAAAAAAAAAP/4\x0a3UKH9fcATA5GnKrrxhAAAAAAAAAAEAAAAAAAAAAASoNbdz8z2QAcIFTMFOWiDwAAAAAAAAAP\x0aAAAAAAAAAAD65FUMWVrSADkNcqnzJBsKAAAAAAAAAAoAAAAAAAAAAPCjaSWhjVYAoNenV/4G\x0auAAAAAAAAAAA\x0a" LogGroup="default" Roles="CC,CD,CP,RV,SS,TL" />
```

The `ActorStack` provides the actor call stack encoded in `base64` format. `bin/acac`  can be used to decode it:

```bash
echo "Aqy0WAAAAAAACgAAAKy0WAAAAAAAAMSnmgeH5A8AM7xFqXxiyn3UBgAAAAAAfdQGAAAAAAAA\x0a33tAouRphQB0jvDCp9xVcNQGAAAAAABw1AYAAAAAAABrOYScWM6NACJ87BsiNawN0gYAAAAA\x0aAA3SBgAAAAAAAKX+hucu6gYAEcSkqfQdtgg9AAAAAAAACD0AAAAAAAAAdzyLHjsZVAAscBZA\x0a7qvuHxUAAAAAAAAfFQAAAAAAAABnQ52wLR0zAEOE5LCoMdQUFAAAAAAAABQUAAAAAAAAAP/4\x0a3UKH9fcATA5GnKrrxhAAAAAAAAAAEAAAAAAAAAAASoNbdz8z2QAcIFTMFOWiDwAAAAAAAAAP\x0aAAAAAAAAAAD65FUMWVrSADkNcqnzJBsKAAAAAAAAAAoAAAAAAAAAAPCjaSWhjVYAoNenV/4G\x0auAAAAAAAAAAA\x0a" | bin/acac
```

The output is:

```bash
     5813420 /root/src/fdbserver/storageserver.actor.cpp:getValueQ  <ACTIVE>
      447613 /root/src/fdbserver/storageserver.actor.cpp:serveGetValueRequests
      447600 /root/src/fdbserver/storageserver.actor.cpp:storageServerCore
      446989 /root/src/fdbserver/storageserver.actor.cpp:storageServer
       15624 /root/src/fdbserver/worker.actor.cpp:storageServerRollbackRebooter
        5407 /root/src/fdbserver/worker.actor.cpp:workerServer
        5140 /root/src/fdbserver/worker.actor.cpp:fdbd
          16 /root/src/fdbserver/SimulatedCluster.actor.cpp:simulatedFDBDRebooter
          15 /root/src/fdbserver/SimulatedCluster.actor.cpp:simulatedMachine
          10 /root/src/fdbserver/SimulatedCluster.actor.cpp:simulationSetupAndRun
```

which provides the actor ID, an analog of process ID in the actor context; the path of the source file, which contains the actor; the name of the actor; and a `<ACTIVE>` mark for the current running actor.

### Reporting the actor context on demand

To report the actor context on demand, the function

```c++
std::string encodeActorContext(const ActorContextDumpType dumpType);
```

is declared in `flow/ActorContext.h`. The `dumpType` can be one of

```c++ 
enum class ActorContextDumpType : uint8_t {
	FULL_CONTEXT,
	CURRENT_STACK,
	CURRENT_CALL_BACKTRACE,
};
```

`FULL_CONTEXT` will dump *all* running actors, while `CURRENT_CALL_BACKTRACE` will dump the call backtrace of the current running actor. `CURRENT_STACK` is not as helpful since it will dump the current call stack and is for debugging purposes only. The result will be encoded in `base64` format and returned as a `std::string` object. The object can be consumed by `bin/acac`.

### Dump the actor call backtrace in the debugger

It is possible to dump the encoded actor call backtrace within `gdb` or `lldb` via `dumpActorCallBacktrace()`, e.g.,

```
(lldb) p dumpActorCallBacktrace()
AsfmEwAAAAAACQAAAMfmEwAAAAAAAM2oiSx71iIAGrSuCzh6ICZdEwAAAAAAJl0TAAAAAAAA
xtSlzYb4jwDwjH12k/QMyyITAAAAAADLIhMAAAAAAAB494CVpeUhADemI+QonWM4IgAAAAAA
ADgiAAAAAAAAABGOhIZwfcMAYcMZsUHF4yoiAAAAAAAAKiIAAAAAAAAAZ0OdsC0dMwBDhOSw
qDHU7CAAAAAAAADsIAAAAAAAAAD/+N1Ch/X3AEwORpyq68Y8AAAAAAAAADwAAAAAAAAAAEqD
W3c/M9kAHCBUzBTlojsAAAAAAAAAOwAAAAAAAAAA+uRVDFla0gA5DXKp8yQbCgAAAAAAAAAK
AAAAAAAAAADwo2kloY1WAKDXp1f+BrgAAAAAAAAAAA==
```

```bash
# echo "AsfmEwAAAAAACQAAAMfmEwAAAAAAAM2oiSx71iIAGrSuCzh6ICZdEwAAAAAAJl0TAAAAAAAA
\ xtSlzYb4jwDwjH12k/QMyyITAAAAAADLIhMAAAAAAAB494CVpeUhADemI+QonWM4IgAAAAAA
\ ADgiAAAAAAAAABGOhIZwfcMAYcMZsUHF4yoiAAAAAAAAKiIAAAAAAAAAZ0OdsC0dMwBDhOSw
\ qDHU7CAAAAAAAADsIAAAAAAAAAD/+N1Ch/X3AEwORpyq68Y8AAAAAAAAADwAAAAAAAAAAEqD
\ W3c/M9kAHCBUzBTlojsAAAAAAAAAOwAAAAAAAAAA+uRVDFla0gA5DXKp8yQbCgAAAAAAAAAK
\ AAAAAAAAAADwo2kloY1WAKDXp1f+BrgAAAAAAAAAAA=="|bin/acac
     1304263 /root/src/fdbserver/workloads/FuzzApiCorrectness.actor.cpp:loadAndRun  <ACTIVE>
     1269030 /root/src/fdbserver/tester.actor.cpp:runWorkloadAsync
     1254091 /root/src/fdbserver/tester.actor.cpp:testerServerWorkload
        8760 /root/src/fdbserver/tester.actor.cpp:testerServerCore
        8746 /root/src/fdbserver/worker.actor.cpp:workerServer
        8428 /root/src/fdbserver/worker.actor.cpp:fdbd
          60 /root/src/fdbserver/SimulatedCluster.actor.cpp:simulatedFDBDRebooter
          59 /root/src/fdbserver/SimulatedCluster.actor.cpp:simulatedMachine
          10 /root/src/fdbserver/SimulatedCluster.actor.cpp:simulationSetupAndRun
```

The reason for not being able to generate the actor call backtrace inside the debugger directly is that the actor names are internally represented as UUIDs, and the mapping requires external files generated by the actor compiler.

## Implementation Details

<TODO>

## Future Developments

* Since AcAC has the information about the current actor, `ActorIdentifier` can be introduced to the `Error` class. It is then possible for the recipient to know the sender of the error.
  The reason this feature is not implemented is that, for some unknown reason, adding any field to the `Error` class would cause
  * All binding tests fail
  * Random failure on the `fdbserver`
* It is possible for AcAC to report the line number of the source code where the actor is created.

