# Special-Key-Space
This document discusses why we need the proposed special-key-space framework. And for what problems the framework aims to solve and in what scenarios a developer should use it.   

## Motivation
Currently, there are several client functions implemented as FDB calls by passing through special keys(`prefixed with \xff\xff`). Below are all existing features:
- **status/json**: `get("\xff\xff/status/json")`
- **cluster_file_path**: `get("\xff\xff/cluster_file_path)`
- **connection_string**: `get("\xff\xff/connection_string)`
- **worker_interfaces**: `getRange("\xff\xff/worker_interfaces", <any_key>)`
- **conflicting_keys**: `getRange("\xff\xff/transaction/conflicting_keys/", "\xff\xff/transaction/conflicting_keys/\xff")`

At present, implementations are hard-coded and the pain points are obvious:
- **Maintainability**: As more features added, the hard-coded snippets are hard to maintain 
- **Granularity**: It is impossible to scale up and down. For example, you want a cheap call like `get("\xff\xff/status/json/<certain_field>")` instead of calling `status/json` and parsing the results. On the contrary, sometime you want to aggregate results from several similar features like `getRange("\xff\xff/transaction/, \xff\xff/transaction/\xff")` to get all transaction related info. Both of them are not achievable at present.
- **Consistency**: While using FDB calls like `get` or `getRange`, the behavior that the result of `get("\xff\xff/B")` is not included in `getRange("\xff\xff/A", "\xff\xff/C")` is inconsistent with general FDB calls.

Consequently, the special-key-space framework wants to integrate all client functions using special keys(`prefixed with \xff`) and solve the pain points listed above.

## When
If your feature is exposing information to clients and the results are easily formatted as key-value pairs, then you can use special-key-space to implement your client function.

## How
If you choose to use, you need to implement a function class that inherits from `SpecialKeyRangeReadImpl`, which has an abstract method `Future<Standalone<RangeResultRef>> getRange(ReadYourWritesTransaction* ryw, KeyRangeRef kr)`.
This method can be treated as a callback, whose implementation details are determined by the developer.
Once you fill out the method, register the function class to the corresponding key range.
Below is a detailed example.
```c++
// Implement the function class,
// the corresponding key range is [\xff\xff/example/, \xff\xff/example/\xff)
class SKRExampleImpl : public SpecialKeyRangeReadImpl {
public:
    explicit SKRExampleImpl(KeyRangeRef kr): SpecialKeyRangeReadImpl(kr) {
        // Our implementation is quite simple here, the key-value pairs are formatted as:
        // \xff\xff/example/<country_name> : <capital_city_name>
        CountryToCapitalCity[LiteralStringRef("USA")] = LiteralStringRef("Washington, D.C.");
        CountryToCapitalCity[LiteralStringRef("UK")] = LiteralStringRef("London");
        CountryToCapitalCity[LiteralStringRef("Japan")] = LiteralStringRef("Tokyo");
        CountryToCapitalCity[LiteralStringRef("China")] = LiteralStringRef("Beijing");
    }
    // Implement the getRange interface
    Future<Standalone<RangeResultRef>> getRange(ReadYourWritesTransaction* ryw,
                                            KeyRangeRef kr) const override {
        
        Standalone<RangeResultRef> result;
        for (auto const& country : CountryToCapitalCity) {
            // the registered range here: [\xff\xff/example/, \xff\xff/example/\xff]
            Key keyWithPrefix = country.first.withPrefix(range.begin);
            // check if any valid keys are given in the range
            if (kr.contains(keyWithPrefix)) {
                result.push_back(result.arena(), KeyValueRef(keyWithPrefix, country.second));
                result.arena().dependsOn(keyWithPrefix.arena());
            }
        }
        return result;
    }
private:
    std::map<Key, Value> CountryToCapitalCity;
};
// Instantiate the function object
// In development, you should have a function object pointer in DatabaseContext(DatabaseContext.h) and initialize in DatabaseContext's constructor(NativeAPI.actor.cpp)
const KeyRangeRef exampleRange(LiteralStringRef("\xff\xff/example/"), LiteralStringRef("\xff\xff/example/\xff"));
SKRExampleImpl exampleImpl(exampleRange);
// Assuming the database handler is `cx`, register to special-key-space
// In development, you should register all function objects in the constructor of DatabaseContext(NativeAPI.actor.cpp)
cx->specialKeySpace->registerKeyRange(exampleRange, &exampleImpl);
// Now any ReadYourWritesTransaction associated with `cx` is able to query the info
state ReadYourWritesTransaction tr(cx);
// get
Optional<Value> res1 = wait(tr.get("\xff\xff/example/Japan"));
ASSERT(res1.present() && res.getValue() == LiteralStringRef("Tokyo"));
// getRange
// Note: for getRange(key1, key2), both key1 and key2 should prefixed with \xff\xff
// something like getRange("normal_key", "\xff\xff/...") is not supported yet
Standalone<RangeResultRef> res2 = wait(tr.getRange(LiteralStringRef("\xff\xff/example/U"), LiteralStringRef("\xff\xff/example/U\xff")));
// res2 should contain USA and UK
ASSERT(
    res2.size() == 2 &&
    res2[0].value == LiteralStringRef("London") &&
    res2[1].value == LiteralStringRef("Washington, D.C.")
);
```

## Module
We introduce this `module` concept after a [discussion](https://forums.foundationdb.org/t/versioning-of-special-key-space/2068) on cross module read on special-key-space. By default, range reads cover more than one module will not be allowed with `special_keys_cross_module_read` errors. In addition, range reads touch no modules will come with `special_keys_no_module_found` errors. The motivation here is to avoid unexpected blocking or errors happen in a wide-scope range read. In particular, you write code `getRange("A", "Z")` when all registered calls between `[A, Z)` happen locally, thus your code does not have any error-handling. However, if in the future, anyone register a new call in `[A, Z)` and sometimes throw errors like `time_out()`, then your original code is broken. The `module` is like a top-level directory where inside the module, calls are homogeneous. So we allow cross range read inside each module by default but cross module reads are forbidden. Right now, there are two modules available to use:

- TRANSACTION : `\xff\xff/transaction/, \xff\xff/transaction0`, all transaction related information like *read_conflict_range*, *write_conflict_range*, *conflicting_keys*.(All happen locally). Right now we have:
  - `\xff\xff/transaction/conflicting_keys/, \xff\xff/transaction/conflicting_keys0` : conflicting keys that caused conflicts
  - `\xff\xff/transaction/read_conflict_range/, \xff\xff/transaction/read_conflict_range0` : read conflict ranges of the transaction
  - `\xff\xff/transaction/write_conflict_range/, \xff\xff/transaction/write_conflict_range0` : write conflict ranges of the transaction
- METRICS: `\xff\xff/metrics/, \xff\xff/metrics0`, all metrics like data-distribution metrics or healthy metrics are planned to put here. All need to call the rpc, so time_out error s may happen. Right now we have:
  - `\xff\xff/metrics/data_distribution_stats/, \xff\xff/metrics/data_distribution_stats0` : stats info about data-distribution
- WORKERINTERFACE : `\xff\xff/worker_interfaces/, \xff\xff/worker_interfaces0`, which is compatible with previous implementation, thus should not be used to add new functions.

In addition, all singleKeyRanges are formatted as modules and cannot be used again. In particular, you should call `get` not `getRange` on these keys. Below are existing ones:

- STATUSJSON : `\xff\xff/status/json`
- CONNECTIONSTRING : `\xff\xff/connection_string`
- CLUSTERFILEPATH : `\xff\xff/cluster_file_path`
