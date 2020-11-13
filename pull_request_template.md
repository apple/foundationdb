This PR is resolves #...

Changes in this PR:

-
-
-

## General guideline:

- If this PR is ready to be merged (and all checkboxes below are either ticked or not applicable), make this a regular PR
- If this PR still needs work, please make this a draft PR
  - If you wish to get feedback/code-review, please add the label RFC to this PR

Please verify that all things listed below were considered and check them. If an item doesn't apply to this type of PR (for example a documentation change doesn't need to be performance tested), you should make a ~~strikethrough~~ (markdown syntax: `~~strikethrough~~`). More infos on the guidlines can be found [here](https://github.com/apple/foundationdb/wiki/FoundationDB-Commit-Process).

### Style
- [ ] All variable and function names make sense.
- [ ] The code is properly formatted (consider running `git clang-format`).

### Performance
- [ ] All CPU-hot paths are well optimized.
- [ ] The proper containers are used (for example `std::vector` vs `VectorRef`).
- [ ] There are no new known `SlowTask` traces.

### Testing
- [ ] The code was sufficiently tested in simulation.
- [ ] If there are new parameters or knobs, different values are tested in simulation.
- [ ] `ASSERT`, `ASSERT_WE_THINK`, and `TEST` macros are added in appropriate places.
- [ ] Unit tests were added for new algorithms and data structure that make sense to unit-test
- [ ] If this is a bugfix: there is a test that can easily reproduce the bug.
