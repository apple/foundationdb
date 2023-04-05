This folder contains target-specific config files and code snippets.

Basically to add a new target one needs to provide an .ini file with basic platform info
(like pointer sizes) and code templates for
  * shim code which checks that real function address is available (and either jumps there or calls the slow path)
  * the "slow path" code which
    - saves function arguments (to avoid trashing them in next steps)
    - calls code which loads the target library and locates function addresses
    - restores saved arguments and returns
