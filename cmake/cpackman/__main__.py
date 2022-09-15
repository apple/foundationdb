import os
import sys
import importlib.util

from . import eprint


if os.name == 'nt':
    # CPackMan is not supported on Windows
    sys.exit(0)
assert len(sys.argv) >= 3
assert sys.argv[1] == 'FIND_PACKAGE'
package_name = sys.argv[2]
module_name = 'cpackman.pellets.{}'.format(package_name.lower())
spec = importlib.util.find_spec(module_name)
if spec is not None:
    with open('cpackman_out.cmake', 'w') as out:
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        assert spec.loader is not None
        spec.loader.exec_module(module)
        module.provide_module(out, sys.argv[2:])
else:
    sys.exit(1)
