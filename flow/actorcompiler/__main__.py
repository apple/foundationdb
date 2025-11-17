"""
Allow running the actorcompiler as a module:
    python3 -m flow.actorcompiler input.actor.cpp output.g.cpp
"""

import sys
from .main import main

if __name__ == "__main__":
    sys.exit(main())
