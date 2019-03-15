#!/usr/bin/env python

import sys
import json

lst = []
for filename in sys.argv[1:]:
    commands = json.load(open(filename))
    lst.extend(commands)

json.dump(lst, open("compile_commands.json", "w"))
