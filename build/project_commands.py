#!/usr/bin/env python

import sys
import os
import os.path
import json

flags = sys.argv[1]
all_files = sys.argv[2]
outfile = sys.argv[3]

cwd = os.getcwd()

commands = []
for fname in all_files.split(' '):
  d = {}
  d["directory"] = cwd
  if fname.endswith("cpp") or fname.endswith(".h"):
    compiler = "clang++ -x c++ "
  if fname.endswith("c"):
    compiler = "clang -x c "
  d["command"] = compiler + flags.replace('-DNO_INTELLISENSE', '').replace("/opt/boost", cwd+"/../boost") + "-c " + fname
  d["file"] = fname
  commands.append(d)

json.dump(commands, open(outfile, "w"))
