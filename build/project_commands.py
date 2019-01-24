#!/usr/bin/env python

import argparse
import json
import os
import os.path
import sys

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--cflags', help="$(CFLAGS)")
    parser.add_argument('--cxxflags', help="$(CXXFLAGS)")
    parser.add_argument('--sources', help="All the source files")
    parser.add_argument('--out', help="Output file name")
    return parser.parse_args()

def main():
    args = parse_args()
    cwd = os.getcwd()

    args.cflags = args.cflags.replace('-DNO_INTELLISENSE', '').replace("/opt/boost", cwd+"/../boost")

    commands = []
    for fname in args.sources.split(' '):
      d = {}
      d["directory"] = cwd
      compiler = ""
      if fname.endswith("cpp") or fname.endswith(".h"):
        compiler = "clang++ -x c++ " + args.cflags + args.cxxflags
      if fname.endswith("c"):
        compiler = "clang -x c " + args.cflags
      d["command"] = compiler
      d["file"] = fname
      commands.append(d)

    json.dump(commands, open(args.out, "w"))

if __name__ == '__main__':
    main()
