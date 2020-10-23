#!/usr/bin/env python3
from argparse import ArgumentParser
import os
import json
import re

def actorFile(actor: str, build: str, src: str):
    res = actor.replace(build, src, 1)
    res = res.replace('actor.g.cpp', 'actor.cpp')
    return res.replace('actor.g.h', 'actor.h')

def rreplace(s, old, new, occurrence = 1):
    li = s.rsplit(old, occurrence)
    return new.join(li)


def actorCommand(cmd: str, build:str, src: str):
    r1 = re.compile('-c (.+)(actor\.g\.cpp)')
    m1 = r1.search(cmd)
    if m1 is None:
        return cmd
    cmd1 = r1.sub('\\1actor.cpp', cmd)
    return rreplace(cmd1, build, src)


parser = ArgumentParser(description="Generates a new compile_commands.json for rtags+flow")
parser.add_argument("-b", help="Build directory", dest="builddir", default=os.getcwd())
parser.add_argument("-s", help="Build directory", dest="srcdir", default=os.getcwd())
parser.add_argument("-o", help="Output file", dest="out", default="processed_compile_commands.json")
parser.add_argument("input", help="compile_commands.json", default="compile_commands.json", nargs="?")
args = parser.parse_args()

print("transform {} with build directory {}".format(args.input, args.builddir))

with open(args.input) as f:
    cmds = json.load(f)

result = []

for cmd in cmds:
    additional_flags = ['-Wno-unknown-attributes']
    cmd['command'] = cmd['command'].replace(' -DNO_INTELLISENSE ', ' {} '.format(' '.join(additional_flags)))
    if cmd['file'].endswith('actor.g.cpp'):
        # here we need to rewrite the rule
        cmd['command'] = actorCommand(cmd['command'], args.builddir, args.srcdir)
        cmd['file'] = actorFile(cmd['file'], args.builddir, args.srcdir)
        result.append(cmd)
    else:
        result.append(cmd)

with open(args.out, 'w') as f:
    json.dump(result, f, indent=4)
