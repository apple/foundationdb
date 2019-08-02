#!/usr/bin/env python

import json
import argparse
import filecmp
import os
import shutil


class InternalError(Exception):
    pass


class TemplateGenerator:
    def __init__(self, args):
        self.targetName = args.target
        self.numFiles = args.num_files
        self.fileName = args.out
        with open(args.file, 'r') as f:
            self.descr = json.load(f)
        if args.target not in self.descr:
            print('ERROR: undefined target {}'.format(args.target))
            raise InternalError

    def generate(self):
        target = self.descr[self.targetName]
        dependencies = []
        includesForFile = []
        sysIncludesForFile = []
        typesForFile = []
        for dep in target['dependencies'] if 'dependencies' in target else []:
            if dep not in self.descr:
                print("ERROR: {} was declared as dependency of {} but does not exist".format(dep, self.targetName))
                raise InternalError
            dependencies.append((dep, self.descr[dep]))
        for i in range(0, self.numFiles):
            includesForFile.append(set())
            sysIncludesForFile.append(set())
            typesForFile.append(set())
            
            for include in target["includes"] if 'includes' in target else []:
                includesForFile[i].add(include)
            for include in target["sysincludes"] if 'sysincludes' in target else []:
                sysIncludesForFile[i].add(include)
            j = 0
            templates = target['templates'] if 'templates' in target else []
            for template in templates:
                if j % self.numFiles == i:
                    for inc in template["includes"] if "includes" in template else []:
                        includesForFile[i].add(inc)
                    for inc in template["sysincludes"] if "sysincludes" in template else []:
                        includesForFile[i].add(inc)
                    for t in template["types"]:
                        typesForFile[i].add(t)
                j += 1

        for i in range(0, self.numFiles):
            outFile = "{}{}.cpp".format(self.fileName, i)
            tmpFile = "{}.tmp".format(outFile)
            with open(tmpFile, 'w') as f:
                f.write('// This file was generated - DO NOT CHANGE\n\n')
                for inc in sysIncludesForFile[i]:
                    f.write('#include <{}>\n'.format(inc))
                f.write("\n")
                for inc in includesForFile[i]:
                    f.write('#include "{}"\n'.format(inc))
                f.write("\n\n")
                for t in typesForFile[i]:
                    f.write('IMPLEMENT_SERIALIZATION_FOR({})\n'.format(t))
                # Compiling these files will be quite expensive. So we make sure to only
                # write them if they have changed
            if (not os.path.exists(outFile)) or (not filecmp.cmp(outFile, tmpFile)):
                shutil.copyfile(tmpFile, outFile)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Generate explicit template specification")
    parser.add_argument('file', type=str, help='The json file with the definitions')
    parser.add_argument('-N', '--num-files', type=int, default=8)
    parser.add_argument('-o', '--out', type=str, default="SerializeImpl")
    parser.add_argument('-t', '--target', type=str, required=True)
    args = parser.parse_args()
    generator = TemplateGenerator(args)
    generator.generate()