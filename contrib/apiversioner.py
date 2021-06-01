#!/usr/bin/env python3
#
# apiversioner.py
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2021 Apple Inc. and the FoundationDB project authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import argparse
import logging
import os
import re
import sys
import traceback


LOG_FORMAT = '%(created)f [%(levelname)s] %(message)s'

EXCLUDED_FILES = list(map(re.compile, [
    # Output directories
    r'\.git/.*', r'bin/.*', r'packages/.*', r'\.objs/.*', r'\.deps/.*', r'bindings/go/build/.*', r'documentation/sphinx/\.out/.*',

    # Generated files
    r'.*\.g\.cpp$', r'.*\.g\.h$', r'(^|.*/)generated.mk$', r'.*\.g\.S$',
    r'.*/MutationType\.java', r'.*/generated\.go',

    # Binary files
    r'.*\.class$', r'.*\.o$', r'.*\.a$', r'.*[\.-]debug', r'.*\.so$', r'.*\.dylib$', r'.*\.dll$', r'.*\.tar[^/]*$', r'.*\.jar$', r'.*pyc$', r'bindings/flow/bin/.*',
    r'.*\.pdf$', r'.*\.jp[e]*g', r'.*\.png', r'.*\.ico',
    r'packaging/msi/art/.*',

    # Project configuration files
    r'.*foundationdb\.VC\.db$', r'.*foundationdb\.VC\.VC\.opendb$', r'.*iml$',

    # Source files from someone else
    r'(^|.*/)Hash3\..*', r'(^|.*/)sqlite.*',
    r'bindings/go/godoc-resources/.*',
    r'bindings/go/src/fdb/tuple/testdata/tuples.golden',
    r'fdbcli/linenoise/.*',
    r'fdbrpc/rapidjson/.*', r'fdbrpc/rapidxml/.*', r'fdbrpc/zlib/.*', r'fdbrpc/sha1/.*',
    r'fdbrpc/xml2json.hpp$', r'fdbrpc/libcoroutine/.*', r'fdbrpc/libeio/.*', r'fdbrpc/lib64/.*',
    r'fdbrpc/generated-constants.cpp$',

    # Miscellaneous
    r'bindings/nodejs/node_modules/.*', r'bindings/go/godoc/.*', r'.*trace.*xml$', r'.*log$', r'.*\.DS_Store$', r'simfdb/\.*', r'.*~$', r'.*.swp$'
]))

SUSPECT_PHRASES = map(re.compile, [
    r'#define\s+FDB_API_VERSION\s+(\d+)',
    r'\.\s*selectApiVersion\s*\(\s*(\d+)\s*\)',
    r'\.\s*APIVersion\s*\(\s*(\d+)\s*\)',
    r'\.\s*MustAPIVersion\s*\(\s*(\d+)\s*\)',
    r'header_version\s+=\s+(\d+)',
    r'\.\s*apiVersion\s*\(\s*(\d+)\s*\)',
    r'API_VERSION\s*=\s*(\d+)',
    r'fdb_select_api_version\s*\((\d+)\)'
])

DIM_CODE = '\033[2m'
BOLD_CODE = '\033[1m'
RED_COLOR = '\033[91m'
GREEN_COLOR = '\033[92m'
END_COLOR = '\033[0m'


def positive_response(val):
    return val.lower() in {'y', 'yes'}


# Returns: new line list + a dirty flag
def rewrite_lines(lines, version_re, new_version, suspect_only=True, print_diffs=False, ask_confirm=False, grayscale=False):
    new_lines = []
    dirty = False
    new_str = str(new_version)
    regexes = SUSPECT_PHRASES if suspect_only else [version_re]
    group_index = 1 if suspect_only else 2
    for line_no, line in enumerate(lines):
        new_line = line
        offset = 0

        for regex in regexes:
            for m in regex.finditer(line):
                # Replace suspect code with new version.
                start = m.start(group_index)
                end = m.end(group_index)
                new_line = new_line[:start + offset] + new_str + new_line[end + offset:]
                offset += len(new_str) - (end - start)

        if (print_diffs or ask_confirm) and line != new_line:
            print('Rewrite:')
            print('\n'.join(map(lambda pair: ' {:4d}: {}'.format(line_no - 1 + pair[0], pair[1]), enumerate(lines[line_no - 2:line_no]))))
            print((DIM_CODE if grayscale else RED_COLOR) + '-{:4d}: {}'.format(line_no + 1, line) + END_COLOR)
            print((BOLD_CODE if grayscale else GREEN_COLOR) + '+{:4d}: {}'.format(line_no + 1, new_line) + END_COLOR)
            print('\n'.join(map(lambda pair: ' {:4d}: {}'.format(line_no + 2 + pair[0], pair[1]), enumerate(lines[line_no + 1:line_no + 3]))))

            if ask_confirm:
                text = input('Looks good (y/n)? ')
                if not positive_response(text):
                    print('Okay, skipping.')
                    new_line = line

        dirty = dirty or (new_line != line)
        new_lines.append(new_line)

    return new_lines, dirty


def address_file(base_path, file_path, version, new_version=None, suspect_only=False, show_diffs=False,
                 rewrite=False, ask_confirm=True, grayscale=False, paths_only=False):
    if any(map(lambda x: x.match(file_path), EXCLUDED_FILES)):
        logging.debug('skipping file %s as matches excluded list', file_path)
        return True

    # Look for all instances of the version number where it is not part of a larger number
    version_re = re.compile('(^|[^\\d])(' + str(version) + ')([^\\d]|$)')
    try:
        contents = open(os.path.join(base_path, file_path), 'r').read()
        lines = contents.split('\n')
        new_lines = lines
        dirty = False

        if suspect_only:
            # Look for suspect lines (lines that attempt to set a version)
            found = False
            for line_no, line in enumerate(lines):
                for suspect_phrase in SUSPECT_PHRASES:
                    for match in suspect_phrase.finditer(line):
                        curr_version = int(match.groups()[0])
                        if (new_version is None and curr_version < version) or (new_version is not None and curr_version < new_version):
                            found = True
                            logging.info('Old version: %s:%d:%s', file_path, line_no + 1, line)

            if found and new_version is not None and (show_diffs or rewrite):
                new_lines, dirty = rewrite_lines(lines, version_re, new_version, True, print_diffs=True,
                                                 ask_confirm=(rewrite and ask_confirm), grayscale=grayscale)

        else:
            matching_lines = filter(lambda pair: version_re.search(pair[1]), enumerate(lines))

            # Look for lines with the version
            if matching_lines:
                if paths_only:
                    logging.info('File %s matches', file_path)
                else:
                    for line_no, line in matching_lines:
                        logging.info('Match: %s:%d:%s', file_path, line_no + 1, line)
                    if new_version is not None and (show_diffs or rewrite):
                        new_lines, dirty = rewrite_lines(lines, version_re, new_version, False, print_diffs=True,
                                                         ask_confirm=(rewrite and ask_confirm), grayscale=grayscale)
            else:
                logging.debug('File %s does not match', file_path)

        if dirty and rewrite:
            logging.info('Rewriting %s', os.path.join(base_path, file_path))
            with open(os.path.join(base_path, file_path), 'w') as fout:
                fout.write('\n'.join(new_lines))

        return True
    except (OSError, UnicodeDecodeError) as e:
        logging.exception('Unable to read file %s due to OSError', os.path.join(base_path, file_path))
        return False


def address_path(path, version, new_version=None, suspect_only=False, show_diffs=False, rewrite=False, ask_confirm=True, grayscale=False, paths_only=False):
    try:
        if os.path.exists(path):
            if os.path.isdir(path):
                status = True
                for dir_path, dir_names, file_names in os.walk(path):
                    for file_name in file_names:
                        file_path = os.path.relpath(os.path.join(dir_path, file_name), path)
                        status = address_file(path, file_path, version, new_version, suspect_only, show_diffs,
                                              rewrite, ask_confirm, grayscale, paths_only) and status
                return status
            else:
                base_name, file_name = os.path.split(path)
                return address_file(base_name, file_name, version, new_version, suspect_only, show_diffs, rewrite, ask_confirm, grayscale)
        else:
            logging.error('Path %s does not exist', path)
            return False
    except OSError as e:
        logging.exception('Unable to find all API versions due to OSError')
        return False


def run(arg_list):
    parser = argparse.ArgumentParser(description='finds and rewrites the API version in FDB source files')
    parser.add_argument('path', help='path to search for FDB source files')
    parser.add_argument('version', type=int, help='current/old version to search for')
    parser.add_argument('--new-version', type=int, default=None, help='new version to update to')
    parser.add_argument('--suspect-only', action='store_true', default=False, help='only look for phrases trying to set the API version')
    parser.add_argument('--show-diffs', action='store_true', default=False, help='show suggested diffs for fixing version')
    parser.add_argument('--rewrite', action='store_true', default=False, help='rewrite offending files')
    parser.add_argument('-y', '--skip-confirm', action='store_true', default=False, help='do not ask for confirmation before rewriting')
    parser.add_argument('--grayscale', action='store_true', default=False,
                        help='print diffs using grayscale output instead of red and green')
    parser.add_argument('--paths-only', action='store_true', default=False, help='display only the path instead of the offending lines')
    args = parser.parse_args(arg_list)
    return address_path(args.path, args.version, args.new_version, args.suspect_only, args.show_diffs,
                        args.rewrite, not args.skip_confirm, args.grayscale, args.paths_only)


if __name__ == '__main__':
    logging.basicConfig(format=LOG_FORMAT, level=logging.INFO)
    if not run(sys.argv[1:]):
        exit(1)
