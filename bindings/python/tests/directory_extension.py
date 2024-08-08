#
# directory_extension.py
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

import fdb
import fdb.directory_impl

ops_that_create_dirs = [
    "DIRECTORY_CREATE_SUBSPACE",
    "DIRECTORY_CREATE_LAYER",
    "DIRECTORY_CREATE_OR_OPEN",
    "DIRECTORY_CREATE",
    "DIRECTORY_OPEN",
    "DIRECTORY_MOVE",
    "DIRECTORY_MOVE_TO",
    "DIRECTORY_OPEN_SUBSPACE",
]

log_all = False

log_instructions = False
log_ops = False
log_dirs = False
log_errors = False


def log_op(msg, force=False):
    if log_ops or log_all or force:
        print(msg)


class DirectoryExtension:
    def __init__(self):
        self.dir_list = [fdb.directory]
        self.dir_index = 0
        self.error_index = 0

    def pop_tuples(self, stack, num=None):
        actual_num = num
        if actual_num is None:
            actual_num = 1

        tuples = tuple([tuple(stack.pop(stack.pop())) for i in range(actual_num)])

        if num is None:
            return tuples[0]

        return tuples

    def append_dir(self, inst, dir):
        if log_dirs or log_all:
            print(
                "pushed %s at %d (op=%s)"
                % (dir.__class__.__name__, len(self.dir_list), inst.op)
            )

        self.dir_list.append(dir)

    def process_instruction(self, inst):
        try:
            if log_all or log_instructions:
                print("%d. %s" % (inst.index, inst.op))

            directory = self.dir_list[self.dir_index]
            if inst.op == "DIRECTORY_CREATE_SUBSPACE":
                path = self.pop_tuples(inst.stack)
                raw_prefix = inst.pop()
                log_op("created subspace at %r: %r" % (path, raw_prefix))
                self.append_dir(inst, fdb.Subspace(path, raw_prefix))
            elif inst.op == "DIRECTORY_CREATE_LAYER":
                index1, index2, allow_manual_prefixes = inst.pop(3)
                if self.dir_list[index1] is None or self.dir_list[index2] is None:
                    log_op("create directory layer: None")
                    self.append_dir(inst, None)
                else:
                    log_op(
                        "create directory layer: node_subspace (%d) = %r, content_subspace (%d) = %r, allow_manual_prefixes = %d"
                        % (
                            index1,
                            self.dir_list[index1].rawPrefix,
                            index2,
                            self.dir_list[index2].rawPrefix,
                            allow_manual_prefixes,
                        )
                    )
                    self.append_dir(
                        inst,
                        fdb.DirectoryLayer(
                            self.dir_list[index1],
                            self.dir_list[index2],
                            allow_manual_prefixes == 1,
                        ),
                    )
            elif inst.op == "DIRECTORY_CHANGE":
                self.dir_index = inst.pop()
                if not self.dir_list[self.dir_index]:
                    self.dir_index = self.error_index
                if log_dirs or log_all:
                    new_dir = self.dir_list[self.dir_index]
                    clazz = new_dir.__class__.__name__
                    new_path = (
                        repr(new_dir._path) if hasattr(new_dir, "_path") else "<na>"
                    )
                    print(
                        "changed directory to %d (%s @%r)"
                        % (self.dir_index, clazz, new_path)
                    )
            elif inst.op == "DIRECTORY_SET_ERROR_INDEX":
                self.error_index = inst.pop()
            elif inst.op == "DIRECTORY_CREATE_OR_OPEN":
                path = self.pop_tuples(inst.stack)
                layer = inst.pop()
                log_op(
                    "create_or_open %r: layer=%r" % (directory.get_path() + path, layer)
                )
                d = directory.create_or_open(inst.tr, path, layer or b"")
                self.append_dir(inst, d)
            elif inst.op == "DIRECTORY_CREATE":
                path = self.pop_tuples(inst.stack)
                layer, prefix = inst.pop(2)
                log_op(
                    "create %r: layer=%r, prefix=%r"
                    % (directory.get_path() + path, layer, prefix)
                )
                self.append_dir(
                    inst, directory.create(inst.tr, path, layer or b"", prefix)
                )
            elif inst.op == "DIRECTORY_OPEN":
                path = self.pop_tuples(inst.stack)
                layer = inst.pop()
                log_op("open %r: layer=%r" % (directory.get_path() + path, layer))
                self.append_dir(inst, directory.open(inst.tr, path, layer or b""))
            elif inst.op == "DIRECTORY_MOVE":
                old_path, new_path = self.pop_tuples(inst.stack, 2)
                log_op(
                    "move %r to %r"
                    % (directory.get_path() + old_path, directory.get_path() + new_path)
                )
                self.append_dir(inst, directory.move(inst.tr, old_path, new_path))
            elif inst.op == "DIRECTORY_MOVE_TO":
                new_absolute_path = self.pop_tuples(inst.stack)
                log_op("move %r to %r" % (directory.get_path(), new_absolute_path))
                self.append_dir(inst, directory.move_to(inst.tr, new_absolute_path))
            elif inst.op == "DIRECTORY_REMOVE":
                count = inst.pop()
                if count == 0:
                    log_op("remove %r" % (directory.get_path(),))
                    directory.remove(inst.tr)
                else:
                    path = self.pop_tuples(inst.stack)
                    log_op("remove %r" % (directory.get_path() + path,))
                    directory.remove(inst.tr, path)
            elif inst.op == "DIRECTORY_REMOVE_IF_EXISTS":
                count = inst.pop()
                if count == 0:
                    log_op("remove_if_exists %r" % (directory.get_path(),))
                    directory.remove_if_exists(inst.tr)
                else:
                    path = self.pop_tuples(inst.stack)
                    log_op("remove_if_exists %r" % (directory.get_path() + path,))
                    directory.remove_if_exists(inst.tr, path)
            elif inst.op == "DIRECTORY_LIST":
                count = inst.pop()
                if count == 0:
                    result = directory.list(inst.tr)
                    log_op("list %r" % (directory.get_path(),))
                else:
                    path = self.pop_tuples(inst.stack)
                    result = directory.list(inst.tr, path)
                    log_op("list %r" % (directory.get_path() + path,))

                inst.push(fdb.tuple.pack(tuple(result)))
            elif inst.op == "DIRECTORY_EXISTS":
                count = inst.pop()
                if count == 0:
                    result = directory.exists(inst.tr)
                    log_op("exists %r: %d" % (directory.get_path(), result))
                else:
                    path = self.pop_tuples(inst.stack)
                    result = directory.exists(inst.tr, path)
                    log_op("exists %r: %d" % (directory.get_path() + path, result))

                if result:
                    inst.push(1)
                else:
                    inst.push(0)
            elif inst.op == "DIRECTORY_PACK_KEY":
                key_tuple = self.pop_tuples(inst.stack)
                inst.push(directory.pack(key_tuple))
            elif inst.op == "DIRECTORY_UNPACK_KEY":
                key = inst.pop()
                log_op(
                    "unpack %r in subspace with prefix %r" % (key, directory.rawPrefix)
                )
                tup = directory.unpack(key)
                for t in tup:
                    inst.push(t)
            elif inst.op == "DIRECTORY_RANGE":
                tup = self.pop_tuples(inst.stack)
                rng = directory.range(tup)
                inst.push(rng.start)
                inst.push(rng.stop)
            elif inst.op == "DIRECTORY_CONTAINS":
                key = inst.pop()
                result = directory.contains(key)
                if result:
                    inst.push(1)
                else:
                    inst.push(0)
            elif inst.op == "DIRECTORY_OPEN_SUBSPACE":
                path = self.pop_tuples(inst.stack)
                log_op("open_subspace %r (at %r)" % (path, directory.key()))
                self.append_dir(inst, directory.subspace(path))
            elif inst.op == "DIRECTORY_LOG_SUBSPACE":
                prefix = inst.pop()
                inst.tr[prefix + fdb.tuple.pack((self.dir_index,))] = directory.key()
            elif inst.op == "DIRECTORY_LOG_DIRECTORY":
                prefix = inst.pop()
                exists = directory.exists(inst.tr)
                if exists:
                    children = tuple(directory.list(inst.tr))
                else:
                    children = ()
                logSubspace = fdb.Subspace((self.dir_index,), prefix)
                inst.tr[logSubspace["path"]] = fdb.tuple.pack(directory.get_path())
                inst.tr[logSubspace["layer"]] = fdb.tuple.pack((directory.get_layer(),))
                inst.tr[logSubspace["exists"]] = fdb.tuple.pack((int(exists),))
                inst.tr[logSubspace["children"]] = fdb.tuple.pack(children)
            elif inst.op == "DIRECTORY_STRIP_PREFIX":
                s = inst.pop()
                if not s.startswith(directory.key()):
                    raise Exception(
                        "String %r does not start with raw prefix %r"
                        % (s, directory.key())
                    )

                inst.push(s[len(directory.key()) :])
            else:
                raise Exception("Unknown op: %s" % inst.op)
        except Exception as e:
            if log_all or log_errors:
                print(e)
                # traceback.print_exc(file=sys.stdout)

            if inst.op in ops_that_create_dirs:
                self.append_dir(inst, None)

            inst.push(b"DIRECTORY_ERROR")
