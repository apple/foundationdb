import json
import math
import random
import struct
from collections import OrderedDict
from itertools import chain

Byte = 1
UByte = 2
Bool = 3
Short = 4
UShort = 5
Int = 6
UInt = 7
Float = 8
Long = 9
ULong = 10
Double = 11

# TODO(anoyes): This is becoming unwieldly.

class Type:
    pass


def choose_type(depth, allow_vector=True):
    choices = [Primitive]
    if depth > 0:
        #if allow_vector:
        #    choices.append(Vector)
        #choices.extend([Struct, Table, Union])
        choices.extend([Table])
    return random.choice(choices).choose_type(depth)


def sample_bernoulli(p):
    result = 0
    while True:
        if random.random() > p:
            return result
        result += 1


def sample_string():
    return ''.join(random.choice(
        'abcdefghijklmnopqrstuvwxyz') for _ in range(sample_bernoulli(0.7)))


class Primitive(Type):
    def __init__(self, prim):
        assert(Byte <= prim <= Double)
        self.prim = prim

    def __repr__(self):
        return 'Primitive(%s)' % {
            1: 'Byte',
            2: 'UByte',
            3: 'Bool',
            4: 'Short',
            5: 'UShort',
            6: 'Int',
            7: 'UInt',
            8: 'Float',
            9: 'Long',
            10: 'ULong',
            11: 'Double',
        }[self.prim]

    def to_fbidl(self, tables=None):
        return {
            1: 'byte',
            2: 'ubyte',
            3: 'bool',
            4: 'short',
            5: 'ushort',
            6: 'int',
            7: 'uint',
            8: 'float',
            9: 'long',
            10: 'ulong',
            11: 'double',
            12: 'string',
        }[self.prim]

    def to_cpp(self, tables=None):
        return {
            1: 'int8_t',
            2: 'uint8_t',
            3: 'bool',
            4: 'int16_t',
            5: 'uint16_t',
            6: 'int32_t',
            7: 'uint32_t',
            8: 'float',
            9: 'int64_t',
            10: 'uint64_t',
            11: 'double',
            12: 'std::string',
        }[self.prim]

    def to_fb_cpp(self):
        if self.prim == Bool:
            return 'uint8_t'
        return self.to_cpp()

    @staticmethod
    def choose_type(depth=1):
        return Primitive(random.choice(list(range(Byte, Double + 1))))

    def __eq__(self, other):
        return isinstance(other, Primitive) and self.prim == other.prim

    def __hash__(self):
        return hash(self.prim)


class Vector(Type):
    def __init__(self, elem):
        assert(isinstance(elem, Type))
        self.elem = elem

    @staticmethod
    def choose_type(depth):
        return Vector(choose_type(depth - 1, allow_vector=False))

    def __repr__(self):
        return 'Vector(%s)' % self.elem.__repr__()

    def to_fbidl(self, tables=None):
        return '[%s]' % self.elem.to_fbidl(tables)

    def to_cpp(self, tables=None):
        return 'std::vector<%s>' % self.elem.to_cpp(tables)

    def to_fb_cpp(self):
        if isinstance(self.elem, Struct):
            return 'flatbuffers::Offset<flatbuffers::Vector<const {0}*>>'.format(self.elem.to_fb_cpp())
        return 'flatbuffers::Offset<flatbuffers::Vector<{0}>>'.format(self.elem.to_fb_cpp())

    def __eq__(self, other):
        return isinstance(other, Vector) and self.elem == other.elem

    def __hash__(self):
        return hash(self.elem)


global_counter = 0
def fresh():
    global global_counter
    result = global_counter
    global_counter += 1
    return result

class Table(Type):
    def __init__(self, fields, name):
        assert(all(isinstance(elem, Type) for (_, elem) in fields.items()))
        assert(all(type(field) == str for (field, _) in fields.items()))
        self.fields = sorted(fields.items(), key=lambda pair: pair[0])
        self.name = name

    @staticmethod
    def choose_type(depth):
        name = 'Table%d' % fresh()
        num_fields = sample_bernoulli(0.9)
        fields = {'m_' + sample_string(): choose_type(
            depth - 1) for _ in range(num_fields)}
        return Table(fields, name)

    def to_fbidl(self, tables):
        return tables.new_table(self.name, self.fields)

    def to_cpp(self, tables):
        return tables.new_table(self.name, self.fields)

    def to_fb_cpp(self):
        return 'flatbuffers::Offset<theirs::testfb::{0}>'.format(self.name)

    def __repr__(self):
        return 'Table(%s, %s)' % (self.fields.__repr__(), self.name.__repr__())

    def __eq__(self, other):
        return isinstance(other, Table) and self.name == other.name

    def __hash__(self):
        return hash(self.name)

class Union(Type):
    def __init__(self, fields_and_types, name):
        assert (all(isinstance(t, Type) for (_, t) in fields_and_types))
        assert (all(type(name) == str for (name, _) in fields_and_types))
        self.fields_and_types = fields_and_types
        self.name = name

    @staticmethod
    def choose_type(depth):
        name = 'Union%d' % fresh()
        num_fields = (min(10, 1 + sample_bernoulli(0.9)) if depth > 0 else random.randint(1, Double))
        names = set()
        types = OrderedDict()
        while len(names) < num_fields:
            names.add('m_' + sample_string())
        while len(types) < num_fields:
            types[Table.choose_type(0)] = None
        return Union(list(zip(sorted(names), types.keys())), name)

    def to_fbidl(self, tables):
        return tables.new_union(self.name, self.fields_and_types)

    def to_cpp(self, tables):
        return tables.new_union(self.name, self.fields_and_types)

    def __repr__(self):
        return 'Union(%s, %s)' % (self.fields_and_types.__repr__(), self.name.__repr__())

    def __eq__(self, other):
        return isinstance(other, Union) and self.name == other.name

    def __hash__(self):
        return hash(self.name)

class Struct(Type):
    def __init__(self, fields, name):
        assert(all(isinstance(elem, Type) for (_, elem) in fields.items()))
        assert(all(type(field) == str for (field, _) in fields.items()))
        self.fields = sorted(fields.items(), key=lambda pair: pair[0])
        self.name = name

    @staticmethod
    def choose_type(depth):
        name = 'Struct%d' % fresh()
        num_fields = 1 + sample_bernoulli(0.9)
        fields = {
            'm_' + str(i): Primitive.choose_type()
            for i in range(num_fields)
        }
        return Struct(fields, name)

    def to_fbidl(self, tables):
        return tables.new_struct(self.name, self.fields)

    def to_cpp(self, tables):
        return tables.new_struct(self.name, self.fields)

    def to_fb_cpp(self):
        return 'theirs::testfb::{0}'.format(self.name)

    def __repr__(self):
        return 'Struct(%s, %s)' % (self.fields.__repr__(), self.name.__repr__())

    def __eq__(self, other):
        return isinstance(other, Struct) and self.name == other.name

    def __hash__(self):
        return hash(self.name)

class CollectIdlTables:
    tables = []

    def new_struct(self, name, fields):
        struct = 'struct %s {\n' % name
        struct += '\n'.join('    %s:%s;' % (f, t.to_fbidl(self)) for (
            f, t) in fields)
        struct += '\n}\n'
        self.tables.append(struct)
        return name

    def new_table(self, name, fields):
        table = 'table %s {\n' % name
        table += '\n'.join('    %s:%s;' % (f, t.to_fbidl(self)) for (
            f, t) in fields)
        table += '\n}\n'
        self.tables.append(table)
        return name

    def new_union(self, name, fields):
        union = 'union %s {\n' % name
        union += ',\n'.join('    %s' % t.to_fbidl(self) for (
            f, t) in fields)
        union += '\n}\n'
        self.tables.append(union)
        return name



class CollectCppTables:
    tables = []
    names = set()
    counter = 0

    def _get_random(self, args):
        (k, v) = args
        if isinstance(v, Union):
            result = '    theirs::testfb::{2} {0}_type = (theirs::testfb::{2})std::uniform_int_distribution<uint8_t>(1, {1})(r);\n'.format(k, len(v.fields_and_types), v.name)
            result += '    flatbuffers::Offset<void> {0};\n'.format(k)
            for i, (alternative_k, alternative_v) in enumerate(v.fields_and_types):
                result += '    if ({0}_type == (theirs::testfb::{2}){1}) {{ {3} x; Randomize(r, x, fbb); {0} = x.Union(); }}\n'.format(k, i + 1, v.name, alternative_v.to_fb_cpp())
            return result;
        if isinstance(v, Vector) and isinstance(v.elem, Union):
            result = '    int {0}_len = std::geometric_distribution<>(0.1)(r);\n'.format(k)
            result += '    std::vector<uint8_t> {0}_types({0}_len);\n'.format(k)
            result += '    std::vector<flatbuffers::Offset<void>> {0}_vector({0}_len);\n'.format(k)
            result += '    for (int i = 0; i < {0}_len; ++i) {{\n'.format(k)
            result += '        {0}_types[i] = std::uniform_int_distribution<uint8_t>(1, {1})(r);\n'.format(k, len(v.elem.fields_and_types), v.elem.name)
            for i, (alternative_k, alternative_v) in enumerate(v.elem.fields_and_types):
                result += '        if ({0}_types[i] == {1}) {{ {3} x; Randomize(r, x, fbb); {0}_vector[i] = x.Union(); }}\n'.format(k, i + 1, v.elem.name, alternative_v.to_fb_cpp())
            result += '    }\n'
            result += '    flatbuffers::Offset<flatbuffers::Vector<uint8_t>> {0}_type = fbb.CreateVector({0}_types);\n'.format(k)
            result += '    flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<void>>> {0} = fbb.CreateVector({0}_vector);\n'.format(k)
            return result;
        return '    {1} {0}; Randomize(r, {0}, fbb);'.format(k, v.to_fb_cpp())

    def new_table(self, name, fields):
        if name in self.names:
            return name
        self.names.add(name)
        self.counter += 1

        def verify(args):
            (k, v) = args
            if isinstance(v, Union):
                verifier = '    Verify(lhs.{0}.index(), (size_t)(rhs->{0}_type() - 1), context + ".{0}_type");\n'.format(k)
                for i, (alternative_name, alternative_type) in enumerate(v.fields_and_types):
                    verifier += '    if (lhs.{0}.index() == {1} && rhs->{0}_type() - 1 == {1}) Verify(std::get<{3}>(lhs.{0}), rhs->{0}_as_{3}(), context + ".{0}.{2}");\n'.format(
                            k, i, alternative_name, alternative_type.to_cpp(self))
                return verifier.rstrip();
            if isinstance(v, Vector) and isinstance(v.elem, Union):
                verifier = '    CHECK_MESSAGE(lhs.{0}.size() == rhs->{0}()->size(), context);\n'.format(k)
                verifier += '    CHECK_MESSAGE(lhs.{0}.size() == rhs->{0}_type()->size(), context);\n'.format(k)
                verifier += '    for (int i = 0; i < lhs.{0}.size(); ++i) {{\n'.format(k)
                verifier += '        Verify(lhs.{0}[i].index(), (size_t)(rhs->{0}_type()->Get(i) - 1), context + "[" + std::to_string(i) + "].{0}_type");\n'.format(k)
                verifier += '        Verify(lhs.{0}[i].index(), (size_t)(rhs->{0}_type()->Get(i) - 1), context + "[" + std::to_string(i) + "].{0}_type");\n'.format(k)
                for i, (alternative_name, alternative_type) in enumerate(v.elem.fields_and_types):
                    verifier += '        if (lhs.{0}[i].index() == {1} && rhs->{0}_type()->Get(i) - 1 == {1})'.format(k, i, alternative_name, alternative_type.to_cpp(self))
                    verifier += '            Verify(std::get<{3}>(lhs.{0}[i]), rhs->{0}()->GetAs<theirs::testfb::{3}>(i), context + "[" + std::to_string(i) + "].{0}.{2}");\n'.format(k, i, alternative_name, alternative_type.to_cpp(self))
                verifier += '    }\n'
                return verifier.rstrip();
            return '    Verify(lhs.{0}, rhs->{0}(), context + ".{0}");'.format(k)

        table = 'struct %s {\n' % name
        table += '\n'.join('    %s %s = {};' % (t.to_cpp(self), f)
                for (f, t) in fields) + '\n'
        table += '    template <class Archiver>\n'
        table += '    void serialize(Archiver& ar) {\n'
        table += '        serializer(%s);\n' % ', '.join(
                chain(['ar'], (k for (k, _) in fields)))
        table += '    }\n'
        table += '};\n'
        table += 'bool operator==(const ours::' + name + '& lhs, const ours::' + name + '& rhs) {\n'
        table += '    return %s;\n' % ' && '.join(
            chain(['true'],
                  ('lhs.{0} == rhs.{0}'.format(k) for (k, _) in fields)))
        table += '}\n'
        table += 'void Randomize(std::mt19937_64& r, ours::{0}& x) {{\n'.format(name)
        table += '{0}\n'.format('\n'.join(('    Randomize(r, x.{0});'.format(k) for (k, _) in fields)))
        table += '}\n'
        table += 'void Verify(const ours::' + name + '& lhs, const theirs::testfb::' + name + '* rhs, std::string context) {\n'
        table += '{0}\n'.format('\n'.join(map(verify, fields)))
        table += '}\n'
        table += 'void Randomize(std::mt19937_64& r, flatbuffers::Offset<theirs::testfb::{0}>& result, flatbuffers::FlatBufferBuilder& fbb) {{\n'.format(name)
        table += '{0}\n'.format('\n'.join(map(self._get_random, fields)))
        def field_helper(arg):
            (k, v) = arg
            if isinstance(v, Struct):
                return ['&' + k]
            if isinstance(v, Union) or isinstance(v, Vector) and isinstance(v.elem, Union):
                return [k + '_type', k]
            return [k]
        table += '    result = theirs::testfb::Create{0}({1});\n'.format(name, ', '.join(chain(['fbb'], chain(*map(field_helper, fields)))))
        table += '}\n'
        self.tables.append(table)

        return name

    def new_union(self, name, fields):
        return 'std::variant<%s>' % ', '.join((t.to_cpp(self) for (_, t) in fields))

    def new_struct(self, name, fields):
        t = 'std::tuple<%s>' % ', '.join((t.to_cpp(self) for (_, t) in fields))
        if name in self.names:
            return t
        self.names.add(name)
        table = 'void Verify(const {0}& lhs, const theirs::testfb::{1}* rhs, std::string context) {{\n'.format(t, name)
        table += '{0}\n'.format('\n'.join(('    Verify(std::get<{0}>(lhs), rhs->{1}(), context + ".{1}");'.format(i, k) for (i, (k, _)) in enumerate(fields))))
        table += '}\n'
        table += 'void Randomize(std::mt19937_64& r, theirs::testfb::{0}& result, flatbuffers::FlatBufferBuilder& fbb) {{\n'.format(name)
        table += '{0}\n'.format('\n'.join(map(self._get_random, fields)))
        table += '    result = theirs::testfb::{0}({1});\n'.format(name, ', '.join(x[0] for x in fields))
        table += '}\n'
        self.tables.append(table)
        return t

def to_fbidl(t):
    collectTables = CollectIdlTables()
    t.to_fbidl(collectTables)
    result = 'namespace testfb;\n'
    result += '\n'.join(collectTables.tables)
    result += '\nroot_type Table0;'
    return result


def to_cpp(t, seed):
    collectTables = CollectCppTables()
    t.to_cpp(collectTables)
    result = '#pragma once\n'
    result += '#include <stdint.h>\n'
    result += '#include <vector>\n'
    result += '#include <string>\n'
    result += 'constexpr long kSeed = {0};\n'.format(seed)
    result += '\n'
    result += '\n'.join(collectTables.tables)
    return result


if __name__ == "__main__":
    import sys
    s = int(sys.argv[1], 0)
    random.seed(s)
    t = Table.choose_type(3)
    if sys.argv[2] == 'cpp':
        print(to_cpp(t, s))
    if sys.argv[2] == 'fbs':
        print(to_fbidl(t))
