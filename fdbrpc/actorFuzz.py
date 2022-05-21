#
# actorFuzz.py
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

import random
import copy


class Context:
    tok = 0
    inLoop = False
    indent = 0

    def __init__(self):
        self.random = random.Random()

    def unique_id(self):
        return self.random.randint(100000, 999999)


class InfiniteLoop(Exception):
    pass


class ExecContext:
    iterationsLeft = 1000
    ifstate = 0

    def __init__(self, input_seq):
        self.input = iter(input_seq)
        self.output = []

    def inp(self):
        return next(self.input)

    def out(self, x):
        self.output.append(x)

    def infinity_check(self):
        self.iterationsLeft -= 1
        if self.iterationsLeft <= 0:
            raise InfiniteLoop()


OK = 1
BREAK = 2
THROW = 3
RETURN = 4
CONTINUE = 5


def indent(cx):
    return "\t" * cx.indent


class F(object):
    def unreachable(self):
        return False

    def containsbreak(self):
        return False


class HashF(F):
    def __init__(self, cx):
        self.cx = cx
        self.uniqueID = cx.unique_id()

    def __str__(self):
        return indent(self.cx) + "outputStream.send( %d );\n" % self.uniqueID

    def eval(self, ecx):
        ecx.infinity_check()
        ecx.out(self.uniqueID)
        return OK


class CompoundF(F):
    def __init__(self, cx, children):
        self.cx = cx
        self.children = []
        for c in children:
            self.children.append(c)
            if c.unreachable():
                self.unreachable = lambda: 1
                break

    def __str__(self):
        return "".join(str(c) for c in self.children)

    def eval(self, ecx):
        for c in self.children:
            ecx.infinity_check()
            result = c.eval(ecx)
            if result != OK:
                break
        return result

    def containsbreak(self):
        return any(c.containsbreak() for c in self.children)


class LoopF(F):
    def __init__(self, cx):
        self.cx = cx
        ccx = copy.copy(cx)
        ccx.indent += 1
        ccx.inLoop = True
        self.body = CompoundF(ccx, [HashF(ccx)] + [fuzz_code(ccx)(ccx)] + [HashF(ccx)])
        self.uniqueID = cx.unique_id()
        self.forever = cx.random.random() < 0.1

    def __str__(self):
        if self.forever:
            return (
                indent(self.cx) + "loop {\n" + str(self.body) + indent(self.cx) + "}\n"
            )
        else:
            return (
                indent(self.cx)
                + "state int i%d; for(i%d = 0; i%d < 5; i%d++) {\n"
                % ((self.uniqueID,) * 4)
                + str(self.body)
                + indent(self.cx)
                + "}\n"
            )

    def eval(self, ecx):
        if self.forever:
            while True:
                ecx.infinity_check()
                result = self.body.eval(ecx)
                if result == BREAK:
                    break
                elif result not in (OK, CONTINUE):
                    return result
        else:
            for i in range(5):
                ecx.infinity_check()
                result = self.body.eval(ecx)
                if result == BREAK:
                    break
                elif result not in (OK, CONTINUE):
                    return result
        return OK

    def unreachable(self):
        return self.forever and not self.body.containsbreak()


class RangeForF(F):
    def __init__(self, cx):
        self.cx = cx
        ccx = copy.copy(cx)
        ccx.indent += 1
        ccx.inLoop = True
        self.body = CompoundF(ccx, [HashF(ccx)] + [fuzz_code(ccx)(ccx)] + [HashF(ccx)])
        self.uniqueID = cx.unique_id()

    def __str__(self):
        return (
            indent(self.cx)
            + ("\n" + indent(self.cx))
            .join(
                [
                    "state std::vector<int> V;",
                    "V.push_back(1);",
                    "V.push_back(2);",
                    "V.push_back(3);",
                    "for( auto i : V ) {\n",
                ]
            )
            .replace("V", "list%d" % self.uniqueID)
            + indent(self.cx)
            + "\t(void)i;\n"
            + str(self.body)  # Suppress -Wunused-variable warning in generated code
            + indent(self.cx)
            + "}\n"
        )

    def eval(self, ecx):
        for i in range(1, 4):
            ecx.infinity_check()
            result = self.body.eval(ecx)
            if result == BREAK:
                break
            elif result not in (OK, CONTINUE):
                return result
        return OK

    def unreachable(self):
        return False


class IfF(F):
    def __init__(self, cx):
        self.cx = cx
        ccx = copy.copy(cx)
        ccx.indent += 1
        self.toggle = cx.random.randint(0, 1)
        self.ifbody = CompoundF(ccx, [HashF(ccx)] + [fuzz_code(ccx)(ccx)] + [HashF(ccx)])
        if cx.random.random() < 0.5:
            ccx = copy.copy(cx)
            ccx.indent += 1
            self.elsebody = CompoundF(
                ccx, [HashF(ccx)] + [fuzz_code(ccx)(ccx)] + [HashF(ccx)]
            )
        else:
            self.elsebody = None

    def __str__(self):
        s = (
            indent(self.cx)
            + "if ( (++ifstate&1) == %d ) {\n" % self.toggle
            + str(self.ifbody)
        )
        if self.elsebody:
            s += indent(self.cx) + "} else {\n" + str(self.elsebody)
        s += indent(self.cx) + "}\n"
        return s

    def eval(self, ecx):
        ecx.infinity_check()
        ecx.ifstate += 1
        if (ecx.ifstate & 1) == self.toggle:
            return self.ifbody.eval(ecx)
        elif self.elsebody:
            return self.elsebody.eval(ecx)
        else:
            return OK

    def unreachable(self):
        return (
            self.elsebody and self.ifbody.unreachable() and self.elsebody.unreachable()
        )

    def containsbreak(self):
        return self.ifbody.containsbreak() or (
            self.elsebody and self.elsebody.containsbreak()
        )


class TryF(F):
    def __init__(self, cx):
        self.cx = cx
        ccx = copy.copy(cx)
        ccx.indent += 1
        self.body = CompoundF(ccx, [HashF(ccx)] + [fuzz_code(ccx)(ccx)] + [HashF(ccx)])
        ccx = copy.copy(cx)
        ccx.indent += 1
        self.catch = CompoundF(ccx, [HashF(ccx)] + [fuzz_code(ccx)(ccx)] + [HashF(ccx)])

    def __str__(self):
        return (
            indent(self.cx)
            + "try {\n"
            + str(self.body)
            + indent(self.cx)
            + "} catch (...) {\n"
            + str(self.catch)
            + indent(self.cx)
            + "}\n"
        )

    def eval(self, ecx):
        ecx.infinity_check()
        result = self.body.eval(ecx)
        if result != THROW:
            return result
        return self.catch.eval(ecx)

    def unreachable(self):
        return self.body.unreachable() and self.catch.unreachable()

    def containsbreak(self):
        return self.body.containsbreak() or self.catch.containsbreak()


def double_f(cx):
    return CompoundF(cx, [fuzz_code(cx)(cx)] + [HashF(cx)] + [fuzz_code(cx)(cx)])


class BreakF(F):
    def __init__(self, cx):
        self.cx = cx

    def __str__(self):
        return indent(self.cx) + "break;\n"

    def unreachable(self):
        return True

    def eval(self, ecx):
        ecx.infinity_check()
        return BREAK

    def containsbreak(self):
        return True


class ContinueF(F):
    def __init__(self, cx):
        self.cx = cx

    def __str__(self):
        return indent(self.cx) + "continue;\n"

    def unreachable(self):
        return True

    def eval(self, ecx):
        ecx.infinity_check()
        return CONTINUE


class WaitF(F):
    def __init__(self, cx):
        self.cx = cx
        self.uniqueID = cx.unique_id()

    def __str__(self):
        return (
            indent(self.cx)
            + "int input = waitNext( inputStream );\n"
            + indent(self.cx)
            + "outputStream.send( input + %d );\n" % self.uniqueID
        )

    def eval(self, ecx):
        ecx.infinity_check()
        input = ecx.inp()
        ecx.out((input + self.uniqueID) & 0xFFFFFFFF)
        return OK


class ThrowF(F):
    def __init__(self, cx):
        self.cx = cx

    def __str__(self):
        return indent(self.cx) + "throw operation_failed();\n"

    def unreachable(self):
        return True

    def eval(self, ecx):
        ecx.infinity_check()
        return THROW


class ThrowF2(ThrowF):
    def __str__(self):
        return indent(self.cx) + "throw_operation_failed();\n"

    def unreachable(self):
        return False  # The actor compiler doesn't know the function never returns


class ThrowF3(ThrowF):
    def __str__(self):
        return indent(self.cx) + "wait( error ); // throw operation_failed()\n"

    def unreachable(self):
        return False  # The actor compiler doesn't know that 'error' always contains an error


class ReturnF(F):
    def __init__(self, cx):
        self.cx = cx
        self.uniqueID = cx.unique_id()

    def __str__(self):
        return indent(self.cx) + "return %d;\n" % self.uniqueID

    def unreachable(self):
        return True

    def eval(self, ecx):
        ecx.infinity_check()
        ecx.returnValue = self.uniqueID
        return RETURN


def fuzz_code(cx):
    choices = [LoopF, RangeForF, TryF, double_f, IfF]
    if cx.indent < 2:
        choices *= 2
    choices += [WaitF, ReturnF]
    if cx.inLoop:
        choices += [BreakF, ContinueF]
    choices = choices * 3 + [ThrowF, ThrowF2, ThrowF3]
    return cx.random.choice(choices)


def random_actor(index):
    while 1:
        cx = Context()
        cx.indent += 1
        actor = fuzz_code(cx)(cx)
        actor = CompoundF(
            cx, [actor, ReturnF(cx)]
        )  # Add a return at the end if the end is reachable
        name = "actorFuzz%d" % index
        text = (
            "ACTOR Future<int> %s( FutureStream<int> inputStream, PromiseStream<int> outputStream, Future<Void> error ) {\n"
            % name
            + "\tstate int ifstate = 0;\n"
            + str(actor)
            + "}"
        )
        ecx = actor.ecx = ExecContext((i + 1) * 1000 for i in range(1000000))
        try:
            result = actor.eval(ecx)
        except InfiniteLoop:
            print("Infinite loop for actor %s" % name)
            continue
        if result == RETURN:
            ecx.out(ecx.returnValue)
        elif result == THROW:
            ecx.out(1000)
        else:
            print(text)
            raise Exception("Invalid eval result: " + str(result))
        actor.name = name
        actor.text = text

        return actor


header = """
/*
 * ActorFuzz.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

"""


testCaseCount = 30
outputFile = open("ActorFuzz.actor.cpp", "wt")
print(header, file=outputFile)
print(
    "// THIS FILE WAS GENERATED BY actorFuzz.py; DO NOT MODIFY IT DIRECTLY\n",
    file=outputFile,
)
print('#include "ActorFuzz.h"\n', file=outputFile)
print("#ifndef WIN32\n", file=outputFile)

actors = [random_actor(i) for i in range(testCaseCount)]

for actor in actors:
    print(actor.text + "\n", file=outputFile)

print("std::pair<int,int> actorFuzzTests() {\n\tint testsOK = 0;", file=outputFile)
for actor in actors:
    print(
        '\ttestsOK += testFuzzActor( &%s, "%s", {%s} );'
        % (actor.name, actor.name, ",".join(str(e) for e in actor.ecx.output)),
        file=outputFile,
    )
print("\treturn std::make_pair(testsOK, %d);\n}" % len(actors), file=outputFile)
print("#endif // WIN32\n", file=outputFile)
outputFile.close()
