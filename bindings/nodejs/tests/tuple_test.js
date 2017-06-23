/*
 * tuple_test.js
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

var fdb = require('../lib/fdb.js').apiVersion(500);
var fdbModule = require('../lib/fdbModule.js');

console.log(fdb.tuple.pack([-Math.pow(2,53)]));
console.log(fdb.tuple.pack([-Math.pow(2,53)+1]));

console.log(fdb.tuple.unpack(fdb.tuple.pack([-Math.pow(2,53)])));
console.log(fdb.tuple.unpack(fdb.tuple.pack([-Math.pow(2,53)+1])));

try {
	console.log(fdb.tuple.unpack(fdb.buffer.fromByteLiteral('\x0d\xdf\xff\xff\xff\xff\xff\xfe')));
}
catch(err) {
	console.log(err);
}

console.log(fdb.tuple.pack([0xff * 0xff]));
console.log(fdb.tuple.pack([0xffffffff + 100 ]));
console.log(fdb.buffer.printable(fdb.tuple.pack(['begin', [true, null, false], 'end'])))
console.log(fdb.tuple.unpack(fdb.buffer.fromByteLiteral('\x1a\xff\xff\xff\xff\xff\xff')));
console.log(fdb.tuple.unpack(fdb.tuple.pack(['TEST', 'herp', 1, -10, 393493, '\u0000abc', 0xffffffff + 100, true, false, [new Boolean(true), null, new Boolean(false), 0, 'asdf'], null])));
console.log(fdb.buffer.printable(fdb.tuple.pack([[[[['three']]], 'two'], 'one'])))
console.log(fdb.tuple.range(['TEST', 1]));
console.log(fdb.buffer.printable(fdb.tuple.pack([fdb.tuple.Float.fromBytes(new Buffer('402df854', 'hex')), fdb.tuple.Double.fromBytes(new Buffer('4005BF0A8B145769', 'hex')), new fdb.tuple.UUID(new Buffer('deadc0deba5eba115ca1ab1edeadc0de', 'hex'))])))
console.log(fdb.tuple.unpack(fdb.tuple.pack([fdb.tuple.Float.fromBytes(new Buffer('2734236f', 'hex'))])))

tuples = [
    [1,2],
    [1],
    [2],
    [true],
    [false],
    [1,true],
    [1,false],
    [1, []],
    [1, [null]],
    [1, [0]],
    [1, [1]],
    [1, [0,1,2]],
    [null],
    []
];
tuples.sort(fdb.tuple.compare);
console.log(tuples);

tuples = [
    [fdb.tuple.Float.fromBytes(new Buffer('2734236f', 'hex'))], // A really small value.
    [fdb.tuple.Float.fromBytes(new Buffer('80000000', 'hex'))], // -0.0
    [new fdb.tuple.Float(0.0)],
    [new fdb.tuple.Float(3.14)],
    [new fdb.tuple.Float(-3.14)],
    [new fdb.tuple.Float(2.7182818)],
    [new fdb.tuple.Float(-2.7182818)],
    [fdb.tuple.Float.fromBytes(new Buffer('7f800000', 'hex'))], // Infinity
    [fdb.tuple.Float.fromBytes(new Buffer('7fffffff', 'hex'))], // NaN
    [fdb.tuple.Float.fromBytes(new Buffer('ffffffff', 'hex'))], // -NaN
];
tuples.sort(fdb.tuple.compare);
console.log(tuples);

// Float overruns.
const floats = [ 2.037036e90, -2.037036e90, 4.9090935e-91, -4.9090935e-91, 2.345624805922133125e14, -2.345624805922133125e14 ];
for (var i = 0; i < floats.length; i++) {
    var f = floats[i];
    console.log(f + " -> " + fdb.tuple.Float.fromBytes((new fdb.tuple.Float(f)).toBytes()).value);
}

// Float type errors.
try {
    console.log((new fdb.tuple.Float("asdf")).toBytes());
} catch (e) {
    console.log("Caught!");
    console.log(e);
}

try {
    console.log(fdbModule.toFloat(3.14, 2.718));
} catch (e) {
    console.log("Caught!");
    console.log(e);
}
