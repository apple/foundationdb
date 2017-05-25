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

var fdb = require('../lib/fdb.js').apiVersion(200);

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
console.log(fdb.tuple.unpack(fdb.buffer.fromByteLiteral('\x1a\xff\xff\xff\xff\xff\xff')));
console.log(fdb.tuple.unpack(fdb.tuple.pack(['TEST', 'herp', 1, -10, 393493, '\u0000abc', 0xffffffff + 100])));
console.log(fdb.tuple.range(['TEST', 1]));
