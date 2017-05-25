var fdb = require('../lib/fdb').apiVersion(200);

db = fdb.open(null, null, _);

var tr = db.createTransaction();

tr.set('foo1', 'bar1');
tr.set('foo2', 'bar2');
tr.set('foo3', 'bar3');
tr.set('foo4', 'bar4');
tr.set('foo5', 'bar5');
tr.set('bar1', 'foo1');
tr.set('bar2', 'foo2');

tr.commit(_);

console.log('get range: foo1-foo4');
var itr = tr.getRange('foo1', 'foo4', null);

a = itr.forEach(function(val, cb) {
	console.log(val.key.toString(), val.value.toString()); 
	cb(null, null);
});

console.log('get range starts with: foo');
itr = tr.getRangeStartsWith('foo');

b = itr.forEachBatch(function(arr, cb) {
	console.log('processing array', arr.length);
	for(var i in arr) 
		console.log(arr[i].key.toString(), arr[i].value.toString());
	cb(null, null);
});
c = itr.forEachBatch(function(arr, cb) {
	console.log('processing array concurrent', arr.length);
	for(var i in arr)
		console.log(arr[i].key.toString(), arr[i].value.toString());
	cb(null, null);
});

console.log(a(_) + b(_) + c(_));
