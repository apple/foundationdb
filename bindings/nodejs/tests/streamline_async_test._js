var fdb = require('../lib/fdb').apiVersion(200);

function doSomething(_){
	console.log("start");
	db = fdb.open(null, null);
	db = db(_);
	db = fdb.open(null, null);
	db = db(_);

	a = db.get('foo');
	console.log('foo = ', a(_));
	db.clear('foo', 'bar', _);
	console.log('foo = ', db.get('foo', _));
	b = db.set('foo', 'bar');
	b(_);
	console.log('foo = ', db.get('foo', _));

	/*var tr = db.createTransaction();
	tr.set(new Buffer('foo'), new Buffer('bar'));
	tr.commit(_);

	var a = tr.get(new Buffer('foo'))
	var b = tr.get(new Buffer('bar'))

	console.log(a(_));
	console.log(b(_));

	var c = tr.get(new Buffer('a'));
	console.log(c(_));*/
}

doSomething(_);
console.log("after");
