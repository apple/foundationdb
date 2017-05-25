var fdb = require('../lib/fdb').apiVersion(200);

function set(db, key, value, _) {
	db.doTransaction(function(tr, _) {
		console.log("setting key");
		tr.set(key, value);
		return;
	}, _);
};

function getAndClear(db, key, _) {
	a = db.doTransaction(function(tr, _) {
		console.log("getting key");
		res = tr.get(key, _);
		tr.clear(key);
		return res;
	});

	b = db.doTransaction(function(tr, _) {
		console.log("getting key");
		res = tr.get(key, _);
		tr.clear(key);
		return res;
	});

	return a(_) + b(_);
};

getAndClearTxn = fdb.transactional(function(tr, key, _) {
	console.log("getting key");
	tr.getKey(fdb.KeySelector.firstGreaterOrEqual(key));
	res = tr.get(key, _);
	tr.clear(key);
	return res;
});

db = fdb.open(null, null, _);
set(db, 'foo', 'bar', _);

//res = getAndClear(db, 'foo');
//console.log("Called get and clear", res(_).toString());

a = getAndClearTxn(db, 'foo');
b = getAndClearTxn(db, 'foo');

res = a(_) + b(_);
console.log("Result:", res.toString());

