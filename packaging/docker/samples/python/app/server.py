from flask import Flask
import fdb

app = Flask(__name__)

fdb.api_version(510)
db=fdb.open()

COUNTER_KEY=fdb.tuple.pack(('counter',))
def _increment_counter(tr):
    counter_value = tr[COUNTER_KEY]
    if counter_value == None:
        counter = 1
    else:
        counter = fdb.tuple.unpack(counter_value)[0] + 1
    tr[COUNTER_KEY] = fdb.tuple.pack((counter,))
    return counter

@app.route("/counter", methods=['GET'])
def get_counter():
    return str(fdb.tuple.unpack(db[COUNTER_KEY])[0])

@app.route("/counter/increment", methods=['POST'])
def increment_counter():
    return str(_increment_counter(db))