#
# bulk.py
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

''' FoundationDB Bulk Loader Layer.

Provides a BulkLoader class for loading external datasets to FoundationDB. The
layer assumes that the loading operation has no requirements for atomicity or
isolation.

The layer is designed to be extensibly via subclasses of BulkLoader that handle
external data sources and internal data models. Example subclasses are supplied
for:

    - Reading CSV
    - Reading JSON
    - Reading Blobs
    - Writing key-value pairs
    - Writing SimpleDoc
    - Writing Blobs

'''

import csv
import glob
import json
import numbers
import os
import os.path

import gevent
from gevent.queue import Queue, Empty

import blob
import fdb
import fdb.tuple
import simpledoc

fdb.api_version(22)

db = fdb.open(event_model="gevent")

#####################################
## This defines a Subspace of keys ##
#####################################


class Subspace (object):
    def __init__(self, prefixTuple, rawPrefix=""):
        self.rawPrefix = rawPrefix + fdb.tuple.pack(prefixTuple)

    def __getitem__(self, name):
        return Subspace((name,), self.rawPrefix)

    def key(self):
        return self.rawPrefix

    def pack(self, tuple):
        return self.rawPrefix + fdb.tuple.pack(tuple)

    def unpack(self, key):
        assert key.startswith(self.rawPrefix)
        return fdb.tuple.unpack(key[len(self.rawPrefix):])

    def range(self, tuple=()):
        p = fdb.tuple.range(tuple)
        return slice(self.rawPrefix + p.start, self.rawPrefix + p.stop)


@fdb.transactional
def clear_subspace(tr, subspace):
    tr.clear_range_startswith(subspace.key())


##############################
## Base class for the layer ##
##############################

class BulkLoader(Queue):
    '''
    Supports the use of multiple concurrent transactions for efficiency, with a
    default of 50 concurrent transactions.
    '''

    def __init__(self, number_producers=1, number_consumers=50, **kwargs):
        # Setting maxsize to the number of consumers will make producers
        # wait to put a task in the queue until some consumer is free
        super(BulkLoader, self).__init__(maxsize=number_consumers)
        self._number_producers = number_producers
        self._number_consumers = number_consumers
        self._kwargs = kwargs  # To be used by reader and writer in subclasses

    def _producer(self):
        # put will block if maxsize of queue is reached
        for data in self.reader():
            self.put(data)

    def _consumer(self):
        try:
            while True:
                data = self.get(block=False)
                self.writer(db, data)
                gevent.sleep(0)  # yield
        except Empty:
            pass

    def produce_and_consume(self):
        producers = [gevent.spawn(self._producer) for _ in xrange(self._number_producers)]
        consumers = [gevent.spawn(self._consumer) for _ in xrange(self._number_consumers)]
        gevent.joinall(producers)
        gevent.joinall(consumers)

    # Interface stub to be overridden by a subclass. Method should be a
    # generator that yields data in increments of size appropriate to be written
    # by a single transaction.
    def reader(self):
        return (i for i in range(5))

    # Interface stub to be overridden by a subclass. Method should ideally be
    # designed to write around 10kB of data per transaction.
    @fdb.transactional
    def writer(self, tr, data):
        print "Would write", data


def test_loader():
    tasks = BulkLoader(1, 5)
    tasks.produce_and_consume()


####################
## Reader classes ##
####################

class ReadCSV(BulkLoader):
    '''
    Reads CSV files. Supported keyword arguments are:

    filename=<filename>. Specifies the csv file to read. If no filename is
        given, then all files in the specified dir will be read.

    dir=<path>. Specifies the directory of the file(s) to be read. Defaults to
        current working directory.

    delimiter=<string>. Defaults to ','. Use '\t' for a tsv file.

    skip_empty=<bool>. If True, skip empty fields in csv files. Otherwise, replace empty
        fields with the empty string (''). Default is False.

    header=<bool>. If True, assume the first line of csv files consists of field
        names and skip it. Otherwise, treat the first line as data to be read.
        Default is False.
    '''

    def __init__(self, number_producers=1, number_consumers=50, **kwargs):
        super(ReadCSV, self).__init__(number_producers, number_consumers, **kwargs)
        self._filename = kwargs.get('filename', '*')
        self._delimiter = kwargs.get('delimiter', ',')
        self._dir = kwargs.get('dir', os.getcwd())
        self._skip_empty = kwargs.get('skip_empty', False)
        self._header = kwargs.get('header', False)

    def reader(self):
        for fully_pathed in glob.iglob(os.path.join(self._dir, self._filename)):
            if not os.path.isfile(fully_pathed):
                continue
            with open(fully_pathed, 'rb') as csv_file:
                csv_reader = csv.reader(csv_file, delimiter=self._delimiter)
                first_line = True
                for line in csv_reader:
                    if self._header and first_line:
                        first_line = False
                        continue
                    if self._skip_empty:
                        line = [v for v in line if v != '']
                    yield tuple(line)


class ReadJSON(BulkLoader):
    '''
    Reads JSON files. Assumes there is one JSON object per file. Supported
    keyword arguments for initialization are:

    filename=<filename>. Specifies the json file to read. If no filename is
        given, then all files in the specified dir will be read.

    dir=<path>. Specifies the directory of the file(s) to be read. Defaults to
        current working directory.

    convert_unicode=<bool>. If True, returns byte strings rather than unicode
        in the deserialized object. Default is True.

    convert_numbers=<bool>. If True, returns byte strings rather than numbers or
        unicode in the deserialized object. Default is False.
    '''

    def __init__(self, number_producers=1, number_consumers=50, **kwargs):
        super(ReadJSON, self).__init__(number_producers, number_consumers, **kwargs)
        self._filename = kwargs.get('filename', '*')
        self._dir = kwargs.get('dir', os.getcwd())
        self._convert_unicode = kwargs.get('convert_unicode', True)
        self._convert_numbers = kwargs.get('convert_numbers', False)

    def _convert(self, input, number=False):
        if isinstance(input, dict):
            return {self._convert(key, number): self._convert(value, number)
                    for key, value in input.iteritems()}
        elif isinstance(input, list):
            return [self._convert(element, number) for element in input]
        elif isinstance(input, unicode):
            return input.encode('utf-8')
        elif number and isinstance(input, numbers.Number):
            return str(input).encode('utf-8')
        else:
            return input

    def reader(self):
        for fully_pathed in glob.iglob(os.path.join(self._dir, self._filename)):
            if not os.path.isfile(fully_pathed):
                continue
            with open(fully_pathed, 'r') as json_file:
                if self._convert_numbers:
                    json_object = json.load(json_file,
                                            object_hook=lambda x: self._convert(x, True))
                elif self._convert_unicode:
                    json_object = json.load(json_file,
                                            object_hook=self._convert)
                else:
                    json_object = json.load(json_file)
                yield json_object


class ReadBlob(BulkLoader):
    '''
    Reads files, treating data in each file as a single blob. Supported
    keyword arguments for initialization are:

    filename=<filename>. Specifies the blob file to read. If no filename is
        given, the reader will look for a file in the specified directory, but
        any file found must be unique.

    dir=<path>. Specifies the directory of the file(s) to be read. Defaults to
        current working directory.

    chunk_size=<int>. Number of bytes to read from file. Default is 10240.
    '''

    def __init__(self, number_producers=1, number_consumers=50, **kwargs):
        super(ReadBlob, self).__init__(number_producers, number_consumers, **kwargs)
        self._filename = kwargs.get('filename', '*')
        self._dir = kwargs.get('dir', os.getcwd())
        self._chunk_size = kwargs.get('chunk_size', 10240)

    def reader(self):
        files_found = list(glob.iglob(os.path.join(self._dir, self._filename)))
        if len(files_found) != 1:
            raise Exception("Must specify single file")
        fully_pathed = files_found[0]
        if not os.path.isfile(fully_pathed):
            raise Exception("No file found")
        with open(fully_pathed, 'rb') as blob_file:
            file_size = os.stat(fully_pathed).st_size
            position = 0
            while (position < file_size):
                try:
                    chunk = blob_file.read(self._chunk_size)
                    if not chunk:
                        break
                    offset = position
                    position += self._chunk_size
                    yield offset, chunk
                except IOError as e:
                    print "I/O error({0}): {1}".format(e.errno, e.strerror)


####################
## Writer classes ##
####################

class WriteKVP(BulkLoader):
    '''
    Writes key-value pairs directly from tuples. Supported keyword arguments for
    initialization are:

    empty_value=<bool>. If True, uses all tuple elements to form the key and sets
        the value to ''. Otherwise, uses the last element as the value and all
        others to form the key. Default is False.

    subspace=<Subspace()>. Specifies the subspace to which data is written.
        Default is Subspace(('bulk_kvp',)).

    clear=<bool>. If True, clears the specified subspace before writing to it.
        Default is False.
    '''

    def __init__(self, number_producers=1, number_consumers=50, **kwargs):
        super(WriteKVP, self).__init__(number_producers, number_consumers, **kwargs)
        self._empty_value = kwargs.get('empty_value', False)
        self._subspace = kwargs.get('subspace', Subspace(('bulk_kvp',)))
        self._clear = kwargs.get('clear', False)
        if self._clear:
            clear_subspace(db, self._subspace)

    @fdb.transactional
    def writer(self, tr, data):
        if self._empty_value:
            tr[self._subspace.pack(data)] = ''
        else:
            tr[self._subspace.pack(data[:-1])] = data[-1]


class WriteDoc(BulkLoader):
    '''
    Writes document-oriented data into a SimpleDoc database. Data must be a
    a JSON object without JSON arrays. Supported keyword arguments for
    initialization are:

    clear=<bool>. If True, clears the specified SimpleDoc object before writing
        to it. Default is False.

    document=<Doc()>. Specifies the SimpleDoc object to which data is written.
        Can be used to load a specified collection or arbitrary subdocument.
        Defaults to root.
    '''

    def __init__(self, number_producers=1, number_consumers=50, **kwargs):
        super(WriteDoc, self).__init__(number_producers, number_consumers, **kwargs)
        self._document = kwargs.get('document', simpledoc.root)
        self._clear = kwargs.get('clear', False)
        if self._clear:
            _simpledoc_clear(db, self._document)

    def writer(self, tr, data):
        _writer_doc(db, self._document, data)


# @simpledoc.transaction is not signature-preserving and so is used with
# functions rather than methods.

@simpledoc.transactional
def _simpledoc_clear(document):
    document.clear_all()


def no_arrays(input):
    if isinstance(input, dict):
        return all(no_arrays(value) for value in input.itervalues())
    elif isinstance(input, list):
        return False
    else:
        return True


@simpledoc.transactional
def _writer_doc(document, data):
    assert no_arrays(data), 'JSON object contains arrays'
    document.update(data)


class WriteBlob(BulkLoader):
    '''
    Writes data as a blob using the Blob layer. Supported keyword arguments for
    initialization are:

    clear=<bool>. If True, clears the specified Blob object before writing
        to it. Default is False.

    blob=<Doc()>. Specifies the Blob object to which data is written. Default is
        Blob(Subspace('bulk_blob',)).
    '''

    def __init__(self, number_producers=1, number_consumers=50, **kwargs):
        super(WriteBlob, self).__init__(number_producers, number_consumers, **kwargs)
        self._blob = kwargs.get('blob', blob.Blob(Subspace(('bulk_blob',))))
        self._clear = kwargs.get('clear', False)
        if self._clear:
            self._blob.delete(db)

    @fdb.transactional
    def writer(self, tr, data):
        offset = data[0]
        chunk = data[1]
        self._blob.write(tr, offset, chunk)


##################################
## Combined readers and writers ##
##################################

class CSVtoKVP(ReadCSV, WriteKVP):
    def __init__(self, number_producers=1, number_consumers=50, **kwargs):
        super(CSVtoKVP, self).__init__(number_producers, number_consumers, **kwargs)


class JSONtoDoc(ReadJSON, WriteDoc):
    def __init__(self, number_producers=1, number_consumers=50, **kwargs):
        super(JSONtoDoc, self).__init__(number_producers, number_consumers, **kwargs)


class BlobToBlob(ReadBlob, WriteBlob):
    def __init__(self, number_producers=1, number_consumers=50, **kwargs):
        super(BlobToBlob, self).__init__(number_producers, number_consumers, **kwargs)


'''
The following functions illustrate the format for using the combined subclasses:


def test_csv_kvp():
    tasks = CSVtoKVP(1, 5, dir='CSVDir', subspace=Subspace(('bar',)), clear=True)
    tasks.produce_and_consume()


def test_json_doc():
    tasks = JSONtoDoc(1, 5, dir='PetsDir', clear=True, document=simpledoc.root.animals)
    tasks.produce_and_consume()


def test_blob_blob():
    my_blob = blob.Blob(Subspace(('my_blob',)))
    tasks = BlobToBlob(1, 5, dir='BlobDir', filename='hamlet.txt', blob=my_blob)
    tasks.produce_and_consume()

'''
