######
Tables
######

**Python** :doc:`Java <tables-java>`

Goal
====

Create a table data structure suitable for sparse data.

Challenge
=========

Support efficient random access to individual cells in a table, as well as retrieval of all cells in a particular row or all cells in a particular column.

Explanation
===========

Tables give us a representation for two-dimensional data with labeled rows and columns. (Column labels are common in data sets. For rows, a primary key, such as an entity ID, can be used.) Each cell in the table will be modeled using two key-value pairs, one in row-dominant order and one in column-dominant order.

Ordering
========

By storing the table in both row order and column order, we can support efficient retrieval of entire rows or columns with a single range read.

Pattern
=======

We construct a key from a tuple containing the row and column identifiers. Unassigned cells in the tables will consume no storage, so sparse tables are stored very efficiently. As a result, a table can safely have a very large number of columns.

Using the lexicographic order of tuples, we can store the data in a row-oriented or column-oriented manner by placing either the row or column first in the tuple, respectively. Placing the row first makes it efficient to read all the cells in a particular row with a single range read; placing the column first makes reading a column efficient. We can support both access patterns by storing cells in both row-oriented and column-oriented layouts, allowing efficient retrieval of either an entire row or an entire column.

We can create a subspace for the table and nested subspaces for the row and column indexes. Setting a cell would then look like::

 tr[row_index[row][column]] = _pack(value)
 tr[col_index[column][row]] = _pack(value)

Extensions
==========

*Higher dimensions*

This approach can be straightforwardly extended to N dimensions for N > 2. Unless N is small and your data is very sparse, you probably won't want to store all N! index orders, as that could consume a prohibitive amount of space. Instead, you'll want to select the most common access patterns for direct storage.

Code
====

Here’s a simple implementation of the basic table pattern::

    table = fdb.Subspace(('T',))
     
    row_index = table['R']
    col_index = table['C']
     
    def _pack(value):
        return fdb.tuple.pack((value,))
     
    def _unpack(value):
        return fdb.tuple.unpack(value)[0]
     
    @fdb.transactional
    def table_set_cell(tr, row, column, value):
        tr[row_index[row][column]] = _pack(value)
        tr[col_index[column][row]] = _pack(value)
     
    @fdb.transactional
    def table_get_cell(tr, row, column):
        return tr[row_index[row][column]]
     
    @fdb.transactional
    def table_set_row(tr, row, cols):
        del tr[row_index[row].range()]
        for c, v in cols.iteritems():
            table_set_cell(tr, row, c, v)
     
    @fdb.transactional
    def table_get_row(tr, row):
        cols = {}
        for k, v in tr[row_index[row].range()]:
            r, c = row_index.unpack(k)
            cols[c] = _unpack(v)
        return cols
     
    @fdb.transactional
    def table_get_col(tr, col):
        rows = {}
        for k, v in tr[col_index[col].range()]:
            c, r = col_index.unpack(k)
            rows[r] = _unpack(v)
        return rows

That’s about all you need to store and retrieve data from simple tables.
