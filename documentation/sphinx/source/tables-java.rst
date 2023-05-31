######
Tables
######

:doc:`Python <tables>` **Java**

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

We can create a subspace for the table and nested subspaces for the row and column indexes. Setting a cell would then look like:

.. code-block:: java

    tr.set(rowIndex.subspace(Tuple.from(row, column)).getKey(), pack(value));
    tr.set(colIndex.subspace(Tuple.from(column, row)).getKey(), pack(value));

Extensions
==========

*Higher dimensions*

This approach can be straightforwardly extended to N dimensions for N > 2. Unless N is small and your data is very sparse, you probably won't want to store all N! index orders, as that could consume a prohibitive amount of space. Instead, you'll want to select the most common access patterns for direct storage.

Code
====

Here’s a simple implementation of the basic table pattern:

.. code-block:: java

    import java.util.Map;
    import java.util.TreeMap;
    public class MicroTable {
        private static final FDB fdb;
        private static final Database db;
        private static final Subspace table;
        private static final Subspace rowIndex;
        private static final Subspace colIndex;

        static {
            fdb = FDB.selectAPIVersion(730);
            db = fdb.open();
            table = new Subspace(Tuple.from("T"));
            rowIndex = table.subspace(Tuple.from("R"));
            colIndex = table.subspace(Tuple.from("C"));
        }

        // Packing and unpacking helper functions.
        private static byte[] pack(Object value){
            return Tuple.from(value).pack();
        }

        private static Object unpack(byte[] value){
            return Tuple.fromBytes(value).get(0);
        }

        public static void setCell(TransactionContext tcx, final String row,
                                    final String column, final Object value){
            tcx.run(tr -> {
                tr.set(rowIndex.subspace(Tuple.from(row, column)).getKey(),
                        pack(value));
                tr.set(colIndex.subspace(Tuple.from(column,row)).getKey(),
                        pack(value));

                return null;
            });
        }

        public static Object getCell(TransactionContext tcx, final String row,
                                    final String column){
            return tcx.run(tr -> {
                return unpack(tr.get(rowIndex.subspace(
                        Tuple.from(row,column)).getKey()).get());
            });
        }

        public static void setRow(TransactionContext tcx, final String row,
                                    final Map<String,Object> cols){
            tcx.run(tr -> {
                tr.clear(rowIndex.subspace(Tuple.from(row)).range());

                for(Map.Entry<String,Object> cv : cols.entrySet()){
                    setCell(tr, row, cv.getKey(), cv.getValue());
                }
                return null;
            });
        }

        public static void setColumn(TransactionContext tcx, final String column,
                                        final Map<String,Object> rows){
            tcx.run(tr -> {
                tr.clear(colIndex.subspace(Tuple.from(column)).range());
                for(Map.Entry<String,Object> rv : rows.entrySet()){
                    setCell(tr, rv.getKey(), column, rv.getValue());
                }
                return null;
            });
        }

        public static TreeMap<String,Object> getRow(TransactionContext tcx,
                                                    final String row){
            return tcx.run(tr -> {
                TreeMap<String,Object> cols = new TreeMap<String,Object>();

                for(KeyValue kv : tr.getRange(
                        rowIndex.subspace(Tuple.from(row)).range())){
                    cols.put(rowIndex.unpack(kv.getKey()).getString(1),
                            unpack(kv.getValue()));
                }

                return cols;
            });
        }


        public static TreeMap<String,Object> getColumn(TransactionContext tcx,
                                                    final String column){
            return tcx.run(tr -> {
                TreeMap<String,Object> rows = new TreeMap<String,Object>();

                for(KeyValue kv : tr.getRange(
                        colIndex.subspace(Tuple.from(column)).range())){
                    rows.put(colIndex.unpack(kv.getKey()).getString(1),
                            unpack(kv.getValue()));
                }

                return rows;
            });
        }
    }

That’s about all you need to store and retrieve data from simple tables.
