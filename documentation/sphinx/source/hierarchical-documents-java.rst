######################
Hierarchical Documents
######################

:doc:`Python <hierarchical-documents>` **Java**

Goal
====

Create a representation for hierarchical `documents <http://en.wikipedia.org/wiki/Document-oriented_database>`_.

Challenge
=========

Support efficient storage and retrieval of documents, both as a whole and by subdocuments specified by paths.

Explanation
===========

A hierarchical document has a tree-like structure, with the document ID as the root. We'll map the hierarchy to a list of tuples in which each tuple corresponds to a path from the root to a leaf. These tuples will form keys, so each leaf is indexed by the path leading to it.

Ordering
========

Because each tuple represents a path from the document root to a leaf, the lexicographic ordering of tuples guarantees that adjacent paths will be stored in adjacent keys. Each tuple prefix will correspond to a subdocument that can be retrieved using a prefix range read. Likewise, a range read using the root as a prefix will retrieve the entire document.

Pattern
=======

A document will consist of a dictionary whose values may be simple data types (e.g., integers or strings), lists, or (nested) dictionaries. Each document will be stored under a unique ID. If a document ID has not already been supplied, we randomly generate one.

We convert the document to a list of tuples representing each path from the root to a leaf. Each tuple is used to construct a composite key within a subspace. The document ID becomes the first element after the subspace prefix, followed by the remainder of the path. We store the leaf (the last element of the tuple) as the value, which enables storage of larger data sizes (see :ref:`Key and value sizes <data-modeling-performance-guidelines>`).

If we're given a serialized JSON object to start with, we just deserialize it before converting it to tuples. To distinguish the list elements in the document (a.k.a. JSON arrays) from dictionary elements and preserve the order of the lists, we include the index of each list element before it in the tuple.

We can retrieve any subdocument based on the partial path to its root. The partial path will just be a tuple that the query function uses as a key prefix for a range read. The retrieved data will be a list of tuples. The final step before returning the data is to convert it back to a document.

Extensions
==========

Indexing
--------

We could extend the document model to allow selective indexing of keys or values at specified locations with a document.

Query language
--------------

We could extend the document to support more powerful query capabilities, either with query functions or a full query language. Either would be designed to take advantage of existing indexes.

Code
====

Here’s a basic implementation of the recipe.

.. code-block:: java

    import java.util.ArrayList;
    import java.util.HashMap;
    import java.util.Map;
    import java.util.Map.Entry;

    public class MicroDoc {

        private static final FDB fdb;
        private static final Database db;
        private static final Subspace docSpace;
        private static final long EMPTY_OBJECT = -2;
        private static final long EMPTY_ARRAY = -1;

        static {
            fdb = FDB.selectAPIVersion(730);
            db = fdb.open();
            docSpace = new Subspace(Tuple.from("D"));
        }

        @SuppressWarnings("unchecked")
        private static ArrayList<Tuple> toTuplesSwitch(Object o){
            if(o instanceof ArrayList){
                return toTuples((ArrayList<Object>) o);
            } else if(o instanceof Map){
                return toTuples((Map<Object,Object>) o);
            } else {
                return toTuples(o);
            }
        }

        private static ArrayList<Tuple> toTuples(ArrayList<Object> item){
            if(item.isEmpty()){
                ArrayList<Tuple> val = new ArrayList<Tuple>();
                val.add(Tuple.from(EMPTY_ARRAY, null));
                return val;
            } else {
                ArrayList<Tuple> val = new ArrayList<Tuple>();
                for(int i = 0; i < item.size(); i++){
                    for(Tuple sub : toTuplesSwitch(item.get(i))){
                        val.add(Tuple.from(i).addAll(sub));
                    }
                }
                return val;
            }
        }

        private static ArrayList<Tuple> toTuples(Map<Object,Object> item){
            if(item.isEmpty()){
                ArrayList<Tuple> val = new ArrayList<Tuple>();
                val.add(Tuple.from(EMPTY_OBJECT, null));
                return val;
            } else {
                ArrayList<Tuple> val = new ArrayList<Tuple>();
                for(Entry<Object,Object> e : item.entrySet()){
                    for(Tuple sub : toTuplesSwitch(e.getValue())){
                        val.add(Tuple.from(e.getKey()).addAll(sub));
                    }
                }
                return val;
            }
        }

        private static ArrayList<Tuple> toTuples(Object item){
            ArrayList<Tuple> val = new ArrayList<Tuple>();
            val.add(Tuple.from(item));
            return val;
        }

        private static ArrayList<Tuple> getTruncated(ArrayList<Tuple> vals){
            ArrayList<Tuple> list = new ArrayList<Tuple>();
            for(Tuple val : vals){
                list.add(val.popFront());
            }
            return list;
        }

        private static Object fromTuples(ArrayList<Tuple> tuples){
            if(tuples == null){
                return null;
            }

            Tuple first = tuples.get(0); // Determine kind of object from
                                         // first tuple.
            if(first.size() == 1){
                return first.get(0); // Primitive type.
            }

            if(first.equals(Tuple.from(EMPTY_OBJECT, null))){
                return new HashMap<Object,Object>(); // Empty map.
            }

            if(first.equals(Tuple.from(EMPTY_ARRAY))){
                return new ArrayList<Object>(); // Empty list.
            }

            HashMap<Object,ArrayList<Tuple>> groups = new HashMap<Object,ArrayList<Tuple>>();
            for(Tuple t : tuples){
                if(groups.containsKey(t.get(0))){
                    groups.get(t.get(0)).add(t);
                } else {
                    ArrayList<Tuple> list = new ArrayList<Tuple>();
                    list.add(t);
                    groups.put(t.get(0),list);
                }
            }

            if(first.get(0).equals(0l)){
                // Array.
                ArrayList<Object> array = new ArrayList<Object>();
                for(Entry<Object,ArrayList<Tuple>> g : groups.entrySet()){
                    array.add(fromTuples(getTruncated(g.getValue())));
                }
                return array;
            } else {
                // Object.
                HashMap<Object,Object> map = new HashMap<Object,Object>();
                for(Entry<Object,ArrayList<Tuple>> g : groups.entrySet()){
                    map.put(g.getKey(), fromTuples(getTruncated(g.getValue())));
                }
                return map;
            }
        }

        public static Object insertDoc(TransactionContext tcx, final Map<Object,Object> doc){
            return tcx.run(tr -> {
                if(!doc.containsKey("doc_id")){
                    doc.put("doc_id", getNewID(tr));
                }
                for(Tuple t : toTuples(doc)){
                    tr.set(docSpace.pack(Tuple.from(doc.get("doc_id")).addAll(t.popBack())),
                            Tuple.from(t.get(t.size() - 1)).pack());
                }
                return doc.get("doc_id");
            });
        }

        public static Object getDoc(TransactionContext tcx, final Object ID){
            return getDoc(tcx, ID, Tuple.from());
        }

        public static Object getDoc(TransactionContext tcx, final Object ID, final Tuple prefix){
            return tcx.run(tr -> {
                Future<byte[]> v = tr.get(docSpace.pack(Tuple.from(ID).addAll(prefix)));
                if(v.get() != null){
                    // One single item.
                    ArrayList<Tuple> vals = new ArrayList<Tuple>();
                    vals.add(prefix.addAll(Tuple.fromBytes(v.get())));
                    return fromTuples(vals);
                } else {
                    // Multiple items.
                    ArrayList<Tuple> vals =  new ArrayList<Tuple>();
                    for(KeyValue kv : tr.getRange(docSpace.range(Tuple.from(ID).addAll(prefix)))){
                        vals.add(docSpace.unpack(kv.getKey()).popFront().addAll(Tuple.fromBytes(kv.getValue())));
                    }
                    return fromTuples(vals);
                }
            });
        }

        private static int getNewID(TransactionContext tcx){
            return tcx.run(tr -> {
                boolean found = false;
                int newID;
                do {
                    newID = (int)(Math.random()*100000000);
                    found = true;
                    for(KeyValue kv : tr.getRange(docSpace.range(Tuple.from(newID)))){
                        // If not empty, this is false.
                        found = false;
                        break;
                    }
                } while(!found);
                return newID;
            });
        }
    }
