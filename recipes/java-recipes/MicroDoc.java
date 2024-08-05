/*
 * MicroDoc.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.foundationdb.*;
import com.foundationdb.async.Function;
import com.foundationdb.async.Future;
import com.foundationdb.subspace.Subspace;
import com.foundationdb.tuple.Tuple;

// TODO Everything in this class up until the few methods at the end
// (also marked) are in the recipe book.

public class MicroDoc {

	private static final FDB fdb;
	private static final Database db;
	private static final Subspace docSpace;
	private static final long EMPTY_OBJECT = -2;
	private static final long EMPTY_ARRAY = -1;

	static {
		fdb = FDB.selectAPIVersion(300);
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
		return tcx.run(new Function<Transaction,Object>() {
			public Object apply(Transaction tr){
				if(!doc.containsKey("doc_id")){
					doc.put("doc_id", getNewID(tr));
				}
				for(Tuple t : toTuples(doc)){
					tr.set(docSpace.pack(Tuple.from(doc.get("doc_id")).addAll(t.popBack())),
							Tuple.from(t.get(t.size() - 1)).pack());
				}
				return doc.get("doc_id");
			}
		});
	}

	public static Object getDoc(TransactionContext tcx, final Object ID){
		return getDoc(tcx, ID, Tuple.from());
	}

	public static Object getDoc(TransactionContext tcx, final Object ID, final Tuple prefix){
		return tcx.run(new Function<Transaction,Object>() {
			public Object apply(Transaction tr){
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
			}
		});
	}

	private static int getNewID(TransactionContext tcx){
		return tcx.run(new Function<Transaction,Integer>() {
			@SuppressWarnings("unused")
			public Integer apply(Transaction tr){
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
			}
		});
	}

	//TODO Everything in this class before here is in the doc.

	private static String tupleRep(Tuple t){
		StringBuilder sb = new StringBuilder();
		sb.append('(');
		for(Object o : t){
			if(o instanceof Tuple){
				sb.append(tupleRep((Tuple)o));
			} else if(o == null){
				sb.append("null");
			} else {
				sb.append(o.toString());
			}
			sb.append(',');
		}
		sb.append(')');
		return sb.toString();
	}

	public static void printSubspace(TransactionContext tcx, final Subspace s){
		tcx.run(new Function<Transaction,Void>() {
			public Void apply(Transaction tr){
				for(KeyValue kv : tr.getRange(s.range())){
					System.out.println(tupleRep(s.unpack(kv.getKey()))
							+ " "
							+ Tuple.fromBytes(kv.getValue()).get(0).toString());
				}
				return null;
			}
		});
	}

	public static void clearSubspace(TransactionContext tcx, final Subspace s){
		tcx.run(new Function<Transaction,Void>() {
			public Void apply(Transaction tr){
				tr.clear(s.range());
				return null;
			}
		});
	}

	public static void smokeTest(){
		// Construct a hash map suitable for a doc.
		HashMap<Object,Object> h1 = new HashMap<Object,Object>();
		HashMap<Object,Object> h2 = new HashMap<Object,Object>();
		HashMap<Object,Object> h3 = new HashMap<Object,Object>();
		ArrayList<Object> a1 = new ArrayList<Object>();
		a1.add("sales"); a1.add("service");
		h3.put("friend_of", "smith"); h3.put("group", a1);
		HashMap<Object,Object> h4 = new HashMap<Object,Object>();
		ArrayList<Object> a2 = new ArrayList<Object>();
		a2.add("dev"); a2.add("research");
		h4.put("friend_of", "jones"); h4.put("group",a2);
		h2.put("jones", h3); h2.put("smith",h4);
		h1.put("user", h2);

//		ArrayList<Tuple> tupList = toTuples(h1);
//		for(Tuple t : tupList){
//			System.out.println(tupleRep(t));
//		}
//
//		Object ret = fromTuples(tupList);
//		System.out.println(ret);

		Object id = insertDoc(db, h1);
		System.out.println(id);

		printSubspace(db, docSpace);
		System.out.println(getDoc(db, id, Tuple.from("user","jones","friend_of")));
	}

	public static void main(String[] args) {
		clearSubspace(db, docSpace);
		smokeTest();
	}

}
