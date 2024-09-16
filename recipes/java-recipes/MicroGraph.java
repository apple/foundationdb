/*
 * MicroGraph.java
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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;

import com.foundationdb.*;
import com.foundationdb.async.Function;
import com.foundationdb.subspace.Subspace;
import com.foundationdb.tuple.Tuple;

public class MicroGraph {

	private static final FDB fdb;
	private static final Database db;
	private static final Subspace graph;
	private static final Subspace edge;
	private static final Subspace inverse;
	private static final Subspace pathTwo;
	private static final Subspace wEdge;
	private static final Subspace wInverse;

	static {
		fdb = FDB.selectAPIVersion(300);
		db = fdb.open();
		graph = new Subspace(Tuple.from("G"));
		edge = graph.subspace(Tuple.from("E"));
		inverse = graph.subspace(Tuple.from("I"));
		pathTwo = graph.subspace(Tuple.from("P"));
		wEdge = graph.subspace(Tuple.from("WE"));
		wInverse = graph.subspace(Tuple.from("WI"));
	}

	// TODO These next five methods (setEdge, deleteEdge, getOutNeighbors, getInNeighbors,
	// and getNeighbors) are all in the recipe book.
	public static void setEdge(TransactionContext tcx, final String node, final String neighbor){
		tcx.run(new Function<Transaction,Void>() {
			public Void apply(Transaction tr){
				tr.set(edge.pack(Tuple.from(node,neighbor)), Tuple.from().pack());
				tr.set(inverse.pack(Tuple.from(neighbor,node)), Tuple.from().pack());
				return null;
			}
		});
	}

	public static void deleteEdge(TransactionContext tcx, final String node, final String neighbor){
		tcx.run(new Function<Transaction,Void>() {
			public Void apply(Transaction tr){
				tr.clear(edge.pack(Tuple.from(node,neighbor)));
				tr.clear(inverse.pack(Tuple.from(neighbor,node)));
				return null;
			}
		});
	}

	public static ArrayList<String> getOutNeighbors(TransactionContext tcx, final String node){
		return getNeighbors(tcx, node, edge);
	}

	public static ArrayList<String> getInNeighbors(TransactionContext tcx, final String node){
		return getNeighbors(tcx, node, inverse);
	}

	private static ArrayList<String> getNeighbors(TransactionContext tcx, final String node, final Subspace domain){
		 return tcx.run(new Function<Transaction,ArrayList<String> >(){
			public ArrayList<String> apply(Transaction tr){
				ArrayList<String> neighbors = new ArrayList<String>();
				for(KeyValue kv : tr.getRange(
						domain.subspace(Tuple.from(node)).range())){
					neighbors.add(domain.unpack(kv.getKey()).getString(1));
				}
				return neighbors;
			}
		 });
	}

	// TODO These next two methods contain code that is referenced
	// in the extensions of graph, namely code to handle weighted graphs.
	// The third is not mentioned but is a necessary add on to read weights.
	public static void setEdgeWeighted(TransactionContext tcx, final String node, final String neighbor, final long weight){
		tcx.run(new Function<Transaction,Void>(){
			public Void apply(Transaction tr){
				ByteBuffer b = ByteBuffer.allocate(8);
				b.order(ByteOrder.LITTLE_ENDIAN);
				b.putLong(weight);
				tr.set(wEdge.pack(Tuple.from(node,neighbor)), b.array());
				tr.set(wInverse.pack(Tuple.from(neighbor,node)), b.array());
				return null;
			}
		});
	}

	public static void updateEdgeWeight(TransactionContext tcx, final String node, final String neighbor, final long change){
		tcx.run(new Function<Transaction,Void>() {
			public Void apply(Transaction tr){
				ByteBuffer b = ByteBuffer.allocate(8);
				b.order(ByteOrder.LITTLE_ENDIAN);
				b.putLong(change);
				tr.mutate(MutationType.ADD, wEdge.pack(Tuple.from(node,neighbor)), b.array());
				tr.mutate(MutationType.ADD, wInverse.pack(Tuple.from(neighbor,node)), b.array());
				return null;
			}
		});
	}

	public static long getWeight(TransactionContext tcx, final String node, final String neighbor){
		return tcx.run(new Function<Transaction,Long>() {
			public Long apply(Transaction tr){
				ByteBuffer b = ByteBuffer.allocate(8);
				b.order(ByteOrder.LITTLE_ENDIAN);
				b.put(tr.get(wEdge.pack(Tuple.from(node,neighbor))).get());
				return b.getLong(0);
			}
		});
	}


	public static void setNeighborNeighbors(TransactionContext tcx, final String node, final String neighbor, final String neighborsNeighbor){
		tcx.run(new Function<Transaction,Void>() {
			public Void apply(Transaction tr){
				// TODO These two lines are the code in the recipe book to handle
				// the addition of a "pathTwo" subspace that would handle remembering
				// where all the neighbor's neighbors are.
				tr.set(edge.pack(Tuple.from(node,neighbor)),Tuple.from().pack());
				tr.set(pathTwo.pack(Tuple.from(node,neighbor)),Tuple.from().pack());
				return null;
			}
		});
	}

	public static void clearSubspace(TransactionContext tcx, final Subspace s){
		tcx.run(new Function<Transaction,Void>() {
			public Void apply(Transaction tr){
				//tr.clear(Range.startsWith(s.getKey()));
				tr.clear(s.range());
				return null;
			}
		});
	}

	public static void smokeTest(){
		/*
		 * A simple directed graph:
		 *
		 *   A <->  B ->  C
		 *          ^
		 *          |
		 *          D <-  E
		 */

		setEdge(db,"A","B");
		setEdge(db,"B","A");
		setEdge(db,"B","C");
		setEdge(db,"D","B");
		setEdge(db,"E","D");

		String[] nodes = {"A","B","C","D","E"};

		for(String node : nodes){
			System.out.println(node + " "
					+ getOutNeighbors(db,node).toString() + " "
					+ getInNeighbors(db,node).toString());
		}

		/*
		 * Simple weighted graph.
		 *
		 * A -4-> B <-2- C
		 *        ^
		 *        3
		 *        |
		 *        D -(-1)-> E
 		 */
		setEdgeWeighted(db,"A","B",4l);
		setEdgeWeighted(db,"C","B",2l);
		setEdgeWeighted(db,"D","B",3l);
		setEdgeWeighted(db,"D","E",-1l);

		System.out.println(getWeight(db,"A","B"));
		System.out.println(getWeight(db,"C","B"));
		System.out.println(getWeight(db,"D","B"));
		System.out.println(getWeight(db,"D","E"));

		updateEdgeWeight(db,"D","B",27l);

		System.out.println(getWeight(db,"A","B"));
		System.out.println(getWeight(db,"C","B"));
		System.out.println(getWeight(db,"D","B"));
		System.out.println(getWeight(db,"D","E"));
	}

	public static void main(String[] args) {
		clearSubspace(db, graph);
		smokeTest();
	}

}
