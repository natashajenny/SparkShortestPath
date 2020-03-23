import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;


import scala.Tuple2;

/**
 * @author Natasha Jenny
 */
public class SparkShortestPath {
	public enum Status {
		/**
		 * A node that has not been visited by BFS
		 */
		UNVISITED,
		
		/**
		 * A node that is currently being visited by BFS
		 */
		VISITING,
		
		/**
		 * A node that has been visited by BFS
		 */
		VISITED
	}
	
	public static class EdgeTuple implements Serializable {

		private static final long serialVersionUID = 1L;
		
		/**
		 * neighbour - nextNodeID which is connected to a node
		 */
		String neighbour;
		
		/**
		 * edge - distance from a node to this nextNodeID
		 */
		Integer edge;
		
		public EdgeTuple(String neighbour, Integer edge) {
			this.neighbour = neighbour;
			this.edge = edge;
		}
		
		public String getNeighbour() {
			return neighbour;
		}

		public void setNeighbour(String neighbour) {
			this.neighbour = neighbour;
		}

		public Integer getEdge() {
			return edge;
		}

		public void setEdge(Integer edge) {
			this.edge = edge;
		}
		
		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append('(').append(neighbour).append(',').append(edge).append(')');
			return sb.toString();
		}		
	}
	
	public static class Node implements Serializable {

		private static final long serialVersionUID = 1L;

		/**
		 * nodeID - current node ID [N0, N1, N2, ...]
		 */
		String nodeID;
		
		/**
		 * edges - node connected to this node by an edge in the form of (nextNodeID, distance) tuple
		 */
		ArrayList<EdgeTuple> edges;
		
		/**
		 * path - list of path to current node
		 */
		LinkedList<String> path;
		
		/**
		 * distance - distance from source node
		 */
		int distance;

		/**
		 * status - to check if current node has been visited by BFS
		 */
		Status status;
		
		public Node(String nodeID, ArrayList<EdgeTuple> edge, LinkedList<String> path, int distance, Status status) {
			this.nodeID = nodeID;
			this.edges = new ArrayList<EdgeTuple>();
			this.path = new LinkedList<String>();
			this.distance = distance;
			this.status = status;
			this.path.addAll(path);
			this.edges.addAll(edge);
		}
		
		public String getNodeID() {
			return nodeID;
		}


		public void setNodeID(String nodeID) {
			this.nodeID = nodeID;
		}		
		

		public ArrayList<EdgeTuple> getEdges() {
			return edges;
		}

		public void addEdge(ArrayList<EdgeTuple> edge) {
			this.edges.addAll(edge);
		}

		public LinkedList<String> getPath() {
			return path;
		}

		public void addPath(String path) {
			this.path.add(path);
		}

		public int getDistance() {
			return distance;
		}

		public void setDistance(int distance) {
			this.distance = distance;
		}

		public Status getStatus() {
			return status;
		}

		public void setStatus(Status status) {
			this.status = status;
		}

		/**
		 * toString() method is overridden to format the output as per specifications
		 */
		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append(nodeID)
			.append(",")
			.append(distance)
			.append(",");
			
			String prefix = "";
			for (String p:path) {
				sb.append(prefix);
				prefix = "-";
				sb.append(p);
			}
			return sb.toString();
		}
	}

	
	public static void main(String[] args) {
		String source = args[0];
		SparkConf conf = new SparkConf().setAppName("Assignment 2").setMaster("local");
		JavaSparkContext context = new JavaSparkContext(conf);
		JavaRDD<String> input = context.textFile(args[1]);
		
		/**
		 * Input is parsed and grouped by key:NodeID -> value:(nextNodeID, distance)
		 */
		JavaPairRDD<String, EdgeTuple> groupedNodes = input.mapToPair(new PairFunction<String, String, EdgeTuple>(){

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, EdgeTuple> call(String line) throws Exception {
				String[] tokens = line.split(",");
				String curr = tokens[0];
				String next = tokens[1];
				int edge = Integer.parseInt(tokens[2]);
				return new Tuple2<String, EdgeTuple>(curr, new EdgeTuple(next, edge));
			}
			
		});
		
		
		/**
		 * For each key key:NodeID -> value:(nextNodeID, distance), instantiate a node which stores the (nextNodeID, distance) of the node's neighbours as an array of tuples
		 */
		JavaPairRDD<String, Node> initialisedNodes = groupedNodes.groupByKey().flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<EdgeTuple>>, String, Node>(){

			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<Tuple2<String, Node>> call(Tuple2<String, Iterable<EdgeTuple>> input)
					throws Exception {
				String currNode = input._1;
				
				ArrayList<EdgeTuple> edges = new ArrayList<EdgeTuple>();
				ArrayList<Tuple2<String, Node>> nodes = new ArrayList<Tuple2<String, Node>>();
				
				for (EdgeTuple i:input._2) {
					edges.add(i);
				}
				
				Node node;
				if (currNode.equals(source)) {
					node = new Node(currNode, edges, new LinkedList<String>(Arrays.asList(source)), 0, Status.VISITING);
				} else {
					node = new Node(currNode, edges, new LinkedList<String>(), -1, Status.UNVISITED);
				}
				
				nodes.add(new Tuple2<String, Node>(currNode, node));
				
				return nodes.iterator();
			}
			
		});
		
		/**
		 * isCompleted: a status check to check if all the nodes in the graph have been visited and signal BFS algorithm to conclude
		 */
		boolean isCompleted = false;
		
		JavaRDD<String> checkSource = initialisedNodes.map(data -> data._1);
		if (!checkSource.collect().contains(source)) {
			isCompleted = true;
			JavaRDD<Node> resultNoKey = initialisedNodes.map(data -> data._2);
			resultNoKey.saveAsTextFile(args[2]);
			context.close();
		}
		
		while (!isCompleted) {
			
			/**
			 * For each node that is currently being visited (Status.VISITING), emit the neighbours of the node as new nodes with a new path, distance, status and empty edges
			 * Set status of current node to Status.VISITED
			 * Then and otherwise emit current node unchanged
			 */
			JavaPairRDD<String, Node> emittedNodes = initialisedNodes.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Node>, String, Node>() {
	
				private static final long serialVersionUID = 1L;

				@Override
				public Iterator<Tuple2<String, Node>> call(Tuple2<String, Node> input) throws Exception {
					Node currNode = input._2;
					Set<Tuple2<String, Node>> result = new HashSet<>();
					
					if (currNode.getStatus() == Status.VISITING) {
						for (EdgeTuple edge : currNode.getEdges()) {
							LinkedList<String> path = new LinkedList<String>(currNode.getPath());
							String neighbour = edge.getNeighbour();
							if (!path.contains(currNode.getNodeID())) path.add(currNode.getNodeID());
							path.add(neighbour);
							result.add(new Tuple2<>(neighbour, new Node(neighbour, new ArrayList<EdgeTuple>(), path, currNode.getDistance() + edge.getEdge(), Status.VISITING)));
						}
						currNode.setStatus(Status.VISITED);
					}
					result.add(new Tuple2<>(currNode.getNodeID(), currNode));
					return result.iterator();
				}
			});
			
			
			/**
			 * Each node now has two nodes mapped to the same NodeID key: one with its original neighbours, and one with the shortest distance/path
			 * For each key Node ID, we create a new node with its original neighbours, the shortest distance, and higher status 
			 */
			JavaPairRDD<String, Node> reducedNodes = emittedNodes.reduceByKey(new Function2<Node, Node, Node>() {
	
				private static final long serialVersionUID = 1L;

				@Override
				public Node call(Node nodeA, Node nodeB) throws Exception {
					ArrayList<EdgeTuple> edgesA = nodeA.getEdges();
					ArrayList<EdgeTuple> edgesB = nodeB.getEdges();
					
					ArrayList<String> neighboursA = new ArrayList<String>();
					for (EdgeTuple e:edgesA) {
						neighboursA.add(e.getNeighbour());
					}
					
					ArrayList<String> neighboursB = new ArrayList<String>();
					for (EdgeTuple e:edgesB) {
						neighboursB.add(e.getNeighbour());
					}
	
					ArrayList<EdgeTuple> chosenEdges;
					if (neighboursA.isEmpty()) {
						chosenEdges = edgesB;
					} else {
						chosenEdges = edgesA;
					}
					
					LinkedList<String> path;
					int distance;
					/**
					 * Check for shortest distance which is reachable
					 */
					if (nodeA.getDistance() == -1) {
						distance = nodeB.getDistance();
						path = nodeB.getPath();
					} else if (nodeB.getDistance() == -1) {
						distance = nodeA.getDistance();
						path = nodeA.getPath();
					} else {
						distance = nodeA.getDistance() < nodeB.getDistance() ? nodeA.getDistance() : nodeB.getDistance();
						path = nodeA.getDistance() < nodeB.getDistance() ? nodeA.getPath() : nodeB.getPath();
					}
					
					Status status;
					if (nodeA.getStatus().ordinal() < nodeB.getStatus().ordinal()) {
						status = nodeB.getStatus();
					} else {
						status = nodeA.getStatus();
					}
					return new Node(nodeA.getNodeID(), chosenEdges, path, distance, status);
				}
			});
		
			/**
			 * Iterate through the values mapped to each NodeID, check if any of them is still of VISITING or UNVISITED status 
			 */
			ArrayList<Status> statusCheck = new ArrayList<Status>();
			ArrayList<Tuple2<String, Node>> temp = new ArrayList<Tuple2<String, Node>>();
			for (Tuple2<String, Iterable<Node>> r:reducedNodes.groupByKey().collect()){
				for (Node n:r._2) {
					statusCheck.add(n.getStatus());
					temp.add(new Tuple2<String, Node>(r._1, n));
				}
			}

			/**
			 * Remove source node from result set as it is not required in output
			 */
			if (!statusCheck.contains(Status.VISITING)) {
				JavaPairRDD<String, Node> resultWithKey = reducedNodes.filter(new Function<Tuple2<String, Node>, Boolean>(){

					private static final long serialVersionUID = 1L;

					@Override
					public Boolean call(Tuple2<String, Node> input) throws Exception {
						return (!input._1.equals(source));
					}
					
				});
				
				/**
				 * Extract only the values mapped to each NodeID key
				 */
				JavaRDD<Node> resultNoKey = resultWithKey.map(data -> data._2);
				
				/**
				 * Sort the values by shortest distance from source node
				 */
				JavaRDD<Node> result = resultNoKey.sortBy(new Function<Node, Integer>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Integer call(Node node) throws Exception {
						return node.getDistance();
					}
				}, true, 1);

				result.saveAsTextFile(args[2]);
				context.close();
				
				/**
				 * Signal that BFS is completed and break out of loop
				 */
				isCompleted = true;
			} else {
				initialisedNodes = reducedNodes.coalesce(1);				
			}
		}
	}

}
