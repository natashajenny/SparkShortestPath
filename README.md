# Spark Shortest Path
Spark Implementation of Shortest Path Algorithm

* This implementation assumes there is no abandoned node, meaning that each node at least has one edge to another node.
* The minimum weight of an edge between two individual nodes is 1.
* The output is sorted (ascending) by length of shortest path and written to a txt file

![alt text](https://github.com/natashajenny/SparkShortestPath/Weighted-Graph.png)


## Input format
The input represents a connected directed graph.
The input is formatted as below. Each line of the input file represents a vertex of the graph formatted like: starting node, end node, and the distance
```
N0,N1,4
N0,N2,3
N1,N2,2
N1,N3,2
N2,N3,7
N3,N4,2
N4,N0,4
N4,N1,4
N4,N5,6
```

## Output format
The output is in the format below. Each line of output represents the distance from the starting node to another node, and it is formatted as: the destination node, shortest distance, and the path from the starting node to the destination node. The file should be sorted by the shortest path:
```
N2,3,N0-N2
N1,4,N0-N1
N3,6,N0-N1-N3
```

## Implementation
My implementation uses a Breadth First Search approach to compute the shortest path from the source node to any node in the graph.


Variable | Purpose |
--- | --- |
| Node     | a representation of the graph's vertices |
| Neighbour |  a node which is connected to a current node by an edge |
| Edge | distance between two nodes  |

&nbsp;

**Algorithm:**
1. Parse the input and group them by ```key:NodeID -> value:(nextNodeID, distance)```
2. For each key ```key:NodeID -> value:(nextNodeID, distance)```, instantiate a node which stores the (nextNodeID, distance) of the node's neighbours as an array of tuples
3. While there is an unvisited node in the graph, do:
For each node that is currently being visited ```Status.VISITING```, emit the neighbours of the node as new nodes with:
    * path to neighbour
    * empty edges
    * distance to current node + edge from current node to neighbour as the new distance
    * status of visiting
4. Set status of current node to ```Status.VISITED``` or otherwise emit current node unchanged</br>
At the next stage, each key nodeID has two nodes, one with its original neighbours, and one with the current path and distance from source node to current node.</br>
This is then reduced to one node per NodeID which is one with the shortest distance/path from source node, and the higher status `VISITED > VISITING > UNVISITED`
At the end of each iteration, the graph is checked for `UNVISITED` or `VISITING` nodes.</br>
If they are present, loop restarts. If all nodes are visited, the loop terminates.</br>
5. The output, after being extracted and sorted, is saved to output path.
  
&nbsp;

## Running The Project
> $ $ javac -cp ".:Spark-Core.jar" SparkShortestPath.java <br>
> $ java -cp ".:Spark-Core.jar" SparkShortestPath STARTING_NODE INPUT_PATH OUTPUT_PATH

