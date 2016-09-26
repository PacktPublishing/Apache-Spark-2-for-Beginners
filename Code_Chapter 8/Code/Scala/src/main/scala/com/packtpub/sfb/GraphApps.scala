/**
The following program can be compiled and run using SBT
Wrapper scripts have been provided with this
The following script can be run to compile the code
./compile.sh

The following script can be used to run this application in Spark
./submit.sh com.packtpub.sfb.GraphApps
**/

package com.packtpub.sfb
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.PartitionStrategy._

object GraphApps {
	// Create the Spark Session and the spark context			
	val spark = SparkSession
			.builder
			.appName(getClass.getSimpleName)
			.getOrCreate()
	val sc = spark.sparkContext	
  def main(args: Array[String]) {
	  createAndExploreGraph()
	  graphProcessing1()
	  graphProcessing2()
	  graphStructureProcessing()
	  tennisTournamentAnalysis()
	  pageRank()
	  connectedComponent()
  }
  def createAndExploreGraph(){
	  //Create an RDD of users containing tuple values with a mandatory Long and another String type as the property of the vertex
	  val users: RDD[(Long, String)] = sc.parallelize(Array((1L, "Thomas"), (2L, "Krish"),(3L, "Mathew")))
	  //Created an RDD of Edge type with String type as the property of the edge
	  val userRelationships: RDD[Edge[String]] = sc.parallelize(Array(Edge(1L, 2L, "Follows"),    Edge(1L, 2L, "Son"),Edge(2L, 3L, "Follows")))
	  //Create a graph containing the vertex and edge RDDs as created before
	  val userGraph = Graph(users, userRelationships)
	  //Number of edges in the graph
	  userGraph.numEdges
	  //Number of vertices in the graph
	  userGraph.numVertices
	  //Number of edges coming to each of the vertex. 
	  userGraph.inDegrees
	  //The first element in the tuple is the vertex id and the second element in the tuple is the number of edges coming to that vertex
	  userGraph.inDegrees.foreach(println)
	  //Number of edges going out of each of the vertex. 
	  userGraph.outDegrees
	  //The first element in the tuple is the vertex id and the second element in the tuple is the number of edges going out of that vertex
	  userGraph.outDegrees.foreach(println)
	  //Total number of edges coming in and going out of each vertex. 
	  userGraph.degrees
	  //The first element in the tuple is the vertex id and the second element in the tuple is the total number of edges coming in and going out of that vertex.
	  userGraph.degrees.foreach(println)
	  //Get the vertices of the graph
	  userGraph.vertices
	  //Get all the vertices with the vertex number and the property as a tuple
	  userGraph.vertices.foreach(println)
	  //Get the edges of the graph
	  userGraph.edges
	  //Get all the edges properties with source and destination vertex numbers
	  userGraph.edges.foreach(println)
	  //Get the triplets of the graph
	  userGraph.triplets
	  userGraph.triplets.foreach(println)
  }  
  def graphProcessing1(){
	  //Create the vertices with the stops
	  val stops: RDD[(Long, String)] = sc.parallelize(Array((1L, "Manchester"), (2L, "London"),(3L, "Colombo"), (4L, "Bangalore")))
	  //Create the edges with travel legs
	  val legs: RDD[Edge[String]] = sc.parallelize(Array(Edge(1L, 2L, "air"),    Edge(2L, 3L, "air"),Edge(3L, 4L, "air")))
	  //Create the onward journey graph
	  val onwardJourney = Graph(stops, legs)
	  onwardJourney.triplets.map(triplet => (triplet.srcId, (triplet.srcAttr, triplet.dstAttr))).sortByKey().collect().foreach(println)
	  val returnJourney = onwardJourney.reverse
	  returnJourney.triplets.map(triplet => (triplet.srcId, (triplet.srcAttr,triplet.dstAttr))).sortByKey(ascending=false).collect().foreach(println)
	  returnJourney.triplets.map(triplet => (triplet.srcId,triplet.dstId,triplet.attr,triplet.srcAttr,triplet.dstAttr)).foreach(println)
  }
  def graphProcessing2(){
	  // Create the vertices
	  val stops: RDD[(Long, String)] = sc.parallelize(Array((1L, "Manchester"), (2L, "London"),(3L, "Colombo"), (4L, "Bangalore")))
	  //Create the edges
	  val legs: RDD[Edge[Long]] = sc.parallelize(Array(Edge(1L, 2L, 50L),    Edge(2L, 3L, 100L),Edge(3L, 4L, 80L)))
	  //Create the graph using the vertices and edges
	  val journey = Graph(stops, legs)
	  //Convert the stop names to upper case
	  val newStops = journey.vertices.map {case (id, name) => (id, name.toUpperCase)}
	  //Get the edges from the selected journey and add 10% price to the original price
	  val newLegs = journey.edges.map { case Edge(src, dst, prop) => Edge(src, dst, (prop + (0.1*prop))) }
	  //Create a new graph with the original vertices and the new edges
	  val newJourney = Graph(newStops, newLegs)
	  //Print the contents of the original graph
	  journey.triplets.foreach(println)
	  //Print the contents of the transformed graph
	  newJourney.triplets.foreach(println)
  }
  def graphStructureProcessing(){
	  //Create an RDD of users containing tuple values with a mandatory Long and another String type as the property of the vertex
	  val users: RDD[(Long, String)] = sc.parallelize(Array((1L, "Thomas"), (2L, "Krish"),(3L, "Mathew")))
	  //Created an RDD of Edge type with String type as the property of the edge
	  val userRelationships: RDD[Edge[String]] = sc.parallelize(Array(Edge(1L, 2L, "Follows"), Edge(1L, 2L, "Son"),Edge(2L, 3L, "Follows"), Edge(1L, 4L, "Follows"), Edge(3L, 4L, "Follows")))
	  //Create a vertex property object to fill in if an invalid vertex id is given in the edge
	  val missingUser = "Missing"
	  //Create a graph containing the vertex and edge RDDs as created before
	  val userGraph = Graph(users, userRelationships, missingUser)
	  //List the graph triplets and find some of the invalid vertex ids given and for them the missing vertex property is assigned with the value “Missing”
	  userGraph.triplets.foreach(println)
	  //Since the edges with the invalid vertices are invalid too, filter out those vertices and create a valid graph. The vertex predicate here can be any valid filter condition of a vertex. Similar to vertex predicate, if the filtering is to be done on the edges, instead of the vpred, use epred as the edge predicate.
	  val fixedUserGraph = userGraph.subgraph(vpred = (vertexId, attribute) => attribute != "Missing")
	  fixedUserGraph.triplets.foreach(println)
	  // Partition the user graph. This is required to group the edges
	  val partitionedUserGraph = fixedUserGraph.partitionBy(CanonicalRandomVertexCut)
	  // Generate the graph without parallel edges and combine the properties of duplicate edges
	  val graphWithoutParallelEdges = partitionedUserGraph.groupEdges((e1, e2) => e1 + " and " + e2)
	  // Print the details
	  graphWithoutParallelEdges.triplets.foreach(println)
  }
  def tennisTournamentAnalysis(){
	  //Define a property class that is going to hold all the properties of the vertex which is nothing but player information
	  case class Player(name: String, country: String)
	  // Create the player vertices
	  val players: RDD[(Long, Player)] = sc.parallelize(Array((1L, Player("Novak Djokovic", "SRB")), (3L, Player("Roger Federer", "SUI")),(5L, Player("Tomas Berdych", "CZE")), (7L, Player("Kei Nishikori", "JPN")), (11L, Player("Andy Murray", "GBR")),(15L, Player("Stan Wawrinka", "SUI")),(17L, Player("Rafael Nadal", "ESP")),(19L, Player("David Ferrer", "ESP"))))
	  //Define a property class that is going to hold all the properties of the edge which is nothing but match information
	  case class Match(matchType: String, points: Int, head2HeadCount: Int)
	  // Create the match edges
	  val matches: RDD[Edge[Match]] = sc.parallelize(Array(Edge(1L, 5L, Match("G1", 1,1)), Edge(1L, 7L, Match("G1", 1,1)), Edge(3L, 1L, Match("G1", 1,1)), Edge(3L, 5L, Match("G1", 1,1)), Edge(3L, 7L, Match("G1", 1,1)), Edge(7L, 5L, Match("G1", 1,1)), Edge(11L, 19L, Match("G2", 1,1)), Edge(15L, 11L, Match("G2", 1, 1)), Edge(15L, 19L, Match("G2", 1, 1)), Edge(17L, 11L, Match("G2", 1, 1)), Edge(17L, 15L, Match("G2", 1, 1)), Edge(17L, 19L, Match("G2", 1, 1)), Edge(3L, 15L, Match("S", 5, 1)), Edge(1L, 17L, Match("S", 5, 1)), Edge(1L, 3L, Match("F", 11, 1))))
	  //Create a graph with the vertices and edges
	  val playGraph = Graph(players, matches)
	  //Print the match details
	  playGraph.triplets.foreach(println)
	  //Print matches with player names and the match type and the result
	  playGraph.triplets.map(triplet => triplet.srcAttr.name + " won over " + triplet.dstAttr.name + " in  " + triplet.attr.matchType + " match").foreach(println)
	  //Group 1 winners with their group total points
	  playGraph.triplets.filter(triplet => triplet.attr.matchType == "G1").map(triplet => (triplet.srcAttr.name, triplet.attr.points)).foreach(println)
	  //Find the group total of the players
	  playGraph.triplets.filter(triplet => triplet.attr.matchType == "G1").map(triplet => (triplet.srcAttr.name, triplet.attr.points)).reduceByKey(_+_).foreach(println)
	  //Group 2 winners with their group total points
	  playGraph.triplets.filter(triplet => triplet.attr.matchType == "G2").map(triplet => (triplet.srcAttr.name, triplet.attr.points)).foreach(println)
	  //Find the group total of the players
	  playGraph.triplets.filter(triplet => triplet.attr.matchType == "G2").map(triplet => (triplet.srcAttr.name, triplet.attr.points)).reduceByKey(_+_).foreach(println)
	  //Semi final winners with their group total points
	  playGraph.triplets.filter(triplet => triplet.attr.matchType == "S").map(triplet => (triplet.srcAttr.name, triplet.attr.points)).foreach(println)
	  //Find the group total of the players
	  playGraph.triplets.filter(triplet => triplet.attr.matchType == "S").map(triplet => (triplet.srcAttr.name, triplet.attr.points)).reduceByKey(_+_).foreach(println)
	  //Final winner with the group total points
	  playGraph.triplets.filter(triplet => triplet.attr.matchType == "F").map(triplet => (triplet.srcAttr.name, triplet.attr.points)).foreach(println)
	  //Tournament total point standing
	  playGraph.triplets.map(triplet => (triplet.srcAttr.name, triplet.attr.points)).reduceByKey(_+_).foreach(println)
	  //Find the winner of the tournament by finding the top scorer of the tournament
	  playGraph.triplets.map(triplet => (triplet.srcAttr.name, triplet.attr.points)).reduceByKey(_+_).map{ case (k,v) => (v,k)}.sortByKey(ascending=false).take(1).map{ case (k,v) => (v,k)}.foreach(println)
	  //Find how many head to head matches held for a given set of players in the descending order of head2head count
	  playGraph.triplets.map(triplet => (Set(triplet.srcAttr.name , triplet.dstAttr.name) , triplet.attr.head2HeadCount)).reduceByKey(_+_).map{case (k,v) => (k.mkString(" and "), v)}.map{ case (k,v) => (v,k)}.sortByKey().map{ case (k,v) => v + " played " + k + " time(s)"}.foreach(println)
	  //List of players who have won at least one match
	  val winners = playGraph.triplets.map(triplet => triplet.srcAttr.name).distinct
	  winners.foreach(println)
	  //List of players who have lost at least one match
	  val loosers = playGraph.triplets.map(triplet => triplet.dstAttr.name).distinct
	  loosers.foreach(println)
	  //List of players who have won at least one match and lost at least one match
	  val wonAndLost = winners.intersection(loosers)
	  wonAndLost.foreach(println)
	  //List of players who have no wins at all
	  val lostAndNoWins = loosers.collect().toSet -- wonAndLost.collect().toSet
	  lostAndNoWins.foreach(println)
	  //List of players who have no loss at all
	  val wonAndNoLosses = winners.collect().toSet -- loosers.collect().toSet
	  //The val wonAndNoLosses returned an empty set which means that there is no single player in this tournament who have only wins
	  wonAndNoLosses.foreach(println)
  }
  def pageRank(){
	  //Define a property class that is going to hold all the properties of the vertex which is nothing but player information
	  case class Player(name: String, country: String)
	  // Create the player vertices
	  val players: RDD[(Long, Player)] = sc.parallelize(Array((1L, Player("Novak Djokovic", "SRB")), (3L, Player("Roger Federer", "SUI")),(5L, Player("Tomas Berdych", "CZE")), (7L, Player("Kei Nishikori", "JPN")), (11L, Player("Andy Murray", "GBR")),(15L, Player("Stan Wawrinka", "SUI")),(17L, Player("Rafael Nadal", "ESP")),(19L, Player("David Ferrer", "ESP"))))
	  //Define a property class that is going to hold all the properties of the edge which is nothing but match information
	  case class Match(matchType: String, points: Int, head2HeadCount: Int)
	  // Create the match edges
	  val matches: RDD[Edge[Match]] = sc.parallelize(Array(Edge(1L, 5L, Match("G1", 1,1)), Edge(1L, 7L, Match("G1", 1,1)), Edge(3L, 1L, Match("G1", 1,1)), Edge(3L, 5L, Match("G1", 1,1)), Edge(3L, 7L, Match("G1", 1,1)), Edge(7L, 5L, Match("G1", 1,1)), Edge(11L, 19L, Match("G2", 1,1)), Edge(15L, 11L, Match("G2", 1, 1)), Edge(15L, 19L, Match("G2", 1, 1)), Edge(17L, 11L, Match("G2", 1, 1)), Edge(17L, 15L, Match("G2", 1, 1)), Edge(17L, 19L, Match("G2", 1, 1)), Edge(3L, 15L, Match("S", 5, 1)), Edge(1L, 17L, Match("S", 5, 1)), Edge(1L, 3L, Match("F", 11, 1))))
	  //Create a graph with the vertices and edges
	  val playGraph = Graph(players, matches)
	  //Reverse this graph to have the winning player coming in the destination vertex
	  val rankGraph = playGraph.reverse
	  //Run the PageRank algorithm to calculate the rank of each vertex
	  val rankedVertices = rankGraph.pageRank(0.0001).vertices
	  //Extract the vertices sorted by the rank
	  val rankedPlayers = rankedVertices.join(players).map{case (id,(importanceRank,Player(name,country))) => (importanceRank, name)}.sortByKey(ascending=false)
	  rankedPlayers.collect().foreach(println)
  }
  def connectedComponent(){
	  // Create the RDD with users as the vertices
	  val users: RDD[(Long, String)] = sc.parallelize(Array((1L, "Thomas"), (2L, "Krish"),(3L, "Mathew"), (4L, "Martin"), (5L, "George"), (6L, "James")))
	  // Create the edges connecting the users
	  val userRelationships: RDD[Edge[String]] = sc.parallelize(Array(Edge(1L, 2L, "Follows"),Edge(2L, 3L, "Follows"), Edge(4L, 5L, "Follows"), Edge(5L, 6L, "Follows")))
	  // Create a graph
	  val userGraph = Graph(users, userRelationships)
	  // Find the connected components of the graph
	  val cc = userGraph.connectedComponents()
	  // Extract the triplets of the connected components
	  val ccTriplets = cc.triplets
	  // Print the structure of the triplets
	  ccTriplets.foreach(println)
	  //Print the vertex numbers and the corresponding connected component id. The connected component id is generated by the system and it is to be taken only as a unique identifier for the connected component
	  val ccProperties = ccTriplets.map(triplet => "Vertex " + triplet.srcId + " and " + triplet.dstId + " are part of the CC with id " + triplet.srcAttr)
	  ccProperties.foreach(println)
	  //Find the users in the source vertex with their CC id
	  val srcUsersAndTheirCC = ccTriplets.map(triplet => (triplet.srcId, triplet.srcAttr))
	  //Find the users in the destination vertex with their CC id
	  val dstUsersAndTheirCC = ccTriplets.map(triplet => (triplet.dstId, triplet.dstAttr))
	  //Find the union
	  val usersAndTheirCC = srcUsersAndTheirCC.union(dstUsersAndTheirCC)
	  //Join with the name of the users
	  val usersAndTheirCCWithName = usersAndTheirCC.join(users).map{case (userId,(ccId,userName)) => (ccId, userName)}.distinct.sortByKey()
	  //Print the user names with their CC component id. If two users share the same CC id, then they are connected
	  usersAndTheirCCWithName.collect().foreach(println)
	  
  }
  
  
  
}




