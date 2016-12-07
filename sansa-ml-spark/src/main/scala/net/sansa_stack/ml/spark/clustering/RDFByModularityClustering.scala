package net.sansa_stack.ml.spark.clustering

import org.apache.log4j.{ Level, Logger }
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{ SparkConf, SparkContext }

import scala.util.control.Breaks._

/**
 * Created by hpetzka on 09.11.2016.
 */
object RDFByModularityClustering {

  def apply(sc: SparkContext, numIterations: Int, graphFile: String, outputFile: String) = {

    // DEFAULT INPUT
    // val (numIterations, graphFile) = (100 , "C:/Users/hpetzka/IdeaProjects/Clustering_in_Spark/Graphs/testRDF.txt")

    val graphRDD = sc.textFile(graphFile) // returns list with entry = triple form "URI1 URI2 URI3 ."

    val nodeURIsRDD = graphRDD.map(x => x.split(" ").filter(y => y.trim() != "")).flatMap(array => Array(array(0), array(2))).distinct

    // GET INFORMATION ON GRAPH
    val numEdges = graphRDD.count
    val numVertices: Int = nodeURIsRDD.count.toInt

    val edgesRDD: RDD[(String, String)] = graphRDD.
      map(x => x.split(" ").filter(y => y.trim() != "")).
      map(array => (array(0), array(2)))

    println("The number of nodes in the knowledge graph is " + numVertices + " and the number of edges is " + numEdges + ".")

    println("The first ten edges of the graph look like the following: ")
    for (x <- edgesRDD.take(10)) println(x)

    /*
        If weight edges come in, one probably needa a map as follows
        val adjacencyMatrix: Array[Array[Int]] = Array.ofDim[Int](numVertices, numVertices)
        var adjacencies: Map[(String, String), Int] = Map[(String, String),Int]()
        for (x <- edgesRDD.collect()){
          // TODO add the weights here if they exist
          if(x(0) < x(1)) adjacencies += ( (x(0),x(1)) -> 1 )
          else adjacencies += ( (x(1),x(0)) -> 1 )
        }
        val adjacenciesBC: Broadcast[Map[(String, String), Int]] = sc.broadcast(adjacencies)
        edgesRDD.unpersist()

    */

    val flattendedEdgeList: Array[String] = edgesRDD.flatMap(x => Array(x._1, x._2)).collect
    val vertexDegreesRDD: RDD[(String, Int)] = nodeURIsRDD.map(x => (x, flattendedEdgeList.count(y => y == x)))

    val vertexDegreesBC: Broadcast[Map[String, Int]] = sc.broadcast(vertexDegreesRDD.collect.toMap)
    val edgesBC: Broadcast[Array[(String, String)]] = sc.broadcast(edgesRDD.collect)

    // Initialize clusters

    var clusterMapRDD: RDD[List[String]] = nodeURIsRDD.map(x => List(x))

    /* Old version with indices, don't think I need them
    val nodeURIsAsListsRDD: RDD[List[String]] = nodeURIsRDD.map(x => List(x))
    var clusterMapRDD: RDD[(Int, List[String])] = sc.parallelize(0 until numVertices).zip(nodeURIsAsListsRDD)
    */
    nodeURIsRDD.unpersist()
    vertexDegreesRDD.unpersist()
    graphRDD.unpersist()

    println("Starting iteration")
    println()

    var stopSign = false

    breakable {
      for (k <- 1 to numIterations) {
        println(k)
        val (clusterMapRDD2, stopSign2) = iterationStepClusteringRDFByModularity(
          numEdges,
          edgesBC,
          vertexDegreesBC,
          clusterMapRDD,
          sc)
        clusterMapRDD.unpersist()
        clusterMapRDD = clusterMapRDD2
        stopSign = stopSign2
        // check last entry of clusterAssociations for whether we should stop
        if (stopSign) {
          System.out.println("Algorithm stopped, because no further improvement is possible")
          break
        }
      }
    }

    val clusters = clusterMapRDD.collect()

    val writer = new java.io.PrintWriter(new java.io.File(outputFile))
    var counter = 0
    println("The computed clusters are:")
    for (list <- clusters) {
      counter += 1
      println("Cluster" + counter + "  contains:")
      for (entry <- list) {
        print(entry + ", ")
        writer.write(entry + ", ")
      }

      println()
      println()
      writer.write("\n \n")

    }

  }

  def iterationStepClusteringRDFByModularity(numEdges: Long,
                                             edgesBC: Broadcast[Array[(String, String)]],
                                             vertexDegreesBC: Broadcast[Map[String, Int]],
                                             clusterMapRDD: RDD[List[String]],
                                             sc: SparkContext): (RDD[List[String]], Boolean) = {
    // Start iteration

    // The following RDD contains distinct pairs of clusters for which there is an edge between them
    val pairsOfClustersRDD: RDD[(List[String], List[String])] = clusterMapRDD.cartesian(clusterMapRDD).
      filter({ case (list1, list2) => list1.head < list2.head }). // excludes not only equal lists, but also considers each pair only once
      filter({
        case (list1, list2) =>
          var check_connection = false
          for (x <- list1) {
            for (y <- list2) {
              if (edgesBC.value.contains((x, y)) || edgesBC.value.contains((y, x))) {
                check_connection = true
              }
            }
          }
          check_connection
      })

    // For each of the pair of clusters compute the deltaQ
    val deltaQRDD: RDD[((List[String], List[String]), Double)] = pairsOfClustersRDD.
      map({
        case (list1, list2) =>
          ((list1, list2), deltaQ(numEdges, vertexDegreesBC, edgesBC, list1, list2))
      })

    val ((bestList1, bestList2), bestQ): ((List[String], List[String]), Double) =
      deltaQRDD.
        takeOrdered(1)(Ordering[Double].reverse.on[((List[String], List[String]), Double)](_._2)).head
    // takeOrdered(n,[ordering]), n = first n elements
    // reverse reverses the given ordering
    // Ordering has a method 'on' which takes as input a function from (here:) ((Int,Int), Double) to (here:) Double,
    //          so that it creates an ordering on ((Int, Int), Double) from the natural ordering on Double
    //          here, the function is take x => x._2 which takes second argument of tuple
    // the head (or alternatively a zero '(0)') means taking out the first entry, as the result is a list of 1 elements

    deltaQRDD.unpersist()
    pairsOfClustersRDD.unpersist()

    if (bestQ < 0) {
      (clusterMapRDD, true)
    } else {
      (clusterMapRDD.
        filter(list => list != bestList1 && list != bestList2).
        union(sc.parallelize(List(bestList1.union(bestList2)))), false)
    }

  }

  // The function that computes delta Q for the merge of two clusters

  def deltaQ(numEdges: Long,
             vertexDegreesBC: Broadcast[Map[String, Int]],
             edgesBC: Broadcast[Array[(String, String)]],
             clusterI: List[String],
             clusterJ: List[String]): Double = {

    val clusterPairs: List[(String, String)] = clusterI.flatMap(x => clusterJ.map(y => (x, y)))

    val summand: List[Double] = clusterPairs.map({
      case (x, y) =>
        var value: Double = vertexDegreesBC.value(x) * vertexDegreesBC.value(y) / (2.0 * numEdges) //
        if (edgesBC.value.contains((x, y)) || edgesBC.value.contains((y, x))) value -= 1
        value
    })
    1.0 / numEdges * summand.fold(0.0)((a: Double, b: Double) => a - b)
  }

}


