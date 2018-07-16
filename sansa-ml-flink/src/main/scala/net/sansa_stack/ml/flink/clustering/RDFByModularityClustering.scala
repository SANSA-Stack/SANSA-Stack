package net.sansa_stack.ml.flink.clustering

import java.io.StringWriter

import scala.collection.JavaConverters._
import scala.util.control.Breaks._

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.java.operators.DataSink
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.util.Collector

object RDFByModularityClustering {

  def apply(env: ExecutionEnvironment, numIterations: Int, graphFile: String, outputFile: String): DataSink[String] = {

    val graphDataSet = env.readTextFile(graphFile) // returns list with entry = triple form "URI1 URI2 URI3 ."
    val nodeURIsDataSet = graphDataSet.map(x => x.split(" ").filter(y => y.trim() != "")).flatMap(array => Array(array(0), array(2))).distinct

    val numEdges = graphDataSet.count
    val numVertices: Int = nodeURIsDataSet.count.toInt

    val edgesDataSet: DataSet[(String, String)] = graphDataSet.
      map(x => x.split(" ").filter(y => y.trim() != "")).
      map(array => (array(0), array(2)))

    println("The number of nodes in the knowledge graph is " + numVertices + " and the number of edges is " + numEdges + ".")

    for (x <- edgesDataSet.collect().take(10)) println(x)

    val flattendedEdgeList: Array[String] = edgesDataSet.flatMap(x => Array(x._1, x._2)).collect.toArray
    val vertexDegreesDataSet: DataSet[(String, Int)] = nodeURIsDataSet.map(x => (x, flattendedEdgeList.count(y => y == x)))

    /* val vertexDegreesBCFlink =nodeURIsDataSet.map(new RichMapFunction[String, Int]() {

          var broadcastSet: Traversable[(String, Int)] = _

          override def open(config: Configuration): Unit = {
            broadcastSet = getRuntimeContext().getBroadcastVariable[(String, Int)]("vertexDegreesBC").asScala
          }

          override def map(t: (String, Int)):Unit= broadcastSet.map(f=>(f._1,f._2))
        })
        .withBroadcastSet(vertexDegreesDataSet, "vertexDegreesBC")
      */

    // TODO: this need to be broadcasted
    val vertexDegreesBC = vertexDegreesDataSet.collect.toMap
    val edgesBC = edgesDataSet.collect.toArray

    // Initialize clusters

    var clusterMapDataSet: DataSet[List[String]] = nodeURIsDataSet.map(x => List(x))

    println("Starting iteration")
    println()

    var stopSign = false

    breakable {
      for (k <- 1 to numIterations) {
        println(k)
        val (clusterMapDataSet2, stopSign2) = iterationStepClusteringRDFByModularity(
          numEdges,
          edgesBC,
          vertexDegreesBC,
          clusterMapDataSet,
          env)
        val clusterMap2 = clusterMapDataSet2.collect
        clusterMapDataSet = env.fromCollection(clusterMap2)
        stopSign = stopSign2
        // check last entry of clusterAssociations for whether we should stop
        if (stopSign) {
          System.out.println("Algorithm stopped, because no further improvement is possible")
          break
        }
      }
    }

    val clusters = clusterMapDataSet.collect()

    val writer = new StringWriter
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

    val cClustersInfo: DataSet[String] = env.fromCollection(Seq(writer.toString()))
    WriteToFile(cClustersInfo, outputFile, (true, 1))

  }
  def iterationStepClusteringRDFByModularity(
    numEdges: Long,
    edgesBC: Array[(String, String)],
    vertexDegreesBC: Map[String, Int],
    clusterMapDataSet: DataSet[List[String]],
    env: ExecutionEnvironment): (DataSet[List[String]], Boolean) = {
    // Start iteration

    // The following DataSet contains distinct pairs of clusters for which there is an edge between them

    val pairsOfClustersDataSet: DataSet[(List[String], List[String])] = clusterMapDataSet.cross(clusterMapDataSet).
      filter({ _ match { case (list1, list2) => list1.head < list2.head } }). // excludes not only equal lists, but also considers each pair only once
      filter({
        _ match {
          case (list1, list2) =>
            var check_connection = false
            for (x <- list1) {
              for (y <- list2) {
                if (edgesBC.contains((x, y)) || edgesBC.contains((y, x))) {
                  check_connection = true
                }
              }
            }
            check_connection
        }
      })

    // For each of the pair of clusters compute the deltaQ
    val deltaQDataSet: DataSet[((List[String], List[String]), Double)] = pairsOfClustersDataSet.
      map({
        _ match {
          case (list1, list2) =>
            ((list1, list2), deltaQ(numEdges, vertexDegreesBC, edgesBC, list1, list2))
        }
      })

    val ((bestList1, bestList2), bestQ: Double): ((List[String], List[String]), Double) = {

      val s = deltaQDataSet.groupBy(0).sortGroup(1, Order.ASCENDING).reduceGroup {
        (in, out: Collector[((List[String], List[String]), Double)]) =>
          var prev: (String, String) = null
          for (t <- in) {
            if (prev == null || prev != t) {
              out.collect(t)
            }
          }
      }

      /*   val ss = deltaQDataSet.map(f => (f._1.toString(), f._2.toString())).coGroup(pairsOfClustersDataSet.map(f => (f._1.toString(), f._2.toString()))).where(0).equalTo(0)
        .sortFirstGroup(0, Order.DESCENDING)
        .sortSecondGroup(1, Order.ASCENDING).sortSecondGroup(0, Order.ASCENDING)
      */

      /* val s = deltaQDataSet.coGroup(pairsOfClustersDataSet).where(0).equalTo(0)
        .sortFirstGroup(0, Order.DESCENDING)
        .sortSecondGroup(1, Order.ASCENDING).sortSecondGroup(0, Order.ASCENDING) */ // {
      //      {
      //        (bestL1: List[String], bestL2: List[String], out: ((List[String], List[String]), Double)) =>
      //          bestL1.buffered.head
      //      }
      s.collect().head
      // ss.collect().asInstanceOf[((List[String], List[String]), Double)]

    }

    if (bestQ < 0) {
      (clusterMapDataSet, true)
    } else {
      (clusterMapDataSet.
        filter(list => list != bestList1 && list != bestList2).
        union(env.fromCollection(List(bestList1.union(bestList2)))), false)
    }

  }

  // The function that computes delta Q for the merge of two clusters

  def deltaQ(
    numEdges: Long,
    vertexDegreesBC: Map[String, Int],
    edgesBC: Array[(String, String)],
    clusterI: List[String],
    clusterJ: List[String]): Double = {

    val clusterPairs: List[(String, String)] = clusterI.flatMap(x => clusterJ.map(y => (x, y)))

    val summand: List[Double] = clusterPairs.map({
      case (x, y) =>
        var value: Double = vertexDegreesBC(x) * vertexDegreesBC(y) / (2.0 * numEdges) //
        if (edgesBC.contains((x, y)) || edgesBC.contains((y, x))) value -= 1
        value
    })
    1.0 / numEdges * summand.fold(0.0)((a: Double, b: Double) => a - b)
  }

  def WriteToFile[T](dataset: DataSet[T], file: String, parallelism: (Boolean, Int) = (false, 0)): DataSink[T] =
    parallelism._1 match {
      case true => dataset.writeAsText(file, writeMode = FileSystem.WriteMode.OVERWRITE).setParallelism(parallelism._2)
      case false => dataset.writeAsText(file, writeMode = FileSystem.WriteMode.OVERWRITE)
    }
}
