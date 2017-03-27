package net.sansa_stack.ml.spark.clustering

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.clustering.{ PowerIterationClusteringModel, PowerIterationClustering }
import org.apache.spark.graphx.{ Graph, EdgeDirection }
import scala.math.BigDecimal
import org.apache.commons.math.util.MathUtils
import org.apache.spark.sql.SparkSession

class RDFGraphPICClustering(@transient val sparkSession: SparkSession,
                            val graph: Graph[Int, Int],
                            private val k: Int,
                            private val maxIterations: Int) extends Serializable {

  def clusterRdd(): RDD[(Long, Long, Double)] = {
    SimilaritesInPIC
  }

  /*
   * Computes different similarities function for a given graph @graph.
   */
  def SimilaritesInPIC(): RDD[(Long, Long, Double)] = {
    //****************************************************************************************************
    //****collect the edges***************
    val edg = graph.edges.collect()
    var simJaccard = 0.0

    //***** collecting neighbors**********
    val neighbor = graph.collectNeighborIds(EdgeDirection.Either)

    //************************************

    val vertex = graph.vertices.distinct()
    val v1 = vertex.id

    // ***********************jaccard similarity function ************************************
    def jaccard[A](a: Set[A], b: Set[A]): Double = {

      if (a.isEmpty || b.isEmpty) { return 0.0 }
      a.intersect(b).size / a.union(b).size.toDouble
    }
    //**************************************************************

    //*********** similarity of Strategies based on Information Theory****************************

    val c = vertex.count()
    var icc = 0.0
    val logC = MathUtils.log(10.0, c.toDouble)

    def informationContent(a: Long): Double = {
      if (a == 0) { return 0.0 }
      1 - (MathUtils.log(10.0, a.toDouble) / logC)

    }
    //val ic = informationContent(vertexId)

    def ic(a: Long): Double = {
      val d = neighbor.lookup(a).distinct.head.toSet
      if (d.isEmpty) { return 0.0 }
      else {
        val iC = d.size.toLong
        val sumIC = informationContent(iC)
        return sumIC.abs
      }
    }

    def mostICA(a: Long, b: Long): Double = {

      val an = neighbor.lookup(a).distinct.head.toSet
      val an1 = neighbor.lookup(b).distinct.head.toSet
      if (an.isEmpty || an1.isEmpty) { return 0.0 }
      val int = an.intersect(an1).toArray
      if (int.isEmpty) { return 0.0 }
      else {
        val icmica = int.size.toLong
        val sumMICA = informationContent(icmica)
        return sumMICA
      }

    }
    //******************************************************* Lin similarity ***************************************************************
    def simLin(e: Long, d: Long): Double = {
      if (ic(e) > 0.0 || ic(d) > 0.0) {
        (2.0.abs * (mostICA(e, d)).abs) / (ic(e).abs + ic(d).abs)
      } else { return 0.0 }
    }
    //***************************************************************************************************
    //difference of 2 sets : uses in below similarities
    def n(a: Long, b: Long): Double = {
      val ansec = neighbor.lookup(a).distinct.head.toSet
      val ansec1 = neighbor.lookup(b).distinct.head.toSet
      if (ansec.isEmpty) { return 0.0 }
      val differ = ansec.diff(ansec1)
      if (differ.isEmpty) { return 0.0 }

      differ.size.toDouble
    }
    // intersection of 2 sets
    def in(a: Long, b: Long): Double = {
      val inters = neighbor.lookup(a).distinct.head.toSet
      val inters1 = neighbor.lookup(b).distinct.head.toSet
      if (inters.isEmpty || inters1.isEmpty) { return 0.0 }
      val rst = inters.intersect(inters1).toArray
      if (rst.isEmpty) { return 0.0 }

      rst.size.toDouble
    }
    //logarithm base 2 
    val LOG2 = math.log(2)

    val log2 = { x: Double => math.log(x) / LOG2 }
    //************************************ Batet similarity*********************************************************
    def simBatet(a: Long, b: Long): Double = {
      val cal = 1 + ((n(a, b) + n(b, a)) / (n(a, b) + n(b, a) + in(a, b))).abs
      log2(cal.toDouble)
    }

    //************************************************* Rodríguez and Egenhofer similarity***********************************
    var g = 0.5
    def simRE(a: Long, b: Long): Double = {
      (in(a, b) / ((g * n(a, b)) + ((1 - g) * n(b, a)) + in(a, b))).toDouble.abs
    }
    //************************************************************the contrast model similarity****************************************
    var y = 0.3
    var al = 0.3
    var be = 0.3
    def simCM(a: Long, b: Long): Double = {
      ((y * in(a, b)) - (al * n(a, b)) - (be * n(b, a))).toDouble.abs
    }

    //********************************************************the ratio model similarity***********************************************************
    var alph = 0.5
    var beth = 0.5
    def simRM(a: Long, b: Long): Double = {
      ((in(a, b)) / ((alph * n(a, b)) + (beth * n(b, a)) + in(a, b))).toDouble.abs
    }

    //*************************************************************************************************************************

    val ver = edg.map { x =>
      {
        val x1 = x.dstId.toLong
        val x2 = x.srcId.toLong
        val allneighbor = neighbor.lookup(x1).distinct.head
        val allneighbor1 = neighbor.lookup(x2).distinct.head

        simJaccard = (jaccard(allneighbor.toSet, allneighbor1.toSet))
        // below for applying jaccard similarity use "simi" and for applying similarity of Strategies based on Information Theory use "sim(x1,x2).abs"          
        (x1, x2, simBatet(x1, x2).abs)
      }
    }

    ver.foreach { x => println(x) }
    sparkSession.sparkContext.parallelize(ver)
  }

  def pic() = {
    val pic = new PowerIterationClustering()
      .setK(k)
      .setMaxIterations(maxIterations)
    pic
  }

  def model = pic.run(clusterRdd())

  /*
   * Cluster the graph data into two classes using PowerIterationClustering
   */
  def run() = model

  /*
   * Save the model.
   * @path - path for a model.
   */
  def save(path: String) = model.save(sparkSession.sparkContext, path)

  /*
   * Load the model.
   * @path - the given model.
   */
  def load(path: String) = PowerIterationClusteringModel.load(sparkSession.sparkContext, path)

}

object RDFGraphPICClustering {
  def apply(sparkSession: SparkSession, graph: Graph[Int, Int], k: Int, maxIterations: Int) = new RDFGraphPICClustering(sparkSession, graph, k, maxIterations)
}