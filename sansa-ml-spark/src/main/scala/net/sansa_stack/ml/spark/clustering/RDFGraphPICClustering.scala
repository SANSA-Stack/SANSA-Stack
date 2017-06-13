package net.sansa_stack.ml.spark.clustering

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.clustering.{ PowerIterationClusteringModel, PowerIterationClustering }
import org.apache.spark.graphx.{ Graph, EdgeDirection }
import scala.math.BigDecimal
import org.apache.commons.math3.util.MathUtils
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
    /*
	 * Collect all the edges of the graph
	*/
    val edge = graph.edges.collect()
    /*
	 * Collect neighbor IDs of all the vertices
	 */
    val neighbors = graph.collectNeighborIds(EdgeDirection.Either)
    /*
	 * Collect distinct vertices of the graph
	 */
    val vertices = graph.vertices.distinct()
    /*
	 * Difference between two set of vertices, used in different similarity measures
	 */
    def difference(a: Long, b: Long): Double = {
      val ansec = neighbors.lookup(a).distinct.head.toSet
      val ansec1 = neighbors.lookup(b).distinct.head.toSet
      if (ansec.isEmpty) { return 0.0 }
      val differ = ansec.diff(ansec1)
      if (differ.isEmpty) { return 0.0 }

      differ.size.toDouble
    }

    /*
	 * Intersection of two set of vertices, used in different similarity measures
	 */
    def intersection(a: Long, b: Long): Double = {
      val inters = neighbors.lookup(a).distinct.head.toSet
      val inters1 = neighbors.lookup(b).distinct.head.toSet
      if (inters.isEmpty || inters1.isEmpty) { return 0.0 }
      val rst = inters.intersect(inters1).toArray
      if (rst.isEmpty) { return 0.0 }
      rst.size.toDouble
    }

    /*
			 * Union of two set of vertices, used in different similarity measures
			 */
    def union(a: Long, b: Long): Double = {
      val inters = neighbors.lookup(a).distinct.head.toSet
      val inters1 = neighbors.lookup(b).distinct.head.toSet
      val rst = inters.union(inters1).toArray
      if (rst.isEmpty) { return 0.0 }

      rst.size.toDouble
    }
    // Logarithm base 2 
    val LOG2 = math.log(2)
    val log2 = { x: Double => math.log(x) / LOG2 }

    /*
			 * Jaccard similarity measure 
			 */
    def simJaccard(a: Long, b: Long): Double = {
      intersection(a, b) / union(a, b).toDouble

    }
    /*
			 * Batet similarity measure 
			 */
    def simBatet(a: Long, b: Long): Double = {
      val cal = 1 + ((difference(a, b) + difference(b, a)) / (difference(a, b) + difference(b, a) + intersection(a, b))).abs
      log2(cal.toDouble)
    }
    /*
			 * Rodríguez and Egenhofer similarity measure
			 */

    var g = 0.8
    def simRE(a: Long, b: Long): Double = {
      (intersection(a, b) / ((g * difference(a, b)) + ((1 - g) * difference(b, a)) + intersection(a, b))).toDouble.abs
    }
    /*
			 * The Contrast model similarity
			 */
    var gamma = 0.3
    var alpha = 0.3
    var beta = 0.3
    def simCM(a: Long, b: Long): Double = {
      ((gamma * intersection(a, b)) - (alpha * difference(a, b)) - (beta * difference(b, a))).toDouble.abs
    }
    /*
			 * The Ratio model similarity
			 */
    var alph = 0.5
    var beth = 0.5
    def simRM(a: Long, b: Long): Double = {
      ((intersection(a, b)) / ((alph * difference(a, b)) + (beth * difference(b, a)) + intersection(a, b))).toDouble.abs
    }

    /*
			 * Calculate similarities between different pair of vertices in the given graph
			 */

    val ver = edge.map { x =>
      {
        val x1 = x.dstId.toLong
        val x2 = x.srcId.toLong
        val allneighbor = neighbors.lookup(x1).distinct.head
        val allneighbor1 = neighbors.lookup(x2).distinct.head
        //(x1, x2, jaccard(x1, x2).abs)
        //(x1, x2, simBatet(x1, x2).abs)
        (x1, x2, simRE(x1, x2).abs)
      }
    }

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