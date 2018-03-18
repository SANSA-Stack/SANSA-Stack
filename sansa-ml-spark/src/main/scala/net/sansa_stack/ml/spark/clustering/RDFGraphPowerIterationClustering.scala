package net.sansa_stack.ml.spark.clustering

import scala.reflect.runtime.universe._
import scopt.OptionParser
import org.apache.log4j.{ Level, Logger }
import org.apache.spark.mllib.util.MLUtils
import java.io.{ FileReader, FileNotFoundException, IOException }
import org.apache.spark.mllib.linalg.Vectors
import java.lang.{ Long => JLong }
import java.lang.{ Long => JLong }
import breeze.linalg.{ squaredDistance, DenseVector, Vector }
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx.GraphLoader
import org.apache.jena.datatypes.{ RDFDatatype, TypeMapper }
import org.apache.jena.graph.{ Node => JenaNode, Triple => JenaTriple, _ }
import org.apache.jena.riot.writer.NTriplesWriter
import org.apache.jena.riot.{ Lang, RDFDataMgr }
import org.apache.jena.graph.{ Node_ANY, Node_Blank, Node_Literal, Node_URI, Node => JenaNode, Triple => JenaTriple }
import org.apache.jena.vocabulary.RDF
import java.io.ByteArrayInputStream
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import java.io.StringWriter
import java.io._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.clustering.{ PowerIterationClusteringModel, PowerIterationClustering }
import org.apache.spark.graphx.{ Graph, EdgeDirection }
import scala.math.BigDecimal
import org.apache.commons.math3.util.MathUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import java.net.URI

object RDFGraphPowerIterationClustering {

  def apply(spark: SparkSession, graph: Graph[String, String], output: String, outevl: String, outputsim: String, k: Int = 2, maxIterations: Int = 50) = {

    /**
     *
     *
     * Jaccard similarity measure : selectYourSimilarity = 0
     * Batet similarity measure : selectYourSimilarity = 1
     * Rodríguez and Egenhofer similarity measure : selectYourSimilarity = 2
     * The Contrast model similarity : selectYourSimilarity = 3
     * The Ratio model similarity : selectYourSimilarity = 4
     */

    val selectYourSimilarity = 0

    def clusterRdd(): RDD[Iterable[String]] = {
      SimilaritesInPIC(selectYourSimilarity)
    }

    def SimilaritesInPIC(f: Int): RDD[Iterable[String]] = {
      /*
	 * Collect all the edges of the graph
	*/
      val edge = graph.edges.persist()
      val nodes = graph.vertices

      /*
	 * Collect neighbor IDs of all the vertices
	 */

      val neighbors = graph.collectNeighborIds(EdgeDirection.Either).persist()
      /*
	 * Collect distinct vertices of the graph
	 *
	 */

      val node = nodes.map(e =>(e._1))
      val lenghtOfNodes = node.count()
      /*
	 * Difference between two set of vertices, used in different similarity measures
	 */
      def difference(a: Array[VertexId], b: Array[VertexId]): Double = {

        if (a.length == 0) { return 0.0 }
        val differ = a.diff(b)
        if (differ.isEmpty) { return 0.0 }

        differ.size.toDouble
      }

      /*
	 * Intersection of two set of vertices, used in different similarity measures
	 */
      def intersection(a: Array[VertexId], b: Array[VertexId]): Double = {

        val rst = a.intersect(b)

        if (rst.isEmpty) { return 0.0 }
        rst.size.toDouble
      }

      /*
			 * Union of two set of vertices, used in different similarity measures
			 */
      def union(a: Array[VertexId], b: Array[VertexId]): Double = {

        val rst = a.union(b).distinct

        if (rst.isEmpty) { return 0.0 }

        rst.size.toDouble
      }
      /*
			 * computing algorithm based 2
			 */
      val LOG2 = math.log(2)
      val log2 = { x: Double => math.log(x) / LOG2 }

      /*
			 * computing similarities
			 */

      def selectSimilarity(a: Array[VertexId], b: Array[VertexId], c: Int): Double = {
        var s = 0.0
        if (c == 0) {

          /*
			 * Jaccard similarity measure
			 */

          val sim = intersection(a, b) / union(a, b).toDouble

          if (sim == 0.0) { s = (1 / lenghtOfNodes) }
          else { s = sim }

        }
        if (c == 1) {

          /*
			 * Rodríguez and Egenhofer similarity measure
			 */

          var g = 0.8

          val sim = (intersection(a, b) / ((g * difference(a, b)) + ((1 - g) * difference(b, a)) + intersection(a, b))).toDouble.abs
          if (sim == 0.0) { s = (1 / lenghtOfNodes) }
          else { s = sim }

        }
        if (c == 2) {
          /*
			 * The Ratio model similarity
			 */
          var alph = 0.5
          var beth = 0.5

          val sim = ((intersection(a, b)) / ((alph * difference(a, b)) + (beth * difference(b, a)) + intersection(a, b))).toDouble.abs
          if (sim == 0.0) { s = (1 / lenghtOfNodes) }
          else { s = sim }

        }

        if (c == 3) {
          /*
			 * Batet similarity measure
			 */

          val cal = 1 + ((difference(a, b) + difference(b, a)) / (difference(a, b) + difference(b, a) + intersection(a, b))).abs
          val sim = log2(cal.toDouble)
          if (sim == 0.0) { s = (1 / lenghtOfNodes) }
          else { s = sim }

        }
        s
      }
      /*
			 * Calculate similarities between different pair of vertices in the given graph
			 */

      val verticesOfEdge = edge.map(e => (e.srcId, e.dstId))
      val neighborsJoinToEdge = neighbors.keyBy(e => (e._1)).join(verticesOfEdge).map(e => e._2).keyBy(e => e._2).join(neighbors)
      val weightedGraph = neighborsJoinToEdge.map(e => { (e._2._1._1._1.toLong, e._1.toLong, selectSimilarity(e._2._1._1._2, e._2._2, f)) })

      val weightedGraphstring = weightedGraph.toString()
      val graphRDD = spark.sparkContext.parallelize(weightedGraphstring)
      graphRDD.saveAsTextFile(outputsim)

      def pic() = {
        val pic = new PowerIterationClustering()
          .setK(k)
          .setMaxIterations(maxIterations)
        pic
      }

      def model = pic.run(weightedGraph)

      /*
			 * Cluster the graph data into two classes using PowerIterationClustering
			 */
      def run() = model

      val vts = nodes.map(e =>(e._1.toLong , e._2))
     
     
     val modelAssignments = model.assignments
    
     val rddClusters= modelAssignments.map(f => {
      
       val id = f.id
       val fcluster = f.cluster
       (id,fcluster)})
     
      val findrdf = rddClusters.join(vts).groupBy(_._2._1).mapValues(_.map(_._2._2)).map(_._2)
      
      
      findrdf.saveAsTextFile(output)

      val arrayWeightedGraph = weightedGraph.collect()
      
      def findingSimilarity(a: Long, b: Long): Double = {
        var f3 = 0.0
        arrayWeightedGraph.map(f => {
          if ((f._1 == a && f._2 == b) || (f._1 == b && f._2 == a)) { f3 = f._3 }

        })
        f3
      }
      //println(s"RDF Cluster assignments: $m\n")

      /*
			 * Sillouhette Evaluation
			 */
      val clusters = modelAssignments.groupBy(_.cluster).mapValues(_.map(_.id)).map(_._2).collect()
      def avgA(c: Iterable[Long], d: Long): Double = {
        var sumA = 0.0
        val sizeC = c.size

        c.foreach(ck => {
          val scd = findingSimilarity(ck, d)
          sumA = sumA + scd
        })

        sumA / sizeC
      }

      def avgB(c: Iterable[Long], d: Long): Double = {
        var sumB = 0.0
        val sizeC = c.size
        if (sizeC == 0) return 0.0
        c.foreach(ck => {
          val scd = findingSimilarity(ck, d)

          sumB = sumB + scd
        })

        sumB / sizeC
      }
      def SI(a: Double, b: Double): Double = {
        var s = 0.0
        if (a > b) {
          s = 1 - (b / a)
        }
        if (a == b) {
          s = 0.0
        }
        if (a < b) {
          s = (a / b) - 1
        }
        s
      }

      def AiBi(m: Array[Iterable[Long]], n: RDD[VertexId]): List[Double] = {
        var Ai = 0.0
        var Bi = 0.0
        var bi = 0.0
        var avg: List[Double] = List()

        var sx: List[Double] = List()

        n.foreach(nk => {
          avg = List()
          m.foreach(mp => { 
            if (mp.exists(_.==(nk.toLong))) {
              Ai = avgA(mp, nk)
            } else {
              avg = avg.::(avgB(mp, nk))
            }
          })
          if (avg.length != 0) {
            bi = avg.max
          } else { bi = 0.0 }

          val v = SI(Ai, bi)
          sx = sx.::(v)

        })
        sx

      }
      val evaluate = AiBi(clusters, node)
      val averageSil = evaluate.sum / evaluate.size
      val evaluateString: List[String] = List(averageSil.toString())
      val evaluateStringRDD = spark.sparkContext.parallelize(evaluateString)

      evaluateStringRDD.saveAsTextFile(outevl)

      //println(s"averageSil: $averageSil\n")

      /*
			 * Save the model.
			 * @path - path for a model.
			 */
      // def save(path: String) = model.save(spark.sparkContext, path)

      /*
			 * Load the model.
			 * @path - the given model.
			 */
      // def load(path: String) = PowerIterationClusteringModel.load(spark.sparkContext, path)

      (findrdf)
    }
    val clrdd = clusterRdd()

  }
}
