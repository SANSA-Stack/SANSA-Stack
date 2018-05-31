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

  def apply(spark: SparkSession, graph: Graph[String, String], output: String, outevl: String, outputsim: String, k: Int = 2, maxIterations: Int = 5) = {

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

    def clusterRdd(): RDD[(Int, String)] = {
      SimilaritesInPIC(selectYourSimilarity)
    }

    def SimilaritesInPIC(f: Int): RDD[(Int, String)] = {
      /*
	 * Collect all the edges of the graph
	*/
      val edge = graph.edges
      val nodes = graph.vertices

      /*
	 * Collect neighbor IDs of all the vertices
	 */

      val neighbors = graph.collectNeighborIds(EdgeDirection.Either)
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
      
      def SI(a: (Double,Double)): Double = {
        var s = 0.0
        
        if (a._1 > a._2) {
          s = 1 - (a._2 / a._1)
        }
        if (a._1 == a._2) {
          s = 0.0
        }
        if (a._1 < a._2) {
          s = (a._1 / a._2) - 1
        }
        
        s
      }

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
     
      val findIterable = rddClusters.join(vts).map(_._2)
    
    findIterable.repartition(100).saveAsTextFile(output)

   /* val joinv1 = weightedGraph.keyBy(_._1).join(rddClusters).keyBy(_._2._1._2).join(rddClusters)
    val joinv2 = weightedGraph.keyBy(_._2).join(rddClusters).keyBy(_._2._1._1).join(rddClusters)
    val simnode = joinv1.map(e => {
      var clid = 0
      val v1 = e._2._1._1
      val v2 = e._1
      val sim = e._2._1._2._1._3
      val clid1 = e._2._1._2._2
      val clid2 = e._2._2
      if(clid1 == clid2){clid = 1}
      if(clid1 != clid2){clid = 2}
      (v1,v2,sim,clid,clid1,clid2)
    })
    
    val simnode1 = joinv2.map(e => {
      var clid = 0
      
      val v1 = e._1
      val v2 = e._2._1._1
      val sim = e._2._1._2._1._3
      val clid1 = e._2._2
      val clid2 = e._2._1._2._2
      
      if(clid1 == clid2){clid = 1}
      if(clid1 != clid2){clid = 2}
      (v1,v2,sim,clid,clid1,clid2)
    })
   
   
   
      
      val lenght1 = simnode.keyBy(_._6).join(findIterable).map(e => {
        val l = e._2._2.size
        val sim = e._2._1._3
        val avg = sim/l
        val v1 = e._2._1._1
        val v2 = e._2._1._2
        val aorb = e._2._1._4
        val i1 = e._2._1._5
        val i2 = e._2._1._6
        (v1,v2,avg,aorb,i1,i2)
        }).groupBy(_._1)
        val lenght2 = simnode1.keyBy(_._5).join(findIterable).map(e => {
        val l = e._2._2.size
        val sim = e._2._1._3
        val avg = sim/l
        val v1 = e._2._1._1
        val v2 = e._2._1._2
        val aorb = e._2._1._4
        val i1 = e._2._1._5
        val i2 = e._2._1._6
        (v2,v1,avg,aorb,i2,i1)
        }).groupBy(_._1)
      
   
     
      val allLinkstoNodeV = lenght1.join(lenght2).map(e => {
        val l1 = e._2._1.toArray
        val l2 = e._2._2.toArray
        val l1l2 = l1.union(l2).distinct.toIterable.partition(_._4 == 1)
        
        val par1 = l1l2._1.map(_._3)
        val par2 = l1l2._2.map(_._3)
        val par3 = l1l2._2.groupBy(g => {(g._5 , g._6)})
        
        val ps = par3.mapValues(_.map(_._3).sum)
        
        var bi = 0.0
        if(ps.size > 0) {bi = ps.maxBy(_._2)._2}
        val a = par1.sum
        val b = par2.sum
        val si = SI((a,bi))
        val v = e._1
        (si)
      }).sum()
       * 
      */
      val silouhette = 0.0
      
      val evaluateString: List[String] = List(silouhette.toString())
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

      (findIterable)
    }
    val clrdd = clusterRdd()

  }
}
