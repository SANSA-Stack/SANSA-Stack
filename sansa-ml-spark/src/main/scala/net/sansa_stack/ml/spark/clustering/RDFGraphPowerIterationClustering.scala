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
import net.sansa_stack.rdf.spark.io.NTripleReader
import net.sansa_stack.rdf.spark.graph.LoadGraph
import java.net.URI

object RDFGraphPowerIterationClustering {

  def apply(spark: SparkSession, input: String, output: String, outputevl: String, outputsim: String, k: Int = 2, maxIterations: Int = 50) = {

    val triplesRDD = NTripleReader.load(spark, URI.create(input))

    val graph = LoadGraph.asString(triplesRDD)

    /*
	 *
	 *
	 * Jaccard similarity measure : selectYourSimilarity = 0
	 * Batet similarity measure : selectYourSimilarity = 1
	 * Rodríguez and Egenhofer similarity measure : selectYourSimilarity = 2
	 * The Contrast model similarity : selectYourSimilarity = 3
	 * The Ratio model similarity : selectYourSimilarity = 4
	 */

    val selectYourSimilarity = 0

    def clusterRdd(): List[Array[Long]] = {
      SimilaritesInPIC(selectYourSimilarity)
    }

    def SimilaritesInPIC(f: Int): List[Array[Long]] = {
      /*
	 * Collect all the edges of the graph
	*/
      val edge = graph.edges
      /*
	 * Collect neighbor IDs of all the vertices
	 */

      val neighbors = graph.collectNeighborIds(EdgeDirection.Either)
      /*
	 * Collect distinct vertices of the graph
	 *
	 */

      graph.unpersist()
      val neighborSort = neighbors.sortBy(_._2.length, false)

      val sort = neighborSort.map(f => {
        val x = f._1
        x
      })
      
      val collectvertices = graph.vertices.collect()
      val vList = sort.collect()
      sort.unpersist()
      val neighborcollect = neighbors.collect()
      
      def findneighbors(a: VertexId): Array[VertexId] ={
      var b:Array[VertexId] = Array()
      
    neighborcollect.map(f => {
     
      if(f._1 == a)
      {b = f._2
        
        }
    })
    b
    }

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

          s = intersection(a, b) / union(a, b).toDouble

        }
        if (c == 1) {
          /*
			 * Batet similarity measure
			 */

          val cal = 1 + ((difference(a, b) + difference(b, a)) / (difference(a, b) + difference(b, a) + intersection(a, b))).abs
          s = log2(cal.toDouble)

        }

        if (c == 2) {
          /*
			 * Rodríguez and Egenhofer similarity measure
			 */

          var g = 0.8

          s = (intersection(a, b) / ((g * difference(a, b)) + ((1 - g) * difference(b, a)) + intersection(a, b))).toDouble.abs

        }

        if (c == 4) {
          /*
			 * The Ratio model similarity
			 */
          var alph = 0.5
          var beth = 0.5

          s = ((intersection(a, b)) / ((alph * difference(a, b)) + (beth * difference(b, a)) + intersection(a, b))).toDouble.abs
          // s = BigDecimal(s).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble

        }
        s
      }
      /*
			 * Calculate similarities between different pair of vertices in the given graph
			 */

      val weightedGraph = edge.map { x =>
        {
          val x2 = x.dstId.toLong
          val x1 = x.srcId.toLong

          val x11 = x.srcId
          val x22 = x.dstId
          val nx1 = findneighbors(x11).:+(x1)
          val nx2 = findneighbors(x22).+:(x2)
	  val similarity = selectSimilarity(nx1, nx2, f)

          (x1, x2, similarity)
        }
      }

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
      val arrayWeightedGraph = weightedGraph.collect()
      weightedGraph.unpersist()

      val clusters = model.assignments.collect().groupBy(_.cluster).mapValues(_.map(_.id))
      val assignments = clusters.toList.sortBy { case (k, v) => v.length }
      val assignmentsStr = assignments
        .map {
          case (k, v) =>
            s"$k -> ${v.sorted.mkString("[", ",", "]")}"
        }.mkString(",")
      val sizesStr = assignments.map {
        _._2.size
      }.sorted.mkString("(", ",", ")")

      //println(s"Cluster assignments: $assignmentsStr\ncluster sizes: $sizesStr")
      def makerdf(a: Array[Long]): List[String] = {
        var listuri: List[String] = List()
        val b: Array[VertexId] = a
        for (i <- 0 until b.length) {
          collectvertices.map(v => {
            if (b(i) == v._1) listuri = listuri.::(v._2)
          })

        }
        listuri

      }
      val listCluster = assignments.map(f => f._2)
      val m = listCluster.map(f => makerdf(f))
      val rdfRDD = spark.sparkContext.parallelize(m)
      rdfRDD.saveAsTextFile(output)
	    
     
	    
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

      def avgA(c: Array[Long], d: Long): Double = {
        var sumA = 0.0
        val sizeC = c.length

        c.map(ck => {
          val scd = findingSimilarity(ck, d)
          sumA = sumA + scd
        })

        sumA / sizeC
      }

      def avgB(c: Array[Long], d: Long): Double = {
        var sumB = 0.0
        val sizeC = c.length
        if (sizeC == 0) return 0.0
        c.map(ck => {
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

      def AiBi(m: List[Array[Long]], n: Array[VertexId]): List[Double] = {
        var Ai = 0.0
        var Bi = 0.0
        var bi = 0.0
        var avg: List[Double] = List()

        var sx: List[Double] = List()

        n.map(nk => {
          avg = List()
          m.map(mp => {
            if (mp.contains(nk)) {
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
      val evaluate = AiBi(listCluster, vList)
      val averageSil = evaluate.sum / evaluate.size
      val evaluateString: List[String] = List(averageSil.toString())
      val evaluateStringRDD = spark.sparkContext.parallelize(evaluateString)

      evaluateStringRDD.saveAsTextFile(outputevl)
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

      (listCluster)
    }
    val clrdd = clusterRdd()

  }
}
