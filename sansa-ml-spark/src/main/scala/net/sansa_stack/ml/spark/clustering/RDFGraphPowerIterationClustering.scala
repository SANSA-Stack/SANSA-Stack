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
import org.apache.spark.storage.StorageLevel
import org.apache.spark.graphx._
import scala.collection.mutable

object RDFGraphPowerIterationClustering {

  def apply(spark: SparkSession, graph: Graph[String, String], output: String, k: Int = 2, maxIterations: Int = 5) = {

    

    def clusterRdd(): RDD[(Int, String)] = {
      SimilaritesInPIC()
    }

    def SimilaritesInPIC(): RDD[(Int, String)] = {

      /*
	 * Collect all the edges of the graph
	*/
      val edge = graph.edges
      val nodes = graph.vertices

      /*
	 * Collect distinct vertices of the graph
	 *
	 */

      val node = nodes.map(e => (e._1))

      def Similarity(a: Int, b: Int): Double = {

        val union = (a + b).toDouble
        val jac = 1 / union
        jac

      }

      val verticesOfEdge = edge.map(e => (e.srcId, e.dstId))
      val reverseverticesOfEdge = verticesOfEdge.map(v => {
        val v1 = v._1
        val v2 = v._2
        (v2, v1)
      })
      val verticesOfEdgeWithoutDirection = verticesOfEdge.union(reverseverticesOfEdge).distinct()

      val initialSetE = mutable.ArrayBuffer.empty[(VertexId)]
      val addToSetE = (s: mutable.ArrayBuffer[(VertexId)], v: (VertexId)) => s += v
      val mergePartitionSetsE = (p1: mutable.ArrayBuffer[(VertexId)], p2: mutable.ArrayBuffer[(VertexId)]) => p1.++=(p2)
      val neighbors = verticesOfEdgeWithoutDirection.aggregateByKey(initialSetE)(addToSetE, mergePartitionSetsE)
      val lenghtNeighbors = neighbors.map(m => (m._1, m._2.length))
      val neighborsJoinToEdge = lenghtNeighbors.join(verticesOfEdgeWithoutDirection).keyBy(e => e._2._2).join(lenghtNeighbors)
      val weightedGraph = neighborsJoinToEdge.map(e => { (e._2._1._1.toLong, e._1.toLong, Similarity(e._2._1._2._1, e._2._2)) })

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

      val modelAssignments = model.assignments

      val rddClusters = modelAssignments.map(f => {

        val id = f.id
        val fcluster = f.cluster
        (id, fcluster)
      })

      def SI(a: (Double, Double)): Double = {
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

      val joinv = weightedGraph.keyBy(_._1).join(rddClusters).keyBy(_._2._1._2).join(rddClusters)

      val simnode = joinv.map(e => {
        var clid = 0
        val v1 = e._2._1._1
        val v2 = e._1
        val sim = e._2._1._2._1._3
        val clid1 = e._2._1._2._2
        val clid2 = e._2._2
        if (clid1 == clid2) { clid = 1 }
        if (clid1 != clid2) { clid = 2 }
        (v1, v2, sim, clid, clid1, clid2)
      })
      val clusterA = simnode.filter(_._4 == 1)
      val clusterC = simnode.filter(_._4 == 2)
      val v1clusterA = clusterA.map(m => {
        val m1 = m._1
        val m2 = m._3
        (m1, m2)
      })

      val initialList1 = mutable.ListBuffer.empty[(Double)]
      val addToList1 = (s: mutable.ListBuffer[(Double)], v: (Double)) => s += v
      val mergePartitionLists1 = (p1: mutable.ListBuffer[(Double)], p2: mutable.ListBuffer[(Double)]) => p1.++=(p2)
      val uniqueByKeyA = v1clusterA.aggregateByKey(initialList1)(addToList1, mergePartitionLists1)

      val avgClusterA = uniqueByKeyA.map(m => {
        val v = m._1
        val sumsim = m._2.sum
        val sizesim = m._2.length
        val avg = sumsim / sizesim
        (v, avg)
      })
      val v1clusterC = clusterC.map(m => {
        val m1 = m._1
        val m2 = m._3
        val m3 = m._6
        ((m1, m3), m2)
      })
      val initialList2 = mutable.ListBuffer.empty[(Double)]
      val addToList2 = (s: mutable.ListBuffer[(Double)], v: (Double)) => s += v
      val mergePartitionLists2 = (p1: mutable.ListBuffer[(Double)], p2: mutable.ListBuffer[(Double)]) => p1.++=(p2)
      val uniqueByKeyA2 = v1clusterC.aggregateByKey(initialList2)(addToList2, mergePartitionLists2)
      val avgclusterC = uniqueByKeyA2.map(m => {
        val m1 = m._1._1
        val m2 = m._1._2
        val simsum = m._2.sum
        val simsize = m._2.length
        val avg = simsum / simsize
        (m1, (m2, avg))
      })
      val initialList3 = mutable.ListBuffer.empty[(Int, Double)]
      val addToList3 = (s: mutable.ListBuffer[(Int, Double)], v: (Int, Double)) => s += v
      val mergePartitionLists3 = (p1: mutable.ListBuffer[(Int, Double)], p2: mutable.ListBuffer[(Int, Double)]) => p1.++=(p2)
      val uniqueByKeyA3 = avgclusterC.aggregateByKey(initialList3)(addToList3, mergePartitionLists3)
      val maxavgsimclusterC = uniqueByKeyA3.map(m => {
        val m1 = m._1
        val m2 = m._2.maxBy(_._2)._2
        (m1, m2)
      })
      val clusterac = avgClusterA.keyBy(_._1).join(maxavgsimclusterC)
      val computeSI = clusterac.map(m => {
        val v = m._1
        val simA = m._2._1._2
        val simC = m._2._2
        val si = SI(simA, simC)
        (v, si)
      })
      val remainva = avgClusterA.map(_._1).subtract(computeSI.map(_._1))
      val remainvc = maxavgsimclusterC.map(_._1).subtract(computeSI.map(_._1))
      val computeremainSIa = remainva.map(m => {
        val si = 1.0
        val v = m
        (v, si)
      })
      val computeremainSIc = remainvc.map(m => {
        val si = -1.0
        val v = m
        (v, si)
      })
      val computeallSI = computeSI.union(computeremainSIa).union(computeremainSIc)
      val rddSI = computeallSI.values
      val silouhette = rddSI.sum() / rddSI.count()

      println(s"averageSil: $silouhette\n")

      val vts = nodes.map(e => (e._1.toLong, e._2))
      val ClustersAsURL = rddClusters.join(vts).map(_._2)

      (ClustersAsURL)
    }
    val clrdd = clusterRdd()
    clrdd

  }
}
