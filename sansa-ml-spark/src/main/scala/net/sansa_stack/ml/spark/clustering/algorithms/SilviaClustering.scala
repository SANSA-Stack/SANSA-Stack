package net.sansa_stack.ml.spark.clustering.algorithms

import java.io._
import java.io.{ ByteArrayInputStream, FileNotFoundException, FileReader, IOException, StringWriter }
import java.lang.{ Long => JLong }
import java.net.URI

import scala.math.BigDecimal
import scala.reflect.runtime.universe._
import scala.util.control.Breaks._

import breeze.linalg.{ squaredDistance, DenseVector, Vector }
import net.sansa_stack.rdf.spark.model.graph._
import org.apache.jena.graph.{ Node, Triple }
import org.apache.jena.riot.{ Lang, RDFDataMgr }
import org.apache.log4j.{ Level, Logger }
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.graphx.{ EdgeDirection, Graph, GraphLoader }
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SilviaClustering {

  def apply(spark: SparkSession, graph: Graph[String, String], output: String, outputeval: String): Unit = {

    Logger.getRootLogger.setLevel(Level.WARN)

    /**
     * undirected graph : orient =0
     * directed graph : orient =1.
     *
     * Jaccard similarity measure : selectYourSimilarity = 0
     * Batet similarity measure : selectYourSimilarity = 1
     * Rodriguez and Egenhofer similarity measure : selectYourSimilarity = 2
     * The Contrast model similarity : selectYourSimilarity = 3
     * The Ratio model similarity : selectYourSimilarity = 4
     */
    val orient = 1
    val selectYourSimilarity = 0

    def clusterRdd(): RDD[List[String]] = {
      val a = graph.triplets
      graphXinBorderFlow(graph, orient, selectYourSimilarity)
    }

    /**
     * Computes different similarities function for a given graph @graph.
     */
    def graphXinBorderFlow(graph: Graph[String, String], e: Int, f: Int): RDD[List[String]] = {

      val edge = graph.edges.collect()
      val M = graph.edges.count().toDouble
      val vtx = graph.vertices.count().toDouble
      val collectVertices = graph.vertices.collect()
      var vArray = collectVertices.map(x => x._1.toLong).toList

      def neighbors(d: Int): VertexRDD[Array[VertexId]] = {
        var neighbor: VertexRDD[Array[VertexId]] = graph.collectNeighborIds(EdgeDirection.Either)

        if (d == 1) {
          neighbor = graph.collectNeighborIds(EdgeDirection.Out)
        }
        neighbor
      }
      val neighbor = graph.collectNeighborIds(EdgeDirection.Either)
      val neighbori = graph.collectNeighborIds(EdgeDirection.In)

      // Logarithm base 2
      val LOG2 = math.log(2)
      val log2 = { x: Double => math.log(x) / LOG2 }

      /**
       * Difference between two set of vertices, used in different similarity measures
       */
      def difference(a: Long, b: Long): Double = {
        val ansec = neighbor.lookup(a).distinct.head.toSet
        val ansec1 = neighbor.lookup(b).distinct.head.toSet
        if (ansec.isEmpty) { return 0.0 }
        val differ = ansec.diff(ansec1)
        if (differ.isEmpty) { return 0.0 }

        differ.size.toDouble
      }

      /**
       * Intersection of two set of vertices, used in different similarity measures
       */
      def intersection(a: Long, b: Long): Double = {
        val inters = neighbor.lookup(a).distinct.head.toList
        val inters1 = neighbor.lookup(b).distinct.head.toList
        // if (inters.isEmpty || inters1.isEmpty) { return 0.0 }
        val intersA = inters.::(a)

        val intersB = inters1.::(b)

        val rst = intersA.intersect(intersB).toArray
        if (rst.isEmpty) { return 0.0 }
        rst.size.toDouble
      }

      /**
       * Union of two set of vertices, used in different similarity measures
       */
      def union(a: Long, b: Long): Double = {
        val uni = neighbor.lookup(a).distinct.head.toList
        val uni1 = neighbor.lookup(b).distinct.head.toList
        val uniA = uni.::(a)
        val uniB = uni1.::(b)
        val rst = uniA.union(uniB).distinct.toArray
        if (rst.isEmpty) { return 0.0 }

        rst.size.toDouble
      }

      def selectSimilarity(a: Long, b: Long, c: Int): Double = {
        var s = 0.0
        if (c == 0) {

          /**
           * Jaccard similarity measure
           */

          val sim = intersection(a, b) / union(a, b).toDouble

          s = sim

        }

        if (c == 1) {

          /**
           * Rodríguez and Egenhofer similarity measure
           */

          var g = 0.8

          val sim = (intersection(a, b) / ((g * difference(a, b)) + ((1 - g) * difference(b, a)) + intersection(a, b))).toDouble.abs

          s = sim

        }
        if (c == 2) {

          /**
           * The Ratio model similarity
           */
          var alph = 0.5
          var beth = 0.5

          val sim = ((intersection(a, b)) / ((alph * difference(a, b)) + (beth * difference(b, a)) + intersection(a, b))).toDouble.abs

          s = sim

        }

        if (c == 3) {
          /**
           * Batet similarity measure
           */

          val cal = 1 + ((difference(a, b) + difference(b, a)) / (difference(a, b) + difference(b, a) + intersection(a, b))).abs
          val sim = log2(cal.toDouble)

          s = sim

        }
        s
      }

      val edgeArray = edge.map { x =>
        {
          var listedge: List[Long] = List()
          var listlistedge: List[List[Long]] = List()
          val x1 = x.srcId.toLong

          val x2 = x.dstId.toLong
          listedge = listedge.::(x2)
          listedge = listedge.::(x1)

          listlistedge = listlistedge.::(listedge)
          listlistedge
        }
      }

      val edgeList = edgeArray.toList

      var listSim: List[(Long, Long, Double)] = List()
      for (i <- 0 until vArray.length) {
        val vArrayi = vArray(i)
        for (j <- i + 1 until vArray.length) {
          val vArrayj = vArray(j)
          listSim = listSim.::((vArrayi, vArrayj, selectSimilarity(vArrayi, vArrayj, f)))

        }
      }

      def findingSimilarity(a: Long, b: Long): Double = {
        var f3 = 0.0

        listSim.map(f => {
          if ((f._1 == a && f._2 == b) || (f._1 == b && f._2 == a)) { f3 = f._3 }
        })
        f3

      }

      def jacEdges(a: List[Long], b: List[Long], c: Int): Double = {
        var sj = 0.0

        if (a(1) == b(1)) {

          sj = findingSimilarity(a(0), b(0))

        }

        if (a(0) == b(0) && c == 0) {

          sj = findingSimilarity(a(1), b(1))

        }
        if (a(1) == b(0) && c == 0) {

          sj = findingSimilarity(a(0), b(1))

        }
        if (a(0) == b(1) && c == 0) {

          sj = findingSimilarity(a(1), b(0))

        }

        sj
      }

      def clusterSimlilarity(a: List[List[Long]], b: List[List[Long]]): Double = {
        var maxsim = 0.0
        for (i <- 0 until a.length) {
          for (j <- 0 until b.length) {
            val ms = jacEdges(a(i), b(j), e)
            if (ms > maxsim) {
              maxsim = ms
            }
          }
        }

        maxsim
      }

      def density(a: List[List[List[Long]]]): Double = {
        var unionN: List[Long] = List()
        var clusterDensity = 0.0
        var den = 0.0
        for (i <- 0 until a.length) {
          val mc = a(i).length.toLong
          unionN = List()

          if (a(i).length == 0) return 0.0
          for (j <- 0 until a(i).length) {

            val u = a(i)(j)

            unionN = unionN.union(u).distinct

          }
          val nc = unionN.length.toLong

          if (mc != 1 && nc != 2) {

            val den1 = ((nc - 2) * (nc - 1)).toDouble
            val den2 = (mc - (nc - 1)).toDouble
            val den3 = (den2 / den1).toDouble
            val den4 = (mc * den3).toDouble
            den = den4

          } else { den = 0 }
          val denDouble = den.toDouble

          clusterDensity = clusterDensity + denDouble

        }
        return ((2 / M) * clusterDensity)
      }

      var dens = 0.0
      var flist: List[List[List[Long]]] = edgeList

      def findingsubset(c: List[List[List[Long]]]): List[List[List[Long]]] = {
        var C = c

        for (i <- 0 until c.length) {

          var counter = 0
          val cii = c(i)
          for (j <- i + 1 until c.length) {

            val cj = c(j)

            if ((cii.diff(cj)).size == 0 && (cj.diff(cii)).size == 0) {
              counter = counter + 1
              if (counter > 1) {
                val ci = cj
                C = C.diff(List(cj))

              }
            }
            if ((cii.diff(cj)).size == 0 && (cj.diff(cii)).size != 0) {
              C = C.diff(List(cii))

            }
            if ((cii.diff(cj)).size != 0 && (cj.diff(cii)).size == 0) {
              C = C.diff(List(cj))
            }
          }

        }
        C
      }

      def makerdf(a: List[Long]): List[String] = {
        var listuri: List[String] = List()
        val b: List[VertexId] = a
        for (i <- 0 until b.length) {
          graph.vertices.collect().map(v => {
            if (b(i) == v._1) listuri = listuri.::(v._2)
          })

        }
        listuri

      }

      var cluster1: List[List[List[Long]]] = List()
      def mergeMaxSim(a: List[List[List[Long]]]): List[List[List[Long]]] = {
        var append = a
        var maxS = 0.0
        var ei: List[List[List[Long]]] = List()
        var union: List[List[List[Long]]] = List()
        var ui: List[List[Long]] = List()
        var minMerge: List[List[List[Long]]] = List()

        if (append.length == 1) return flist
        for (i <- 0 until append.length) {

          val appendi = append(i)
          for (j <- i + 1 until append.length) {
            val appendj = append(j)
            if (appendi != appendj) {

              val mse = clusterSimlilarity(appendi, appendj)

              if (mse >= maxS && mse > 0) {
                if (mse == maxS) {
                  ei = ei.::(appendi)
                  ei = ei.::(appendj)
                  ui = ui.union(appendi).distinct
                  ui = ui.union(appendj).distinct

                }
                if (mse > maxS) {
                  maxS = mse
                  ei = List()
                  minMerge = List()
                  ui = List()
                  union = List()
                  ei = ei.::(appendi)
                  ei = ei.::(appendj)
                  ui = ui.union(appendi).distinct
                  ui = ui.union(appendj).distinct
                }

              }

            }
          }

          if (ui.length != 0) {

            minMerge = minMerge.::(ui).distinct

            ui = List()

          }

          union = union.union(ei).distinct
          ei = List()

        }

        if (maxS == 0.0) return flist

        append = append.diff(union)

        val distinctminMerge = findingsubset(minMerge)
        append = append.union(distinctminMerge).distinct

        append = findingsubset(append)

        val dst = density(append)

        if (dst >= dens) {
          dens = dst
          flist = append
        }

        mergeMaxSim(append)
      }

      cluster1 = mergeMaxSim(edgeList)

      var unionList: List[Long] = List()
      var unionList1: List[List[Long]] = List()
      var rdfString: List[List[String]] = List()

      for (i <- 0 until cluster1.length) {
        for (j <- 0 until cluster1(i).length) {
          if (cluster1(i).length >= 1) {
            unionList = unionList.union(cluster1(i)(j)).distinct

          }

        }
        if (unionList.length != 0) {
          unionList1 = unionList1.::(unionList)
          val rdf = makerdf(unionList)
          rdfString = rdfString.::(rdf)
        }

        unionList = List()

      }
      val rdfRDD = spark.sparkContext.parallelize(rdfString)

      def avgAsoft(c: List[Long], d: Long): Double = {
        var sumA = 0.0
        val sizeC = c.length

        for (k <- 0 until c.length) {
          val scd = findingSimilarity(c(k), d)
          sumA = sumA + scd
        }
        sumA / sizeC
      }

      def avgBsoft(c: List[Long], d: Long): Double = {
        var sumB = 0.0
        val sizeC = c.length
        if (sizeC == 0) return 0.0
        for (k <- 0 until sizeC) {
          val scd = findingSimilarity(c(k), d)

          sumB = sumB + scd
        }

        sumB / sizeC
      }
      def SIsoft(a: Double, b: Double): Double = {
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

      def AiBiSoft(m: List[List[Long]], n: List[Long]): List[Double] = {
        var Ai: List[Double] = List()
        var Bi = 0.0
        var bi = 0.0
        var avg: List[Double] = List()
        var ab: List[Double] = List()

        var sx: List[Double] = List()
        for (k <- 0 until n.length) {
          avg = List()
          Ai = List()
          for (p <- 0 until m.length) {

            if (m(p).contains(n(k))) {
              Ai = Ai.::(avgAsoft(m(p), n(k)))
            } else {
              avg = avg.::(avgBsoft(m(p), n(k)))
            }
          }
          if (avg.length != 0) {
            bi = avg.max
          } else { bi = 0.0 }
          val ai = Ai.sum / Ai.size

          val v = SIsoft(ai, bi)
          sx = sx.::(v)

        }
        sx
      }

      val evaluateSoft = AiBiSoft(unionList1, vArray)

      val avsoft = evaluateSoft.sum / evaluateSoft.size

      val evaluateString: List[String] = List(avsoft.toString())
      val evaluateStringRDD = spark.sparkContext.parallelize(evaluateString)

      evaluateStringRDD.saveAsTextFile(outputeval)

      val result = rdfRDD

      result
    }
    val cRdd = clusterRdd()
    val zipwithindex = cRdd.zipWithIndex().map(f => (f._2, f._1))
    zipwithindex.saveAsTextFile(output)
  }
}
