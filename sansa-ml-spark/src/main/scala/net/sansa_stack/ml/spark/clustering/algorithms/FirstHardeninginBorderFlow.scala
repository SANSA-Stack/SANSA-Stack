package net.sansa_stack.ml.spark.clustering.algorithms

import java.io._
import java.io.{ ByteArrayInputStream, FileNotFoundException, FileReader, IOException, StringWriter }
import java.lang.{ Long => JLong }
import java.net.URI

import scala.math.BigDecimal
import scala.reflect.runtime.universe._
import scala.util.control.Breaks._

import breeze.linalg.{ squaredDistance, DenseVector, Vector }
import org.apache.jena.graph.Node
import org.apache.log4j.{ Level, Logger }
import org.apache.spark.graphx._
import org.apache.spark.graphx.{ EdgeDirection, Graph }
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession




object FirstHardeninginBorderFlow {

  def apply(spark: SparkSession, graph: Graph[Node, Node], output: String, outputeval: String): Unit = {

    /**
     *
     * Jaccard similarity measure : selectYourSimilarity = 0
     * Batet similarity measure : selectYourSimilarity = 1
     * Rodríguez and Egenhofer similarity measure : selectYourSimilarity = 2
     * The Contrast model similarity : selectYourSimilarity = 3
     * The Ratio model similarity : selectYourSimilarity = 4
     */

    val selectYourSimilarity = 0

    def clusterRdd(): List[List[Long]] = {
      graphXinBorderFlow(selectYourSimilarity)
    }

    /**
     * Computes different similarities function for a given graph @graph.
     */
    def graphXinBorderFlow(f: Int): List[List[Long]] = {

      val edge = graph.edges
      val vertex = graph.vertices.count().toDouble

      val neighbor = graph.collectNeighborIds(EdgeDirection.Either)

      val neighborSort = neighbor.sortBy(_._2.length, false)

      val sort = neighborSort.map(f => {
        val x = f._1
        x
      })
      println("hard")
      sort.foreach(println)
      var X = sort.collect()

      neighborSort.unpersist()
      sort.unpersist()
      val neighborcollect = neighbor.collect()
      val verticescollect = graph.vertices.collect()

      /**
       * finding neighbors for node a
       */

      def findneighbors(a: VertexId): Array[VertexId] = {
        var b: Array[VertexId] = Array()

        neighborcollect.map(f => {

          if (f._1 == a) {
            b = f._2

          }
        })
        b
      }

      /**
       * Computing logarithm based 2
       */
      val LOG2 = math.log(2)
      val log2 = { x: Double => math.log(x) / LOG2 }

      /**
       * Difference between two set of vertices, used in different similarity measures
       */

      def difference(a: Array[VertexId], b: Array[VertexId]): Double = {
        if (a.length == 0) { return 0.0 }
        val differ = a.diff(b)
        if (differ.isEmpty) { return 0.0 }
        differ.size.toDouble
      }

      /**
       * Intersection of two set of vertices, used in different similarity measures
       */
      def intersection(a: Array[VertexId], b: Array[VertexId]): Double = {
        if ((a.length == 0) || (b.length == 0)) { return 0.0 }
        val rst = a.intersect(b)
        if (rst.isEmpty) { return 0.0 }
        rst.size.toDouble
      }

      /**
       * Union of two set of vertices, used in different similarity measures
       */

      def union(a: Array[VertexId], b: Array[VertexId]): Double = {
        val rst = a.union(b)
        if (rst.isEmpty) { return 0.0 }
        rst.size.toDouble
      }

      /**
       * similarity measures
       */

      def selectSimilarity(a: Array[VertexId], b: Array[VertexId], c: Int): Double = {
        var s = 0.0
        if (c == 0) {

          /**
           * Jaccard similarity measure
           */

          val sim = intersection(a, b) / union(a, b).toDouble
          if (sim == 0.0) { s = (1 / vertex) }
          else { s = sim }

        }

        if (c == 1) {

          /**
           * Rodríguez and Egenhofer similarity measure
           */

          var g = 0.8

          val sim = (intersection(a, b) / ((g * difference(a, b)) + ((1 - g) * difference(b, a)) + intersection(a, b))).toDouble.abs
          if (sim == 0.0) { s = (1 / vertex) }
          else { s = sim }

        }
        if (c == 2) {
          /**
           * The Ratio model similarity
           */
          var alph = 0.5
          var beth = 0.5

          val sim = ((intersection(a, b)) / ((alph * difference(a, b)) + (beth * difference(b, a)) + intersection(a, b))).toDouble.abs
          if (sim == 0.0) { s = (1 / vertex) }
          else { s = sim }

        }

        if (c == 3) {
          /**
           * Batet similarity measure
           */

          val cal = 1 + ((difference(a, b) + difference(b, a)) / (difference(a, b) + difference(b, a) + intersection(a, b))).abs
          val sim = log2(cal.toDouble)
          if (sim == 0.0) { s = (1 / vertex) }
          else { s = sim }
        }
        s
      }

      val weightedGraph = edge.map { x =>
        {

          val x1 = x.srcId.toLong
          val x2 = x.dstId.toLong
          val x11 = x.srcId
          val x22 = x.dstId
          val nx1 = findneighbors(x11)
          val nx2 = findneighbors(x22)

          (x1, x2, selectSimilarity(nx1, nx2, f).abs)
        }
      }

      val arrayWeightedGraph = weightedGraph.collect()
      def findingSimilarity(a: Long, b: Long): Double = {
        var f3 = 0.0
        arrayWeightedGraph.map(f => {
          if ((f._1 == a && f._2 == b) || (f._1 == b && f._2 == a)) { f3 = f._3 }

        })
        f3
      }

      def sumsimilarity(a: Array[VertexId]): List[(Long, Double)] = {
        var sumsrdd = 0.0
        var Listsumrdd: (Long, Double) = (0, 0.0)
        var Listsum1rdd: List[(Long, Double)] = List()

        a.map(ak => {
          val nb = findneighbors(ak)

          nb.map(nbl => {
            val sisu = findingSimilarity(ak, nbl)
            sumsrdd = sumsrdd + sisu

          })
          Listsumrdd = (ak, sumsrdd)
          Listsum1rdd = Listsum1rdd.::(Listsumrdd)

        })

        (Listsum1rdd.sortBy(_._2))
      }

      val sortsim = sumsimilarity(X)

      // println(s"sortsim: $sortsim\n")

      var node = sortsim.map(f => {
        f._1
      }).reverse.toArray

      val nnode = node

      neighbor.unpersist()

      // computing F(X) for BorderFlow

      def fX(x: List[Long]): Double = {
        var jaccardX = 0.0
        var jaccardN = 0.0

        def listOfN(b: List[Long]): Array[Long] = {

          var listN: Array[Long] = Array()
          if (b.length > 0) {

            b.map(bk => {

              val nX = findneighbors(bk)
              val nxX = nX.intersect(node)
              val nXa = nxX.diff(b)
              listN = listN.union(nXa).distinct
            })

          }
          (listN)
        }

        def listOfB(b: List[Long]): Array[Long] = {

          var listN: List[Long] = List()
          b.map(bk => {
            val nX = findneighbors(bk)

            val nxX = nX.intersect(node)
            val nXa = nxX.diff(b)
            if (nXa.size.>(0)) { listN = listN.::(bk) }
          })

          (listN.toArray)
        }

        val n = listOfN(x)
        val b = listOfB(x)

        if (b.size == 0) return 0.0

        b.map(bi => {
          x.map(xj => {
            if (bi.!=(xj)) { jaccardX = jaccardX. + (findingSimilarity(bi, xj).abs) }
          })
        })

        b.map(bi => {
          n.map(nj => {
            jaccardN = jaccardN. + (findingSimilarity(bi, nj).abs)
          })
        })

        (jaccardX / jaccardN)

      }

      def omega(u: Long, x: List[Long]): Double = {

        def listOfN(b: List[Long]): Array[Long] = {

          var listN: Array[Long] = Array()

          b.map(bk => {
            val nX = findneighbors(bk)

            val nxX = nX.intersect(node)
            val nXa = nxX.diff(b)
            listN = listN.union(nXa).distinct
          })

          (listN)
        }
        val n = listOfN(x)
        var jaccardNU = 0.0
        n.map(ni => {
          if (ni.!=(u)) { jaccardNU = jaccardNU. + (findingSimilarity(u, ni).abs) }
        })

        jaccardNU

      }

      /**
       * Use Non-Heuristics(normal) method for producing clusters.
       */

      def nonHeuristicsCluster(a: List[Long], d: List[Long]): List[Long] = {
        var nj: List[Long] = List()
        var nj2: List[Long] = List()
        var maxF = 0.0
        var appends = a

        var maxfcf = 0.0
        var compare = d

        def neighborsOfList(c: List[Long]): Array[Long] = {

          var listN: Array[Long] = Array()

          c.map(ck => {
            val nX = findneighbors(ck)
            val nxX = nX.intersect(node)
            val nXa = nxX.diff(c)
            listN = listN.union(nXa).distinct

          })

          (listN)
        }

        var maxFf = fX(appends)

        val neighborsOfX = neighborsOfList(appends)

        if (neighborsOfX.size <= 0) return appends

        neighborsOfX.map(neighborsOfXk => {
          appends = appends.::(neighborsOfXk)
          val fx = fX(appends)

          if (fx == maxF) {
            maxF = fx
            nj = nj.::(neighborsOfXk)
            appends = appends.tail
          }
          if (fx > maxF) {
            maxF = fx
            nj = List(neighborsOfXk)
            appends = appends.tail
          }

          if (fx < maxF) { appends = appends.tail }
        })

        nj.map(njk => {
          val fCF = omega(njk, appends)

          if (fCF >= maxfcf) {
            if (fCF == maxfcf) {
              maxfcf = fCF
              nj2 = nj2.::(njk)
            }
            if (fCF > maxfcf) {
              maxfcf = fCF
              nj2 = List(njk)
            }

          }
        })

        appends = appends.union(nj2)
        if (appends == compare) return appends
        val nAppends = neighborsOfList(appends)
        if (nAppends.size == 0) return appends
        if (fX(appends) < maxFf) {
          appends = appends.diff(nj2)
          return appends
        }

        compare = appends

        nonHeuristicsCluster(appends, compare)

      }

      /**
       * Input for nonHeuristics nonHeuristicsCluster(element,List())  .
       */

      def makerdf(a: List[Long]): List[String] = {
        var listuri: List[String] = List()
        val b: List[VertexId] = a
        for (i <- 0 until b.length) {
          verticescollect.map(v => {
            if (b(i) == v._1) listuri = listuri.::(v._2.toString())
          })

        }
        listuri
      }

      def makeClusters(a: Long): List[Long] = {

        var clusters: List[Long] = List()

        clusters = nonHeuristicsCluster(List(a), List())

        node = node.diff(clusters)

        (clusters)

      }

      var bigList: List[List[Long]] = List()
      var rdfcluster: List[List[String]] = List()

      do {

        if (node.size != 0) {

          val finalClusters = makeClusters(node(0))

          bigList = bigList.::(finalClusters)

          rdfcluster = rdfcluster.::(makerdf(finalClusters))

          node = node.diff(finalClusters)
          val sortsim = sumsimilarity(node)

          node = sortsim.map(f => {
            f._1
          }).reverse.toArray

        }
      } while (node.size > 0)

      neighborSort.unpersist()
      // println(s"RDF Cluster assignments: $rdfcluster\n")
      val rdfRDD = spark.sparkContext.parallelize(rdfcluster)
      rdfRDD.saveAsTextFile(output)

      /**
       * Sillouhette Evaluation
       */

      def avgA(c: List[Long], d: Long): Double = {
        var sumA = 0.0
        val sizeC = c.length

        c.map(ck => {
          val scd = findingSimilarity(ck, d)
          sumA = sumA + scd
        })

        sumA / sizeC
      }

      def avgB(c: List[Long], d: Long): Double = {
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

      def AiBi(m: List[List[Long]], n: Array[Long]): List[Double] = {
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
      val evaluate = AiBi(bigList, nnode)

      val av = evaluate.sum / evaluate.size
      // println(s"average: $av\n")
      val evaluateString: List[String] = List(av.toString())
      val evaluateStringRDD = spark.sparkContext.parallelize(evaluateString)

      evaluateStringRDD.saveAsTextFile(outputeval)

      return bigList
    }

    val rdf = clusterRdd()
    // println(s"RDF Cluster assignments: $rdf\n")
  }
}
