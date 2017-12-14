package net.sansa_stack.ml.spark.clustering

import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.{ Graph, EdgeDirection }
import scala.math.BigDecimal
import org.apache.spark.sql.SparkSession
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
import scala.util.control.Breaks._
import org.apache.jena.riot.{ Lang, RDFDataMgr }
import java.io.ByteArrayInputStream
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import java.io.StringWriter
import java.io._
import org.apache.jena.graph.{ Node, Triple }
import net.sansa_stack.rdf.spark.graph.LoadGraph
import net.sansa_stack.rdf.spark.io.NTripleReader
import java.net.URI

object SilviaClustering {

  def apply(spark: SparkSession, input: String, output: String, outputeval: String) = {

    Logger.getRootLogger.setLevel(Level.WARN)

    // Load the graph
    /*

    val RDFfile = spark.sparkContext.textFile(input).map(line =>
      RDFDataMgr.createIteratorTriples(new ByteArrayInputStream(line.getBytes), Lang.NTRIPLES, null).next())

    val r = RDFfile.map(f => {
      val s = f.getSubject.getURI
      val p = f.getPredicate.getURI
      val o = f.getObject.getURI

      (s, p, o)
    })

    val v1 = r.map(f => f._1)
    val v2 = r.map(f => f._3)
    val indexedmap = (v1.union(v2)).distinct().zipWithIndex()

    val vertices: RDD[(VertexId, String)] = indexedmap.map(x => (x._2, x._1))
    val _iriToId: RDD[(String, VertexId)] = indexedmap.map(x => (x._1, x._2))

    val tuples = r.keyBy(f => f._1).join(indexedmap).map({
      case (k, ((s, p, o), si)) => (o, (si, p))
    })

    val edgess: RDD[Edge[String]] = tuples.join(indexedmap).map({
      case (k, ((si, p), oi)) => Edge(si, oi, p)
    })

    val graph = org.apache.spark.graphx.Graph(vertices, edgess)
    */

    val triplesRDD = NTripleReader.load(spark, URI.create(input))

    val graph = LoadGraph.asString(triplesRDD)

    /*
	 * undirected graph : orient =0
	 * directed graph : orient =1.
	 *
	 * Jaccard similarity measure : selectYourSimilarity = 0
	 * Batet similarity measure : selectYourSimilarity = 1
	 * Rodríguez and Egenhofer similarity measure : selectYourSimilarity = 2
	 * The Contrast model similarity : selectYourSimilarity = 3
	 * The Ratio model similarity : selectYourSimilarity = 4
	 */
    val orient = 0
    val selectYourSimilarity = 1
    val Hardening = 1
    var result: List[List[Long]] = List()

    def clusterRdd(): List[List[Long]] = {
      graphXinBorderFlow(graph, orient, selectYourSimilarity, Hardening)
    }

    /*
	 * Computes different similarities function for a given graph @graph.
	 */
    def graphXinBorderFlow(graph: Graph[String, String], e: Int, f: Int, g: Int): List[List[Long]] = {

      val edge = graph.edges.collect()
      val M = graph.edges.count().toDouble
      val vtx = graph.vertices.count().toDouble

      val collectVertices = graph.vertices.collect()
      val vArray = collectVertices.map(x => x._1.toLong).toList

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

      /*
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

      /*
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

      /*
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

          /*
			 * Jaccard similarity measure
			 */

          val sim = intersection(a, b) / union(a, b).toDouble

          s = sim
          // s = BigDecimal(s).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble

        }

        if (c == 1) {

          /*
			 * Rodríguez and Egenhofer similarity measure
			 */

          var g = 0.8

          val sim = (intersection(a, b) / ((g * difference(a, b)) + ((1 - g) * difference(b, a)) + intersection(a, b))).toDouble.abs

          s = sim
          //s = BigDecimal(s).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble

        }
        if (c == 2) {
          /*
			 * The Ratio model similarity
			 */
          var alph = 0.5
          var beth = 0.5

          val sim = ((intersection(a, b)) / ((alph * difference(a, b)) + (beth * difference(b, a)) + intersection(a, b))).toDouble.abs

          s = sim
          // s = BigDecimal(s).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
        }

        if (c == 3) {
          /*
			 * Batet similarity measure
			 */

          val cal = 1 + ((difference(a, b) + difference(b, a)) / (difference(a, b) + difference(b, a) + intersection(a, b))).abs
          val sim = log2(cal.toDouble)

          s = sim
          //s = BigDecimal(s).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble

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

      val cluster1 = mergeMaxSim(edgeList)

      var unionList: List[Long] = List()
      var unionList1: List[List[Long]] = List()

      for (i <- 0 until cluster1.length) {
        for (j <- 0 until cluster1(i).length) {
          if (cluster1(i).length >= 1) {
            unionList = unionList.union(cluster1(i)(j)).distinct

          }

        }
        if (unionList.length != 0) {
          unionList1 = unionList1.::(unionList)
        }

        unionList = List()

      }

      val minm = 1 / vtx

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

      def subset(c: List[List[Long]]): List[List[Long]] = {
        var C = c
        var counter = 0

        for (i <- 0 until c.length) {

          counter = 0
          for (j <- i + 1 until c.length) {

            if (counter == 0) {

              if ((c(i).diff(c(j))).size == 0 && (c(j).diff(c(i))).size == 0) {

                C = C.diff(List(c(j)))

              }
              if ((c(i).diff(c(j))).size == 0 && (c(j).diff(c(i))).size != 0) {

                C = C.diff(List(c(i)))

                counter = 1
              }
              if ((c(i).diff(c(j))).size != 0 && (c(j).diff(c(i))).size == 0) {
                C = C.diff(List(c(j)))

              }
            }

          }
        }
        C
      }
      val hardening = subset(unionList1)

      val hardening1 = subset(hardening).sortBy(_.length).reverse

      def takeAllElements(c: List[List[Long]], x: List[Long]): List[List[Long]] = {

        var Cl: List[Long] = List()
        var cluster: List[List[Long]] = List()
        val y = (x.diff(Cl))

        for (i <- 0 until c.length) {

          if ((x.diff(Cl)).size != 0) {
            Cl = Cl.union(c(i))
            cluster = cluster.::(c(i))
          }
        }
        cluster
      }

      val hardening2 = takeAllElements(hardening1, vArray)

      def reassignment(c: List[List[Long]], x: List[Long]): List[List[Long]] = {
        var C = c

        for (i <- 0 until x.length) {
          var f = 0.0
          var ssim = 0.0
          var nj: List[Long] = List()
          for (j <- 0 until C.length) {
            ssim = 0.0
            val cj = C(j)
            if (cj.contains(x(i))) {
              val listneighbors = neighbor.lookup(x(i)).distinct.head.toList
              val listsim = listneighbors.intersect(cj)
              val xi = x(i)

              if (listsim.length > 1) {
                for (l <- 0 until listsim.length) {
                  for (m <- l + 1 until listsim.length) {
                    ssim = ssim + findingSimilarity(listsim(l), listsim(m))
                  }
                }
              }
              if (listsim.length == 1) { ssim = minm }
              if (listsim.length == 0) { ssim = 0.0 }

            }

            if (ssim > f) {
              f = ssim
              nj = cj

            }

          }
          C = C.diff(List(nj))
          var di: List[List[Long]] = List()
          for (k <- 0 until C.length) {
            val t = C(k).diff(List(x(i)))
            di = di.::(t)
          }

          C = di
          val cj = nj.::(x(i)).distinct
          C = C.::(cj)

        }

        C
      }

      def nul(c: List[List[Long]]): List[List[Long]] = {
        var C = c
        var newCluster: List[List[Long]] = List()
        for (k <- 0 until C.length) {
          if (C(k).size != 0) {
            newCluster = newCluster.::(C(k))
          }
        }
        newCluster
      }

      val hardening3 = reassignment(hardening2, vArray)
      val hardening4 = nul(hardening3)

      def avgA(c: List[Long], d: Long): Double = {
        var sumA = 0.0
        var size = 0.0

        val listneighbors = neighbor.lookup(d).distinct.head.toList

        val listsim = listneighbors.intersect(c)
        val e = listsim.length
        if (e == 0.0) return 0.0
        if (e == 1) { size = 1 }
        if (e > 1) {
          size = ((e * (e - 1)) / 2)
        }

        if (listsim.length > 1) {
          for (i <- 0 until listsim.length) {
            for (j <- i + 1 until listsim.length) {
              sumA = sumA + findingSimilarity(listsim(i), listsim(j))
            }
          }
        }
        if (listsim.length == 1) { sumA = minm }
        if (listsim.length == 0) { sumA = 0.0 }

        sumA / size
      }

      def avgB(c: List[Long], d: Long): Double = {
        var sumB = 0.0
        var size = 0.0
        val listneighbors = neighbor.lookup(d).distinct.head.toList

        val listsim = listneighbors.intersect(c)
        val e = listsim.length
        if (e == 0.0) return 0.0
        if (e == 1) { size = 1 }
        if (e > 1) {
          size = ((e * (e - 1)) / 2)
        }

        if (listsim.length > 1) {
          for (i <- 0 until listsim.length) {
            for (j <- i + 1 until listsim.length) {
              sumB = sumB + findingSimilarity(listsim(i), listsim(j))
            }
          }
        }
        if (listsim.length == 1) { sumB = minm }
        if (listsim.length == 0) { sumB = 0.0 }

        sumB / size
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

      def AiBi(m: List[List[Long]], n: List[Long]): List[Double] = {
        var Ai = 0.0
        var Bi = 0.0
        var bi = 0.0
        var avg: List[Double] = List()
        var ab: List[Double] = List()

        var sx: List[Double] = List()
        for (k <- 0 until n.length) {
          avg = List()
          for (p <- 0 until m.length) {

            if (m(p).contains(n(k))) {
              Ai = avgA(m(p), n(k))

            } else {
              avg = avg.::(avgB(m(p), n(k)))

            }
          }
          if (avg.length != 0) {
            bi = avg.max
          } else { bi = 0.0 }

          val v = SI(Ai, bi)
          sx = sx.::(v)

        }
        sx
      }

      val evaluate = AiBi(hardening4, vArray)

      val avgsil = evaluate.sum / evaluate.size

      val avsoft = evaluateSoft.sum / evaluateSoft.size

      //println(s" Cluster assignments2: $unionList1\n")

      //println(s"averagesoft: $avsoft\n")

      val evaluateString: List[String] = List(avsoft.toString())
      val evaluateStringRDD = spark.sparkContext.parallelize(evaluateString)

      evaluateStringRDD.saveAsTextFile(outputeval.mkString("\n"))

      if (g == 0) {
        result = hardening4
      }
      if (g == 1) {
        result = unionList1
      }
      result
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

    val rdf = clusterRdd.map(x => makerdf(x))

    //println(s"RDF Cluster assignments: $result\n")
    //println(s"RDF Cluster assignments: $rdf\n")
    val rdfRDD = spark.sparkContext.parallelize(rdf)

    rdfRDD.saveAsTextFile(output.mkString("\n"))

  }
}