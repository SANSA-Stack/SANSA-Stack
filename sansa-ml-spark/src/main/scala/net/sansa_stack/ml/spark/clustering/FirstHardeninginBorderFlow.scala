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
import breeze.linalg.{ squaredDistance, DenseVector, Vector }
import scala.util.control.Breaks._
import java.io.ByteArrayInputStream
import org.apache.spark.rdd.PairRDDFunctions
import java.io.StringWriter
import java.io._
import net.sansa_stack.rdf.spark.io.NTripleReader
import java.net.URI
import net.sansa_stack.rdf.spark.graph.LoadGraph
import org.apache.spark.graphx._

object FirstHardeninginBorderFlow {

  def apply(spark: SparkSession, input: String) = {

    /*
	 * Load the RDF file and convert it to a graph.
	 */

    val triplesRDD = NTripleReader.load(spark, URI.create(input))

    val inputPath = URI.create(input)
    val output = inputPath.getPath.substring(0, inputPath.getPath.lastIndexOf("/")) + "/output"
    val outputeval = output + "/outputeval"

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
    val selectYourSimilarity = 0

    def clusterRdd(): List[List[Long]] = {
      graphXinBorderFlow(orient, selectYourSimilarity)
    }

    /*
	 * Computes different similarities function for a given graph @graph.
	 */
    def graphXinBorderFlow(e: Int, f: Int): List[List[Long]] = {

      val edge = graph.edges.collect()
      val vertex = graph.vertices.count().toDouble

      def neighbors(d: Int): VertexRDD[Array[VertexId]] = {
        var neighbor: VertexRDD[Array[VertexId]] = graph.collectNeighborIds(EdgeDirection.Either)

        if (d == 1) {
          neighbor = graph.collectNeighborIds(EdgeDirection.Out)
        }
        neighbor
      }
      val neighbor = neighbors(e)

      val neighborSort = neighbor.sortBy(_._2.length, false)

      val sort = neighborSort.map(f => {
        val x = f._1.toLong
        val nx = f._2.clone().toList
        (x, nx)
      })

      var X: List[Long] = sort.map(_._1).collect().toList

      val xsort = X
      val nx = sort.map(_._2).collect()

      /*
	 * Computing logarithm based 2
	 */
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
        val inters = neighbor.lookup(a).distinct.head.toSet
        val inters1 = neighbor.lookup(b).distinct.head.toSet
        if (inters.isEmpty || inters1.isEmpty) { return 0.0 }
        val rst = inters.intersect(inters1).toArray
        if (rst.isEmpty) { return 0.0 }
        rst.size.toDouble
      }

      /*
			 * Union of two set of vertices, used in different similarity measures
			 */

      def union(a: Long, b: Long): Double = {
        val inters = neighbor.lookup(a).distinct.head.toSet
        val inters1 = neighbor.lookup(b).distinct.head.toSet
        val rst = inters.union(inters1).toArray
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
          if (sim == 0.0) { s = (1 / vertex) }
          else { s = sim }

        }

        if (c == 1) {

          /*
			 * Rodríguez and Egenhofer similarity measure
			 */

          var g = 0.8

          val sim = (intersection(a, b) / ((g * difference(a, b)) + ((1 - g) * difference(b, a)) + intersection(a, b))).toDouble.abs
          if (sim == 0.0) { s = (1 / vertex) }
          else { s = sim }

        }
        if (c == 2) {
          /*
			 * The Ratio model similarity
			 */
          var alph = 0.5
          var beth = 0.5

          val sim = ((intersection(a, b)) / ((alph * difference(a, b)) + (beth * difference(b, a)) + intersection(a, b))).toDouble.abs
          if (sim == 0.0) { s = (1 / vertex) }
          else { s = sim }

        }

        if (c == 3) {
          /*
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

          (x1, x2, selectSimilarity(x1, x2, f).abs)
        }
      }

      def findingSimilarity(a: Long, b: Long): Double = {
        var f3 = 0.0
        weightedGraph.map(f => {
          if ((f._1 == a && f._2 == b) || (f._1 == b && f._2 == a)) { f3 = f._3 }

        })
        f3
      }

      def sumsimilarity(a: List[Long]): List[(Double, Double)] = {
        var sums = 0.0
        var Listsum: (Double, Double) = (0.0, 0.0)
        var Listsum1: List[(Double, Double)] = List()

        for (k <- 0 until a.length) {
          val nb = neighbor.lookup(a(k)).distinct.head.toList
          for (l <- 0 until nb.length) {
            val sisu = findingSimilarity(a(k), nb(l))
            sums = sums + sisu
          }
          Listsum = (a(k).toDouble, sums)
          Listsum1 = Listsum1.::(Listsum)
        }

        Listsum1
      }
      val susim = sumsimilarity(X)

      val sortsim = susim.sortBy(_._2)

      var node = sortsim.map(f => {
        f._1.toLong
      }).reverse

      val nnode = node

      //computing f(X,V) for Heuristics BorderFlow

      def fOmega(x: List[Long], v: Long): Double = {
        var numberFlow = 0

        def listOfB(b: List[Long]): List[Long] = {

          var listN: List[Long] = List()

          for (k <- 0 until b.length) yield {
            val nX = neighborSort.lookup(b(k)).distinct.head

            val nxX = nX.intersect(node)
            val nXa = nxX.diff(b).toList
            if (nXa.size > 0) {
              listN = listN.::(b(k))

            }
          }
          (listN)
        }

        val b = listOfB(x)
        val VX = node.diff(x)
        var jaccardBV = 0.0
        if (b.size == 0) return 0.0
        for (i <- 0 until b.length) yield {

          jaccardBV = jaccardBV.+(findingSimilarity(b(i), v).abs)

        }

        var jaccardVXV = 0.0

        for (i <- 0 until VX.length) yield {
          if (VX(i) != v) {

            jaccardVXV = jaccardVXV.+(findingSimilarity(VX(i), v).abs)

          }

        }

        (jaccardVXV / jaccardBV)
        /*
         *  without similarity
         val nv = neighborSort.lookup(v).distinct.head.toSet
         val nvX = nv.intersect(X.toSet)
         val nvx = nvX.toList.diff(x).size


          for(k <- 0 until x.length) yield{
            if(x.length>0){

           val xk = x(k)
           val bX = neighborSort.lookup(xk).distinct.head.toSet
           val bxX = bX.intersect(X.toSet)

           if(bxX.toList.diff(x).size > 0 && bxX.toList.diff(x).contains(v)) {
             numberFlow = numberFlow + 1
             }

            }

         }

        ( 1/(numberFlow.toDouble/ nvx.toDouble))
        *
        */

      }

      //computing F(X) for BorderFlow

      def fX(x: List[Long]): Double = {
        var jaccardX = 0.0
        var jaccardN = 0.0

        def listOfN(b: List[Long]): List[Long] = {

          var listN: List[Long] = List()
          if (b.length > 0) {
            for (k <- 0 until b.length) yield {
              val nX = neighborSort.lookup(b(k)).distinct.head

              val nxX = nX.intersect(node)
              val nXa = nxX.diff(b).toList
              listN = listN.union(nXa).distinct

            }
          }
          (listN)
        }

        def listOfB(b: List[Long]): List[Long] = {

          var listN: List[Long] = List()

          for (k <- 0 until b.length) yield {
            val nX = neighborSort.lookup(b(k)).distinct.head

            val nxX = nX.intersect(node)
            val nXa = nxX.diff(b).toList
            if (nXa.size > 0) {
              listN = listN.::(b(k))

            }
          }
          (listN)
        }

        val n = listOfN(x)
        val b = listOfB(x)

        if (b.size == 0) return 0.0

        def makeomegaB(b: List[Long], c: List[Long]): Double = {

          var listN: List[Long] = List()

          for (k <- 0 until b.length) yield {
            val nX = neighborSort.lookup(b(k)).distinct.head

            val nxX = nX.intersect(node)

            listN = listN.++(((nxX.intersect(c).toList)))
          }
          listN.size.toDouble
        }

        for (i <- 0 until b.length) yield {
          for (j <- 0 until x.length) yield {
            if (b(i) != x(j)) {
              jaccardX = jaccardX.+(findingSimilarity(b(i), x(j)).abs)

            }
          }
        }

        for (i <- 0 until b.length) yield {
          for (j <- 0 until n.length) yield {

            jaccardN = jaccardN.+(findingSimilarity(b(i), n(j)).abs)

          }
        }

        (jaccardX / jaccardN)

        //  ( ( listOfNb(listOfB(x)).intersect(x)).size.toDouble / (listOfNb(listOfB(x)).intersect(listOfN(x))).size.toDouble)

        //(makeomegaB(b,x) / makeomegaB(b,n))
      }

      def omega(u: Long, x: List[Long]): Double = {

        def listOfN(b: List[Long]): List[Long] = {

          var listN: List[Long] = List()

          for (k <- 0 until b.length) yield {
            val nX = neighborSort.lookup(b(k)).distinct.head

            val nxX = nX.intersect(node)
            val nXa = nxX.diff(b).toList
            listN = listN.union(nXa).distinct

          }
          (listN)
        }
        val n = listOfN(x)
        var jaccardNU = 0.0

        for (i <- 0 until n.length) yield {
          if (n(i) != u) {

            jaccardNU = jaccardNU.+(findingSimilarity(u, n(i)).abs)

          }

        }
        /*
         * without similarity
         val nu = neighborSort.lookup(u).distinct.head.toSet
         val nuX = nu.intersect(X.toSet).toList
        ( (nuX.intersect(listOfN(x))).size.toDouble)

         */
        jaccardNU

      }

      /*
	 * Use Heuristics method for producing clusters.
	 */

      def heuristicsCluster(a: List[Long]): List[Long] = {
        var nj = 0.0
        var minF = 100000000000000.0
        var appends = a

        def neighborsOfList(c: List[Long]): List[Long] = {

          var listN: List[Long] = List()

          for (k <- 0 until c.length) yield {
            val nX = neighborSort.lookup(c(k)).distinct.head

            val nxX = nX.intersect(node)
            val nXa = nxX.diff(c).toList
            listN = listN.union(nXa).distinct

          }

          (listN)
        }

        var maxFf = fX(appends)

        val neighborsOfX = neighborsOfList(appends)

        if (neighborsOfX.size <= 0) return appends
        else {
          for (k <- 0 until neighborsOfX.length) yield {

            val f = fOmega(appends, neighborsOfX(k))

            if (f < minF) {
              minF = f
              nj = neighborsOfX(k)

            }

          }

          appends = appends.::(nj.toLong)

          if (neighborsOfList(appends).size == 0) return appends
          if (fX(appends) < maxFf) {
            appends = appends.tail
            return appends
          }

          heuristicsCluster(appends)

        }

      }

      /*
	 * Use Non-Heuristics(normal) method for producing clusters.
	 */

      def nonHeuristicsCluster(a: List[Long], d: List[Long]): List[Long] = {
        var nj: List[Long] = List()
        var nj2: List[Long] = List()
        var maxF = 0.0
        var appends = a

        var maxfcf = 0.0
        var compare = d

        def neighborsOfList(c: List[Long]): List[Long] = {

          var listN: List[Long] = List()

          for (k <- 0 until c.length) yield {
            val nX = neighborSort.lookup(c(k)).distinct.head

            val nxX = nX.intersect(node)
            val nXa = nxX.diff(c).toList
            listN = listN.union(nXa).distinct

          }

          (listN)
        }

        var maxFf = fX(appends)

        val neighborsOfX = neighborsOfList(appends)

        if (neighborsOfX.size <= 0) return appends

        for (k <- 0 until neighborsOfX.length) yield {

          appends = appends.::(neighborsOfX(k))
          val fx = fX(appends)

          if (fx == maxF) {
            maxF = fx
            nj = nj.::(neighborsOfX(k))
            appends = appends.tail
          }
          if (fx > maxF) {
            maxF = fx
            nj = List(neighborsOfX(k))
            appends = appends.tail
          }

          if (fx < maxF) { appends = appends.tail }

        }

        for (k <- 0 until nj.length) yield {
          val fCF = omega(nj(k), appends)

          if (fCF >= maxfcf) {
            if (fCF == maxfcf) {
              maxfcf = fCF
              nj2 = nj2.::(nj(k))
            }
            if (fCF > maxfcf) {
              maxfcf = fCF
              nj2 = List(nj(k))
            }

          }

        }

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

      /*
	 * Input for heuristics heuristicsCluster(element)    .
	 * Input for nonHeuristics nonHeuristicsCluster(element,List())  .
	 */

      def makeClusters(a: Long): List[Long] = {

        var clusters: List[Long] = List()

        clusters = nonHeuristicsCluster(List(a), List())
        //if(b == 1){
        // clusters = heuristicsCluster(List(a))}

        node = node.diff(clusters)

        (clusters)

      }

      var bigList: List[List[Long]] = List()

      do {

        if (node.size != 0) {

          val finalClusters = makeClusters(node(0))

          bigList = bigList.::(finalClusters)

          node = node.diff(finalClusters)
          val susim = sumsimilarity(node)

          val sortsim = susim.sortBy(_._2)

          node = sortsim.map(f => {
            f._1.toLong
          }).reverse

        }
      } while (node.size > 0)

      /*
			 * Sillouhette Evaluation
			 */

      def avgA(c: List[Long], d: Long): Double = {
        var sumA = 0.0
        val sizeC = c.length

        for (k <- 0 until c.length) {
          val scd = findingSimilarity(c(k), d)
          sumA = sumA + scd
        }
        sumA / sizeC
      }

      def avgB(c: List[Long], d: Long): Double = {
        var sumB = 0.0
        val sizeC = c.length
        if (sizeC == 0) return 0.0
        for (k <- 0 until c.length) {
          val scd = findingSimilarity(c(k), d)

          sumB = sumB + scd
        }

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
      val evaluate = AiBi(bigList, nnode)

      val av = evaluate.sum / evaluate.size
      println(s"average: $av\n")
      val evaluateString: List[String] = List(av.toString())
      val evaluateStringRDD = spark.sparkContext.parallelize(evaluateString)

      evaluateStringRDD.saveAsTextFile(outputeval.mkString("\n"))

      return bigList
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
    println(s"RDF Cluster assignments: $rdf\n")
    val rdfRDD = spark.sparkContext.parallelize(rdf)
    rdfRDD.saveAsTextFile(output.mkString("\n"))

  }

}