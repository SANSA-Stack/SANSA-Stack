package net.sansa_stack.ml.spark.clustering

import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.{ Graph, EdgeDirection }
import scala.math.BigDecimal
import org.apache.commons.math3.util.MathUtils
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

object BorderFlow {

  def apply(spark: SparkSession, edgesInputPath: String) = {

    // Load the graph 
    val graph = GraphLoader.edgeListFile(spark.sparkContext, edgesInputPath)

    val edg = graph.edges.collect()

    val neighbor = graph.collectNeighborIds(EdgeDirection.Out)

    val neighborSort = neighbor.sortBy(_._2.length, false)

    val sort = neighborSort.map(f => {
      val x = f._1.toLong
      val nx = f._2.clone().toList
      (x, nx)
    })

    var X: List[Long] = sort.map(_._1).collect().toList
    println(s"Sort List of Vertices: $X\n")

    val nx = sort.map(_._2).collect()

    def fOmega(x: List[Long], v: Long): Double = {
      var numberFlow = 0
      val nv = neighbor.lookup(v).distinct.head.toSet
      val nvX = nv.intersect(X.toSet)
      val nvx = nvX.toList.diff(x).size
      val surat = x.map(f => {
        for (k <- 0 until x.length) yield {

          val xk = x(k)
          val bX = neighbor.lookup(xk).distinct.head.toSet
          val bxX = bX.intersect(X.toSet)
          //println(bX)
          val flow = if (bxX.toList.diff(x).size > 0 && bxX.toList.diff(x).contains(v)) { numberFlow = numberFlow.+(1) }

        }
      })

      (1 / (numberFlow.toDouble / nvx.toDouble))

    }

    def cluster(a: List[Long]): List[Long] = {
      var nj = 0.0
      var minF = 10000000.0
      var appends = a

      appends.map(f => {
        val nX = neighbor.lookup(f).distinct.head
        val nxX = nX.intersect(X)
        val nXa = nxX.diff(appends)
        if (nXa.size == 0) return appends
        nXa.map(x => {
          val f = fOmega(appends, x)

          if (f < minF) {
            minF = f
            nj = x

            appends = appends.::(nj.toLong)

            cluster(appends)
          } else return {

            (appends)
          }

        })
      })

      (appends)

    }

    def makeClusters(a: List[Long]): List[Long] = {

      val element = a.take(1)
      val clusters = cluster(element)

      X = X.diff(clusters)

      (clusters)

    }

    for (i <- 0 until X.length) {
      if (X.length > 0) {
        val finalClusters = makeClusters(X)
        println(s"Cluster assignments: $finalClusters\n")
        X = X.diff(finalClusters)

      }
    }
  }

}