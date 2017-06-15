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

    //computing f(X,V) for Heuristics BorderFlow
      
      def fOmega(x: List[Long] , v:Long) : Double = {
         var numberFlow = 0
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
    
  }
    def heuristicsCluster(a: List[Long] , b: Double) : List[Long] = {
      var nj = 0.0
      var minF = b
      var appends = a
      
      
      
      if(minF == 0.0) return appends
      
     
      
      
      def neighborsOfList(c: List[Long]) : List[Long] = {
        
        var listN : List[Long] = List()
        
        for(k <- 0 until c.length) yield{
       val nX = neighborSort.lookup(c(k)).distinct.head
       
       val nxX = nX.intersect(X)
       val nXa = nxX.diff(c).toList
       listN = listN.union(nXa).distinct
       
        
        }
        
        (listN)
      }
      
      
      val neighborsOfX = neighborsOfList(appends) 
     
       if(neighborsOfX.size <= 0) return appends
       else{
        for(k <- 0 until neighborsOfX.length) yield{
         
         val f = fOmega(appends,neighborsOfX(k))
         
         
          if(f < minF) {minF = f
                       nj = neighborsOfX(k)
                       
                       
                       }
         
         
          }
       
    
     
      appends = appends.::(nj.toLong)
                    
                       heuristicsCluster(appends , minF)
     
   
       }
     
   }
      
      
   
   

    def makeClusters(a: List[Long]): List[Long] = {

      val element = a.take(1)
      val clusters = heuristicsCluster(element,100000000.0)

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
