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
    
    
     //computing F(X) for BorderFlow 
  
  def fX(x: List[Long]) : Double = {
    
    def listOfN(b: List[Long]) : List[Long] = {
        
        var listN : List[Long] = List()
        if (b.length > 0){
        for(k <- 0 until b.length) yield{
       val nX = neighborSort.lookup(b(k)).distinct.head
       
       val nxX = nX.intersect(X)
       val nXa = nxX.diff(b).toList
       listN = listN.union(nXa).distinct
       
        
        }}
        (listN)
      }
    
    def listOfB(b: List[Long]) : List[Long] = {
        
        var listN : List[Long] = List()
        
        for(k <- 0 until b.length) yield{
       val nX = neighborSort.lookup(b(k)).distinct.head
       
       val nxX = nX.intersect(X)
       val nXa = nxX.diff(b).toList
       if(nXa.size > 0){
       listN = listN.::(b(k))
       
       }
        }
        (listN)
      }
    
    /* def listOfNb(b: List[Long]) : List[Long] = {
        
        var listN : List[Long] = List()
        
        for(k <- 0 until b.length) yield{
       val nX = neighborSort.lookup(b(k)).distinct.head
       
       val nxX = nX.intersect(X)
      
       listN = listN.++(nxX)
        }
        (listN)
      } */
     
     def makeomegaB(b: List[Long] , c:List[Long]) : Double = {
        
        var listN : List[Long] = List()
        
        for(k <- 0 until b.length) yield{
       val nX = neighborSort.lookup(b(k)).distinct.head
       
       val nxX = nX.intersect(X)
      
       listN = listN.++(((nxX.intersect(c).toList)))
        }
        listN.size.toDouble
      }
      val n = listOfN(x)
      val b = listOfB(x)
      
     // println(s"list of neighborX: $n\n")
     // println(s"list of bX: $b\n")
     if(listOfB(x).size == 0) return 0.0
 //  ( ( listOfNb(listOfB(x)).intersect(x)).size.toDouble / (listOfNb(listOfB(x)).intersect(listOfN(x))).size.toDouble)
    
     (makeomegaB(b,x) / makeomegaB(b,n))
  }
  
  
   
      def omega( u:Long , x: List[Long]) : Double = {
         
         
         def listOfN(b: List[Long]) : List[Long] = {
        
        var listN : List[Long] = List()
        
        for(k <- 0 until b.length) yield{
       val nX = neighborSort.lookup(b(k)).distinct.head
       
       val nxX = nX.intersect(X)
       val nXa = nxX.diff(b).toList
       listN = listN.union(nXa).distinct
       
        
        }
        (listN)
      }
         
         val nu = neighborSort.lookup(u).distinct.head.toSet
         val nuX = nu.intersect(X.toSet).toList
        ( (nuX.intersect(listOfN(x))).size.toDouble)
         
         
         
    
  }
  
def nonHeuristicsCluster(a: List[Long] ,d: List[Long]) : List[Long] = {
      var nj : List[Long] = List()
      var nj2 : List[Long] = List()
      var maxF = 0.0
      var appends = a
      
      var maxfcf = 0.0
      var compare = d
     
      
      
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
      
      var maxFf = fX(appends)
     // println(s"maxFf: $maxFf\n")
      
      val neighborsOfX = neighborsOfList(appends) 
     
       if(neighborsOfX.size <= 0) return appends
       
        for(k <- 0 until neighborsOfX.length) yield{
        // println(s"appends1: $appends\n")
         appends = appends.::(neighborsOfX(k))
         val f = fX(appends)
         // println(s"appends2: $appends\n")
         // println(s"f: $f\n")
         
          if(f >= maxF && f >= maxFf) {
                       if(f ==maxF){
                       maxF = f
                       nj = nj.::(neighborsOfX(k)) 
                       appends = appends.tail
                       }
                       else{
                          maxF = f
                       nj = List(neighborsOfX(k)) 
                       appends = appends.tail
                       }
                       }
         else appends = appends.tail
         
          }
       
       //println(s"nj: $nj\n")
     
      for(k <- 0 until nj.length) yield{
        val fCF = omega(nj(k) , appends)
       // println(s"fomega: $fCF\n")
        if(fCF >= maxfcf) {
          if(fCF == maxfcf){
          maxfcf = fCF
          nj2 = nj2.::(nj(k)) }
          else{
            maxfcf = fCF
          nj2 = List(nj(k))
          }
                      
                       
                       }
        
      }
     // println(s"nj2: $nj2\n")
      
      appends = appends.union(nj2)
       if(appends == compare) return appends
      if(fX(appends) < maxFf) {return appends.diff(nj2)}
      //println(s"appends3: $appends\n")
      //if(maxfcf == 0.0) return appends
     
      compare = appends
                    
       nonHeuristicsCluster(appends , compare)
     
   
       
     
   }
      
   
   
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
      
    
    def heuristicsCluster(a: List[Long] ) : List[Long] = {
      var nj = 0.0
      var minF = 100000000000000.0
      var appends = a
      
      
      
     // if(minF == 0.0) return appends
      
     
      
      
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
      
     // println(s"appends1: $appends\n")
       var maxFf = fX(appends)
     //  println(s"FX: $maxFf\n")
      
      val neighborsOfX = neighborsOfList(appends) 
     
       if(neighborsOfX.size <= 0) return appends
       else{
        for(k <- 0 until neighborsOfX.length) yield{
         
         val f = fOmega(appends,neighborsOfX(k))
        // println(s"f: $f\n")
         
          if(f < minF) {minF = f
                       nj = neighborsOfX(k)
                       
                       
                       }
         
         
          }
      // println(s"nj: $nj\n")
      // println(s"minF: $minF\n")
    
     
      appends = appends.::(nj.toLong)
     // println(s"appends2: $appends\n")
      if(fX(appends) < maxFf) return appends.tail
                    
       heuristicsCluster(appends)
     
   
       }
     
   }
      
      
      
   
    //input nonHeuristicsCluster(element,List())   for nonHeuristics
    // input heuristicsCluster(element)   for heuristics

    def makeClusters(a: List[Long]): List[Long] = {

      val element = a.take(1)
      val clusters = heuristicsCluster(element)

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
