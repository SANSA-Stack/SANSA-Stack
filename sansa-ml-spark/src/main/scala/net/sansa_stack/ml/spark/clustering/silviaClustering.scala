package net.sansa_stack.ml.spark.clustering


import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.{ Graph, EdgeDirection }
import scala.math.BigDecimal
import org.apache.commons.math.util.MathUtils
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
import org.apache.jena.datatypes.{RDFDatatype, TypeMapper}
import org.apache.jena.graph.{Node => JenaNode, Triple => JenaTriple, _}
import org.apache.jena.riot.writer.NTriplesWriter
import org.apache.jena.riot.{Lang, RDFDataMgr}
import org.apache.jena.graph.{Node_ANY, Node_Blank, Node_Literal, Node_URI, Node => JenaNode, Triple => JenaTriple}
import org.apache.jena.vocabulary.RDF
import java.io.ByteArrayInputStream
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.jena.util._
import java.io.StringWriter
import java.io._

object silviaClustering {
  def main(args: Array[String]) {
    val sparkSession = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName(s"BorderFlow ")
      .getOrCreate()

    Logger.getRootLogger.setLevel(Level.WARN)
    
     // Load the graph 
    
   
     val RDFfile = sparkSession.sparkContext.textFile("/Users/tinaboroukhian/Desktop/tina 3.txt").map(line =>
       RDFDataMgr.createIteratorTriples(new ByteArrayInputStream(line.getBytes), Lang.NTRIPLES, null).next())
   
      val r = RDFfile.map(f => {
        val s =f.getSubject.getURI
        val p = f.getPredicate.getURI
        val o = f.getObject.getURI
       
        (s,p,o)
        })
       
        
       val v1 =r.map(f => f._1)
       val v2 = r.map(f => f._3)
       val indexedmap = ( v1.union(v2)).distinct().zipWithIndex()
       
      
      val vertices: RDD[(VertexId, String)] = indexedmap.map(x => (x._2, x._1))
      val _iriToId: RDD[(String, VertexId)] = indexedmap.map(x => (x._1, x._2))
        
      val tuples = r.keyBy(f => f._1).join(indexedmap).map({
        case (k, ((s, p, o), si)) => (o, (si, p))
      })
    
      val edgess: RDD[Edge[String]] = tuples.join(indexedmap).map({
        case (k, ((si, p), oi)) => Edge(si, oi, p)
      })
    
    
      val graph = org.apache.spark.graphx.Graph(vertices, edgess)

     
        
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
   val Hardening = 0
   var result: List[List[Long]] = List()

  def clusterRdd(): List[List[Long]] = {
    graphXinBorderFlow(orient,selectYourSimilarity,Hardening)
  }
   

  /*
	 * Computes different similarities function for a given graph @graph.
	 */
  def graphXinBorderFlow(e: Int, f:Int, g:Int): List[List[Long]] = {
     
    
    
     val edge = graph.edges.collect()
     val M = graph.edges.count().toDouble
     
     val collectVertices = graph.vertices.collect()
     val vArray = collectVertices.map(x => x._1.toLong).toList
     
      def neighbors(d: Int): VertexRDD[Array[VertexId]]={
      var neighbor: VertexRDD[Array[VertexId]] = graph.collectNeighborIds(EdgeDirection.Either)
      
       if(d==1){
        neighbor = graph.collectNeighborIds(EdgeDirection.Out)
      }
       neighbor
    }
    val neighbor = neighbors(e)
      
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
      val rst = uniA.union(uniB).toArray
      if (rst.isEmpty) { return 0.0 }

      rst.size.toDouble
    }
       
    	 def selectSimilarity(a: Long, b: Long, c: Int): Double ={
      var s = 0.0
      if(c ==0){

    
    /*
			 * Jaccard similarity measure
			 */
    
      
      val sim = intersection(a, b) / union(a, b).toDouble
      
      s = sim
      
      
     }
     
      if(c ==1){

     /*
			 * Rodríguez and Egenhofer similarity measure
			 */

    var g = 0.8
    
      
      val sim = (intersection(a, b) / ((g * difference(a, b)) + ((1 - g) * difference(b, a)) + intersection(a, b))).toDouble.abs
      
      s = sim
      
    }
     if(c ==2){ 
    /*
			 * The Ratio model similarity
			 */
    var alph = 0.5
    var beth = 0.5
    
      
      val sim = ((intersection(a, b)) / ((alph * difference(a, b)) + (beth * difference(b, a)) + intersection(a, b))).toDouble.abs
      
      s = sim
      
    }
    
    if(c ==3){ 
    /*
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
        var listedge : List[Long] = List()
        var listlistedge : List[List[Long]] = List()
        val x1 = x.srcId.toLong
        
        val x2 = x.dstId.toLong
        listedge = listedge.::(x2)
        listedge = listedge.::(x1)
       
        listlistedge = listlistedge.::(listedge)
        listlistedge
      }
    }
    val edgeList = edgeArray.toList
    
    var listSim: List[(Long,Long,Double)] = List()
    for(i <- 0 until vArray.length){
      val vArrayi = vArray(i)
      for(j <- i+1 until vArray.length){
         val vArrayj = vArray(j)
         listSim = listSim.::((vArrayi, vArrayj, selectSimilarity(vArrayi, vArrayj, f)))
      }
    }
    
    
    
    def findingSimilarity(a:Long , b: Long): Double={
      var f3 = 0.0
      
      listSim.map(f => {
        if((f._1 == a && f._2 == b) || (f._1 == b && f._2 == a)) {f3 =f._3}
      })
      f3
       
      
      
    }
    
    def jacEdges(a: List[Long], b: List[Long] ,c: Int): Double ={
      var sj = 0.0
      
     
          if(a(1) == b(1)){
            
            sj = findingSimilarity(a(0),b(0))
           
          }
          
           
           if(a(0) == b(0) && c==0){
            
            sj = findingSimilarity(a(1),b(1))
            
          }
           if(a(1) == b(0) && c==0){
            
            sj = findingSimilarity(a(0),b(1))
             
          }
         if(a(0) == b(1) && c==0){
            
            sj = findingSimilarity(a(1),b(0))
            
          }
          
         
      sj
    }
    
     def clusterSimlilarity(a: List[List[Long]], b: List[List[Long]]): Double ={
       var maxsim = 0.0
       for(i <- 0 until a.length){
        for(j <- 0 until b.length){
          val ms = jacEdges(a(i), b(j), e)
          if(ms > maxsim){
            maxsim = ms
          }
         }
      }
        
       maxsim
     }
    
    
    
    
    
    def density(a: List[List[List[Long]]]): Double={
      var unionN: List[Long] = List()
      var clusterDensity = 0.0
      var den = 0.0
      for(i <- 0 until a.length){
        val mc = a(i).length.toLong
        unionN = List()
        
        if(a(i).length == 0) return 0.0
        for(j <- 0 until a(i).length){
          
          val u = a(i)(j)
          
          unionN = unionN.union(u).distinct
        
        }
        val nc = unionN.length.toLong
        
        if(mc != 1 && nc != 2){
          
           val den1 =  ((nc - 2) * (nc - 1)).toDouble
           val den2 = (mc - (nc - 1)).toDouble
           val den3 = (den2 / den1).toDouble
           val den4 = (mc * den3).toDouble
           den = den4
       
       
           }
        else{den = 0}
        val denDouble = den.toDouble
      
        clusterDensity = clusterDensity + denDouble
        
        
      }
     return ((2/M)*clusterDensity)
    }
    
    
   var dens = 0.0
   var flist: List[List[List[Long]]] = edgeList
    
    
    def findingsubset(c: List[List[List[Long]]]): List[List[List[Long]]] ={
      var C = c
   
  
      for(i <- 0 until c.length){
    
       var counter = 0
       val cii = c(i)
       for(j <- i+1 until c.length){
       
         
         val cj = c(j)
        
         if((cii.diff(cj)).size == 0 && (cj.diff(cii)).size == 0){
           counter = counter + 1
           if(counter > 1){
           val ci = cj
           C = C.diff(List(cj))
        
         }
         }
         if((cii.diff(cj)).size == 0 && (cj.diff(cii)).size != 0){
           C = C.diff(List(cii))
        
         }
         if((cii.diff(cj)).size != 0 && (cj.diff(cii)).size == 0){
           C = C.diff(List(cj))
         }
         }
     
      
      }
    C
   }
   
    
    def mergeMaxSim(a: List[List[List[Long]]]): List[List[List[Long]]] ={
      var append = a
      var maxS = 0.0
       var ei: List[List[List[Long]]] = List() 
      var union: List[List[List[Long]]] = List()
      var ui : List[List[Long]] = List()
      var minMerge: List[List[List[Long]]] = List()
      
      if(append.length == 1) return flist
      for(i <- 0 until append.length){
        
        val appendi = append(i)
        for(j <- i+1 until append.length){
          val appendj = append(j)
          if(appendi != appendj){
          
         
            
          val mse = clusterSimlilarity(appendi,appendj)
          
           
          
          
           if(mse >= maxS && mse > 0){
             if(mse == maxS){
               ei = ei.::(appendi)
               ei = ei.::(appendj)
               ui = ui.union(appendi).distinct
               ui = ui.union(appendj).distinct
               
            }
            if(mse > maxS){
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
        
        if(ui.length != 0){
          
            minMerge = minMerge.::(ui).distinct
           
            
            ui = List()
          
        }
        
          union = union.union(ei).distinct
          ei = List()
       
      }
      
      if(maxS == 0.0) return flist
     
      
      append = append.diff(union)
      
      val distinctminMerge = findingsubset(minMerge)
      append = append.union(distinctminMerge).distinct
      
      
      append = findingsubset(append)
      
      val dst = density(append)
     
      
       
      if(dst >= dens){dens = dst
       flist = append }
      
      mergeMaxSim(append)
      }
   
   
    val cluster1 = mergeMaxSim(edgeList)
    //println(s" Cluster assignments: $cluster1\n")
    
    var unionList: List[Long] = List()
    var unionList1: List[List[Long]] = List()
   
     for(i <- 0 until cluster1.length){
       for(j <- 0 until cluster1(i).length){
         if(cluster1(i).length >= 1){
           unionList = unionList.union(cluster1(i)(j)).distinct
          
           
         }
        
       }
       if(unionList.length != 0){
         unionList1 = unionList1.::(unionList)}
       
       unionList = List()
       
     }
    
    def subset(c: List[List[Long]]): List[List[Long]] ={
      var C = c
   
  
      for(i <- 0 until c.length){
    
       var counter = 0
       val cii = c(i)
       for(j <- i+1 until c.length){
       
         
         val cj = c(j)
        
         if((cii.diff(cj)).size == 0 && (cj.diff(cii)).size == 0){
           counter = counter + 1
           if(counter > 1){
           val ci = cj
           C = C.diff(List(cj))
        
         }
         }
         if((cii.diff(cj)).size == 0 && (cj.diff(cii)).size != 0){
           C = C.diff(List(cii))
        
         }
         if((cii.diff(cj)).size != 0 && (cj.diff(cii)).size == 0){
           C = C.diff(List(cj))
         }
         }
     
      
      }
    C
   }
    val hardening = subset(unionList1)
   // println(s" Cluster assignments: $unionList1\n")
   // println(s" Cluster assignments1: $hardening\n")
    
    
    val hardening1 = subset(hardening).sortBy(_.length).reverse
 
 
    def takeAllElements(c:List[List[Long]], x:List[Long]) : List[List[Long]] = {
   
       var Cl:List[Long] = List()
       var cluster: List[List[Long]] = List()
       val y = (x.diff(Cl))
    
   
       for(i <- 0 until c.length){
     
         if((x.diff(Cl)).size != 0){
           Cl = Cl.union(c(i))
           cluster =cluster.::(c(i))
         }
       }
     cluster
   }
 
  val hardening2 = takeAllElements(hardening1,vArray)
  
 //println(s" Cluster assignments1: $hardening2\n")
 
  def omegaCluster( v:Long , c: List[Long]) : Double = {
         
         
         
     var omega = 0.0
    
  
         
     // * without similarity
     val nv = neighbor.lookup(v).distinct.head.toSet
        
     omega = ( (nv.intersect(c.toSet)).size.toDouble)
         
        
     omega   
    
   }
 
 def reassignment(c:List[List[Long]], x:List[Long]) : List[List[Long]] = {
   var C = c
   for(i <- 0 until x.length ){
     var f = 0.0
     var nj :List[Long] = List()
     for(j <- 0 until C.length){
       val om = omegaCluster(x(i),C(j))
       if(om > f){f = om
         nj = C(j)}
       
      }
      C = C.diff(List(nj))
      var di : List[List[Long]] = List()
      for(k <- 0 until C.length){
       val t = C(k).diff(List(x(i)))
       di = di.::(t)
      }
     
      C = di
      val cj = nj.::(x(i)).distinct
      C = C.::(cj)
     
   }
   C
 }
 
def nul(c:List[List[Long]]) : List[List[Long]] = {
  var C = c
  var newCluster :List[List[Long]] = List()
  for(k <- 0 until C.length){
    if(C(k).size != 0){
      newCluster = newCluster.::(C(k))
    }
  }
  newCluster
}

  val hardening3 = reassignment(hardening2,vArray)
  val hardening4 = nul(hardening3)
  
  /* 
  def avgA(c: List[Long], d:Long, l:Int) : Double = {
    var sumA = 0.0
    val sizeC = c.length
     
     
    for(k <- 0 until c.length){
     val scd = selectSimilarity(c(k), d, l)
     sumA = sumA + scd
    }
    sizeC/sumA
  }
  
  
  
  def avgB(c: List[Long], d:Long, l:Int) : Double = {
    var sumB = 0.0
    val sizeC = c.length
    for(k <- 0 until c.length){
     val scd = selectSimilarity(c(k), d, l)
     sumB = sumB + scd
    }
    sizeC/sumB
  }
  def SI(a: Double, b: Double): Double ={
    var s = 0.0
    if(a > b){
      s = 1 - (b/a)
    }
    if(a == b){
      s = 0.0
    }
    if(a < b){
     s = a/(b - 1)
    }
    s
  }
  
  def AiBi(m: List[List[Long]] , n: List[Long]) : List[Double] = {
    var Ai = 0.0
    var Bi = 0.0
    var avg: List[Double] = List()
    var ab: List[Double] = List()
   
    var sx: List[Double] = List()
    for(k <- 0 until n.length){
      for(p <- 0 until m.length){
       
        if(m(p).contains(n(k))){
          Ai = avgA(m(p), n(k), f)
        }
        else{
          avg = avg.::(avgB(m(p), n(k), f))
        }
      }
      val bi = avg.max
      val v =SI(Ai,bi)
      sx = sx.::(v)
       
      }
    sx
  }
  
  val evaluate = AiBi( hardening4,vArray)
  println(s"evaluate: $evaluate\n")
  * 
  */
  
  //println(s" Cluster assignments2: $unionList1\n")
  
  if(g == 0){
    result = hardening4}
  if(g == 1){
    result = unionList1
  }
  result
  }
  def makerdf(a: List[Long]) : List[String] ={
       var listuri : List[String] = List()
       val b:List[VertexId] = a
       for(i <- 0 until b.length ){
          vertices.collect().map(v => {
            if(b(i)==v._1) listuri = listuri.::(v._2)
          })
        
         
       }
       listuri
      
     }
 val file = "Silvia.txt"

  val pw = new PrintWriter(new File(file))
 val rdf = clusterRdd.map(x => makerdf(x))
 
  println(s"RDF Cluster assignments: $result\n")
  println(s"RDF Cluster assignments: $rdf\n")
  pw.println(s"RDF Cluster assignments: $rdf\n")
  pw.close()

  }
  
}