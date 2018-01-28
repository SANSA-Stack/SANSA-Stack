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

 
 
 
     object poweriterationclustering {
    
  
  case class Params(
      
       input: String = null,
       k: Int = 2,
       
       maxIterations: Int = 50
    ) extends AbstractParams[Params]{
      
  }
  abstract class AbstractParams[T: TypeTag] {
 
   private def tag: TypeTag[T] = typeTag[T]
 
   
   override def toString: String = {
     val tpe = tag.tpe
     val allAccessors = tpe.declarations.collect {
      case m: MethodSymbol if m.isCaseAccessor => m
     }
     val mirror = runtimeMirror(getClass.getClassLoader)
     val instanceMirror = mirror.reflect(this)
     allAccessors.map { f =>
       val paramName = f.name.toString
       val fieldMirror = instanceMirror.reflectField(f)
       val paramValue = fieldMirror.get
       s"  $paramName:\t$paramValue"
     }.mkString("{\n", ",\n", "\n}")
  }
 }
   def main(args : Array[String]) {

   
   
     val defaultParams = Params()
 
     val parser = new OptionParser[Params]("PowerIterationClusteringExample") {
      head("PowerIterationClusteringExample: an example PIC app using concentric circles.")
      
       opt[Int]('k', "k")
         .text(s"number of circles (/clusters), default: ${defaultParams.k}")
         .action((x, c) => c.copy(k = x))
      
       opt[Int]("maxIterations")
         .text(s"number of iterations, default: ${defaultParams.maxIterations}")
         .action((x, c) => c.copy(maxIterations = x))
       
     }
 
     parser.parse(args, defaultParams).map { params =>
       run(params)
     }.getOrElse {
       sys.exit(1)
     }
   }
 
   def run(params: Params) {

    val sparkSession = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName(s"PowerIterationClustering with $params")
      .getOrCreate()

    Logger.getRootLogger.setLevel(Level.WARN)
 
    val Input ="/Users/tinaboroukhian/Desktop/Toothbrush_Toothpaste.txt" //hdfs://172.18.160.17:54310/TinaBoroukhian/input/HairStylist_TaxiDriver.txt" 
    val output="/TinaBoroukhian/output"
    val outputevl="/TinaBoroukhian/output"
 
 
 
 // Load the graph 
 val RDFfile = sparkSession.sparkContext.textFile(Input).map(line =>
      RDFDataMgr.createIteratorTriples(new ByteArrayInputStream(line.getBytes), Lang.NTRIPLES, null).next())
   
      val r = RDFfile.map(f => {
        val s =f.getSubject.getURI
        val p = f.getPredicate.getURI
        val o = f.getObject.getURI
        
        (s,p,o)
        })
       
        
       val v11 =r.map(f => f._1)
       val v22 = r.map(f => f._3)
       val indexedmap = ( v11.union(v22)).distinct().zipWithIndex()
       
      
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
   

  
  def SimilaritesInPIC(f:Int): List[Array[Long]] = {
    /*
	 * Collect all the edges of the graph
	*/
    val edge = graph.edges.collect()
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
      
      val vList = sort.collect()
      sort.unpersist()
   
    /*
	 * Difference between two set of vertices, used in different similarity measures
	 */
    def difference(a: Long, b: Long): Double = {
      val ansec = neighbors.lookup(a).distinct.head
      val ansec1 = neighbors.lookup(b).distinct.head
      if (ansec.isEmpty) { return 0.0 }
      val differ = ansec.diff(ansec1)
      if (differ.isEmpty) { return 0.0 }

      differ.size.toDouble
    }

    /*
	 * Intersection of two set of vertices, used in different similarity measures
	 */
    def intersection(a: Long, b: Long): Double = {
      val inters = neighbors.lookup(a).distinct.head.toList
      val inters1 = neighbors.lookup(b).distinct.head.toList
      
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
      val uni = neighbors.lookup(a).distinct.head.toList
      val uni1 = neighbors.lookup(b).distinct.head.toList
      val uniA = uni.::(a)
      val uniB = uni1.::(b)
      val rst = uniA.union(uniB).distinct.toArray
      
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

    def selectSimilarity(a: Long, b: Long, c: Int): Double ={
      var s = 0.0
      if(c ==0){

    /*
			 * Jaccard similarity measure 
			 */
    
     s = intersection(a, b) / union(a, b).toDouble
     

    }
      if(c ==1){
    /*
			 * Batet similarity measure 
			 */
    
      val cal = 1 + ((difference(a, b) + difference(b, a)) / (difference(a, b) + difference(b, a) + intersection(a, b))).abs
      s = log2(cal.toDouble)
       
    }
      
      if(c ==2){
    /*
			 * Rodríguez and Egenhofer similarity measure
			 */

    var g = 0.8
    
      s = (intersection(a, b) / ((g * difference(a, b)) + ((1 - g) * difference(b, a)) + intersection(a, b))).toDouble.abs
       
    }
      
      if(c ==4){
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
        
        (x1, x2, selectSimilarity(x1, x2, f))
      }
    }
    
    def findingSimilarity(a:Long , b: Long): Double={
      var f3 = 0.0
      weightedGraph.map(f => {
        if((f._1 == a && f._2 == b) || (f._1 == b && f._2 == a)) {f3 =f._3}
        
        
        })
      f3
     }
    

   
    val clusterRdd = sparkSession.sparkContext.parallelize(weightedGraph)
  

  def pic() = {
    val pic = new PowerIterationClustering()
      .setK(params.k)
      .setMaxIterations(params.maxIterations)
    pic
  }

  def model = pic.run(clusterRdd)

  /*
			 * Cluster the graph data into two classes using PowerIterationClustering
			 */
  def run() = model
  
  
  val clusters = model.assignments.collect().groupBy(_.cluster).mapValues(_.map(_.id))
     val assignments = clusters.toList.sortBy { case (k, v) => v.length}
     val assignmentsStr = assignments
       .map { case (k, v) =>
       s"$k -> ${v.sorted.mkString("[", ",", "]")}"
     }.mkString(",")
     val sizesStr = assignments.map {
       _._2.size
     }.sorted.mkString("(", ",", ")")
     
     //println(s"Cluster assignments: $assignmentsStr\ncluster sizes: $sizesStr")
     def makerdf(a: Array[Long]) : List[String] ={
       var listuri : List[String] = List()
       val b:Array[VertexId] = a
       for(i <- 0 until b.length ){
          vertices.collect().map(v => {
            if(b(i)==v._1) listuri = listuri.::(v._2)
          })
        
         
       }
       listuri
      
     }
     val listCluster = assignments.map(f => f._2)
     val m = listCluster.map(f => makerdf(f))
     val rdfRDD = sparkSession.sparkContext.parallelize(m)
     rdfRDD.saveAsTextFile(output)
     //println(s"RDF Cluster assignments: $m\n")
     
     /*
			 * Sillouhette Evaluation
			 */
   
 def avgA(c: Array[Long], d:Long) : Double = {
    var sumA = 0.0
    val sizeC = c.length
     
     c.map(ck => {
       val scd = findingSimilarity(ck, d)
     sumA = sumA + scd
     })
    
    sumA/sizeC
  }
  
  
  
  def avgB(c: Array[Long], d:Long) : Double = {
    var sumB = 0.0
    val sizeC = c.length
    if(sizeC == 0) return 0.0
    c.map(ck => {
      val scd = findingSimilarity(ck, d)
     
     sumB = sumB + scd
    })
    
   
    sumB/sizeC
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
     s = (a/b) - 1
    }
    s
  }
  
  def AiBi(m: List[Array[Long]] , n: Array[VertexId]) : List[Double] = {
    var Ai = 0.0
    var Bi = 0.0
    var bi = 0.0
    var avg: List[Double] = List()
   
   
    var sx: List[Double] = List()
    
    n.map(nk => {
      avg = List()
      m.map(mp => {
        if(mp.contains(nk)){
          Ai = avgA(mp, nk)
        }
        else{
          avg = avg.::(avgB(mp, nk))
        }
      })
      if(avg.length != 0){
      bi = avg.max}
      else{bi = 0.0}
      
      val v =SI(Ai,bi)
      sx = sx.::(v)
       
      })
      sx
    
   
  }
  val evaluate = AiBi(listCluster,vList )
  val averageSil = evaluate.sum / evaluate.size
  val evaluateString : List[String] = List(averageSil.toString())
  val evaluateStringRDD = sparkSession.sparkContext.parallelize(evaluateString)
      
  evaluateStringRDD.saveAsTextFile(outputevl)
  //println(s"averageSil: $averageSil\n")
  

  /*
			 * Save the model.
			 * @path - path for a model.
			 */
  def save(path: String) = model.save(sparkSession.sparkContext, path)

  /*
			 * Load the model.
			 * @path - the given model.
			 */
  def load(path: String) = PowerIterationClusteringModel.load(sparkSession.sparkContext, path)
  
(listCluster)
}
  val clrdd = clusterRdd()
  
  }
     }
