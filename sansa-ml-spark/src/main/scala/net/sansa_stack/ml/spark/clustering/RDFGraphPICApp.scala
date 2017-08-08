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

object RDFGraphPICApp {

  case class Params(
      input: String = null,
      k: Int = 3,
      maxIterations: Int = 50) extends AbstractParams[Params] {
  }
  abstract class AbstractParams[T: TypeTag] {

    private def tag: TypeTag[T] = typeTag[T]

    override def toString: String = {
      val tpe = tag.tpe
      val allAccessors = tpe.decls.collect {
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

  def main(args: Array[String]) {

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

    
    // Load the RDF dataset 
    val RDFfile = sparkSession.sparkContext.textFile("/Users/tinaboroukhian/Desktop/Clustering_sampledata.txt").map(line =>
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
     
   
    val model = RDFGraphPICClustering(sparkSession, graph, params.k, params.maxIterations).run()
    
    val file = "PIC.txt"

    val pw = new PrintWriter(new File(file))
    
   
val clusters = model.assignments.collect().groupBy(_.cluster).mapValues(_.map(_.id))
     val assignments = clusters.toList.sortBy { case (k, v) => v.length}
     val assignmentsStr = assignments
       .map { case (k, v) =>
       s"$k -> ${v.sorted.mkString("[", ",", "]")}"
     }.mkString(",")
     val sizesStr = assignments.map {
       _._2.size
     }.sorted.mkString("(", ",", ")")
    println(s"Cluster assignments: $assignmentsStr\ncluster sizes: $sizesStr")
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
     val listCluster = assignments.map(f => f._2.toList)
     val m = listCluster.map(f => makerdf(f))
     println(s"RDF Cluster assignments: $m\n")
     pw.println(s"RDF Cluster assignments: $m\n")
      pw.close
 
    sparkSession.sparkContext.stop()
  }

}
