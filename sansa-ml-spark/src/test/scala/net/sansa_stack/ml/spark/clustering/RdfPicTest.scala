package net.sansa_stack.ml.spark.clustering
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FunSuite
import org.apache.spark.sql.SparkSession
//import net.sansa_stack.query.spark.graph.jena.expression.Lang
import org.apache.jena.riot.Lang
import scala.collection.mutable
import org.apache.log4j.{ Level, Logger }
import java.io.ByteArrayInputStream
import org.apache.jena.riot.Lang
import net.sansa_stack.rdf.spark.io._
import net.sansa_stack.rdf.spark.model.graph._

//import net.sf.extjwnl.data._

class RdfPicTest extends FunSuite with DataFrameSuiteBase {

  test("Clustering RDF data") {
     val spark = SparkSession.builder
      .appName(s"Power Iteration Clustering example")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    System.setProperty("spark.akka.frameSize", "2000")
    

    println("============================================")
    println("| Power Iteration Clustering   example     |")
    println("============================================")

    val lang = Lang.NTRIPLES
    val path = getClass.getResource("/Cluster/Clustering_sampledata.txt").getPath
    val triples = spark.rdf(lang)(path)
    
    val graph = triples.asStringGraph()
     val cluster = RDFGraphPowerIterationClustering(spark, graph, "/Cluster/output", 4, 10)
   // val cluster = RDFGraphPowerIterationClustering.apply(spark, graph, , "/Cluster/outevl", "/Cluster/outputsim", 4, 10)
    cluster.collect().foreach(println)
    assert(true)
    
  }
  
}
