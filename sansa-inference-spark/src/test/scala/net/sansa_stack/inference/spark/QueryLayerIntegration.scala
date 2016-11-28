package net.sansa_stack.inference.spark

import java.io.File

import net.sansa_stack.rdf.partition.core.RdfPartitionerDefault
import org.apache.commons.io.IOUtils
import org.apache.jena.riot.{Lang, RDFDataMgr}
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._
import net.sansa_stack.rdf.spark.partition.core.RdfPartitionUtilsSpark

/**
  * @author Lorenz Buehmann
  */
object QueryLayerIntegration {
  def main(args: Array[String]): Unit = {
    val tempDirStr = System.getProperty("java.io.tmpdir")
    if(tempDirStr == null) {
      throw new RuntimeException("Could not obtain temporary directory")
    }
    val sparkEventsDir = new File(tempDirStr + "/spark-events")
    if(!sparkEventsDir.exists()) {
      sparkEventsDir.mkdirs()
    }

    val sparkSession = SparkSession.builder
      .master("local")
      .appName("spark session example")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //.config("spark.kryo.registrationRequired", "true")
      .config("spark.eventLog.enabled", "true")
      .config("spark.kryo.registrator", String.join(", ",
        "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator",
        "net.sansa_stack.rdf.spark.sparqlify.KryoRegistratorSparqlify"
      ))
      .config("spark.default.parallelism", "4")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()

    val triplesString =
      """<http://dbpedia.org/resource/Guy_de_Maupassant>	<http://xmlns.com/foaf/0.1/givenName>	"Guy De" .
        |<http://dbpedia.org/resource/Guy_de_Maupassant>	<http://dbpedia.org/ontology/influenced>	<http://dbpedia.org/resource/Tobias_Wolff> .
        |<http://dbpedia.org/resource/Guy_de_Maupassant>	<http://dbpedia.org/ontology/influenced>	<http://dbpedia.org/resource/Henry_James> .
        |<http://dbpedia.org/resource/Guy_de_Maupassant>	<http://dbpedia.org/ontology/deathPlace>	<http://dbpedia.org/resource/Passy> .
        |<http://dbpedia.org/resource/Charles_Dickens>	<http://xmlns.com/foaf/0.1/givenName>	"Charles"@en .
        |<http://dbpedia.org/resource/Charles_Dickens>	<http://dbpedia.org/ontology/deathPlace>	<http://dbpedia.org/resource/Gads_Hill_Place> .""".stripMargin

    val it = RDFDataMgr.createIteratorTriples(IOUtils.toInputStream(triplesString, "UTF-8"), Lang.NTRIPLES, "http://example.org/").asScala.toSeq
    //it.foreach { x => println("GOT: " + (if(x.getObject.isLiteral) x.getObject.getLiteralLanguage else "-")) }
    val graphRdd = sparkSession.sparkContext.parallelize(it)

    //val map = graphRdd.partitionGraphByPredicates
    val partitions = RdfPartitionUtilsSpark.partitionGraph(graphRdd, RdfPartitionerDefault)

    partitions.foreach(p => println(p._1))
  }


}
