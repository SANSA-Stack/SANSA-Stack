package net.sansa_stack.rdf.spark.io.benchmark

import java.io.{ ByteArrayInputStream, File, InputStream }
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._

import net.sansa_stack.rdf.benchmark.io.ReadableByteChannelFromIterator
import net.sansa_stack.rdf.spark.io.NTripleReader
import org.apache.commons.io.IOUtils
import org.apache.jena.ext.com.google.common.base.Stopwatch
import org.apache.jena.graph.Triple
import org.apache.jena.riot.{ Lang, RDFDataMgr }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SansaBenchRdfParse {
  def main(args: Array[String]): Unit = {

    val tempDirStr = System.getProperty("java.io.tmpdir")
    if (tempDirStr == null) {
      throw new RuntimeException("Could not obtain temporary directory")
    }
    val sparkEventsDir = new File(tempDirStr + "/spark-events")
    if (!sparkEventsDir.exists()) {
      sparkEventsDir.mkdirs()
    }

    // File.createTempFile("spark-events")

    val sparkSession = SparkSession.builder
      .master("local")
      .appName("spark session example")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // .config("spark.kryo.registrationRequired", "true")
      .config("spark.eventLog.enabled", "true")
      // .config("spark.kryo.registrator", String.join(
      //  ", ",
      //  "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator"
      //  "net.sansa_stack.query.spark.sparqlify.KryoRegistratorSparqlify")
      // )
      .config("spark.default.parallelism", "4")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()

    sparkSession.conf.set("spark.sql.crossJoin.enabled", "true")

    val triplesString =
      """<http://dbpedia.org/resource/Guy_de_Maupassant> <http://xmlns.com/foaf/0.1/givenName> "Guy De" .
        |<http://dbpedia.org/resource/Guy_de_Maupassant> <http://example.org/ontology/age> "30"^^<http://www.w3.org/2001/XMLSchema#integer> .
        |<http://dbpedia.org/resource/Guy_de_Maupassant> <http://dbpedia.org/ontology/influenced> <http://dbpedia.org/resource/Tobias_Wolff> .
        |<http://dbpedia.org/resource/Guy_de_Maupassant> <http://dbpedia.org/ontology/influenced> <http://dbpedia.org/resource/Henry_James> .
        |<http://dbpedia.org/resource/Guy_de_Maupassant> <http://dbpedia.org/ontology/deathPlace> <http://dbpedia.org/resource/Passy> .
        |<http://dbpedia.org/resource/Charles_Dickens> <http://xmlns.com/foaf/0.1/givenName> "Charles"@en .
        |<http://dbpedia.org/resource/Charles_Dickens> <http://dbpedia.org/ontology/deathPlace> <http://dbpedia.org/resource/Gads_Hill_Place> .
        |<http://someOnt/1> <http://someOnt/184298> <http://someOnt/272277> .
        |<http://someOnt/184298> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#AnnotationProperty> .
        |<http://snomedct-20170731T150000Z> <http://www.w3.org/2002/07/owl#versionInfo> "20170731T150000Z"@en .
        |""".stripMargin

    // val it = RDFDataMgr.createIteratorTriples(IOUtils.toInputStream(triplesString, "UTF-8"), Lang.NTRIPLES, "http://example.org/").asScala.toSeq
    // it.foreach { x => println("GOT: " + (if(x.getObject.isLiteral) x.getObject.getLiteralLanguage else "-")) }
    // val graphRdd : RDD[Triple] = sparkSession.sparkContext.parallelize(it)

    // val textRdd : RDD[String] = sparkSession.sparkContext.parallelize(triplesString.split("\n"))

    if (args.length != 1) {
      sys.error("please provide path to RDF file(s) a argument")
      sparkSession.stop()
      System.exit(0)
    }
    val textRdd: RDD[String] = sparkSession.sparkContext.textFile(args(0), 20)

    println(s"Raw count: ${textRdd.count()}")

    measureLoadingTime(
      textRdd
        .mapPartitions(p => RDFDataMgr.createIteratorTriples(toInputStream(p), Lang.NTRIPLES, null).asScala))

    measureLoadingTime(
      textRdd
        .map(line => RDFDataMgr.createIteratorTriples(new ByteArrayInputStream(line.getBytes), Lang.NTRIPLES, null).next()))

    sparkSession.stop()

  }

  def measureLoadingTime(rdd: RDD[Triple]): Unit = {
    val sw = Stopwatch.createStarted()
    val c = rdd.count()

    println(s"Time: ${sw.stop().elapsed(TimeUnit.MILLISECONDS)}ms")
    println(s"Count: $c")
  }

  def toInputStream(it: Iterator[String]): InputStream =
    ReadableByteChannelFromIterator.toInputStream(it.asJava)
}
