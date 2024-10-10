package net.sansa_stack.owl.spark.rdd

import com.typesafe.scalalogging.{Logger => ScalaLogger}
import net.sansa_stack.owl.common.parsing.{RDFXMLSyntaxParsing, RDFXMLSyntaxPrefixParsing}
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobConf
import org.apache.log4j.{Level, Logger => Log4JLogger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.semanticweb.owlapi.model.OWLAxiom

import scala.collection.JavaConverters._


class RDFXMLSyntaxOWLExpressionsRDDBuilder extends Serializable with RDFXMLSyntaxPrefixParsing with RDFXMLSyntaxParsing {
  private val logger = ScalaLogger(this.getClass)

  /**
    * Builds a snippet conforming to the RDF/XML syntax which then can
    * be parsed by the OWL API RDF/XML syntax parser.
    * A single expression, e.g.
    *
    * <rdf:Description rdf:about="http://swat.cse.lehigh.edu/onto/univ-bench.owl#Person">
    * <rdf:type rdf:resource="http://www.w3.org/2002/07/owl#Class"/>
    * </rdf:Description>
    *
    * has thus to be wrapped into a ontology description as follows
    *
    * <rdf:RDF
    * xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
    * xmlns:owl="http://www.w3.org/2002/07/owl#">
    * <owl:Class rdf:about="http://swat.cse.lehigh.edu/onto/univ-bench.owl#Person"/>
    * </rdf:RDF>
    *
    * @param spark SparkSession
    * @param filePath the path to the input file
    * @return The set of axioms corresponding to the expression.
    */
  def build(spark: SparkSession, filePath: String): OWLAxiomsRDD = {

    val conf = new JobConf()
    val prefixes = new JobConf()

    conf.set("stream.recordreader.class", "org.apache.hadoop.streaming.StreamXmlRecordReader")
    conf.set("stream.recordreader.begin", "<rdf:Description") // start Tag
    conf.set("stream.recordreader.end", "</rdf:Description>") // End Tag

    prefixes.set("stream.recordreader.class", "org.apache.hadoop.streaming.StreamXmlRecordReader")
    prefixes.set("stream.recordreader.begin", "<rdf:RDF") // start Tag
    prefixes.set("stream.recordreader.end", ">") // End Tag

    org.apache.hadoop.mapred.FileInputFormat.addInputPaths(conf, filePath)
    org.apache.hadoop.mapred.FileInputFormat.addInputPaths(prefixes, filePath)

    // RDF/XML Record
    // read data and save in RDD as block-RDF/XML Record
    val rdfXMLRecord: RDD[(Text, Text)] = spark.sparkContext.hadoopRDD(conf,
      classOf[org.apache.hadoop.streaming.StreamInputFormat],
      classOf[org.apache.hadoop.io.Text],
      classOf[org.apache.hadoop.io.Text])

    // Convert the block-RDF/XML record to String
    val rawRDD: RDD[String] = rdfXMLRecord.map { case (x, _) => x.toString }

    // RDF/XML prefixes
    // Read data and save in RDD as block-RDF/XML prefixes
    val rdfXMLPrefixes = spark.sparkContext.hadoopRDD(prefixes,
      classOf[org.apache.hadoop.streaming.StreamInputFormat],
      classOf[org.apache.hadoop.io.Text],
      classOf[org.apache.hadoop.io.Text])

    // Convert the block-RDF/XML prefixes to String
    val tmpPrefixes = rdfXMLPrefixes.map { case (x, _) => x.toString }.distinct()
    val prefixesString: String = tmpPrefixes.reduce((a, b) => a + "\n" + b)

    val owlAxioms: RDD[OWLAxiom] =
      rawRDD.map(record => parseRecord(record, prefixesString))
        .filter(x => !x.isEmpty)
        .flatMap(axioms => axioms.asScala)
        .distinct()

    val refinedRDD = refineOWLAxioms(spark.sparkContext, owlAxioms)
//    logger.debug(s"Axioms count = ${refinedRDD.count()}")

    refinedRDD
  }
}

object RDFXMLSyntaxOWLExpressionsRDDBuilder {
  private val logger = ScalaLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    val input: String = getClass.getResource("/univ-bench.rdf").getPath

    logger.info("================================")
    logger.info("|        RDF/XML Parser        |")
    logger.info("================================")

    @transient val sparkSession = SparkSession.builder()
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("RDF/XML Parser")
      .getOrCreate()

    Log4JLogger.getLogger("akka").setLevel(Level.OFF)
    Log4JLogger.getLogger(this.getClass).setLevel(Level.ERROR)

    val RDFXMLBuilder = new RDFXMLSyntaxOWLExpressionsRDDBuilder
    val rdd: OWLAxiomsRDD = RDFXMLBuilder.build(sparkSession, input)
    rdd.foreach(axiom => logger.info(axiom.toString))

    sparkSession.stop()
  }
}
