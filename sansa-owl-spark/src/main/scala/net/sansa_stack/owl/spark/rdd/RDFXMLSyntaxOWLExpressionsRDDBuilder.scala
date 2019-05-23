package net.sansa_stack.owl.spark.rdd

import collection.JavaConverters._
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.semanticweb.owlapi.model.OWLAxiom

import net.sansa_stack.owl.common.parsing.{RDFXMLSyntaxParsing, RDFXMLSyntaxPrefixParsing}



class RDFXMLSyntaxOWLExpressionsRDDBuilder extends Serializable with RDFXMLSyntaxPrefixParsing with RDFXMLSyntaxParsing {

  /**
    * Builds a snippet conforming to the RDFXML syntax which then can
    * be parsed by the OWLAPI RDFXML syntax parser.
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

    // RDF XML Record
    // read data and save in RDD as block- RDFXML Record
    val RDFXML_Record: RDD[(Text, Text)] = spark.sparkContext.hadoopRDD(conf,
      classOf[org.apache.hadoop.streaming.StreamInputFormat],
      classOf[org.apache.hadoop.io.Text],
      classOf[org.apache.hadoop.io.Text])

    // Convert the block- RDFXML Record to String DataType
    val rawRDD: RDD[String] = RDFXML_Record.map { case (x, y) => (x.toString()) }

    // RDF XML Prefixes
    // read data and save in RDD as block- RDFXML Prefixes
    val RDFXML_Prefixes = spark.sparkContext.hadoopRDD(prefixes,
      classOf[org.apache.hadoop.streaming.StreamInputFormat],
      classOf[org.apache.hadoop.io.Text],
      classOf[org.apache.hadoop.io.Text])

    // Convert the block- RDFXML Prefixes to String DataType
    var tmp_Prefixes = RDFXML_Prefixes.map { case (x, y) => (x.toString()) }.distinct()

    val pref: String = tmp_Prefixes.reduce((a, b) => a + "\n" + b)

    val OWLAxiomsList = rawRDD.map(record => RecordParse(record, pref))
                              .filter(x => !x.isEmpty)

    val OWLAxiomsRDD: RDD[OWLAxiom] = OWLAxiomsList.flatMap(line => line.iterator().asScala)
                        .distinct()

    println("Axioms are : \n")
    OWLAxiomsRDD.foreach(println(_))

    println("Axioms count = " + OWLAxiomsRDD.count())

    OWLAxiomsRDD
  }
}

object RDFXMLSyntaxOWLExpressionsRDDBuilder {

  def main(args: Array[String]): Unit = {

    val input: String = getClass.getResource("/univ-bench.rdf").getPath

    println("================================")
    println("|        RDF/XML Parser        |")
    println("================================")

    @transient val sparkSession = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("RDF/XML Parser")
      .getOrCreate()

 //   Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sc: SparkContext = sparkSession.sparkContext

    sparkSession.sparkContext.setLogLevel("ALL")

    val RDFXMLBuilder = new RDFXMLSyntaxOWLExpressionsRDDBuilder
    val rdd = RDFXMLBuilder.build(sparkSession, input)

    sparkSession.stop
  }
}



