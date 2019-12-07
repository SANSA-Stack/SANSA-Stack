package net.sansa_stack.owl.spark.rdd

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import net.sansa_stack.owl.common.parsing.OWLXMLSyntaxParsing

import scala.collection.immutable

object OWLXMLSyntaxOWLExpressionsRDDBuilder {

  /**
    * definition to build owl expressions out of OWLXML syntax
    * @param spark: spark session
    * @param filePath: absolute path of the OWL file
    * @return RDDs consisting following expressions
    *         xmlVersion string : <?xml version...?>
    *         owlPrefix string : <rdf:RDF xmlns:.. >
    *         owlExpression string : <owl:Class, ObjectProperty, DatatypeProperty.../owl>
    */

  def build(spark: SparkSession, filePath: String): (OWLExpressionsRDD, OWLExpressionsRDD, OWLExpressionsRDD) = {

    val builder = new OWLXMLSyntaxExpressionBuilder(spark, filePath)

    val owlExpressionsRDD : (OWLExpressionsRDD, OWLExpressionsRDD, OWLExpressionsRDD) = builder.getOwlExpressions()

    owlExpressionsRDD
  }
}

class OWLXMLSyntaxExpressionBuilder(spark: SparkSession, filePath: String) extends Serializable {

  // get xml version string as an RDD
  private val xmlVersionRDD = getRecord(OWLXMLSyntaxParsing.OWLXMLSyntaxPattern("versionPattern"))

  // get owlxml prefix string as an RDD
  private val owlPrefixRDD = getRecord(OWLXMLSyntaxParsing.OWLXMLSyntaxPattern("prefixPattern"))

  // get pattern for begin and end tags for owl expressions to be specified for hadoop stream
  private val owlRecordPatterns: Map[String, Map[String, String]] = OWLXMLSyntaxParsing.OWLXMLSyntaxPattern
    .filterKeys(_ != "versionPattern").filterKeys(_ != "prefixPattern")

  /**
    * definition to get owl expressions from hadoop stream as RDD[String]
    *
    * @param pattern a Map object consisting of key-value pairs to define begin and end tags
    * @return OWlExpressionRDD
    */

  def getRecord(pattern: Map[String, String]): OWLExpressionsRDD = {

    val config = new JobConf()
    val beginTag: String = pattern("beginTag")
    val endTag: String = pattern("endTag")

    config.set("stream.recordreader.class", "org.apache.hadoop.streaming.StreamXmlRecordReader")
    config.set("stream.recordreader.begin", beginTag) // start Tag
    config.set("stream.recordreader.end", endTag) // End Tag

    org.apache.hadoop.mapred.FileInputFormat.addInputPaths(config, filePath)

    val rawRDD: RDD[(Text, Text)] = spark.sparkContext.hadoopRDD(config,
      classOf[org.apache.hadoop.streaming.StreamInputFormat], // input format
      classOf[org.apache.hadoop.io.Text], // class for the key
      classOf[org.apache.hadoop.io.Text]) // class for the value

    val recordRDD: OWLExpressionsRDD = rawRDD.map { (x) => (x._1.toString) }

    recordRDD
  }

  /**
    * definition to get owl expressions for each patterns defined in OWLXMLSyntaxPattern
    *
    * @return a tuple consisting of (RDD for xml version, RDD for owlxml prefixes, RDD for owl expressions)
    */

  def getOwlExpressions(): (OWLExpressionsRDD, OWLExpressionsRDD, OWLExpressionsRDD) = {

    val unionOwlExpressionsRDD = for {
      (pattern, tags) <- owlRecordPatterns
    } yield (getRecord(tags))

    val owlExpressionsRDD = unionOwlExpressionsRDD.reduce(_ union _)

    (xmlVersionRDD, owlPrefixRDD, owlExpressionsRDD)
  }
}
