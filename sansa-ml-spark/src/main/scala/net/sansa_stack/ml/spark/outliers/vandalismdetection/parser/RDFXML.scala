package net.sansa_stack.ml.spark.outliers.vandalismdetection.parser

import java.io.ByteArrayInputStream
import java.util.ArrayList
import java.util.regex.Pattern

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.streaming.StreamInputFormat
import org.apache.jena.graph.Triple
import org.apache.jena.rdf.model.ModelFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types.{ DoubleType, IntegerType, StringType, StructField, StructType }

object RDFXML extends Serializable {

  def parse(jobConf: JobConf, jobConfPrefixes: JobConf, spark: SparkSession): RDD[String] = {

    jobConf.set("stream.recordreader.class", "org.apache.hadoop.streaming.StreamXmlRecordReader")
    jobConf.set("stream.recordreader.begin", "<rdf:Description") // start Tag
    jobConf.set("stream.recordreader.end", "</rdf:Description>") // End Tag

    jobConfPrefixes.set("stream.recordreader.class", "org.apache.hadoop.streaming.StreamXmlRecordReader")
    jobConfPrefixes.set("stream.recordreader.begin", "<rdf:RDF") // start Tag
    jobConfPrefixes.set("stream.recordreader.end", ">") // End Tag

    org.apache.hadoop.mapred.FileInputFormat.addInputPaths(jobConf, "hdfs://localhost:9000/mydata/Germany.rdf") // input path from Hadoop
    org.apache.hadoop.mapred.FileInputFormat.addInputPaths(jobConfPrefixes, "hdfs://localhost:9000/mydata/Germany.rdf") // input path from Hadoop

    // ------------ RDF XML Record
    // read data and save in RDD as block- RDFXML Record
    val triples = spark.sparkContext.hadoopRDD(jobConf, classOf[org.apache.hadoop.streaming.StreamInputFormat], classOf[org.apache.hadoop.io.Text], classOf[org.apache.hadoop.io.Text])

    // Convert the block- RDFXML Record to String DataType
    val triplesAsStringBlock = triples.map { case (x, y) => (x.toString()) }

    // -------------RDF XML Prefixes
    // read data and save in RDD as block- RDFXML Prefixes
    val triplesPrefixes = spark.sparkContext.hadoopRDD(jobConfPrefixes, classOf[org.apache.hadoop.streaming.StreamInputFormat], classOf[org.apache.hadoop.io.Text], classOf[org.apache.hadoop.io.Text])

    // Convert the block- RDFXML Prefixes to String DataType
    var triplesAsStringPrefixesWithoutDist = triplesPrefixes.map { case (x, y) => (x.toString()) }
    val triplesAsStringPrefixes = triplesAsStringPrefixesWithoutDist.distinct()

    val pref = triplesAsStringPrefixes.reduce((a, b) => a + "\n" + b)
    val finalRDD_RDF = triplesAsStringBlock.map(record => parseRecord(record, pref)).filter(x => !x.isEmpty)
    val TR = finalRDD_RDF.map(line => arrayListToString(line))

    TR
  }

  def toDF(rdd: RDD[String], spark: SparkSession): DataFrame = {
    // Create an Encoded Schema in a String Format:
    val schemaString = "Subject Predicate Object"
    // Generate schema:
    val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    // Apply Transformation for Reading Data from Text File
    val rowRDD = rdd.map(_.split(" ")).map(e => Row(e(0), e(1), e(2)))
    // Apply RowRDD in Row Data based on Schema:
    val triples = spark.createDataFrame(rowRDD, schema)
    // Store DataFrame Data into Table
    triples.createOrReplaceTempView("SPO")
    // Select Query on DataFrame
    val df = spark.sql("SELECT * FROM SPO")

    df
  }

  // This function for RDFXML case.
  def parseRecord(record: String, prefixes: String): ArrayList[Triple] = {
    var triples = new ArrayList[Triple]()
    var model = ModelFactory.createDefaultModel();
    val modelText = "<?xml version=\"1.0\" encoding=\"utf-8\" ?> \n" + prefixes + record + "</rdf:RDF>"
    model.read(new ByteArrayInputStream(modelText.getBytes()), "http://example.org");
    val iter = model.listStatements()
    while (iter.hasNext()) {
      var triple = iter.next().asTriple()
      triples.add(triple)
    }
    triples
  }

  // This function for RDFXML case.
  def parsePrefix(line: String): ArrayList[String] = {
    var temp = line
    val prefixRegex = Pattern.compile("xmlns\\s*:\\s*[a-zA-Z][a-zA-Z0-9_]*\\s*=\\s*([\"'])(?:(?=(\\\\?))\\2.)*?\\1")
    var matcher = prefixRegex.matcher(temp)
    var vars = new ArrayList[String]()
    while (matcher.find()) {
      var x = temp.substring(matcher.start(), matcher.end())
      vars.add(x.toString())
      temp = temp.substring(0, matcher.start()) + temp.substring(matcher.end(), temp.length())
      matcher = prefixRegex.matcher(temp)
    }
    vars
  }
  // This function for RDFXML case.
  def arrayListToString(Arraylistval: ArrayList[Triple]): String = {
    val str = Arraylistval.get(0).toString()
    str
  }
}
