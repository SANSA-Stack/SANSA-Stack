package net.sansa_stack.ml.spark.outliers.vandalismdetection

import org.apache.spark.SparkContext
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.jena.graph.Triple
import org.apache.jena.rdf.model.ModelFactory
import java.util.ArrayList
import java.util.regex.Pattern
import java.io.ByteArrayInputStream

class ParseRDFXML extends Serializable {

  def start_RDFXML_Parser(jobConf_Record: JobConf, jobConf_Prefixes: JobConf, sc: SparkContext): RDD[String] = {

    jobConf_Record.set("stream.recordreader.class", "org.apache.hadoop.streaming.StreamXmlRecordReader")
    jobConf_Record.set("stream.recordreader.begin", "<rdf:Description") // start Tag
    jobConf_Record.set("stream.recordreader.end", "</rdf:Description>") // End Tag

    jobConf_Prefixes.set("stream.recordreader.class", "org.apache.hadoop.streaming.StreamXmlRecordReader")
    jobConf_Prefixes.set("stream.recordreader.begin", "<rdf:RDF") // start Tag
    jobConf_Prefixes.set("stream.recordreader.end", ">") // End Tag

    org.apache.hadoop.mapred.FileInputFormat.addInputPaths(jobConf_Record, "hdfs://localhost:9000/mydata/Germany.rdf") // input path from Hadoop
    org.apache.hadoop.mapred.FileInputFormat.addInputPaths(jobConf_Prefixes, "hdfs://localhost:9000/mydata/Germany.rdf") // input path from Hadoop

    //------------ RDF XML Record
    // read data and save in RDD as block- RDFXML Record
    val RDFXML_Dataset_Record = sc.hadoopRDD(jobConf_Record, classOf[org.apache.hadoop.streaming.StreamInputFormat], classOf[org.apache.hadoop.io.Text], classOf[org.apache.hadoop.io.Text])
    //      println("HelloRecords" + " " + RDFXML_Dataset_Record.count)

    // Convert the block- RDFXML Record to String DataType
    val RDFXML_Dataset_Record_AsstringBlock = RDFXML_Dataset_Record.map { case (x, y) => (x.toString()) }
    println("HelloRecords" + " " + RDFXML_Dataset_Record_AsstringBlock.count)
    //      RDFXML_Dataset_Record_AsstringBlock.foreach(println)

    //-------------RDF XML Prefixes
    // read data and save in RDD as block- RDFXML Prefixes
    val RDFXML_Dataset_Prefixes = sc.hadoopRDD(jobConf_Prefixes, classOf[org.apache.hadoop.streaming.StreamInputFormat], classOf[org.apache.hadoop.io.Text], classOf[org.apache.hadoop.io.Text])
    println("HelloPrefixes" + " " + RDFXML_Dataset_Prefixes.count)
    //      RDFXML_Dataset_Prefixes.foreach(println)
    // Convert the block- RDFXML Prefixes to String DataType
    var RDFXML_Dataset_AsstringPrefixes_WithoutDist = RDFXML_Dataset_Prefixes.map { case (x, y) => (x.toString()) }
    val RDFXML_Dataset_AsstringPrefixes=RDFXML_Dataset_AsstringPrefixes_WithoutDist.distinct()
    println("HelloPrefixes" + " " + RDFXML_Dataset_AsstringPrefixes.count)
    //      RDFXML_Dataset_AsstringPrefixes.foreach(println)
    val pref = RDFXML_Dataset_AsstringPrefixes.reduce((a, b) => a + "\n" + b)
    //      println("Hello100000000000" + pref)
    val Triples = RDFXML_Dataset_Record_AsstringBlock.map(record => RecordParse(record, pref))
    println("HelloRecords" + " " + Triples.count)
    //      Triples.foreach(println)
    val finalRDD_RDF = Triples.filter(x => !x.isEmpty)
    //      println("Hello17" + " " + finalRDD_RDF.count)
    val TR = finalRDD_RDF.map(line => arrayListTOstring(line))
    println("HelloRecords" + " " + TR.count)

    TR
  }
  // This function for RDFXML case.
  def RecordParse(record: String, prefixes: String): ArrayList[Triple] = {
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
  def prefixParse(line: String): ArrayList[String] = {
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
  def arrayListTOstring(Arraylistval: ArrayList[Triple]): String = {
    val str = Arraylistval.get(0).toString()
    str
  }

}