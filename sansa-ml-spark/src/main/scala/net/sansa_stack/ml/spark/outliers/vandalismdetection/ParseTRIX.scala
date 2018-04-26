package net.sansa_stack.ml.spark.outliers.vandalismdetection
import org.apache.spark.SparkContext
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.jena.graph.Triple
import org.apache.jena.rdf.model.ModelFactory
import java.util.ArrayList
import java.util.regex.Pattern
import java.io.ByteArrayInputStream

class ParseTRIX extends Serializable {

  def Start_TriX_Parser(jobConf_Record: JobConf, sc: SparkContext): RDD[String] = {

    jobConf_Record.set("stream.recordreader.class", "org.apache.hadoop.streaming.StreamXmlRecordReader")
    jobConf_Record.set("stream.recordreader.begin", "<triple>") // start Tag
    jobConf_Record.set("stream.recordreader.end", "</triple>") // End Tag

    org.apache.hadoop.mapred.FileInputFormat.addInputPaths(jobConf_Record, "hdfs://localhost:9000/mydata/xx.trix") // input path from Hadoop

    //------------TRIX Record
    // read data and save in RDD as block- TRIX Record
    val TRIX_Dataset_Record = sc.hadoopRDD(jobConf_Record, classOf[org.apache.hadoop.streaming.StreamInputFormat], classOf[org.apache.hadoop.io.Text], classOf[org.apache.hadoop.io.Text])
    //      println("HelloRecords" + " " + TRIX_Dataset_Record.count)

    // Convert the block- TRIX Record to String DataType
    val TRIX_Dataset_Record_AsstringBlock = TRIX_Dataset_Record.map { case (x, y) => (x.toString()) }
    println("HelloRecords" + " " + TRIX_Dataset_Record_AsstringBlock.count)
    TRIX_Dataset_Record_AsstringBlock.foreach(println)

    val RevisioninOneString = TRIX_Dataset_Record_AsstringBlock.map(line => New_abendRevision(line)).cache()

    RevisioninOneString
  }

  def New_abendRevision(str: String): String = {

    val s1 = str.replaceAll("[\r\n]+", " ");
    val s2 = s1.replace("<triple>", "")
    val s3 = s2.replace("</triple>", "").trim()
    val s4 = s3.replaceAll(">[.\\s]+<", "><").trim()

    s4
  }

  // This function for TRIX case.
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
  // This function for TRIX case.
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
  // This function for TRIX case.
  def arrayListTOstring(Arraylistval: ArrayList[Triple]): String = {
    val str = Arraylistval.get(0).toString()
    str
  }

}