package net.sansa_stack.ml.spark.outliers.vandalismdetection

import java.io.ByteArrayInputStream
import java.util.ArrayList
import java.util.regex.Pattern

import org.apache.hadoop.mapred.JobConf
import org.apache.jena.graph.Triple
import org.apache.jena.rdf.model.ModelFactory
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class ParseJTriple extends Serializable {

  def Start_JTriple_Parser(jobConf_Record: JobConf, sc: SparkContext): RDD[String] = {

    jobConf_Record.set("stream.recordreader.class", "org.apache.hadoop.streaming.StreamXmlRecordReader")
    jobConf_Record.set("stream.recordreader.begin", """"s":""") // start Tag
    jobConf_Record.set("stream.recordreader.end", "}") // End Tag

    org.apache.hadoop.mapred.FileInputFormat.addInputPaths(jobConf_Record, "hdfs://localhost:9000/mydata/xxx.json") // input path from Hadoop
    // ------------JTriple Record
    // read data and save in RDD as block- JTriple Record
    val JTriple_Dataset_Record = sc.hadoopRDD(jobConf_Record, classOf[org.apache.hadoop.streaming.StreamInputFormat], classOf[org.apache.hadoop.io.Text], classOf[org.apache.hadoop.io.Text])
    // println("HelloRecords" + " " + JTriple_Dataset_Record.count)
    // Convert the block- JTriple Record to String DataType
    val JTriple_Dataset_Record_AsstringBlock = JTriple_Dataset_Record.map { case (x, y) => (x.toString()) }
    println("HelloRecords" + " " + JTriple_Dataset_Record_AsstringBlock.count)
    JTriple_Dataset_Record_AsstringBlock.foreach(println)
    val RevisioninOneString = JTriple_Dataset_Record_AsstringBlock.map(line => New_abendRevision(line)).distinct().cache()
    RevisioninOneString
  }
  def New_abendRevision(str: String): String = {

    val s1 = str.replaceAll("[\r\n]+", " ");
    val s2 = s1.replaceAll("[.\\s]", "").trim()
    s2
  }
}
