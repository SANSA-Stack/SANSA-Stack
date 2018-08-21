package net.sansa_stack.ml.spark.outliers.vandalismdetection

import java.io.ByteArrayInputStream
import java.util.ArrayList
import java.util.regex.Pattern

import org.apache.hadoop.mapred.JobConf
import org.apache.jena.graph.Triple
import org.apache.jena.rdf.model.ModelFactory
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class ParseTRIX extends Serializable {

  def Start_TriX_Parser(jobConf_Record: JobConf, sc: SparkContext): RDD[String] = {

    jobConf_Record.set("stream.recordreader.class", "org.apache.hadoop.streaming.StreamXmlRecordReader")
    jobConf_Record.set("stream.recordreader.begin", "<triple>") // start Tag
    jobConf_Record.set("stream.recordreader.end", "</triple>") // End Tag

    org.apache.hadoop.mapred.FileInputFormat.addInputPaths(jobConf_Record, "hdfs://localhost:9000/mydata/xx.trix") // input path from Hadoop

    // ------------TRIX Record
    // read data and save in RDD as block- TRIX Record
    val TRIX_Dataset_Record = sc.hadoopRDD(jobConf_Record, classOf[org.apache.hadoop.streaming.StreamInputFormat], classOf[org.apache.hadoop.io.Text], classOf[org.apache.hadoop.io.Text])
    //      println("HelloRecords" + " " + TRIX_Dataset_Record.count)

    // Convert the block- TRIX Record to String DataType
    val TRIX_Dataset_Record_AsstringBlock = TRIX_Dataset_Record.map { case (x, y) => (x.toString()) }
    println("HelloRecords" + " " + TRIX_Dataset_Record_AsstringBlock.count)
    TRIX_Dataset_Record_AsstringBlock.foreach(println)

    val RevisioninOneString = TRIX_Dataset_Record_AsstringBlock.map(line => New_abendRevision(line)).distinct().cache()

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
  def arrayListTOstring(Arraylistval: ArrayList[Triple]): String = {
    val str = Arraylistval.get(0).toString()
    str
  }
}
