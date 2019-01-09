package net.sansa_stack.ml.spark.outliers.vandalismdetection.parser

import java.util.ArrayList

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.streaming.StreamInputFormat
import org.apache.jena.graph.Triple
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types.{ DoubleType, IntegerType, StringType, StructField, StructType }

object TRIX extends Serializable {

  def parse(jobConf: JobConf, input: String, spark: SparkSession): RDD[String] = {

    jobConf.set("stream.recordreader.class", "org.apache.hadoop.streaming.StreamXmlRecordReader")
    jobConf.set("stream.recordreader.begin", "<triple>") // start Tag
    jobConf.set("stream.recordreader.end", "</triple>") // End Tag

    org.apache.hadoop.mapred.FileInputFormat.addInputPaths(jobConf, input)

    // read data and save in RDD as block- TRIX Record
    val triples = spark.sparkContext.hadoopRDD(jobConf, classOf[org.apache.hadoop.streaming.StreamInputFormat], classOf[org.apache.hadoop.io.Text], classOf[org.apache.hadoop.io.Text])

    // Convert the block- TRIX Record to String DataType
    val triplesAsStringBlock = triples.map { case (x, y) => (x.toString()) }
    val revisionInOneString = triplesAsStringBlock.map(line => abendRevision(line)).distinct().cache()

    revisionInOneString
  }

  def toDF(rdd: RDD[String], spark: SparkSession): DataFrame = {
    // Create an Encoded Schema in a String Format:
    val schemaString = "Subject Predicate Object"
    // Generate schema:
    val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    // Apply Transformation for Reading Data from Text File
    val rowRDD = rdd.map(_.split("><")).map(e => Row(e(0), e(1), e(2)))
    // Apply RowRDD in Row Data based on Schema:
    val triples = spark.createDataFrame(rowRDD, schema)
    // Store DataFrame Data into Table
    triples.createOrReplaceTempView("SPO")
    // Select Query on DataFrame
    val df = spark.sql("SELECT * FROM SPO")

    df
  }

  def abendRevision(str: String): String = {
    val s1 = str.replaceAll("[\r\n]+", " ");
    val s2 = s1.replace("<triple>", "")
    val s3 = s2.replace("</triple>", "").trim()
    val s4 = s3.replaceAll(">[.\\s]+<", "><").trim()

    s4
  }

}
