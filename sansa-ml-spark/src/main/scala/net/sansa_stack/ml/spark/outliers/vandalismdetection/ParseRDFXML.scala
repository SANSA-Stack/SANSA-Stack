package xmlpro

import org.apache.hadoop.streaming.StreamInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.rdd.RDD
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.streaming.StreamXmlRecordReader
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{ DoubleType, StringType, IntegerType, StructField, StructType }
import org.apache.spark.sql.Row
import org.apache.spark.sql
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import Array._
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.List;
import com.google.common.base.Splitter;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.io;
import org.apache.commons.lang3.StringUtils;
import java.util.Scanner;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.jena.graph.Triple
import org.apache.jena.rdf.model.ModelFactory
import org.apache.spark.rdd.RDD
import java.io.ByteArrayInputStream;
import java.util.Scanner;
import java.util._
// ML : 2.11
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.{ Vector, Vectors }
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import java.util.Arrays.asList
import collection.JavaConversions;
import collection.Seq;
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.mllib.classification.{ LogisticRegressionModel, LogisticRegressionWithLBFGS }
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.regression.{ RandomForestRegressionModel, RandomForestRegressor }
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.classification.BinaryLogisticRegressionSummary
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.regression.{ RandomForestRegressionModel, RandomForestRegressor }

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
    val RDFXML_Dataset_AsstringPrefixes = RDFXML_Dataset_Prefixes.map { case (x, y) => (x.toString()) }
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





