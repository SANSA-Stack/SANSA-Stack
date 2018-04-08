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

class FacilitiesClass extends Serializable {
    
  

//  def Prev_Revision(idRe: String, record: String): String = {
//
//    var Parent = ""
//    if (record.contains(idRe + "::ParentID")) {
//
//      val recordListstr: Array[String] = record.split("::")
//
//      val ParentID = recordListstr(2)
//
//      Parent = ParentID
//
//    }
//
//    Parent
//  }

  def cleaner(str: String): String = {

    val cleaned_value1 = str.replace("{", "").trim()
    val cleaned_value2 = str.replace("}", "").trim()
    val cleaned_value3 = cleaned_value2.replace("\"", "");

    cleaned_value3
  }
  def splitBycomma(str: String): Array[String] = {
    val namesList: Array[String] = str.split(",")
    namesList
  }

  def RDD_TO_DFR_RDFXML(rdd: RDD[String], sqlContext: org.apache.spark.sql.SQLContext): DataFrame = {
    //Create an Encoded Schema in a String Format:
    val schemaString = "Subject Predicate Object"
    //Generate schema:
    val schema = StructType(schemaString.split(" ").map(fieldName ⇒ StructField(fieldName, StringType, true)))
    //Apply Transformation for Reading Data from Text File
    val rowRDD = rdd.map(_.split(" ")).map(e ⇒ Row(e(0), e(1), e(2)))
    //Apply RowRDD in Row Data based on Schema:
    val RDFTRIPLE = sqlContext.createDataFrame(rowRDD, schema)
    //Store DataFrame Data into Table
    RDFTRIPLE.registerTempTable("SPO")

    //Select Query on DataFrame
    val dfr = sqlContext.sql("SELECT * FROM SPO")
    dfr.show()

    dfr
  }
  



//  def RDD_TO_DFR_NormalXML_WithoutFeatures(rdd: RDD[String], sqlContext: org.apache.spark.sql.SQLContext): DataFrame = {
//
//
//  val schemaString = "pid&&rid&&label&&parid"
//
//  //            //Generate schema:
//  val schema = StructType(schemaString.split("&&").map(fieldName ⇒ StructField(fieldName, StringType, true)))
//
//  //Apply Transformation for Reading Data from Text File
//  val rowRDD = rdd.map(_.split("NN&LL")).map(e ⇒ Row(e(0).trim(), e(1).trim(), e(2).trim(),e(3).trim()))
//
//  
//  //Apply RowRDD in Row Data based on Schema:
//  val RevisionDF = sqlContext.createDataFrame(rowRDD, schema)
//  
//  
//  //Store DataFrame Data into Table
//  RevisionDF.registerTempTable("pv")
//  
//  
//  //Select Query on DataFrame
//  val dfr1 = sqlContext.sql("SELECT * FROM pv")
//    
//  
//  dfr1
//
//  }

  def stringToInt(str: String): Integer = {

    val results = str.toInt

    results

  }

  def Arraytring_ToVecotrDouble(str: String): Vector = {

    val vector: Vector = Vectors.zeros(0)

    val str_recordList: Array[String] = str.split(",")
    val size: Integer = str_recordList.size
    var double_recordList: Array[Double] = new Array[Double](size)

    // val x= str_recordList.toVector

    for (record <- str_recordList) {

      if (record.nonEmpty) {

        val tem0: String = record.replace("[", "").trim()
        val tem1: String = tem0.replace("]", "").trim()

        val tem2 = tem1.toDouble
        double_recordList +:= tem2

      }

    }

    ToVector(double_recordList)

  }


  def ToVector(arra: Array[Double]): Vector = {

    val vector: Vector = Vectors.dense(arra)

    vector
  }

}