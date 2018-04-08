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

class ItemFeatures extends Serializable {

  
  //1.
  def Get_NumberOfLabels(str: String): Double = {

    // from Label Tag
    val input: String = str
    val pattern: Pattern = Pattern.compile(""""value"""" + ":")
    val matcher: Matcher = pattern.matcher(input)
    var count: Double = 0.0
    while (matcher.find()) { count += 1; count - 1 }

    count

    count
  }

  //2.
  def Get_NumberOfDescription(str: String): Double = {

    // from description tag
    val input: String = str
    val pattern: Pattern = Pattern.compile(""""value"""" + ":")
    val matcher: Matcher = pattern.matcher(input)
    var count: Double = 0.0
    while (matcher.find()) { count += 1; count - 1 }

    count

    count
  }

  //3.
  def Get_NumberOfAliases(str: String): Double = {

    // from Aliases Tag
    val input: String = str
    val pattern: Pattern = Pattern.compile(""""value"""" + ":")
    val matcher: Matcher = pattern.matcher(input)
    var count: Double = 0.0
    while (matcher.find()) { count += 1; count - 1 }

    count

    count
  }

  //4.
  def Get_NumberOfClaim(str: String): Double = {

    // from claim tag
    val input: String = str
    val pattern: Pattern = Pattern.compile(""""mainsnak"""" + ":")
    val matcher: Matcher = pattern.matcher(input)
    var count: Double = 0.0
    while (matcher.find()) { count += 1; count - 1 }

    count

    count
  }
//5.
  def Get_NumberOfSiteLinks(str: String): Double = {

    // from Sitelink tag
    val input: String = str
    val pattern: Pattern = Pattern.compile(""""title"""" + ":")
    val matcher: Matcher = pattern.matcher(input)
    var count: Double = 0.0
    while (matcher.find()) { count += 1; count - 1 }

    count

    count
  }
//6.
  def Get_NumberOfstatements(str: String): Double = {

    // from claims tag
    val input: String = str
    val pattern: Pattern = Pattern.compile(""""statement"""")
    val matcher: Matcher = pattern.matcher(input)
    var count: Double = 0.0
    while (matcher.find()) { count += 1; count - 1 }

    count

    count
  }
  //7.

  def Get_NumberOfReferences(str: String): Double = {

    // from claims tag
    val input: String = str
    val pattern: Pattern = Pattern.compile(""""references"""" + ":")
    val matcher: Matcher = pattern.matcher(input)
    var count: Double = 0.0
    while (matcher.find()) { count += 1; count - 1 }

    count

    count
  }

  //8.
  def Get_NumberOfQualifier(str: String): Double = {

    // from claims tag
    val input: String = str
    val pattern: Pattern = Pattern.compile(""""qualifiers"""" + ":")
    val matcher: Matcher = pattern.matcher(input)
    var count: Double = 0.0
    while (matcher.find()) { count += 1; count - 1 }

    count

    count
  }
//9.
  def Get_NumberOfQualifier_Order(str: String): Double = {
    // from claims tag
    val input: String = str
    val pattern: Pattern = Pattern.compile(""""qualifiers-order"""" + ":")
    val matcher: Matcher = pattern.matcher(input)
    var count: Double = 0.0
    while (matcher.find()) { count += 1; count - 1 }

    count

    count
  }
//10.
  def Get_NumberOfBadges(str: String): Double = {

    // from Sitelink  tag
    val input: String = str
    val pattern: Pattern = Pattern.compile(""""badges"""" + ":")
    val matcher: Matcher = pattern.matcher(input)
    var count: Double = 0.0
    while (matcher.find()) { count += 1; count - 1 }

    count

    count
  }

}