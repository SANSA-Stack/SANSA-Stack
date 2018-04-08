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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SentencesFeature extends Serializable {

  //1.comment tail Lenght  Action subaction param+ tail
  def CommentTailLenght(Full_Comment_Str: String): Integer = {
    val parsedCommment_OBJ = new CommentProcessor()
    val commentTail_Str = parsedCommment_OBJ.Extract_CommentTail(Full_Comment_Str)
    val commentTail_Length = commentTail_Str.length()
    commentTail_Length

  }
  // similarity  between the comment ( suffix of the comment = Tail ) where the comment is normal comment /* .........*/ or  /* .........
  // e.g This comment includes wb...sitelink
  //1-we have to be sure the comment is normal comment take the form /* ........./*
  //2-Next step: we check the Action part if it includes a sitelink word or not.
  //3-we compare the suffix in this case to  site link with pay attention to  the same language.

  // we check the type of Normal comment if it contains Aliases  .
  def extract_CommentAliases_LanguageType(Full_Comment_Str: String): String = {

    var langeType = ""
    var suffix = ""

    val parsedCommment_OBJ = new CommentProcessor()
    val flag = parsedCommment_OBJ.Check_CommentNormal_Or_Not(Full_Comment_Str)

    if (flag == true) { // it is normal comment

      val sitelink_Word = Full_Comment_Str.contains("aliases")

      if (sitelink_Word == true) { // language class is between | and */( e.g | enwiki */)

        val start_point: Int = Full_Comment_Str.indexOf("|")
        val end_point: Int = Full_Comment_Str.indexOf("*/")
        if (start_point != -1 && end_point != -1) {

          val language = Full_Comment_Str.substring(start_point + 1, end_point)
          if (language.nonEmpty) {
            langeType = language.trim()
          } else {
            langeType = "NA"
          }
        }

        val SuffixCommment_OBJ = new CommentProcessor()
        val suffixComment = SuffixCommment_OBJ.Extract_CommentTail(Full_Comment_Str)

        if (suffixComment != "NA") {
          suffix = suffixComment.trim()

        }

      }

    }

    suffix.trim() + "_" + langeType.trim()

  }

  // we check the type of Normal comment if it contains Description  .
  def extract_CommentDescription_LanguageType(Full_Comment_Str: String): String = {

    var langeType = ""
    var suffix = ""

    val parsedCommment_OBJ = new CommentProcessor()
    val flag = parsedCommment_OBJ.Check_CommentNormal_Or_Not(Full_Comment_Str)

    if (flag == true) { // it is normal comment

      val sitelink_Word = Full_Comment_Str.contains("description")

      if (sitelink_Word == true) { // language class is between | and */( e.g | enwiki */)

        val start_point: Int = Full_Comment_Str.indexOf("|")
        val end_point: Int = Full_Comment_Str.indexOf("*/")
        if (start_point != -1 && end_point != -1) {

          val language = Full_Comment_Str.substring(start_point + 1, end_point)
          if (language.nonEmpty) {
            langeType = language.trim()
          } else {
            langeType = "NA"
          }
        }

        val SuffixCommment_OBJ = new CommentProcessor()
        val suffixComment = SuffixCommment_OBJ.Extract_CommentTail(Full_Comment_Str)

        if (suffixComment != "NA") {
          suffix = suffixComment.trim()

        }

      }

    }

    suffix.trim() + "_" + langeType.trim()

  }

  // we check the type of Normal comment if it contains label  .
  def extract_CommentLabel_LanguageType(Full_Comment_Str: String): String = {

    var langeType = ""
    var suffix = ""

    val parsedCommment_OBJ = new CommentProcessor()
    val flag = parsedCommment_OBJ.Check_CommentNormal_Or_Not(Full_Comment_Str)

    if (flag == true) { // it is normal comment

      val sitelink_Word = Full_Comment_Str.contains("label")

      if (sitelink_Word == true) { // language class is between | and */( e.g | en */)

        val start_point: Int = Full_Comment_Str.indexOf("|")
        val end_point: Int = Full_Comment_Str.indexOf("*/")
        if (start_point != -1 && end_point != -1) {

          val language = Full_Comment_Str.substring(start_point + 1, end_point)
          if (language.nonEmpty) {
            langeType = language.trim()
          } else {
            langeType = "NA"
          }
        }

        val SuffixCommment_OBJ = new CommentProcessor()
        val suffixComment = SuffixCommment_OBJ.Extract_CommentTail(Full_Comment_Str)

        if (suffixComment != "NA") {
          suffix = suffixComment.trim()

        }

      }

    }

    langeType.trim()

  }

  // we check the type of Normal comment if it contains Sitelink
  def extract_CommentSiteLink_LanguageType(Full_Comment_Str: String): String = {

    var langeType = ""
    val parsedCommment_OBJ = new CommentProcessor()
    val flag = parsedCommment_OBJ.Check_CommentNormal_Or_Not(Full_Comment_Str)

    if (flag == true) { // it is normal comment

      val sitelink_Word = Full_Comment_Str.contains("sitelink")
      if (sitelink_Word == true) { // language class is between | and */( e.g | enwiki */)
        val start_point: Int = Full_Comment_Str.indexOf("|")
        val end_point: Int = Full_Comment_Str.indexOf("*/")
        if (start_point != -1 && end_point != -1) {
          val language = Full_Comment_Str.substring(start_point + 1, end_point)
          if (language.nonEmpty) {
            langeType = language.trim()
          } else {
            langeType = "NA"
          }
        }

      }

    }

    langeType.trim()

  }

}