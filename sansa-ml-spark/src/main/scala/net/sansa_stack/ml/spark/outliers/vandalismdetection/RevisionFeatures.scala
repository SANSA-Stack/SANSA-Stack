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
import org.apache.spark.broadcast
import java.lang.NumberFormatException
import org.apache.spark.sql._
import java.util.Objects
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser
import java.util.List;
//import scala.util.parsing.json._
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind._
import java.math.BigInteger;
import java.net.InetAddress;
import java.util.Map.Entry;
import java.util.TreeMap;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.Partitioner._
import org.apache.spark.RangePartitioner
import java.util.Set
import java.util.HashSet
class RevisionFeatures extends Serializable {

  // Contain language Latin :

  val latinRegex_Str: String = "(af|ak|an|ang|ast|ay|az|bar|bcl|bi|bm|br|bs|ca|cbk-zam|ceb|ch|chm|cho|chy|co|crh-latn|cs|csb|cv|cy|da|de|diq|dsb|ee|eml|en|eo|es|et|eu|ff|fi|fj|fo|fr|frp|frr|fur|fy|ga|gd|gl|gn|gsw|gv|ha|haw|ho|hr|hsb|ht|hu|hz|id|ie|ig|ik|ilo|io|is|it|jbo|jv|kab|kg|ki|kj|kl|kr|ksh|ku(?!-arab\b)|kw|la|lad|lb|lg|li|lij|lmo|ln|lt|lv|map-bms|mg|mh|min?|ms|mt|mus|mwl|na|nah|nan|nap|nb|nds|nds-nl|ng|nl|nn|nov|nrm|nv|ny|oc|om|pag|pam|pap|pcd|pdc|pih|pl|pms|pt|qu|rm|rn|ro|roa-tara|rup|rw|sc|scn|sco|se|sg|sgs|sk|sl|sm|sn|so|sq|sr-el|ss|st|stq|su|sv|sw|szl|tet|tk|tl|tn|to|tpi|tr|ts|tum|tw|ty|uz|ve|vec|vi|vls|vo|vro|wa|war|wo|xh|yo|za|zea|zu)"
  val pattern_ContainLanguage_Latin: Pattern = Pattern.compile(latinRegex_Str);
  val matcher_ContainLanguage_Latin: Matcher = pattern_ContainLanguage_Latin.matcher("");

  val Non_latinRegex_Str: String = "(ab|am|arc|ar|arz|as|ba|be|be-tarask|bg|bh|bn|bo|bpy|bxr|chr|ckb|cr|cv|dv|dz|el|fa|gan|glk|got|gu|hak|he|hi|hy|ii|iu|ja|ka|kbd|kk|km|kn|ko|koi|krc|ks|ku-arab|kv|ky|lbe|lez|lo|mai|mdf|mhr|mk|ml|mn|mo|mr|mrj|my|myv|mzn|ne|new|or|os|pa|pnb|pnt|ps|ru|rue|sa|sah|sd|si|sr|ta|te|tg|th|ti|tt|tyv|udm|ug|uk|ur|wuu|xmf|yi|zh|zh-classical|zh-hans|zh-hant|zh-tw|zh-cn|zh-hk|zh-sg)"
  val pattern_ContainLanguage_NonLatin: Pattern = Pattern.compile(Non_latinRegex_Str);
  val matcher_ContainLanguage_NonLatin: Matcher = pattern_ContainLanguage_NonLatin.matcher("");

  def Check_ContainLanguageLatin_NonLatin(str: String): Boolean = {

    var Final_Result = false
    var text: String = str
    var result_isLatin: Boolean = false
    var result_isNonLatin: Boolean = false

    if (text != null) {
      text = text.trim()
      text = text.toLowerCase()
      result_isLatin = matcher_ContainLanguage_Latin.reset(text).matches()

    }

    if (text != null) {
      text = text.trim()
      text = text.toLowerCase()
      result_isNonLatin = matcher_ContainLanguage_NonLatin.reset(text).matches()

    }

    if (result_isLatin == true) { // is matched

      Final_Result = true
    } else {
      Final_Result = false

    }

    //      if (result_isNonLatin==true){ // is matched
    //
    //        Final_Result=false
    //
    //      }

    Final_Result
  }

  // For conentType:
  def getContentType(action: String): String = {
    var result: String = null
    if (action == null) {
      result = "MISC"
    } else {
      action match {
        case "wbcreateclaim" | "wbsetclaim" | "wbremoveclaims" |
          "wbsetclaimvalue" | "wbsetreference" | "wbremovereferences" |
          "wbsetqualifier" | "wbremovequalifiers" =>
          result = "STATEMENT"
        case "wbsetsitelink" | "wbcreateredirect" | "clientsitelink" |
          "wblinktitles" =>
          result = "SITELINK"
        case "wbsetaliases" | "wbsetdescription" | "wbsetlabel" =>
          result = "TEXT"
        case "wbeditentity" | "wbsetentity" | "special" | "wbcreate" |
          "wbmergeitems" | "rollback" | "undo" | "restore" | "pageCreation" |
          "emptyComment" | "setPageProtection" | "changePageProtection" |
          "removePageProtection" | "unknownCommentType" | "null" | "" =>
          result = "MISC"
        case _ =>
          result = "MISC"
      }
    }
    result
  }

  def Extract_Revision_Language(Full_Comment_Str: String): String = {

    var langeType = ""
    val parsedCommment_OBJ = new CommentProcessor()
    val flag = parsedCommment_OBJ.Check_CommentNormal_Or_Not(Full_Comment_Str)

    if (flag == true) { // it is normal comment

      //   val sitelink_Word = Full_Comment_Str.contains("sitelink")
      //  if (sitelink_Word == true) { // language class is between | and */( e.g | enwiki */)
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

      //   }

    } else {

      langeType = "NA"
    }

    langeType.trim()

  }

}

  
     

