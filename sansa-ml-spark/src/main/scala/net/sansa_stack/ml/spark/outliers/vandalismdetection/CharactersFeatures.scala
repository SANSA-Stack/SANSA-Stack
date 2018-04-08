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
//import org.apache.spark.mllib.classification.{ LogisticRegressionModel, LogisticRegressionWithLBFGS }
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

class CharactersFeatures extends Serializable {

  def Vector_Characters_Feature(StrValue: String): Vector = {

    
    // Reference for this :  http://www.mpi.nl/corpus/html/elan/apa.html
    var RatioValues = new Array[Double](25) //  Index is Important here
    val characterFeature_OBJ = new CharactersFeatures()
    //1.Double result Value for uppercase Ration
    val uppercase = characterFeature_OBJ.UppercaseRation_Character(StrValue)
    if (!uppercase.isNaN()) {
      RatioValues(0) = uppercase
    }
    //2.Double result Value for lowerCase Ratio
    val lowerCase = characterFeature_OBJ.LowercaseRation_Character(StrValue)
    if (!lowerCase.isNaN()) {
      RatioValues(1) = lowerCase
    }
    //3.Double result Value for  Alphanumeric Ratio
    val Alphanumeric = characterFeature_OBJ.AlphanumericRation_Character(StrValue)
    if (!Alphanumeric.isNaN()) {
      RatioValues(2) = Alphanumeric
    }
    //4.Double result Value for ASCII Ratio
    val ASCII = characterFeature_OBJ.ASCIIRation_Character(StrValue)
    if (!ASCII.isNaN()) {
      RatioValues(3) = ASCII
    }
    //5.Double result Value for Bracket Ratio
    val Bracket = characterFeature_OBJ.BracketRation_Character(StrValue)
    if (!Bracket.isNaN()) {
      RatioValues(4) = Bracket

    }
    //6.Double result Value for Digits Ratio
    val Digits = characterFeature_OBJ.DigitsRation_Character(StrValue)
    if (!Digits.isNaN()) {
      RatioValues(5) = Digits
    }
    //7.Double result Value for Latin Ratio
    val Latin = characterFeature_OBJ.Latin_Character(StrValue)
    if (!Latin.isNaN()) {
      RatioValues(6) = Latin
    }
    //8.Double result Value for WhiteSpace Ratio
    val WhiteSpace = characterFeature_OBJ.WhiteSpace_Character(StrValue)
    if (!WhiteSpace.isNaN()) {
      RatioValues(7) = WhiteSpace
    }
    //9.Double result Value for punc Ratio
    val punc = characterFeature_OBJ.Punct_Character(StrValue)
    if (!punc.isNaN()) {
      RatioValues(8) = punc
    }
    //10. Integer to Double result Value for LongCharacterSequence
    val LongCharacterSequence = characterFeature_OBJ.Longcharactersequence_Character(StrValue)
    if (!LongCharacterSequence.isNaN()) {
      RatioValues(9) = LongCharacterSequence
    }

    //11.Double result Value for ArabicCharacter
    val ArabicCharacter = characterFeature_OBJ.ArabicRation_Character(StrValue)
    if (!ArabicCharacter.isNaN()) {
      RatioValues(10) = ArabicCharacter
    }

    //12.Double result Value for Bengali
    val Bengali = characterFeature_OBJ.BengaliRation_Character(StrValue)
    if (!Bengali.isNaN()) {
      RatioValues(11) = Bengali

    }

    //13.Double result Value for Brahmi
    val Brahmi = characterFeature_OBJ.BrahmiRation_Character(StrValue)
    if (!Brahmi.isNaN()) {
      RatioValues(12) = Brahmi

    }

    //14.Double result Value for Cyrillic
    val Cyrillic = characterFeature_OBJ.CyrillicRation_Character(StrValue)
    if (!Cyrillic.isNaN()) {
      RatioValues(13) = Cyrillic

    }
    //15.Double result Value for Han
    val Han = characterFeature_OBJ.HanRatio_Character(StrValue)
    if (!Han.isNaN()) {
      RatioValues(14) = Han

    }

    //16.Double result Value for Malysia
    val Malysia = characterFeature_OBJ.MalaysRatio_Character(StrValue)
    if (!Malysia.isNaN()) {
      RatioValues(15) = Malysia
    }

    //17.Double result Value for Tami
    val Tami = characterFeature_OBJ.TamilRatio_Character(StrValue)
    if (!Tami.isNaN()) {
      RatioValues(16) = Tami
    }
    //18.Double result Value for Telugu
    val Telugu = characterFeature_OBJ.TeluguRatio_Character(StrValue)
    if (!Telugu.isNaN()) {
      RatioValues(17) = Telugu

    }
    //19.Double result Value for  Symbol
    val Symbol = characterFeature_OBJ.Symbol_Character(StrValue)
    if (!Symbol.isNaN()) {
      RatioValues(18) = Symbol

    }

    //20. Alphabets Ration:
    val Alphabets = characterFeature_OBJ.AlphaBetsRation_Character(StrValue)
    if (!Alphabets.isNaN()) {
      RatioValues(19) = Alphabets
    }
    //21. AVisible character Ratio:
    val Visible = characterFeature_OBJ.VisibleRation_Character(StrValue)
    if (!Visible.isNaN()) {
      RatioValues(20) = Visible
    }

    //22. Printable character Ratio:
    val Printable = characterFeature_OBJ.PrintableRation_Character(StrValue)
    if (!Printable.isNaN()) {
      RatioValues(21) = Printable
    }

    //23. Blank character Ratio:
    val Blank = characterFeature_OBJ.BlankRation_Character(StrValue)
    if (!Blank.isNaN()) {
      RatioValues(22) = Blank
    }

    //24.A control character:
    val Control = characterFeature_OBJ.ControlRation_Character(StrValue)
    if (!Control.isNaN()) {
      RatioValues(23) = Control
    }

    //25. A hexadecimal digit :
    val hexadecimal = characterFeature_OBJ.HexaRation_Character(StrValue)
    if (!hexadecimal.isNaN()) {
      RatioValues(24) = hexadecimal
    }

    val FacilityOBJ = new FacilitiesClass()
    val vector_Values = FacilityOBJ.ToVector(RatioValues)

    vector_Values
  }

  // Character Features: ------ start calculation the Ratio for character:
  def characterRatio(str: String, pattern: Pattern): Double = {
    var charRatio: Double = -1.0;
    if (str != null) {
      val tem: String = pattern.matcher(str).replaceAll("")
      val digits: Double = str.length() - tem.length()
      charRatio = digits / str.length().toDouble
    }
    charRatio
  }

  //1.Uppercase Ratio:
  def UppercaseRation_Character(str: String): Double = {
    val pattern: Pattern = Pattern.compile("\\p{javaUpperCase}")
    val result: Double = characterRatio(str, pattern)
    result
  }
  def LowercaseRation_Character(str: String): Double = {
    val pattern: Pattern = Pattern.compile("\\p{javaLowerCase}")
    val result: Double = characterRatio(str, pattern)
    result
  }
  //3.Alphanumeric
  def AlphanumericRation_Character(str: String): Double = {
    val pattern: Pattern = Pattern.compile("\\p{Alnum}")
    val result: Double = characterRatio(str, pattern)
    result
  }

  //4.ASCII
  def ASCIIRation_Character(str: String): Double = {
    val pattern: Pattern = Pattern.compile("\\p{ASCII}")
    val result: Double = characterRatio(str, pattern)
    result
  }
  //5.Bracket
  def BracketRation_Character(str: String): Double = {
    val pattern: Pattern = Pattern.compile("\\(|\\)|\\}|\\{|\\[|\\]")

    val result: Double = characterRatio(str, pattern)
    result
  }
  //6.Digits
  def DigitsRation_Character(str: String): Double = {
    val pattern: Pattern = Pattern.compile("\\d")
    val result: Double = characterRatio(str, pattern)
    result
  }
  //7.Latin
  def Latin_Character(str: String): Double = {
    val pattern: Pattern = Pattern.compile("\\p{IsLatin}")
    val result: Double = characterRatio(str, pattern)
    result
  }
  //8.WhiteSpace
  def WhiteSpace_Character(str: String): Double = {
    val pattern: Pattern = Pattern.compile("\\s")
    val result: Double = characterRatio(str, pattern)
    result
  }
  //9.Punct
  def Punct_Character(str: String): Double = {
    val pattern: Pattern = Pattern.compile("\\p{Punct}")
    val result: Double = characterRatio(str, pattern)
    result
  }
  //10.Long character sequence:
  def Longcharactersequence_Character(str: String): Double = {
    var text: String = str
    var maxlength: Integer = null
    if (str != null) {
      maxlength = 0
      var prevCharacter: Char = 'a'
      var prevPosision = 0
      text = text.trim()
      var i: Integer = 0
      for (i <- 0 until text.length) {
        val Currcharacter: Char = text.charAt(i)
        if (i > 0 && prevCharacter != Currcharacter) {

          if (i - prevPosision > maxlength) {
            maxlength = i - prevPosision
          }
          prevPosision = i
        }
        prevCharacter = Currcharacter
      }

      if (i > 0) {
        if (i - prevPosision > maxlength) {
          maxlength = prevPosision
        }
      }
    }

    val result: Double = maxlength.toDouble
    result

  }

  //11.ARabic Ratio:
  def ArabicRation_Character(str: String): Double = {
    val pattern: Pattern = Pattern.compile("\\p{IsArabic}")
    val result: Double = characterRatio(str, pattern)
    result
  }
  //12. Bengali Ratio
  def BengaliRation_Character(str: String): Double = {
    val pattern: Pattern = Pattern.compile("\\p{IsBengali}")
    val result: Double = characterRatio(str, pattern)
    result
  }
  //13.Brahmi Ratio
  def BrahmiRation_Character(str: String): Double = {
    val pattern: Pattern = Pattern.compile("\\p{IsBrahmi}")
    val result: Double = characterRatio(str, pattern)
    result
  }
  //14.Cyrillic Ratio
  def CyrillicRation_Character(str: String): Double = {
    val pattern: Pattern = Pattern.compile("\\p{IsCyrillic}")
    val result: Double = characterRatio(str, pattern)
    result
  }
  //15.HanRatio
  def HanRatio_Character(str: String): Double = {
    val pattern: Pattern = Pattern.compile("\\p{IsHan}")
    val result: Double = characterRatio(str, pattern)
    result
  }

  //16.Malaysian Ratio:
  def MalaysRatio_Character(str: String): Double = {
    val pattern: Pattern = Pattern.compile("\\p{IsMalayalam}")
    val result: Double = characterRatio(str, pattern)
    result
  }
  //17.Tamil Ratio:
  def TamilRatio_Character(str: String): Double = {
    val pattern: Pattern = Pattern.compile("\\p{IsTamil}")
    val result: Double = characterRatio(str, pattern)
    result
  }
  //18.Telugu Ration:
  def TeluguRatio_Character(str: String): Double = {
    val pattern: Pattern = Pattern.compile("\\p{IsTelugu}")
    val result: Double = characterRatio(str, pattern)
    result
  }

  //19.Symbols Ratio :
  def Symbol_Character(str: String): Double = {
    val pattern: Pattern = Pattern.compile("[#$%&@+-_+*/]*")
    val result: Double = characterRatio(str, pattern)
    result
  }
  //20.Alphabets Ratio :
  def AlphaBetsRation_Character(str: String): Double = {
    val pattern: Pattern = Pattern.compile("\\p{Alpha}")
    val result: Double = characterRatio(str, pattern)
    result
  }
  //21.A visible character Ratio:
  def VisibleRation_Character(str: String): Double = {
    val pattern: Pattern = Pattern.compile("\\p{Graph}")
    val result: Double = characterRatio(str, pattern)
    result
  }
  //22.A printable character
  def PrintableRation_Character(str: String): Double = {
    val pattern: Pattern = Pattern.compile("\\p{Print}")
    val result: Double = characterRatio(str, pattern)
    result
  }

  //23.A Black(it is different from White space) character Ratio
  def BlankRation_Character(str: String): Double = {
    val pattern: Pattern = Pattern.compile("\\p{Blank}")
    val result: Double = characterRatio(str, pattern)
    result
  }

  //24.Control character  Ratio
  def ControlRation_Character(str: String): Double = {
    val pattern: Pattern = Pattern.compile("\\p{Cntrl}")
    val result: Double = characterRatio(str, pattern)
    result
  }

  //25.HexaDecimal character Ratio
  def HexaRation_Character(str: String): Double = {
    val pattern: Pattern = Pattern.compile("\\p{XDigit}")
    val result: Double = characterRatio(str, pattern)
    result
  }
  // Character features: ------ End calculation the Ratio for character:

}