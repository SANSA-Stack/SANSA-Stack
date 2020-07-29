package net.sansa_stack.ml.spark.utils

import org.apache.spark.ml.Transformer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


class FeatureExtractorModel {
  val spark = SparkSession.builder.getOrCreate()
  private val _availableModes = Array("an", "in", "on", "ar", "ir", "or", "at", "ir", "ot", "as", "is", "os")
  private var _mode: String = "at"
  private var _outputCol: String = "features"

  def setMode(mode: String): this.type = {
    if (_availableModes.contains(mode)) {
      _mode = mode
      this
    }
    else {
      throw new Exception("The specified mode: " + mode + "is not supported. Currently available are: " + _availableModes)
    }
  }

  def setOutputCol(colName: String): this.type = {
    _outputCol = colName
    this
  }

  def transform(df: DataFrame): DataFrame = {

    val dfAsRdd: RDD[Row] = df.rdd

    val unfoldedFeatures: RDD[Tuple2[String, String]] = _mode match {
      case "at" => dfAsRdd.flatMap(r => Seq((r.getString(0), ("-" + r.getString(1) + "->" + r.getString(2))), (r.getString(2), ("<-" + r.getString(1) + "-" + r.getString(0)))))
      case "it" => dfAsRdd.flatMap(r => Seq((r.getString(2), ("-" + r.getString(1) + "->" + r.getString(0)))))
      case "ot" => dfAsRdd.flatMap(r => Seq((r.getString(0), ("<-" + r.getString(1) + "-" + r.getString(2)))))
      case "an" => dfAsRdd.flatMap(r => Seq((r.getString(0), (r.getString(2))), (r.getString(2), (r.getString(0)))))
      case "in" => dfAsRdd.flatMap(r => Seq((r.getString(2), (r.getString(0)))))
      case "on" => dfAsRdd.flatMap(r => Seq((r.getString(0), (r.getString(2)))))
      case "ar" => dfAsRdd.flatMap(r => Seq((r.getString(0), ("-" + r.getString(1) + "->")), (r.getString(2), ("<-" + r.getString(1) + "-"))))
      case "ir" => dfAsRdd.flatMap(r => Seq((r.getString(2), ("<-" + r.getString(1) + "-"))))
      case "or" => dfAsRdd.flatMap(r => Seq((r.getString(0), ("-" + r.getString(1) + "->"))))
      case "as" => dfAsRdd.flatMap(r => Seq((r.getString(0), (r.getString(2))), (r.getString(2), (r.getString(0))), (r.getString(0), ("-" + r.getString(1) + "->")), (r.getString(2), ("<-" + r.getString(1) + "-"))))
      case "is" => dfAsRdd.flatMap(r => Seq((r.getString(2), (r.getString(0))), (r.getString(2), ("<-" + r.getString(1) + "-"))))
      case "os" => dfAsRdd.flatMap(r => Seq((r.getString(0), (r.getString(2))), (r.getString(0), ("-" + r.getString(1) + "->"))))
      case _ => throw new Exception("This mode is currently not supported .\n You selected mode " + _mode + " .\n Currently available modes are: " + _availableModes)
    }
    val tmpRdd: RDD[Row] = unfoldedFeatures
      .filter(t => !(t._1.contains("\"")))
      .groupBy(_._1)
      .mapValues(_.map(_._2))
      .map(tuple => Row(tuple._1, tuple._2.toArray))
    val tmpDf = spark.createDataFrame(
      tmpRdd,
      new StructType()
        .add(StructField("uri", StringType, true))
        .add(StructField(_outputCol, ArrayType(StringType, true), true))
    )
    tmpDf

    /* val dfAsArray: Array[Row] = df.collect()

    val unfoldedFeatures: Array[Tuple2[String, String]] = _mode match {
      case "at" => dfAsArray.flatMap(r => Seq((r.getString(0), ("-" + r.getString(1) + "->" + r.getString(2))), (r.getString(2), ("<-" + r.getString(1) + "-" + r.getString(0)))))
      case "it" => dfAsArray.flatMap(r => Seq((r.getString(2), ("-" + r.getString(1) + "->" + r.getString(0)))))
      case "ot" => dfAsArray.flatMap(r => Seq((r.getString(0), ("<-" + r.getString(1) + "-" + r.getString(2)))))
      case "an" => dfAsArray.flatMap(r => Seq((r.getString(0), (r.getString(2))), (r.getString(2), (r.getString(0)))))
      case "in" => dfAsArray.flatMap(r => Seq((r.getString(2), (r.getString(0)))))
      case "on" => dfAsArray.flatMap(r => Seq((r.getString(0), (r.getString(2)))))
      case "ar" => dfAsArray.flatMap(r => Seq((r.getString(0), ("-" + r.getString(1) + "->")), (r.getString(2), ("<-" + r.getString(1) + "-"))))
      case "ir" => dfAsArray.flatMap(r => Seq((r.getString(2), ("<-" + r.getString(1) + "-"))))
      case "or" => dfAsArray.flatMap(r => Seq((r.getString(0), ("-" + r.getString(1) + "->"))))
      case "as" => dfAsArray.flatMap(r => Seq((r.getString(0), (r.getString(2))), (r.getString(2), (r.getString(0))), (r.getString(0), ("-" + r.getString(1) + "->")), (r.getString(2), ("<-" + r.getString(1) + "-"))))
      case "is" => dfAsArray.flatMap(r => Seq((r.getString(2), (r.getString(0))), (r.getString(2), ("<-" + r.getString(1) + "-"))))
      case "os" => dfAsArray.flatMap(r => Seq((r.getString(0), (r.getString(2))), (r.getString(0), ("-" + r.getString(1) + "->"))))
      case _ => throw new Exception("This mode is currently not supported .\n You selected mode " + _mode + " .\n Currently available modes are: " + _availableModes)
    }
    val colappsedSeq: Seq[Tuple2[String, Array[String]]] = unfoldedFeatures.groupBy(_._1).mapValues(_.map(_._2)).toSeq
    // workaround to filter for URIs ... improvements for sure possible!
    val onlyUri: Seq[Tuple2[String, Array[String]]] = colappsedSeq.filter(t => !(t._1.contains("\"")))
    spark.createDataFrame(onlyUri).toDF("uri", _outputCol) */
  }
}
