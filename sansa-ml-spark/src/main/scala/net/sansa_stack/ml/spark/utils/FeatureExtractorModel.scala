package net.sansa_stack.ml.spark.utils

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.collect_list
import org.apache.spark.sql.types.StructType


class FeatureExtractorModel extends Transformer {
  val spark = SparkSession.builder.getOrCreate()
  private val _availableModes = Array("an", "in", "on", "ar", "ir", "or", "at", "ir", "ot", "as", "is", "os")
  private var _mode: String = "at"
  private var _outputCol: String = "extractedFeatures"

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

  def transform(dataset: Dataset[_]): DataFrame = {
    import spark.implicits._

    val ds: Dataset[(String, String, String)] = dataset.as[(String, String, String)]
    val unfoldedFeatures: Dataset[(String, String)] = _mode match {
      case "at" => ds.flatMap(t => Seq(
        (t._1, "-" + t._2 + "->" + t._3),
        (t._3, "<-" + t._2 + "-" + t._1)))
      case "it" => ds.flatMap(t => Seq(
        (t._3, "-" + t._2 + "->" + t._1)))
      case "ot" => ds.flatMap(t => Seq(
        (t._1, "<-" + t._2 + "-" + t._3)))
      case "an" => ds.flatMap(t => Seq(
        (t._1, t._3),
        (t._3, t._1)))
      case "in" => ds.flatMap(t => Seq(
        (t._3, t._1)))
      case "on" => ds.flatMap(t => Seq(
        (t._1, t._3)))
      case "ar" => ds.flatMap(t => Seq(
        (t._1, "-" + t._2 + "->"),
        (t._3, "<-" + t._2 + "-")))
      case "ir" => ds.flatMap(t => Seq(
        (t._3, "<-" + t._2 + "-")))
      case "or" => ds.flatMap(t => Seq(
        (t._1, "-" + t._2 + "->")))
      case "as" => ds.flatMap(t => Seq(
        (t._1, t._3),
        (t._3, t._1),
        (t._1, "-" + t._2 + "->"),
        (t._3, "<-" + t._2 + "-")))
      case "is" => ds.flatMap(t => Seq(
        (t._3, t._1),
        (t._3, "<-" + t._2 + "-")))
      case "os" => ds.flatMap(t => Seq(
        (t._1, t._3),
        (t._1, "-" + t._2 + "->")))
      case _ => throw new Exception(
        "This mode is currently not supported .\n " +
          "You selected mode " + _mode + " .\n " +
          "Currently available modes are: " + _availableModes)
    }

    val tmpDs = unfoldedFeatures
      .filter(!_._1.contains("\""))
      .groupBy("_1")
      .agg(collect_list("_2"))

    tmpDs
      .withColumnRenamed("_1", "uri")
      .withColumnRenamed("collect_list(_2)", _outputCol)
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType =
    throw new NotImplementedError()

  override val uid: String = "FIXME"
}
