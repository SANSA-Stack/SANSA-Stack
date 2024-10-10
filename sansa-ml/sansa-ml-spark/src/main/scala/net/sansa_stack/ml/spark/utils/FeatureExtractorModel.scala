package net.sansa_stack.ml.spark.utils

import org.apache.jena.graph.{Node, Triple}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.collect_list
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * This class creates from a dataset of triples a feature representing dataframe which is needed for steps like spark mllib countVect
 */
class FeatureExtractorModel extends Transformer {
  val spark = SparkSession.builder().getOrCreate()
  private val _availableModes = Array("an", "in", "on", "ar", "ir", "or", "at", "ir", "ot", "as", "is", "os")
  private var _mode: String = "at"
  private var _outputCol: String = "extractedFeatures"

  /**
   * This method changes the methodology how we flat our graph to get from each URI the desired feature
   * @param mode a string specifying the modes. moders are abbreviations like "at" for all triples
   * @return return da dataframe with two columns one for the string of a respective URI and one for the feature vector al list of strings
   */
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

  /**
   * takes read in dataframe and produces a dataframe with features
   * @param dataset most likely a dataframe read in over sansa rdf layer
   * @return a dataframe with two columns, one for string of URI and one of a list of string based features
   */
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

  /**
   * this is the alternative transform when you read in rdd of triple of jena node over sansa rdf
   *
   * @param triples rdd of triple of jena node
   * @return a dataframe with two columns, one for string of URI and one of a list of string based features
   */
  def transform(triples: RDD[Triple]): DataFrame = {

    val tmpContentColumnName = "tmp_string_content"

    val unfoldedFeatures: RDD[(Node, String)] = _mode match {
      case "at" => triples.flatMap(t => Seq((t.getSubject, t.getPredicate.toString() + t.getObject.toString()), (t.getObject, t.getSubject.toString() + t.getPredicate.toString()))) // 4.1
      case "it" => triples.flatMap(t => Seq((t.getObject, t.getSubject.toString() + t.getPredicate.toString())))
      case "ot" => triples.flatMap(t => Seq((t.getSubject, t.getPredicate.toString() + t.getObject.toString()))) // 4.1
      case "an" => triples.flatMap(t => Seq((t.getSubject, t.getObject.toString()), (t.getObject, t.getSubject.toString())))
      case "in" => triples.flatMap(t => Seq((t.getObject, t.getSubject.toString())))
      case "on" => triples.flatMap(t => Seq((t.getSubject, t.getObject.toString())))
      case "ar" => triples.flatMap(t => Seq((t.getSubject, t.getPredicate.toString()), (t.getObject, t.getPredicate.toString())))
      case "ir" => triples.flatMap(t => Seq((t.getObject, t.getPredicate.toString())))
      case "or" => triples.flatMap(t => Seq((t.getSubject, t.getPredicate.toString())))
      case "as" => triples.flatMap(t => Seq((t.getSubject, t.getObject.toString()), (t.getObject, t.getSubject.toString()), (t.getSubject, t.getPredicate.toString()), (t.getObject, t.getPredicate.toString())))
      case "is" => triples.flatMap(t => Seq((t.getObject, t.getSubject.toString()), (t.getObject, t.getPredicate.toString())))
      case "os" => triples.flatMap(t => Seq((t.getSubject, t.getObject.toString()), (t.getSubject, t.getPredicate.toString())))
      case _ => throw new Exception("This mode is currently not supported .\n You selected mode " + _mode + " .\n Currently available modes are: " + _availableModes)
    }
    val tmp_df = spark.createDataFrame(unfoldedFeatures
      .filter(_._1.isURI) //
      .map({ case (k, v) => (k.toString(), v) }) //
      .mapValues(_.replaceAll("\\s", "")) //
      .groupBy(_._1) //
      .mapValues(_.map(_._2)) //
      .map({ case (k, v) => (k, v.reduceLeft(_ + " " + _)) }) //
      .collect()
      .toSeq
    ).toDF(colNames = "uri", tmpContentColumnName)

    val tokenizer = new Tokenizer().setInputCol(tmpContentColumnName).setOutputCol(_outputCol)
    tokenizer.transform(tmp_df).select("uri", _outputCol)
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType =
    throw new NotImplementedError()

  override val uid: String = "FIXME"
}
