package net.sansa_stack.ml.spark.utils

import org.apache.spark.rdd.RDD
import org.apache.jena.graph.{Node, Triple}
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.sql.{DataFrame, SparkSession}


class RDF_Feature_Extractor {

  private val _available_modes = Array("an", "in", "on", "ar", "ir", "or", "at", "ir", "ot", "as", "is", "os")
  val spark = SparkSession.builder.getOrCreate()

  private var _mode: String = _
  private var _uri_key_column_name: String = "uri"
  private var _features_column_name: String = "features"

  def set_mode(mode: String): this.type = {
    if (_available_modes.contains(mode)) {
      _mode = mode
      this
    }
    else {
      throw new Exception("The specified mode: " + mode + "is not supported. Currently available are: " + _available_modes)
    }
  }

  def set_uri_key_column_name(uri_key_column_name: String): this.type = {
    _uri_key_column_name = uri_key_column_name
    this
  }

  def set_features_column_name(features_column_name: String): this.type = {
    _features_column_name = features_column_name
    this
  }

  def transform(triples: RDD[Triple]): DataFrame = {

    val tmp_content_column_name = "tmp_string_content"

    val unfolded_features: RDD[(Node, String)] = _mode match {
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
      case _ => throw new Exception("This mode is currently not supported .\n You selected mode " + _mode + " .\n Currently available modes are: " + _available_modes)
    }
    val tmp_df = spark.createDataFrame(unfolded_features
      .filter(_._1.isURI) //
      .map({ case (k, v) => (k.toString(), v) }) //
      .mapValues(_.replaceAll("\\s", "")) //
      .groupBy(_._1) //
      .mapValues(_.map(_._2)) //
      .map({ case (k, v) => (k, v.reduceLeft(_ + " " + _)) }) //
      .collect()
      .toSeq
    ).toDF(colNames = _uri_key_column_name, tmp_content_column_name)

    val tokenizer = new Tokenizer().setInputCol(tmp_content_column_name).setOutputCol(_features_column_name)
    tokenizer.transform(tmp_df).select(_uri_key_column_name, _features_column_name)
  }
}
