package net.sansa_stack.ml.spark.kge.linkprediction.convertor

/**
 * ByIndex Class
 * -------------
 *
 * Created by lpfgarcia on 27/11/2017.
 */

import org.apache.spark.sql._

class ByIndex(data: DataFrame, sk: SparkSession) extends Convertor(data: DataFrame) {

  val (e, r) = (entities(), relations())
  val df = numeric()

  def entities() = {
    (data.select("Subject").collect.map(_.getString(0)) ++
      data.select("Object").collect.map(_.getString(0))).distinct
  }

  def relations() = {
    (data.select("Predicate").collect.map(_.getString(0))).distinct
  }

  import sk.implicits._

  def numeric() = {

    var df = Seq[(Int, Int, Int)]()

    data.collect().map { i =>
      df = (e.indexOf(i.getString(0)).toInt + 1, r.indexOf(i.getString(1)).toInt + 1,
        e.indexOf(i.getString(2)).toInt + 1) +: df
    }

    df.toDF()
  }

}