package net.sansa_stack.ml.spark.kge.linkprediction.dataset

import org.apache.spark.sql._

class Dataset(path: String, sep: String, header: String, sk: SparkSession) {

  val tb = read()
  val (s, p) = (gets(), getp())
  val df = numeric()

  def read() = {
    sk.read.format("com.databricks.spark.csv")
      .option("header", header)
      .option("inferSchema", "false")
      .option("delimiter", sep)
      .load(path)
  }

  import sk.implicits._

  def numeric() = {

    val df = read()
    var aux = Seq[(Int, Int, Int)]()

    df.collect().map { i =>
      aux = (s.indexOf(i.getString(0)).toInt + 1, p.indexOf(i.getString(1)).toInt + 1,
        s.indexOf(i.getString(2)).toInt + 1) +: aux
    }

    aux.toDF()
  }

  def gets() = {
    (tb.select("_c0").collect.map(_.getString(0)) ++
      tb.select("_c2").collect.map(_.getString(0))).distinct
  }

  def getp() = {
    (tb.select("_c1").collect.map(_.getString(0))).distinct
  }

}