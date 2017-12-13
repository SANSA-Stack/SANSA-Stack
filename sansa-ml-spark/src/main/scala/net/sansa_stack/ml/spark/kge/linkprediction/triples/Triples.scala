package net.sansa_stack.ml.spark.kge.linkprediction.triples

/**
 * Triples Class
 * ------------------------
 *
 * Created by Hamed Shariat Yazdi
 */

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types._

class Triples(filePath: String, delimiter: String = "\t", header: Boolean = false,
              numeric: Boolean = false, spark: SparkSession) {

  import spark.implicits._

  val stringSchema = StructType(Array(
    StructField("Subject", StringType, true),
    StructField("Predicate", StringType, true),
    StructField("Object", StringType, true)))

  val integerSchema = StructType(Array(
    StructField("Subject", IntegerType, true),
    StructField("Predicate", IntegerType, true),
    StructField("Object", IntegerType, true)))

  val (e, r) = (getEntities(), getRelations())
  val triples = readFromFile()

  def getEntities() = {
    triples.select($"Subject").union(triples.select($"Object")).distinct().collect()
  }

  def getRelations() = {
    triples.select("Predicate").distinct().collect()
  }

  def readFromFile() = {

    val schema = numeric match {
      case true => integerSchema
      case _    => stringSchema
    }

    var data = spark.read.format("com.databricks.spark.csv")
      .option("header", header.toString())
      .option("inferSchema", false)
      .option("delimiter", delimiter)
      .schema(schema)
      .load(filePath)

    if (numeric) {
      data.as[IntegerTriples]
    }

    data.as[StringTriples]
  }

}