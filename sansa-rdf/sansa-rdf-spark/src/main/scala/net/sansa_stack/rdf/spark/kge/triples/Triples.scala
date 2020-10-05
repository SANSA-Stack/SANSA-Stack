package net.sansa_stack.rdf.spark.kge.triples

/**
 * Triples Class
 * -------------
 *
 * Read the file and return an class with entities, relations and triples
 *
 * Created by Hamed Shariat Yazdi
 */

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import scala.reflect.api.materializeTypeTag

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

  val triples = readFromFile()
  val (e, r) = (getEntities(), getRelations())

  def getEntities(): Array[Row] = {
    triples.select($"Subject").union(triples.select($"Object")).distinct().collect()
  }

  def getRelations(): Array[Row] = {
    triples.select("Predicate").distinct().collect()
  }

  def readFromFile(): Dataset[StringTriples] = {

    val schema = numeric match {
      case true => integerSchema
      case _ => stringSchema
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
