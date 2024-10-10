package net.sansa_stack.query.spark.playground

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest._

class TestSparkSqlJoin extends AnyFlatSpec {

  "SPARK SQL processor" should "be capable of handling transitive join conditions" in {

    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("Spark SQL parser bug")
      .getOrCreate()

    spark.conf.set("spark.sql.crossJoin.enabled", "true")

    // The schema is encoded in a string
    val schemaString = "s p o"

    // Generate the schema based on the string of schema
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    val data = List(("s1", "p1", "o1"))
    val dataRDD = spark.sparkContext.parallelize(data).map(attributes => Row(attributes._1, attributes._2, attributes._3))
    val df = spark.createDataFrame(dataRDD, schema).as("TRIPLES").cache()
    df.createOrReplaceTempView("TRIPLES")

    println("First Query")
    spark.sql("SELECT A.s FROM TRIPLES A, TRIPLES B WHERE A.s = B.s AND A.s = 'dbr:Leipzig'").show(10)

    println("Second Query")
    spark.sql("SELECT A.s FROM TRIPLES A, TRIPLES B WHERE A.s = 'dbr:Leipzig' AND B.s = 'dbr:Leipzig'").show(10)
  }

}
