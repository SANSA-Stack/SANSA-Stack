package net.sansa_stack.spark.playground

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest._

/**
  * @author Lorenz Buehmann
  */
class TestSparkSqlJoin extends FlatSpec {

  "SPARK SQL processor" should "be capable of handling transitive join conditions" in {


    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("Spark SQL parser bug")
      .getOrCreate()

    import spark.implicits._

    // The schema is encoded in a string
    val schemaString = "s p o"

    // Generate the schema based on the string of schema
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    val data = List(("s1", "p1", "o1"))
    val dataRDD = spark.sparkContext.parallelize(data).map(attributes => Row(attributes._1, attributes._2, attributes._3))
    val df = spark.createDataFrame(dataRDD, schema).as("TRIPLES")
    df.createOrReplaceTempView("TRIPLES")

    spark.sql("SELECT A.s FROM TRIPLES A, TRIPLES B WHERE A.s = 'dbr:Leipzig' AND B.s = 'dbr:Leipzig'").show(10)
  }

}