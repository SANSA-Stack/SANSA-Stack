package org.dissect.inference.data

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

/**
  * @author Lorenz Buehmann
  */
object EmptyRDFGraphDataFrame {

  def get(sqlContext: SQLContext): DataFrame = {
    // convert RDD to DataFrame
    val schemaString = "subject predicate object"

    // generate the schema based on the string of schema
    val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

    // convert triples RDD to rows
    val rowRDD = sqlContext.sparkContext.emptyRDD[Row]

    // apply the schema to the RDD
    val triplesDataFrame = sqlContext.createDataFrame(rowRDD, schema)

    // register the DataFrame as a table
    triplesDataFrame.createOrReplaceTempView("TRIPLES")

    triplesDataFrame
  }
}