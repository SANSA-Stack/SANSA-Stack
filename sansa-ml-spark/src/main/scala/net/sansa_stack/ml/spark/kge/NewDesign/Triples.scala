package net.sansa_stack.ml.spark.kge.linkprediction.NewDesign


import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import scala.util.Random
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import scala.concurrent.forkjoin.ThreadLocalRandom
import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import scala.collection.Seq
import scala.reflect.api.materializeTypeTag


class Triples ( name: String, 
		filePathTriples : String,
		spark : SparkSession) {


	import spark.implicits._  // to be able to work with """spark.sql("blah blah").as[String].rdd"""

	val schema = StructType(Array(
    StructField("Subject", StringType, true),
    StructField("Predicate", StringType, true),
    StructField("Object", StringType, true)))
			
	implicit 	val encoder = RowEncoder(schema)

	var triples : Dataset[RecordStringTriples] = readFromFile()
	

	def readFromFile(delimiter : String = "\t", 
	                 header : Boolean = false) : Dataset[RecordStringTriples] = {

			triples = spark.read.format("com.databricks.spark.csv")
					.option("header", header.toString() )
					.option("inferSchema",false)
					.option("delimiter", delimiter)
					.schema(schema)
					.load(filePathTriples)
					.as[RecordStringTriples]

			return triples
	}


	def getAllDistinctEntities() : Dataset[String] = {

	  val query = triples.select($"Subject")
	                     .union(triples.select($"Object"))
	                     .distinct()
	                   
	  return query.withColumnRenamed(query.columns(0),"Entities").as[String]
	}

	def getAllDistinctPredicates() : Dataset[String] = {

		val query = triples.select("Predicate").distinct()
		
		return query.withColumnRenamed(query.columns(0),"Predicates").as[String]
	}
}