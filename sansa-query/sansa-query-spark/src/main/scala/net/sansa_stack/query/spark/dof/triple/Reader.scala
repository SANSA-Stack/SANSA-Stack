package net.sansa_stack.query.spark.dof.triple

import net.sansa_stack.query.spark.dof.node._
import net.sansa_stack.rdf.spark.io._
import org.apache.jena.graph.Triple
import org.apache.jena.riot.Lang
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

class Reader(spark: SparkSession, input: String) extends java.io.Serializable {
  import spark.implicits._

  def read: RDD[Triple] = spark.rdf(Lang.RDFXML)(input)

  def readDF: DataFrame =
    spark.read.rdf(Lang.NTRIPLES)(input).toDF(Helper.getNodeMethods: _*)

  def readDS: Dataset[Triple3S] = readDF.as[Triple3S]
}
