package net.sansa_stack.query.spark.dof.triple

import net.sansa_stack.query.spark.dof.node._
import net.sansa_stack.rdf.spark.io._
import org.apache.jena.graph.{ Node, Triple }
import org.apache.jena.riot.Lang
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, Dataset, Row, SparkSession  }

class Reader(spark: SparkSession, input: String) extends java.io.Serializable {
  import spark.implicits._

  def read: RDD[Triple] = {
    val start = Helper.start
    val rdd = spark.rdf(Lang.RDFXML)(input)
    Helper.measureTime(start, s"\nTime measurement RDF")
    rdd
  }

  def readDF: DataFrame = {
    val start = Helper.start
    val df = spark.read.rdf(Lang.NTRIPLES)(input)
      .toDF(Helper.getNodeMethods: _*) // rename columns to corresp. triple methods
    Helper.measureTime(start, s"\nTime measurement DataFrame")
    df
  }

  def readDS: Dataset[Triple3S] = {
    var start = Helper.start
    val ds = readDF.as[Triple3S]
    Helper.measureTime(start, s"\nTime measurement Dataset")
    ds
  }
}
