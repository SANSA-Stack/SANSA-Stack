package net.sansa_stack.inference.spark.data

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import net.sansa_stack.inference.data.RDFTriple

/**
  * A data structure that comprises a set of triples.
  *
  * @author Lorenz Buehmann
  *
  */
class RDFGraphNative(override val triples: RDD[RDFTriple])
  extends AbstractRDFGraph[RDD[RDFTriple], RDFGraphNative](triples) {

  def find(s: Option[String] = None, p: Option[String] = None, o: Option[String] = None): RDFGraphNative = {
      new RDFGraphNative(
        triples.filter(t =>
          (s == None || t.s == s.get) &&
          (p == None || t.p == p.get) &&
          (o == None || t.o == o.get)
      )
      )
  }

  def union(graph: RDFGraphNative): RDFGraphNative = {
    new RDFGraphNative(triples.union(graph.toRDD()))
  }

  def unionAll(graphs: Seq[RDFGraphNative]): RDFGraphNative = {
    //    return graphs.reduceLeft(_ union _)
    val first = graphs(0)
    return new RDFGraphNative(
      first.triples.sparkContext.union(graphs.map(g => g.toRDD()))
    )
  }

  def distinct(): RDFGraphNative = {
    new RDFGraphNative(triples.distinct())
  }

  def size(): Long = {
    triples.count()
  }

  def cache(): this.type = {
    triples.cache()
    this
  }

  def toRDD(): RDD[RDFTriple] = triples

  def toDataFrame(sparkSession: SparkSession): DataFrame = {
    // convert RDD to DataFrame
    val schemaString = "subject predicate object"

    // generate the schema based on the string of schema
    val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

    // convert triples RDD to rows
    val rowRDD = triples.map(t => Row(t.s, t.p, t.o))

    // apply the schema to the RDD
    val triplesDataFrame = sparkSession.createDataFrame(rowRDD, schema)

    // register the DataFrame as a table
    triplesDataFrame.createOrReplaceTempView("TRIPLES")

    triplesDataFrame
  }


}
