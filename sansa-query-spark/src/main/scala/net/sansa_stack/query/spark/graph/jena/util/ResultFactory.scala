package net.sansa_stack.query.spark.graph.jena.util

import net.sansa_stack.query.spark.graph.jena.SparqlParser
import net.sansa_stack.rdf.spark.io.NTripleReader
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

/**
  * Object to create result of graph query from target rdf graph.
  *
  * @author Zhe Wang
  */
object ResultFactory {
  /**
    * Create a result rdd from the given mapping.
    *
    * @param mapping  The given solution mapping
    * @param session  Spark session
    * @tparam VD  Attribute of variables and rdf terms
    */
  def create[VD: ClassTag](mapping: Array[Map[VD, VD]], session: SparkSession): RDD[Result[VD]] = {
    val result = session.sparkContext.parallelize(mapping.map{ array =>
      new Result[VD].addAllMapping(array)
    })
    result
  }

  /**
    * Create a single result from the given mapping
    * @param mapping The given solution mapping
    * @tparam VD Attribute of variables and rdf terms
    * @return
    */
  def create[VD: ClassTag](mapping: Map[VD, VD]): Result[VD] = {
    val result = new Result[VD].addAllMapping(mapping)
    result
  }

  def merge[VD: ClassTag](left: Result[VD], right: Result[VD]): Result[VD] = {
    val mapping = left.getMapping.++(right.getMapping)
    val result = new Result[VD].addAllMapping(mapping)
    result
  }

  def convertToDataFrame[VD: ClassTag](results: RDD[Result[VD]], session: SparkSession): Unit = {
    //session.createDataFrame()
  }
}
