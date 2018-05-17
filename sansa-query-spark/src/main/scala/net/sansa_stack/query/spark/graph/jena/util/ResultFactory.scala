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

  def convertToDataFrame[VD: ClassTag](results: RDD[Result[VD]], session: SparkSession): Unit = {
    //session.createDataFrame()
  }
}
