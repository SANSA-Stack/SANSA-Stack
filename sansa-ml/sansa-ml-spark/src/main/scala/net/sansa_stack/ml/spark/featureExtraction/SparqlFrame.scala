package net.sansa_stack.ml.spark.featureExtraction

import net.sansa_stack.query.spark.ops.rdd.RddToDataFrameMapper
import net.sansa_stack.rdf.common.partition.core.{RdfPartitionerComplex, RdfPartitionerDefault}
import net.sansa_stack.query.spark._
import net.sansa_stack.rdf.spark.partition.RDFPartition
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{BooleanType, DoubleType, FloatType, IntegerType, NullType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row, SparkSession}


import org.aksw.sparqlify.core.sql.common.serialization.SqlEscaperDoubleQuote

/**
 * This SparqlFrame Transformer creates a dataframe based on a SPARQL query
 * the resulting columns correspond to projection variables
 */
class SparqlFrame extends Transformer{
  var _query: String = _
  var _queryExcecutionEngine: SPARQLEngine.Value = SPARQLEngine.Sparqlify

  override val uid: String = Identifiable.randomUID("sparqlFrame")

  protected val spark = SparkSession.builder().getOrCreate()

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  /**
   * setter for query as string
   *
   * @param queryString a sparql query defining which features you want to have
   * @return the set transformer
   */
  def setSparqlQuery(queryString: String): this.type = {
    _query = queryString
    this
  }

  /**
   * setter to specify which query execution engine to be used
   *
   * option one is ontop option two sparqlify
   *
   * @param queryExcecutionEngine a string either ontop or sparqlify
   * @return the set transformer
   */
  def setQueryExcecutionEngine(queryExcecutionEngine: SPARQLEngine.Value): this.type = {
    if (queryExcecutionEngine.toString.toLowerCase() == "sparqlify" | queryExcecutionEngine.toString.toLowerCase() == "ontop" ) {
      _queryExcecutionEngine = queryExcecutionEngine
    }
    else {
      throw new Exception(s"Your set engine: ${_queryExcecutionEngine} not supported. at the moment only ontop and sparqlify")
    }
    this
  }

  override def transformSchema(schema: StructType): StructType =
    throw new NotImplementedError()

  /**
   * creates a native spark Mllib DataFrame with columns corresponding to the projection variables
   *
   * columns are implicitly casted to string or if specified in literals to respective integer ect
   *
   * @param dataset the knowledge graph as dataset of jena triple
   * @return a dataframe with columns corresponding to projection variables
   */
  def transform(dataset: Dataset[_]): DataFrame = {
    val graphRdd: RDD[org.apache.jena.graph.Triple] = dataset.rdd.asInstanceOf[RDD[org.apache.jena.graph.Triple]]
    // graphRdd.foreach(println(_))

    val qef = _queryExcecutionEngine match {
      case SPARQLEngine.Sparqlify =>
        graphRdd.verticalPartition(RdfPartitionerDefault).sparqlify
      case SPARQLEngine.Ontop =>
         graphRdd.verticalPartition(new RdfPartitionerComplex(),
                                   explodeLanguageTags = true,
                                   new SqlEscaperDoubleQuote(),
                                   escapeIdentifiers = true).ontop
      case _ => throw new Exception(s"Your set engine: ${_queryExcecutionEngine} not supported")
    }

    val resultSet = qef.createQueryExecution(_query)
      .execSelectSpark()

    val schemaMapping = RddToDataFrameMapper.createSchemaMapping(resultSet)
    val df = RddToDataFrameMapper.applySchemaMapping(resultSet.getBindings, schemaMapping)

    df
  }
}