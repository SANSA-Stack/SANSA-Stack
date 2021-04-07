package net.sansa_stack.ml.spark.featureExtraction

import net.sansa_stack.query.spark.ops.rdd.RddOfBindingToDataFrameMapper
import net.sansa_stack.query.spark.sparqlify.SparqlifyUtils3
import net.sansa_stack.rdf.common.partition.core.{RdfPartitionerComplex, RdfPartitionerDefault}
import net.sansa_stack.query.spark._
import net.sansa_stack.rdf.spark.partition.RDFPartition
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{BooleanType, DataType, DoubleType, FloatType, IntegerType, NullType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row, SparkSession}
import org.aksw.sparqlify.core.sql.common.serialization.SqlEscaperDoubleQuote
import org.apache.jena.graph.{Node, NodeFactory, Triple}
import org.apache.spark.sql.functions.{col, collect_list, max, min, size}

import scala.collection.mutable

/**
 * This SparqlFrame Transformer creates a dataframe based on a SPARQL query
 * the resulting columns correspond to projection variables
 */
class SparqlFrame extends Transformer{
  var _query: String = _
  var _queryExcecutionEngine: SPARQLEngine.Value = SPARQLEngine.Sparqlify
  var _collapsByKey: Boolean = false
  var _featureDescriptions: mutable.Map[String, Map[String, Any]] = null
  var _keyColumnNameString: String = null

  private var _experimentId: org.apache.jena.graph.Node = null

  override val uid: String = Identifiable.randomUID("sparqlFrame")

  protected val spark = SparkSession.builder().getOrCreate()

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  def setCollapsByKey(keyColumnNameString: String): this.type = {
    _keyColumnNameString = keyColumnNameString
    this
  }

  def setCollapsByKey(collapsByKey: Boolean): this.type = {
    _collapsByKey = collapsByKey
    this
  }

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

  def setExperimentId(experimentURI: org.apache.jena.graph.Node): this.type = {
    _experimentId = experimentURI
    this
  }

  def getRDFdescription(): Array[org.apache.jena.graph.Triple] = {

    val transformerURI: Node = NodeFactory.createURI("sansa-stack/ml/transfomer/Sparqlframe")
    val pipelineElementURI: Node = NodeFactory.createURI("sansa-stack/ml/sansaVocab/ml/pipelineElement")

    val description = List(
      Triple.create(
        _experimentId,
        pipelineElementURI,
        transformerURI
      )
    ).toArray
    description
  }

  override def transformSchema(schema: StructType): StructType =
    throw new NotImplementedError()

  /**
   * SparqlFrame: The collapsByKey is set to true so we collaps the collumns by id column and als collect
   * feature type information which are available in property .getFeatureTypes
   *
   * @param df the input noncollapsed dataframe
   * @return the collapsed dataframe
   */
  def collapsByKey(df: DataFrame): DataFrame = {

    println("SparqlFrame: The collapsByKey is set to true so we collaps the collumns by id column and als collect feature type information which are available in property .getFeatureTypes")

    // specify column names
    val keyColumnNameString: String = if (keyColumnNameString == null) _query.toLowerCase.split("\\?")(1) else keyColumnNameString
    assert(df.columns.contains(keyColumnNameString))
    val featureColumns: Seq[String] = List(df.columns: _*).filter(!Set(keyColumnNameString).contains(_)).toSeq

    /**
     * This dataframe is a temporary df where we join to always a collapsed column for each feature
     * we start with one column df where we only keep the key column
     */
    var collapsedDataframe: DataFrame = df
      .select(keyColumnNameString)
      .dropDuplicates()
      .persist()

    df.unpersist()

    val numberRows: Long = collapsedDataframe.count()

    /**
     * In this Map we collect all gained inforamtion for every feature like type and so on
     */
    var featureDescriptions: mutable.Map[String, Map[String, Any]] = mutable.Map()

    featureColumns.foreach(
      currentFeatureColumnNameString => {
        println(currentFeatureColumnNameString)
        val twoColumnDf = df
          .select(keyColumnNameString, currentFeatureColumnNameString)
          .dropDuplicates()

        val groupedTwoColumnDf = twoColumnDf
          .groupBy(keyColumnNameString)

        val collapsedTwoColumnDfwithSize = groupedTwoColumnDf
          .agg(collect_list(currentFeatureColumnNameString) as currentFeatureColumnNameString)
          .withColumn("size", size(col(currentFeatureColumnNameString)))

        val minNumberOfElements = collapsedTwoColumnDfwithSize
          .select("size")
          .agg(min("size"))
          .head()
          .getInt(0)

        val maxNumberOfElements = collapsedTwoColumnDfwithSize
          .select("size")
          .agg(max("size"))
          .head()
          .getInt(0)

        val nullable: Boolean = if (minNumberOfElements == 0) true else false
        val datatype: DataType = twoColumnDf.select(currentFeatureColumnNameString).schema(0).dataType
        val numberDistinctValues: Int = twoColumnDf.select(currentFeatureColumnNameString).distinct.count.toInt
        val isListOfEntries: Boolean = if (maxNumberOfElements > 1) true else false
        val availability: Double = collapsedTwoColumnDfwithSize.select("size").filter(col("size") > 0).count().toDouble / numberRows.toDouble
        /* val medianAlphaRatio: Double = datatype match {
          case StringType => twoColumnDf.
          case _ => 0
        }
        TODO nlp identification
         */

        val isCategorical: Boolean = if ((numberDistinctValues.toDouble / numberRows.toDouble) < 0.1) true else false

        var featureType: String = ""
        if (isListOfEntries) featureType += "ListOf_" else featureType += "Single_"
        if (isCategorical) featureType += "Categorical_" else featureType += "NonCategorical_"
        featureType += datatype.toString.split("Type")(0)

        val featureSummary: Map[String, Any] = Map(
          "featureType" -> featureType,
          "name" -> currentFeatureColumnNameString,
          "nullable" -> nullable,
          "datatype" -> datatype,
          "numberDistinctValues" -> numberDistinctValues,
          "isListOfEntries" -> isListOfEntries,
          "avalability" -> availability,
        )

        featureDescriptions(currentFeatureColumnNameString) = featureSummary

        val joinableDf = {
          if (isListOfEntries) {
            collapsedTwoColumnDfwithSize
              .select(keyColumnNameString, currentFeatureColumnNameString)
          }
          else {
            twoColumnDf
              .select(keyColumnNameString, currentFeatureColumnNameString)
          }
        }
        collapsedDataframe = collapsedDataframe
          .join(joinableDf.withColumnRenamed(currentFeatureColumnNameString, f"${currentFeatureColumnNameString}(${featureType})"), keyColumnNameString)
      }
    )

    _featureDescriptions = featureDescriptions

    collapsedDataframe
  }


  def getFeatureDescriptions(): mutable.Map[String, Map[String, Any]] = {
    assert(_featureDescriptions != null)
    _featureDescriptions
  }

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
      /* case SPARQLEngine.Ontop =>
         graphRdd.verticalPartition(new RdfPartitionerComplex(),
                                   explodeLanguageTags = true,
                                   new SqlEscaperDoubleQuote(),
                                   escapeIdentifiers = true).ontop
       */
      case _ => throw new Exception(s"Your set engine: ${_queryExcecutionEngine} not supported")
    }

    val resultSet = qef.createQueryExecution(_query)
      .execSelectSpark()

    val schemaMapping = RddOfBindingToDataFrameMapper
      .configureSchemaMapper(resultSet)
      .createSchemaMapping()
    val df = RddOfBindingToDataFrameMapper.applySchemaMapping(resultSet.getBindings, schemaMapping)

    val resultDf = if (_collapsByKey) collapsByKey(df) else df

    resultDf
  }
}