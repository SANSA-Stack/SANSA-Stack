package net.sansa_stack.ml.spark.featureExtraction

import net.sansa_stack.query.spark._
import net.sansa_stack.query.spark.rdd.op.RddOfBindingsToDataFrameMapper
import net.sansa_stack.rdf.common.partition.core.RdfPartitionerDefault
import net.sansa_stack.rdf.spark.partition.RDFPartition
import org.apache.jena.datatypes.xsd.XSDDatatype
import org.apache.jena.graph.{NodeFactory, Triple}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

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

  /**
   * by which column to collapse if it shouldnt be first column
   * @param keyColumnNameString column name to collapse
   * @return transformer itself
   */
  def setCollapsColumnName(keyColumnNameString: String): this.type = {
    _keyColumnNameString = keyColumnNameString
    this
  }

  /**
   * Decide if we want to collaps the dataframe by an idea and collapse the samples so df consists of one row per entity
   * @param collapsByKey if yes, it will be collapsed, default is false
   * @return transformer itself
   */
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

  /* def getRDFdescription(): Array[org.apache.jena.graph.Triple] = {

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

   */

  /**
   * gain all inforamtion from this transformer as knowledge graph
   * @return RDD[Trile] describing the meta information
   */
  def getSemanticTransformerDescription(): RDD[org.apache.jena.graph.Triple] = {
    /*
    svahash type sva
    svaahsh hyerparameter hyperparameterHash1
    hyperparameterHash1 label label
    hyperparameterHash1 value value
    hyperparameterHash1 type hyperparameter
    ...
     */
    val svaNode = NodeFactory.createBlankNode(uid)
    val hyperparameterNodeP = NodeFactory.createURI("sansa-stack/sansaVocab/hyperparameter")
    val hyperparameterNodeValue = NodeFactory.createURI("sansa-stack/sansaVocab/value")
    val nodeLabel = NodeFactory.createURI("rdfs/label")


    val triples = List(
      Triple.create(
        svaNode,
        NodeFactory.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
        NodeFactory.createURI("sansa-stack/sansaVocab/Transformer")
      ), Triple.create(
        svaNode,
        NodeFactory.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
        NodeFactory.createURI("sansa-stack/sansaVocab/SparqlFrame")
      ),
      // _query
      Triple.create(
        svaNode,
        hyperparameterNodeP,
        NodeFactory.createBlankNode((uid + "_query").hashCode.toString)
      ), Triple.create(
        NodeFactory.createBlankNode((uid + "_query").hashCode.toString),
        hyperparameterNodeValue,
        NodeFactory.createLiteralByValue(_query, XSDDatatype.XSDstring)
      ), Triple.create(
        NodeFactory.createBlankNode((uid + "_query").hashCode.toString),
        nodeLabel,
        NodeFactory.createLiteralByValue("_query", XSDDatatype.XSDstring)
      ),
      // _queryExcecutionEngine
      Triple.create(
        svaNode,
        hyperparameterNodeP,
        NodeFactory.createBlankNode((uid + "_queryExcecutionEngine").hashCode.toString)
      ), Triple.create(
        NodeFactory.createBlankNode((uid + "_queryExcecutionEngine").hashCode.toString),
        hyperparameterNodeValue,
        NodeFactory.createLiteralByValue(_queryExcecutionEngine, XSDDatatype.XSDstring)
      ), Triple.create(
        NodeFactory.createBlankNode((uid + "_queryExcecutionEngine").hashCode.toString),
        nodeLabel,
        NodeFactory.createLiteralByValue("_queryExcecutionEngine", XSDDatatype.XSDstring)
      ),
      // _collapsByKey
      Triple.create(
        svaNode,
        hyperparameterNodeP,
        NodeFactory.createBlankNode((uid + "_collapsByKey").hashCode.toString)
      ), Triple.create(
        NodeFactory.createBlankNode((uid + "_collapsByKey").hashCode.toString),
        hyperparameterNodeValue,
        NodeFactory.createLiteralByValue(_collapsByKey, XSDDatatype.XSDboolean)
      ), Triple.create(
        NodeFactory.createBlankNode((uid + "_featureColumns").hashCode.toString),
        nodeLabel,
        NodeFactory.createLiteralByValue("_featureColumns", XSDDatatype.XSDstring)
      ),
      /* // _keyColumnNameString
      Triple.create(
        svaNode,
        hyperparameterNodeP,
        NodeFactory.createBlankNode((uid + "_keyColumnNameString").hashCode.toString)
      ), Triple.create(
        NodeFactory.createBlankNode((uid + "_keyColumnNameString").hashCode.toString),
        hyperparameterNodeValue,
        NodeFactory.createLiteral(_keyColumnNameString)
      ), Triple.create(
        NodeFactory.createBlankNode((uid + "_keyColumnNameString").hashCode.toString),
        nodeLabel,
        NodeFactory.createLiteralByValue("_keyColumnNameString", XSDDatatype.XSDstring)
      )

       */
    )
    spark.sqlContext.sparkContext.parallelize(triples)
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
    val keyColumnNameString: String = if (_keyColumnNameString == null) _query.toLowerCase.split("\\?")(1).stripSuffix(" ") else _keyColumnNameString
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
        val numberDistinctValues: Int = twoColumnDf.select(currentFeatureColumnNameString).distinct().count().toInt
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

  /**
   * get the description of features after sparql extraction to decide over upcoming preprocessing strategies
   * @return map representeing for each column some feature descriptions
   */
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

    val qef = _queryExcecutionEngine match {
      case SPARQLEngine.Sparqlify =>
        graphRdd.verticalPartition(RdfPartitionerDefault).sparqlify()
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

    System.out.println("ResultSet:")
    resultSet.getBindings.toLocalIterator.foreach(System.out.println)

    val schemaMapping = RddOfBindingsToDataFrameMapper
      .configureSchemaMapper(resultSet)
      .createSchemaMapping()
    val df = RddOfBindingsToDataFrameMapper.applySchemaMapping(resultSet.getBindings, schemaMapping)

    val resultDf = if (_collapsByKey) collapsByKey(df) else df

    resultDf
  }
}