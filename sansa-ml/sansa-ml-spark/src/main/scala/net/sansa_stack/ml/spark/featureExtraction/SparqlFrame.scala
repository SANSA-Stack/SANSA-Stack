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
import org.apache.spark.sql.types.{BooleanType, DoubleType, FloatType, IntegerType, NullType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row, SparkSession}
import org.aksw.sparqlify.core.sql.common.serialization.SqlEscaperDoubleQuote
import org.apache.jena.graph.{Node, NodeFactory, Triple}

/**
 * This SparqlFrame Transformer creates a dataframe based on a SPARQL query
 * the resulting columns correspond to projection variables
 */
class SparqlFrame extends Transformer{
  var _query: String = _
  var _queryExcecutionEngine: SPARQLEngine.Value = SPARQLEngine.Sparqlify

  private var _experimentId: org.apache.jena.graph.Node = null

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

    df
  }

    /* if (_queryExcecutionEngine == "sparqlify") {
      val graphRdd: RDD[org.apache.jena.graph.Triple] = dataset.rdd.asInstanceOf[RDD[org.apache.jena.graph.Triple]]
      val qef = dataset.rdd.verticalPartition(RdfPartitionerDefault).sparqlify
      val resultSet = qef.createQueryExcecution(_query)
      val schemaMapping = RddOfBindingToDataFrameMapper.createSchemaMapping(resultSet)
      val df = RddOfBindingToDataFrameMapper.applySchemaMapping(spark, resultSet.getBindings, schemaMapping)
      df
    } */
/*
    val parser = new ParserSPARQL11()
    val projectionVars: Seq[Var] = parser.parse(new Query(), _query).getProjectVars.asScala
    val bindings: RDD[Binding] = getResultBindings(dataset)

    val features: RDD[Seq[Node]] = bindings.map(binding => {
      projectionVars.map(projectionVar => binding.get(projectionVar))
    })

    val columns: Seq[String] = projectionVars.map(_.toString().replace("?", ""))


    // the next block is for the edge case that no elements are in result
    if (features.count() == 0) {
      println(f"[WARNING] the resulting DataFrame is empty!")
      val emptyDf = spark.createDataFrame(
        spark.sparkContext.emptyRDD[Row],
        StructType(columns.map(colName => StructField(colName, StringType, nullable = true)))
      )
      return emptyDf
    }

    val types = features.map(seq => {
      seq.map(node => {
        if (node == null) {
          NullType
        }
        else if (node.isLiteral) {
          // TODO also include possiblity like Datetime and others
          if (node.getLiteralValue.isInstanceOf[Float]) FloatType
          else if (node.getLiteralValue.isInstanceOf[Double]) DoubleType
          else if (node.getLiteralValue.isInstanceOf[Boolean]) BooleanType
          else if (node.getLiteralValue.isInstanceOf[String]) StringType
          else if (node.getLiteralValue.isInstanceOf[Int]) IntegerType
          else StringType
        }
        else StringType
      })
    })

    var columnTypes = ListBuffer.empty[org.apache.spark.sql.types.DataType]
    val firstRow = types.take(1).toSeq
    val numberColumns = firstRow(0).toSeq.size
    // println(f"We have $numberColumns columns")
    for (i <- 0 to numberColumns - 1) {
      val allTypesOfColumn = types.map(_ (i)).filter(_ != NullType)
      if (allTypesOfColumn.distinct.collect().toSeq.size == 1) {
        val elements = allTypesOfColumn.take(1).toSeq
        val element = elements(0).asInstanceOf[org.apache.spark.sql.types.DataType] // .toString
        columnTypes.append(element)
      }
      else {
        val typeDistribution = allTypesOfColumn.groupBy(identity).mapValues(_.size).collect().toSeq
        println(
          f"""
            |[WARNING] the column type is not clear because different or only null types occured.
            |\t type distribution is:
            |\t ${typeDistribution.mkString(" ")}
            |\t this is why we fallback to StringType
            |\t """.stripMargin)
        val element = StringType.asInstanceOf[org.apache.spark.sql.types.DataType] // .toString
        columnTypes.append(element)
      }
    }

    val structTypesList = ListBuffer.empty[StructField]
    for (i <- 0 to numberColumns - 1) {
      structTypesList.append(
        StructField(columns(i), columnTypes(i), nullable = true)
      )
    }
    val schema = StructType(structTypesList.toSeq)

    val featuresNativeScalaDataTypes = features.map(seq => {
      seq.map(node => {
        if (node == null) null
        else {
          node.isLiteral match {
            case true => node.getLiteralValue
            case false => node.toString()
          }
        }
      })
    })

    spark.createDataFrame(featuresNativeScalaDataTypes.map(Row.fromSeq(_)), schema)
  } */
}