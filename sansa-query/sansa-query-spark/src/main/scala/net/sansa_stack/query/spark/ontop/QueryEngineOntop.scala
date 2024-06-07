package net.sansa_stack.query.spark.ontop

import it.unibz.inf.ontop.com.google.common.base.Charsets
import it.unibz.inf.ontop.com.google.common.collect.{ImmutableMap, ImmutableSortedSet}
import it.unibz.inf.ontop.com.google.common.hash.Hashing
import it.unibz.inf.ontop.exception.{OBDASpecificationException, OntopReformulationException}
import it.unibz.inf.ontop.iq.exception.EmptyQueryException
import it.unibz.inf.ontop.model.`type`.DBTermType
import it.unibz.inf.ontop.model.atom.DistinctVariableOnlyDataAtom
import it.unibz.inf.ontop.model.term._
import it.unibz.inf.ontop.spec.dbschema.tools.DBMetadataExtractorAndSerializer
import it.unibz.inf.ontop.substitution.SubstitutionFactory
import net.sansa_stack.rdf.common.partition.r2rml.R2rmlUtils
import net.sansa_stack.rdf.common.partition.utils.SQLUtils
import net.sansa_stack.rdf.spark.utils.{ScalaUtils, SparkSessionUtils}
import org.aksw.r2rml.jena.arq.lib.R2rmlLib
import org.aksw.r2rml.jena.vocab.RR
import org.aksw.rmltk.model.r2rml.TriplesMap
import org.aksw.sparqlify.core.sql.common.serialization.SqlEscaperBacktick
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.apache.jena.sparql.engine.binding.Binding
import org.apache.jena.vocabulary.RDF
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Encoder, SparkSession}
import org.semanticweb.owlapi.model.OWLOntology

import java.util.Properties
import scala.collection.JavaConverters._

trait SPARQL2SQLRewriter[T <: QueryRewrite] {
  def createSQLQuery(sparqlQuery: String): T
}

abstract class QueryRewrite(sparqlQuery: String, sqlQuery: String)
/**
 * Wraps the result of query rewriting of Ontop.
 */
case class OntopQueryRewrite(sparqlQuery: String,
                             sqlQuery: String,
                             rewriteInstruction: RewriteInstruction
                            ) extends QueryRewrite(sparqlQuery, sqlQuery) {}

case class RewriteInstruction(sqlSignature: ImmutableSortedSet[Variable],
                              sqlTypeMap: ImmutableMap[Variable, DBTermType],
                              answerAtom: DistinctVariableOnlyDataAtom,
                              sparqlVar2Term: ImmutableMap[Variable, ImmutableTerm])

/**
 * A SPARQL to SQL rewriter based on Ontop.
 *
 * RDF partitions will be taken into account to generate Ontop mappings to an in-memory H2 database.
 *
 * @constructor create a new Ontop SPARQL to SQL rewriter based on RDF partitions.
 *
 * @param ontopSessionId the current Ontop session ID
 * @param database an optional database name
 * @param jdbcMetaData the JDBC meta data gathered from Spark to simulate a SQL backend via in-memory H2 database
 * @param mappingsModel the R2RML mappings model
 * @param ontology an optional ontology
 */
class OntopSPARQL2SQLRewriter(ontopSessionId: String,
                              ontopProperties: Properties,
                              database: Option[String],
                              jdbcMetaData: Map[String, String],
                              val mappingsModel: Model,
                              ontology: Option[OWLOntology] = None)
  extends SPARQL2SQLRewriter[OntopQueryRewrite]
    with Serializable {

  private val logger = com.typesafe.scalalogging.Logger(classOf[OntopSPARQL2SQLRewriter])

  // the Ontop core
  val reformulationConfiguration = OntopConnection(ontopSessionId, database, mappingsModel, ontopProperties, jdbcMetaData, ontology)

  val termFactory = reformulationConfiguration.getTermFactory
  val typeFactory = reformulationConfiguration.getTypeFactory
  val queryReformulator = reformulationConfiguration.loadQueryReformulator
  val substitutionFactory = reformulationConfiguration.getInjector.getInstance(classOf[SubstitutionFactory])
  val inputQueryFactory = queryReformulator.getInputQueryFactory

  // serialize the DB metadata
  val mappingConfig = OntopUtils.createMappingConfig(ontopProperties, database)
  val extractorAndSerializer = mappingConfig.getInjector.getInstance(classOf[DBMetadataExtractorAndSerializer])
  val dbMetadata = extractorAndSerializer.extractAndSerialize()


  @throws[OBDASpecificationException]
  @throws[OntopReformulationException]
  def createSQLQuery(sparqlQuery: String): OntopQueryRewrite = {
    val inputQuery = inputQueryFactory.createSPARQLQuery(sparqlQuery)

    val executableQuery = queryReformulator.reformulateIntoNativeQuery(inputQuery,
      queryReformulator.getQueryLoggerFactory.create(it.unibz.inf.ontop.com.google.common.collect.ImmutableMultimap.of()))

    val sqlQuery = OntopUtils.extractSQLQuery(executableQuery)
    val constructionNode = OntopUtils.extractRootConstructionNode(executableQuery)
    val nativeNode = OntopUtils.extractNativeNode(executableQuery)
    val signature = nativeNode.getVariables
    val typeMap = nativeNode.getTypeMap

    OntopQueryRewrite(sparqlQuery, sqlQuery,
      RewriteInstruction(signature, typeMap, executableQuery.getProjectionAtom, constructionNode.getSubstitution.getImmutableMap))
  }

  def close(): Unit = OntopConnection.getOrCreateConnection(database).close()
}

/**
 * A SPARQL engine based on Ontop as SPARQL-to-SQL rewriter.
 *
 * @param spark the Spark session
 * @param database an existing Spark database that contains the tables for the RDF partitions
 * @param mappingsModel the RDF partitions
 * @param ontology an (optional) ontology that will be used for query optimization and rewriting
 */
class QueryEngineOntop(val spark: SparkSession,
                       val database: Option[String],
                       val mappingsModel: Model,
                       var ontology: Option[OWLOntology]) {
  require(spark != null, "Spark session must not be null.")
  require(!mappingsModel.isEmpty, "Mappings model must not be empty.")

  private val logger = com.typesafe.scalalogging.Logger[QueryEngineOntop]

  // read the Ontop properties
  val ontopProperties = new Properties()
  ontopProperties.load(getClass.getClassLoader.getResourceAsStream("ontop-spark.properties"))

  val settings = OntopSettings(ontopProperties)

  private val sqlEscaper = new SqlEscaperBacktick()

  // set the current Spark SQL database if given
  if (database.isDefined) {
    spark.sql(s"USE ${sqlEscaper.escapeIdentifier(database.get)}")
  }

  // get the JDBC metadata from the Spark tables
  logger.debug("computing Spark DB metadata ...")
  val jdbcMetaData: Map[String, String] = spark.catalog.listTables().collect().map(t => {
    val fields = spark.table(sqlEscaper.escapeTableName(t.name)).schema.fields.map(f => sqlEscaper.escapeColumnName(f.name)).mkString(",")
    val keyCondition = s"PRIMARY KEY ($fields)"
    (t.name,
      spark.table(sqlEscaper.escapeTableName(t.name)).schema.fields.map(f =>
        s"${sqlEscaper.escapeColumnName(f.name)} ${f.dataType.sql} ${if (!f.nullable) "NOT NULL" else ""}"
      ).mkString(",")
        + s", $keyCondition" // mark the table as duplicate free to avoid DISTINCT in every generated SQL query
    )
  }).toMap
  logger.debug("finished computing Spark DB metadata")

  // if no ontology has been provided, we try to extract it from the dataset
  if (ontology.isEmpty) {
    ontology = None // OntologyExtractor.extract(spark, mappingsModel)
  }

  // we have to add separate mappings for each rdf:type in Ontop.
  expandMappingsWithTypes(mappingsModel)

  // define an ID for the whole setup - this will be used for caching of the whole Ontop setup
  private val sessionId: String = generateSessionId()

  private val sparql2sql = new OntopSPARQL2SQLRewriter(sessionId, ontopProperties, database, jdbcMetaData, mappingsModel, ontology)

  private def generateSessionId(): String = {
    val s = database.getOrElse("") + jdbcMetaData.map(_.productIterator.mkString(":")).mkString("|") + mappingsModel.hashCode()
    Hashing.sha256.hashString(s, Charsets.UTF_8).toString
  }


  /**
   * We have to add separate mappings for each rdf:type in Ontop.
   */
  private def expandMappingsWithTypes(model: Model): Unit = {
    logger.debug("expanding mappings with rdf:type mappings ...")
    // get the rdf:type TripleMaps with o being an IRI
    val tms = R2rmlUtils.triplesMapsForPredicate(RDF.`type`, model)
      .filter(_.getPredicateObjectMaps.asScala.exists(_.getObjectMaps.asScala.exists(_.asTermMap().getTermType == RR.IRI.inModel(model))))

    tms.foreach(tm => {
      val tableName = tm.getLogicalTable.asBaseTableOrView().getTableName

      val s = tm.getSubjectMap.getColumn
      val o = tm.getPredicateObjectMaps.asScala.head.getObjectMaps.asScala.head.asTermMap().getColumn // TODO we assume a single predicate-object map here

      // we have to unwrap the quote from H2 escape and also apply Spark SQL escape
      val tn = SQLUtils.parseTableIdentifier(tableName)
      val to = sqlEscaper.escapeColumnName(ScalaUtils.unQuote(o))
      val df = spark.sql(s"SELECT DISTINCT $to FROM $tn")

      val classes = df.collect().map(_.getString(0))
      classes.foreach(cls => {
        val tm = model.createResource.as(classOf[TriplesMap])
        tm.getOrSetLogicalTable().asR2rmlView().setSqlQuery(s"SELECT $s FROM $tableName WHERE $o = '$cls'")
        val sm = tm.getOrSetSubjectMap()
        sm.setColumn(s)
        sm.setTermType(RR.IRI.inModel(model))

        val pom = tm.addNewPredicateObjectMap()
        pom.addPredicate(RDF.`type`.inModel(model))
        pom.addObject(model.createResource(cls))
      })
      logger.debug(s"finished expanding mappings with rdf:type mappings (added ${classes.length} class mappings)")

      model.removeAll(tm, null, null)
    })
  }

  def prepare(): Unit = {
    // in local mode, there won't be any executor
    if (!spark.sparkContext.isLocal) {
      logger.debug(s"preparing Ontop setup on executors ...")
      val mappingsBC = spark.sparkContext.broadcast(mappingsModel)
      val propertiesBC = spark.sparkContext.broadcast(ontopProperties)
      val jdbcMetadataBC = spark.sparkContext.broadcast(jdbcMetaData)
      val ontologyBC = spark.sparkContext.broadcast(ontology)
      val idBC = spark.sparkContext.broadcast(sessionId)
      val databaseBC = spark.sparkContext.broadcast(database)

      val numExecutors = SparkSessionUtils.currentActiveExecutors(spark).size

      if (numExecutors > 0) {
        // we have to somehow achieve that
        val data = spark.sparkContext.parallelize((1 to 100).toList).repartition(3 * numExecutors)

        // force an Ontop setup
        data.foreachPartition(_ => {
          val id = idBC.value
          val db = databaseBC.value
          val mappings = mappingsBC.value
          val properties = propertiesBC.value
          val jdbcMetadata = jdbcMetadataBC.value
          val ontology = ontologyBC.value
          OntopConnection(
            id,
            db,
            mappings,
            properties,
            jdbcMetadata,
            ontology)
        })
        logger.debug(s"finished preparing Ontop setup on executors.")
      }
    }
  }

  if (settings.preInitializeWorkers) prepare()

  /**
   * Shutdown of the engine, i.e. all open resource will be closed.
   */
  def stop(): Unit = {
    sparql2sql.close()
  }

  /**
   * Free resources, e.g. unregister Spark tables.
   */
  def clear(): Unit = {
    spark.catalog.clearCache()
//    spark.catalog.listTables().foreach { case (table: Table) => spark.catalog.dropTempView(table.name)}
  }

  /**
   * Executes the given SPARQL query on the provided dataset partitions.
   *
   * @param query the SPARQL query
   * @return a DataFrame with the raw result of the SQL query execution and the query rewrite object for
   *         processing the intermediate SQL rows
   *         (None if the SQL query was empty)
   * @throws org.apache.spark.sql.AnalysisException if the query execution fails
   */
  def executeDebug(query: String): (DataFrame, Option[OntopQueryRewrite]) = {
    logger.info(s"SPARQL query:\n$query")

    try {
      // translate to SQL query
      val queryRewrite = sparql2sql.createSQLQuery(query)
      val sql = queryRewrite.sqlQuery
        // .replace("\"", "`") // FIXME omit the schema in Ontop directly (it comes from H2 default schema)
        // .replace("`PUBLIC`.", "")
      logger.info(s"SQL query:\n$sql")

      // execute SQL query
      val resultRaw = spark.sql(sql)
//      resultRaw.show(false)
      //    result.printSchema()

      (resultRaw, Some(queryRewrite))
    } catch {
      case e: EmptyQueryException =>
        logger.warn(s"Empty SQL query generated by Ontop. Returning empty DataFrame for SPARQL query\n$query")
        (spark.emptyDataFrame, None)
      case e: org.apache.spark.sql.AnalysisException =>
        logger.error(s"Spark failed to execute translated SQL query\n$query", e)
        throw e
      case e: Exception => throw e
    }
  }

  /**
   * Computes the bindings of the query independently of the query type. This works,
   * because all non-SELECT queries can be reduced to SELECT queries with a post-processing.
   *
   * @param query the SPARQL query
   * @return an RDD of solution bindings
   * @throws org.apache.spark.sql.AnalysisException if the query execution fails
   */
  def computeBindings(query: String): RDD[Binding] = {
    val df2Rewrite = executeDebug(query)
    val rewriteOpt = df2Rewrite._2

    rewriteOpt match {
      case Some(rewrite) =>
        val df = df2Rewrite._1
        val rwi = rewrite.rewriteInstruction
        if (settings.useLocalEvaluation) {
          logger.warn("computing the bindings locally, i.e. in the driver. This can be time consuming of the result is large." +
            "Please try the non-local evaluation in that case which keeps the data always distributed")
          // compute bindings in driver
          val bindings = evaluateBindingsLocal(df, query, rwi)
          logger.debug(s"got $bindings bindings. Re-distributing on the cluster into ${spark.sparkContext.defaultParallelism} partitions (using 'spark.default.parallelism' value)")
          // re-distribute
          spark.sparkContext.parallelize(bindings)
        } else {
          evaluateBindingsRemote(df, query, rwi)
        }
      case None =>
        spark.sparkContext.emptyRDD
    }
  }

  /**
   * Computes the bindings locally in the driver. This works,
   * because all non-SELECT queries can be reduced to SELECT queries with a post-processing.
   *
   * @param query the SPARQL query
   * @return a list of solution bindings
   * @throws org.apache.spark.sql.AnalysisException if the query execution fails
   */
  def computeBindingsLocal(query: String): Seq[Binding] = {
    val df2Rewrite = executeDebug(query)
    val rewriteOpt = df2Rewrite._2

    rewriteOpt match {
      case Some(rewrite) =>
        val df = df2Rewrite._1
        val rwi = rewrite.rewriteInstruction
        val bindings = evaluateBindingsLocal(df, query, rwi)
        bindings
      case None =>
        Seq()
    }
  }

  private def evaluateBindingsRemote(rows: DataFrame, query: String, rewriteInstruction: RewriteInstruction): RDD[Binding] = {
    val sparqlQueryBC = spark.sparkContext.broadcast(query)
    val mappingsBC = spark.sparkContext.broadcast(mappingsModel)
    val propertiesBC = spark.sparkContext.broadcast(ontopProperties)
    val jdbcMetadataBC = spark.sparkContext.broadcast(jdbcMetaData)
    val ontologyBC = spark.sparkContext.broadcast(ontology)
    val idBC = spark.sparkContext.broadcast(sessionId)
    val databaseBC = spark.sparkContext.broadcast(database)
    val rwiBC = spark.sparkContext.broadcast(rewriteInstruction)
    val dbMetadataBC = spark.sparkContext.broadcast(sparql2sql.dbMetadata)

    implicit val bindingEncoder: Encoder[Binding] = org.apache.spark.sql.Encoders.kryo[Binding]
    var df = rows
    if (settings.maxRowMappers > 0) {
      df = df.coalesce(settings.maxRowMappers)
    }
    val rdd = df.mapPartitions(iterator => {
      val id = idBC.value
      val db = databaseBC.value
      val mappings = mappingsBC.value
      val properties = propertiesBC.value
      val jdbcMetadata = jdbcMetadataBC.value
      val ontology = ontologyBC.value
      val dbMetadata = dbMetadataBC.value

      OntopConnection(
        id,
        db,
        mappings,
        properties,
        jdbcMetadata,
        ontology)

      val rwi = rwiBC.value

      val mapper = new OntopRowMapper(
        id,
        db,
        mappings,
        properties,
        jdbcMetadata,
        sparqlQueryBC.value,
        ontology,
        rwi,
        dbMetadata
      )
      val it = iterator.map(mapper.map)
      //      mapper.close()
      it
    }).rdd

    rdd
  }

  /**
   * Computes the bindings of the query independently of the query type. This works,
   * because all non-SELECT queries can be reduced to SELECT queries with a post-processing.
   *
   * @param query the SPARQL query
   * @return an RDD of solution bindings
   * @throws org.apache.spark.sql.AnalysisException if the query execution fails
   */
  private def evaluateBindingsLocal(rows: DataFrame, query: String, rewriteInstruction: RewriteInstruction): Seq[Binding] = {
    val mapper = new OntopRowMapper(
      sessionId,
      database,
      mappingsModel,
      ontopProperties,
      jdbcMetaData,
      query,
      ontology,
      rewriteInstruction
    )
    rows.collect().map(mapper.map)
  }

}

object QueryEngineOntop {

  def apply(spark: SparkSession,
            databaseName: Option[String],
            mappingsModel: Model,
            ontology: Option[OWLOntology]): QueryEngineOntop = {
    // expand shortcuts of R2RML model
    val expandedMappingsModel = ModelFactory.createDefaultModel()
    expandedMappingsModel.add(mappingsModel)
    R2rmlLib.streamTriplesMaps(expandedMappingsModel).forEach(tm => R2rmlLib.expandShortcuts(tm))

    new QueryEngineOntop(spark, databaseName, expandedMappingsModel, ontology)
  }

}
