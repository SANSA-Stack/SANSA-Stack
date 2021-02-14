package net.sansa_stack.query.spark.ontop

import java.util.Properties

import scala.collection.JavaConverters._

import it.unibz.inf.ontop.answering.reformulation.input.SPARQLQuery
import it.unibz.inf.ontop.answering.resultset.OBDAResultSet
import it.unibz.inf.ontop.com.google.common.collect.{ImmutableMap, ImmutableSortedSet}
import it.unibz.inf.ontop.exception.{OBDASpecificationException, OntopReformulationException}
import it.unibz.inf.ontop.iq.IQ
import it.unibz.inf.ontop.iq.exception.EmptyQueryException
import it.unibz.inf.ontop.iq.node.ConstructionNode
import it.unibz.inf.ontop.model.`type`.{DBTermType, TypeFactory}
import it.unibz.inf.ontop.model.atom.DistinctVariableOnlyDataAtom
import it.unibz.inf.ontop.model.term._
import it.unibz.inf.ontop.substitution.{ImmutableSubstitution, SubstitutionFactory}
import org.aksw.r2rml.jena.arq.lib.R2rmlLib
import org.aksw.r2rml.jena.domain.api.TriplesMap
import org.aksw.r2rml.jena.vocab.RR
import org.aksw.sparqlify.core.sql.common.serialization.SqlEscaperBacktick
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.apache.jena.sparql.engine.binding.Binding
import org.apache.jena.vocabulary.RDF
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Encoder, SparkSession}
import org.semanticweb.owlapi.model.OWLOntology

import net.sansa_stack.rdf.common.partition.r2rml.R2rmlUtils
import net.sansa_stack.rdf.common.partition.utils.SQLUtils

trait SPARQL2SQLRewriter[T <: QueryRewrite] {
  def createSQLQuery(sparqlQuery: String): T
}

abstract class QueryRewrite(sparqlQuery: String, sqlQuery: String)
/**
 * Wraps the result of query rewriting of Ontop.
 */
case class OntopQueryRewrite(sparqlQuery: String,
                             inputQuery: SPARQLQuery[_ <: OBDAResultSet],
                             sqlQuery: String,
                             sqlSignature: ImmutableSortedSet[Variable],
                             sqlTypeMap: ImmutableMap[Variable, DBTermType],
                             constructionNode: ConstructionNode,
                             answerAtom: DistinctVariableOnlyDataAtom,
                             sparqlVar2Term: ImmutableSubstitution[ImmutableTerm],
                             termFactory: TermFactory,
                             typeFactory: TypeFactory,
                             substitutionFactory: SubstitutionFactory,
                             executableQuery: IQ
                            ) extends QueryRewrite(sparqlQuery, sqlQuery) {}

case class RewriteInstruction(sqlSignature: ImmutableSortedSet[Variable],
                              sqlTypeMap: ImmutableMap[Variable, DBTermType],
                              anserAtom: DistinctVariableOnlyDataAtom,
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
                              database: Option[String],
                              jdbcMetaData: Map[String, String],
                              val mappingsModel: Model,
                              ontology: Option[OWLOntology] = None)
  extends SPARQL2SQLRewriter[OntopQueryRewrite]
    with Serializable {

  private val logger = com.typesafe.scalalogging.Logger(classOf[OntopSPARQL2SQLRewriter])

  // load Ontop properties
  val ontopProperties = new Properties()
  ontopProperties.load(getClass.getClassLoader.getResourceAsStream("ontop-spark.properties"))

  // the Ontop core
  val reformulationConfiguration = OntopConnection(ontopSessionId, database, mappingsModel, ontopProperties, jdbcMetaData, ontology)
  val termFactory = reformulationConfiguration.getTermFactory
  val typeFactory = reformulationConfiguration.getTypeFactory
  val queryReformulator = reformulationConfiguration.loadQueryReformulator
  val substitutionFactory = reformulationConfiguration.getInjector.getInstance(classOf[SubstitutionFactory])
  val inputQueryFactory = queryReformulator.getInputQueryFactory


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

    OntopQueryRewrite(sparqlQuery, inputQuery, sqlQuery, signature, typeMap, constructionNode,
      executableQuery.getProjectionAtom, constructionNode.getSubstitution, termFactory, typeFactory, substitutionFactory, executableQuery)
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

  private val sqlEscaper = new SqlEscaperBacktick()

  // set the current Spark SQL database if given
  if (database.isDefined) {
    spark.sql(s"USE ${sqlEscaper.escapeIdentifier(database.get)}")
  }

  // get the JDBC metadata from the Spark tables
  private val jdbcMetaData = spark.catalog.listTables().collect().map(t => {
    val fields = spark.table(sqlEscaper.escapeTableName(t.name)).schema.fields.map(f => sqlEscaper.escapeColumnName(f.name)).mkString(",")
    val keyCondition = s"PRIMARY KEY ($fields)"
    (t.name,
      spark.table(sqlEscaper.escapeTableName(t.name)).schema.fields.map(f =>
        s"${sqlEscaper.escapeColumnName(f.name)} ${f.dataType.sql} ${if (!f.nullable) "NOT NULL" else ""}"
      ).mkString(",")
        + s", $keyCondition" // mark the table as duplicate free to avoid DISTINCT in every generated SQL query
    )
  }).toMap

  // if no ontology has been provided, we try to extract it from the dataset
  if (ontology.isEmpty) {
    ontology = OntologyExtractor.extract(spark, mappingsModel)
  }

  // we have to add separate mappings for each rdf:type in Ontop.
  expandMappingsWithTypes(mappingsModel)

  // define an ID for the whole setup - this will be used for caching of the whole Ontop setup
  private val sessionId: String = generateSessionId()

  private val sparql2sql = new OntopSPARQL2SQLRewriter(sessionId, database, jdbcMetaData, mappingsModel, ontology)

  private def generateSessionId(): String = jdbcMetaData.map(_.productIterator.mkString(":")).mkString("|") + mappingsModel.hashCode()


  /**
   * We have to add separate mappings for each rdf:type in Ontop.
   */
  private def expandMappingsWithTypes(model: Model): Unit = {
    // get the rdf:type TripleMaps with o being an IRI
    val tms = R2rmlUtils.triplesMapsForPredicate(RDF.`type`, model)
      .filter(_.getPredicateObjectMaps.asScala.exists(_.getObjectMaps.asScala.exists(_.asTermMap().getTermType == RR.IRI.inModel(model))))

    tms.foreach(tm => {
      val tableName = tm.getLogicalTable.asBaseTableOrView().getTableName

      val s = tm.getSubjectMap.getColumn
      val o = tm.getPredicateObjectMaps.asScala.head.getObjectMaps.asScala.head.asTermMap().getColumn // TODO we assume a single predicate-object map here

      // we have to unwrap the quote from H2 escape and also apply Spark SQL escape
      // we have to unwrap the quote from H2 escape and also apply Spark SQL escape
      val tn = SQLUtils.parseTableIdentifier(tableName)
      val to = sqlEscaper.escapeColumnName(o.stripPrefix("\"").stripSuffix("\""))
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

      model.removeAll(tm, null, null)
    })
  }

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
      val sql = queryRewrite.sqlQuery.replace("\"", "`") // FIXME omit the schema in Ontop directly (it comes from H2 default schema)
        .replace("`PUBLIC`.", "")
      logger.info(s"SQL query:\n$sql")

      // execute SQL query
      val resultRaw = spark.sql(sql)
      //    result.show(false)
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

        OntopConnection(sessionId, database, mappingsModel, sparql2sql.ontopProperties, jdbcMetaData, ontology)
        val rwi = RewriteInstruction(rewrite.sqlSignature, rewrite.sqlTypeMap, rewrite.answerAtom, rewrite.sparqlVar2Term.getImmutableMap)

        val output = KryoUtils.serialize(rwi, sessionId)
        val outputBC = spark.sparkContext.broadcast(output)

        val sparqlQueryBC = spark.sparkContext.broadcast(query)
        val mappingsBC = spark.sparkContext.broadcast(sparql2sql.mappingsModel)
        val propertiesBC = spark.sparkContext.broadcast(sparql2sql.ontopProperties)
        val metaDataBC = spark.sparkContext.broadcast(jdbcMetaData)
        val ontologyBC = spark.sparkContext.broadcast(ontology)
        val idBC = spark.sparkContext.broadcast(sessionId)
        val databaseBC = spark.sparkContext.broadcast(database)
//        val rewriteBC = spark.sparkContext.broadcast(rwi)

        implicit val bindingEncoder: Encoder[Binding] = org.apache.spark.sql.Encoders.kryo[Binding]
        df
          .coalesce(50)
          .mapPartitions(iterator => {
          //      val mapper = new OntopRowMapper2(mappingsBC.value, propertiesBC.value, metaDataBC.value, sparqlQueryBC.value, ontologyBC.value, idBC.value)
          val mapper = new OntopRowMapper(
            idBC.value,
            databaseBC.value,
            mappingsBC.value,
            propertiesBC.value,
            metaDataBC.value,
            sparqlQueryBC.value,
            ontologyBC.value,
//            rewriteBC.value,
            outputBC.value)
          val it = iterator.map(mapper.map)
          //      mapper.close()
          it
        }).rdd
      case None => spark.sparkContext.emptyRDD
    }
  }

}

object QueryEngineOntop {

  def main(args: Array[String]): Unit = {
    new OntopCLI().run(args)
  }

  def apply(spark: SparkSession,
            databaseName: Option[String],
            mappingsModel: Model,
            ontology: Option[OWLOntology]): QueryEngineOntop = {
    // expand shortcuts of R2RML model
    val newModel = ModelFactory.createDefaultModel()
    newModel.add(mappingsModel)
    R2rmlUtils.streamTriplesMaps(newModel).toList.foreach(R2rmlLib.expandShortcuts)

    new QueryEngineOntop(spark, databaseName, newModel, ontology)
  }

}
