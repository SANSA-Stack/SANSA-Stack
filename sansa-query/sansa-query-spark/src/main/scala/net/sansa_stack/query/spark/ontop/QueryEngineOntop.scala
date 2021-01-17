package net.sansa_stack.query.spark.ontop

import java.sql.{Connection, DriverManager, SQLException}
import java.util.Properties

import scala.collection.JavaConverters._

import com.github.owlcs.ontapi.OntManagers.OWLAPIImplProfile
import it.unibz.inf.ontop.answering.reformulation.input.SPARQLQuery
import it.unibz.inf.ontop.answering.resultset.OBDAResultSet
import it.unibz.inf.ontop.com.google.common.collect.{ImmutableMap, ImmutableSortedSet}
import it.unibz.inf.ontop.exception.{OBDASpecificationException, OntopReformulationException}
import it.unibz.inf.ontop.iq.exception.EmptyQueryException
import it.unibz.inf.ontop.iq.node.ConstructionNode
import it.unibz.inf.ontop.model.`type`.{DBTermType, TypeFactory}
import it.unibz.inf.ontop.model.atom.DistinctVariableOnlyDataAtom
import it.unibz.inf.ontop.model.term._
import it.unibz.inf.ontop.substitution.{ImmutableSubstitution, SubstitutionFactory}
import org.aksw.sparqlify.core.sql.common.serialization.SqlEscaperBacktick
import org.apache.jena.rdf.model.Model
import org.apache.jena.sparql.engine.binding.Binding
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Encoder, SparkSession}
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.model.{IRI, OWLAxiom, OWLOntology}

trait SPARQL2SQLRewriter2[T <: QueryRewrite2] {
  def createSQLQuery(sparqlQuery: String): T
}

abstract class QueryRewrite2(sparqlQuery: String, sqlQuery: String)
/**
 * Wraps the result of query rewriting of Ontop.
 */
case class OntopQueryRewrite2(sparqlQuery: String,
                             inputQuery: SPARQLQuery[_ <: OBDAResultSet],
                             sqlQuery: String,
                             sqlSignature: ImmutableSortedSet[Variable],
                             sqlTypeMap: ImmutableMap[Variable, DBTermType],
                             constructionNode: ConstructionNode,
                             answerAtom: DistinctVariableOnlyDataAtom,
                             sparqlVar2Term: ImmutableSubstitution[ImmutableTerm],
                             termFactory: TermFactory,
                             typeFactory: TypeFactory,
                             substitutionFactory: SubstitutionFactory
                            ) extends QueryRewrite2(sparqlQuery, sqlQuery) {}

/**
 * A SPARQL to SQL rewriter based on Ontop.
 *
 * RDF partitions will be taken into account to generate Ontop mappings to an in-memory H2 database.
 *
 * @constructor create a new Ontop SPARQL to SQL rewriter based on RDF partitions.
 * @param partitions the RDF partitions
 */
class OntopSPARQL2SQLRewriter2(jdbcMetaData: Map[String, String],
                               val mappingsModel: Model,
                               ontology: Option[OWLOntology] = None)
  extends SPARQL2SQLRewriter2[OntopQueryRewrite2]
    with Serializable {

  private val logger = com.typesafe.scalalogging.Logger(classOf[OntopSPARQL2SQLRewriter])

  // load Ontop properties
  val ontopProperties = new Properties()
  ontopProperties.load(getClass.getClassLoader.getResourceAsStream("ontop-spark.properties"))

  // create the tmp DB needed for Ontop
  private val JDBC_URL = "jdbc:h2:mem:sansaontopdb;DATABASE_TO_UPPER=FALSE"
  private val JDBC_USER = "sa"
  private val JDBC_PASSWORD = ""

  private lazy val connection: Connection = try {
    // scalastyle:off classforname
    Class.forName("org.h2.Driver")
    // scalastyle:on classforname
    DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASSWORD)
  } catch {
    case e: SQLException =>
      logger.error("Error occurred when creating in-memory H2 database", e)
      throw e
  }

  JDBCDatabaseGenerator.generateTables(connection, jdbcMetaData)

  // the Ontop core
  val reformulationConfiguration = OntopUtils2.createReformulationConfig(mappingsModel, ontopProperties, ontology)
  val termFactory = reformulationConfiguration.getTermFactory
  val typeFactory = reformulationConfiguration.getTypeFactory
  val queryReformulator = reformulationConfiguration.loadQueryReformulator
  val substitutionFactory = reformulationConfiguration.getInjector.getInstance(classOf[SubstitutionFactory])
  val inputQueryFactory = queryReformulator.getInputQueryFactory


  @throws[OBDASpecificationException]
  @throws[OntopReformulationException]
  def createSQLQuery(sparqlQuery: String): OntopQueryRewrite2 = {
    val inputQuery = inputQueryFactory.createSPARQLQuery(sparqlQuery)

    val executableQuery = queryReformulator.reformulateIntoNativeQuery(inputQuery,
      queryReformulator.getQueryLoggerFactory.create(it.unibz.inf.ontop.com.google.common.collect.ImmutableMultimap.of()))

    val sqlQuery = OntopUtils2.extractSQLQuery(executableQuery)
    val constructionNode = OntopUtils2.extractRootConstructionNode(executableQuery)
    val nativeNode = OntopUtils2.extractNativeNode(executableQuery)
    val signature = nativeNode.getVariables
    val typeMap = nativeNode.getTypeMap

    OntopQueryRewrite2(sparqlQuery, inputQuery, sqlQuery, signature, typeMap, constructionNode,
      executableQuery.getProjectionAtom, constructionNode.getSubstitution, termFactory, typeFactory, substitutionFactory)
  }

  def close(): Unit = OntopConnection2.connection.close()
}

/**
 * A SPARQL engine based on Ontop as SPARQL-to-SQL rewriter.
 *
 * @param spark the Spark session
 * @param databaseName an existing Spark database that contains the tables for the RDF partitions
 * @param mappingsModel the RDF partitions
 * @param ontology an (optional) ontology that will be used for query optimization and rewriting
 */
class QueryEngineOntop(val spark: SparkSession,
                       val databaseName: String,
                       val mappingsModel: Model,
                       var ontology: Option[OWLOntology]) {

  private val logger = com.typesafe.scalalogging.Logger[QueryEngineOntop]

  // set the current Spark SQL database if given
  if (databaseName != null && databaseName.trim.nonEmpty) {
    spark.sql(s"USE $databaseName")
  }

  // if no ontology has been provided, we try to extract it from the dataset
  if (ontology.isEmpty) {
    ontology = createOntology()
  }

  // get the JDBC metadata from the Spark tables
  val sqlEscaper = new SqlEscaperBacktick()
  val jdbcMetaData = spark.catalog.listTables().collect().map(t =>
    (t.name,
      spark.table(sqlEscaper.escapeTableName(t.name)).schema.fields.map(f =>
        s"${sqlEscaper.escapeColumnName(f.name)} ${f.dataType.sql} ${if (!f.nullable) "NOT NULL" else ""}"
      ).mkString(",")
    )
  ).toMap

  private val sparql2sql = new OntopSPARQL2SQLRewriter2(jdbcMetaData, mappingsModel, ontology)

  val typeFactory = sparql2sql.typeFactory

  // mapping from RDF datatype to Spark SQL datatype
  val rdfDatatype2SQLCastName = DatatypeMappings(typeFactory)


  // some debug stuff
  mappingsModel.write(System.out, "Turtle")
  spark.catalog.listTables().collect().foreach(t => spark.table(sqlEscaper.escapeTableName(t.name)).show(false))


  /**
   * creates an ontology from the given partitions.
   * This is necessary for rdf:type information which is handled not in forms of SQL tables by Ontop
   * but by a given set of entities contained in the ontology.
   * TODO we we just use class declaration axioms for now
   *      but it could be extended to extract more sophisticated schema axioms that can be used for inference
   */
  private def createOntology(): Option[OWLOntology] = {
    logger.debug("extracting ontology from dataset")
    // get the partitions that contain the rdf:type triples

    val typePartitions = spark.catalog.listTables().filter(_.name.contains("type")).collect()

    if (typePartitions.nonEmpty) {
      // generate the table names for those rdf:type partitions
      // there can be more than one because the partitioner creates a separate partition for each subject and object type
      val names = typePartitions.map(_.name)

      // create the SQL query as UNION of
      val sql = names.map(name => s"SELECT DISTINCT o FROM ${sqlEscaper.escapeTableName(name)}").mkString(" UNION ")

      val df = spark.sql(sql)

      val classes = df.collect().map(_.getString(0))

      // we just use declaration axioms for now
      val dataFactory = OWLManager.getOWLDataFactory
      val axioms: Set[OWLAxiom] = classes.map(cls =>
            dataFactory.getOWLDeclarationAxiom(dataFactory.getOWLClass(IRI.create(cls)))).toSet
      val ontology = createOntology(axioms)

      Some(ontology)
    } else {
      None
    }
  }

  /**
   * creates a non-concurrent aware ontology - used to avoid overhead during serialization.
   */
  private def createOntology(axioms: Set[OWLAxiom]): OWLOntology = {
    val man = new OWLAPIImplProfile().createManager(false)
    man.createOntology(axioms.asJava)
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
  def executeDebug(query: String): (DataFrame, Option[OntopQueryRewrite2]) = {
    logger.info(s"SPARQL query:\n$query")

    try {
      // translate to SQL query
      val queryRewrite = sparql2sql.createSQLQuery(query)
      val sql = queryRewrite.sqlQuery.replace("\"", "`")
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

    val df = executeDebug(query)._1

    val sparqlQueryBC = spark.sparkContext.broadcast(query)
    val mappingsBC = spark.sparkContext.broadcast(sparql2sql.mappingsModel)
    val propertiesBC = spark.sparkContext.broadcast(sparql2sql.ontopProperties)
    val metaDataBC = spark.sparkContext.broadcast(jdbcMetaData)
    val ontologyBC = spark.sparkContext.broadcast(ontology)

    implicit val bindingEncoder: Encoder[Binding] = org.apache.spark.sql.Encoders.kryo[Binding]
    df.coalesce(20).mapPartitions(iterator => {
//      println("mapping partition")
      val mapper = new OntopRowMapper2(mappingsBC.value, propertiesBC.value, metaDataBC.value, sparqlQueryBC.value, ontologyBC.value)
      val it = iterator.map(mapper.map)
//      mapper.close()
      it
    }).rdd

  }

}

object QueryEngineOntop {

  def main(args: Array[String]): Unit = {
    new OntopCLI().run(args)
  }

  def apply(spark: SparkSession,
            databaseName: String,
            mappingsModel: Model,
            ontology: Option[OWLOntology]): QueryEngineOntop
  = new QueryEngineOntop(spark, databaseName, mappingsModel, ontology)

}
