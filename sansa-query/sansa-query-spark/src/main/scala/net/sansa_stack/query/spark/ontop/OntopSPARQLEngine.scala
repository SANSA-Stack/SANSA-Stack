package net.sansa_stack.query.spark.ontop

import java.sql.{Connection, DriverManager, SQLException}
import java.util
import java.util.Properties

import com.github.owlcs.ontapi.OntManagers.OWLAPIImplProfile
import it.unibz.inf.ontop.answering.reformulation.input.SPARQLQuery
import it.unibz.inf.ontop.answering.resultset.OBDAResultSet
import it.unibz.inf.ontop.com.google.common.collect.{ImmutableMap, ImmutableSortedSet, Sets}
import it.unibz.inf.ontop.exception.{OBDASpecificationException, OntopReformulationException}
import it.unibz.inf.ontop.iq.exception.EmptyQueryException
import it.unibz.inf.ontop.iq.node.ConstructionNode
import it.unibz.inf.ontop.model.`type`.{DBTermType, TypeFactory}
import it.unibz.inf.ontop.model.atom.DistinctVariableOnlyDataAtom
import it.unibz.inf.ontop.model.term._
import it.unibz.inf.ontop.substitution.{ImmutableSubstitution, SubstitutionFactory}

import net.sansa_stack.rdf.common.partition.core.{RdfPartitionStateDefault, RdfPartitioner}
import org.apache.jena.graph.Triple
import org.apache.jena.query.{QueryFactory, QueryType}
import org.apache.jena.sparql.engine.binding.{Binding, BindingUtils}
import org.apache.jena.vocabulary.RDF
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, Encoder, Row, SparkSession}
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.model.{IRI, OWLAxiom, OWLOntology}
import scala.collection.JavaConverters._

import org.aksw.sparqlify.core.sql.common.serialization.{SqlEscaperBacktick, SqlEscaperDoubleQuote}
import org.apache.jena.sparql.modify.TemplateLib
import org.apache.jena.sparql.util.ResultSetUtils

import net.sansa_stack.rdf.common.partition.r2rml.R2rmlUtils
import net.sansa_stack.rdf.spark.partition.core.{BlankNodeStrategy, SQLUtils, SparkTableGenerator}

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
                             substitutionFactory: SubstitutionFactory
                            ) extends QueryRewrite(sparqlQuery, sqlQuery) {}

/**
 * A SPARQL to SQL rewriter based on Ontop.
 *
 * RDF partitions will be taken into account to generate Ontop mappings to an in-memory H2 database.
 *
 * @constructor create a new Ontop SPARQL to SQL rewriter based on RDF partitions.
 * @param partitions the RDF partitions
 */
class OntopSPARQL2SQLRewriter(val partitioner: RdfPartitioner[RdfPartitionStateDefault], val partitions: Set[RdfPartitionStateDefault],
                              blankNodeStrategy: BlankNodeStrategy.Value,
                              ontology: Option[OWLOntology] = None)
  extends SPARQL2SQLRewriter[OntopQueryRewrite]
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
  JDBCDatabaseGenerator.generateTables(connection, partitioner, partitions, blankNodeStrategy)

  // create OBDA mappings
  val mappings = OntopMappingGenerator.createOBDAMappingsForPartitions(partitions, ontology)
  logger.debug(s"Ontop mappings:\n$mappings")

  // the Ontop core
  val reformulationConfiguration = OntopUtils.createReformulationConfig(mappings, ontopProperties, ontology)
  val termFactory = reformulationConfiguration.getTermFactory
  val typeFactory = reformulationConfiguration.getTypeFactory
  val queryReformulator = reformulationConfiguration.loadQueryReformulator
  val substitutionFactory = reformulationConfiguration.getInjector.getInstance(classOf[SubstitutionFactory])
  val inputQueryFactory = queryReformulator.getInputQueryFactory


  @throws[OBDASpecificationException]
  @throws[OntopReformulationException]
  def createSQLQuery(sparqlQuery: String): OntopQueryRewrite = {
    val inputQuery = inputQueryFactory.createSPARQLQuery(sparqlQuery)

    val executableQuery = queryReformulator.reformulateIntoNativeQuery(inputQuery, queryReformulator.getQueryLoggerFactory.create())

    val sqlQuery = OntopUtils.extractSQLQuery(executableQuery)
    val constructionNode = OntopUtils.extractRootConstructionNode(executableQuery)
    val nativeNode = OntopUtils.extractNativeNode(executableQuery)
    val signature = nativeNode.getVariables
    val typeMap = nativeNode.getTypeMap

    OntopQueryRewrite(sparqlQuery, inputQuery, sqlQuery, signature, typeMap, constructionNode,
      executableQuery.getProjectionAtom, constructionNode.getSubstitution, termFactory, typeFactory, substitutionFactory)
  }

  def close(): Unit = connection.close()
}

/**
 * A SPARQL to SQL rewriter based on Ontop.
 *
 * RDF partitions will be taken into account to generate Ontop mappings to an in-memory H2 database.
 *
 */
object OntopSPARQL2SQLRewriter {
  /**
   * Creates a new SPARQL to SQL rewriter based on RDF partitions.
   *
   * @param partitions the RDF partitions
   * @return a new SPARQL to SQL rewriter
   */
  def apply(partitioner: RdfPartitioner[RdfPartitionStateDefault], partitions: Set[RdfPartitionStateDefault], blankNodeStrategy: BlankNodeStrategy.Value, ontology: Option[OWLOntology])
  : OntopSPARQL2SQLRewriter = new OntopSPARQL2SQLRewriter(partitioner, partitions, blankNodeStrategy, ontology)

  def apply(partitioner: RdfPartitioner[RdfPartitionStateDefault], partitions: Set[RdfPartitionStateDefault], blankNodeStrategy: BlankNodeStrategy.Value)
  : OntopSPARQL2SQLRewriter = new OntopSPARQL2SQLRewriter(partitioner, partitions, blankNodeStrategy)

}

/**
 * A SPARQL engine based on Ontop as SPARQL-to-SQL rewriter.
 *
 * @param spark the Spark session
 * @param databaseName an existing Spark database that contains the tables for the RDF partitions
 * @param partitions the RDF partitions
 * @param ontology an (optional) ontology that will be used for query optimization and rewriting
 */
class OntopSPARQLEngine(val spark: SparkSession,
                        val databaseName: String,
                        val partitioner: RdfPartitioner[RdfPartitionStateDefault],
                        val partitions: Set[RdfPartitionStateDefault],
                        var ontology: Option[OWLOntology]) {

  private val logger = com.typesafe.scalalogging.Logger[OntopSPARQLEngine]

  val sqlEscaper = new SqlEscaperBacktick()

  // if no ontology has been provided, we try to extract it from the dataset
  if (ontology.isEmpty) {
    ontology = createOntology()
  }

  val blankNodeStrategy: BlankNodeStrategy.Value = BlankNodeStrategy.Table

  private val sparql2sql = OntopSPARQL2SQLRewriter(partitioner, partitions, blankNodeStrategy, ontology)

  val typeFactory = sparql2sql.typeFactory

  // mapping from RDF datatype to Spark SQL datatype
  val rdfDatatype2SQLCastName = DatatypeMappings(typeFactory)

  if (databaseName != null && databaseName.trim.nonEmpty) {
    spark.sql(s"USE $databaseName")
  }

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
    val typePartitions = partitions.filter(_.predicate == RDF.`type`.getURI)

    if (typePartitions.nonEmpty) {
      // generate the table names for those rdf:type partitions
      // there can be more than one because the partitioner creates a separate partition for each subject and object type
      val names = typePartitions.map(p => R2rmlUtils.createDefaultTableName(p))

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

  private def postProcess(df: DataFrame, queryRewrite: OntopQueryRewrite): DataFrame = {
    var result = df

    // all projected variables
    val signature = queryRewrite.answerAtom.getArguments

    // mapping from SPARQL variable to term, i.e. to either SQL var or other SPARQL 1.1 bindings (BIND ...)
    val sparqlVar2Term = queryRewrite.constructionNode.getSubstitution

    // we rename the columns of the SQL projected vars
    val columnMappings = signature.asScala
      .map(v => (v, sparqlVar2Term.get(v)))
      .filterNot(_._2.isInstanceOf[RDFConstant]) // skip RDF constants which will be added later
      .map { case (v, term) => (v, term.getVariableStream.findFirst().get()) }
      .toMap
    columnMappings.foreach {
      case (sparqlVar, sqlVar) => result = result.withColumnRenamed(sqlVar.getName, sparqlVar.getName)
    }

    // append the lang tags
    // todo other post processing stuff?
//    signature.asScala
//      .map(v => (v, sparqlVar2Term.get(v)))
//      .foreach {case (v, term) =>
//        if (term.isInstanceOf[NonGroundFunctionalTerm]) {
//          if (term.asInstanceOf[NonGroundFunctionalTerm].getFunctionSymbol.isInstanceOf[RDFTermFunctionSymbol]) {
//            val t = term.asInstanceOf[NonGroundFunctionalTerm]
//            if (t.getArity == 2 && t.getTerm(2).asInstanceOf[NonGroundFunctionalTerm].getFunctionSymbol.isInstanceOf[RDFTermTypeFunctionSymbol]) {
//              val map = t.getTerm(2).asInstanceOf[NonGroundFunctionalTerm].getFunctionSymbol.asInstanceOf[RDFTermTypeFunctionSymbol].getConversionMap
// //              map.get(new DBConstantImpl())
//            }
//          }
//        }
//      }


    // and we also add columns for literal bindings which are not already returned by the converted SQL query but
    // are the result of static bindings, e.g. BIND(1 as ?z)
    Sets.difference(new util.HashSet[Variable](signature), columnMappings.keySet.asJava).asScala.foreach(v => {
      val simplifiedTerm = sparqlVar2Term.apply(v).simplify()
      simplifiedTerm match {
        case constant: Constant =>
          if (simplifiedTerm.isInstanceOf[RDFConstant]) { // the only case we cover
            simplifiedTerm match {
              case iri: IRIConstant => // IRI will be String in Spark
                result = result.withColumn(v.getName, lit(iri.getIRI.getIRIString))
              case l: RDFLiteralConstant => // literals casted to its corresponding type, otherwise String
                val lexicalValue = l.getValue
                val castType = rdfDatatype2SQLCastName.getOrElse(l.getType, StringType)
                result = result.withColumn(v.getName, lit(lexicalValue).cast(castType))
              case _ =>
            }
          } else {
            if (constant.isNull) {

            }
            if (constant.isInstanceOf[DBConstant]) {
              //                throw new SQLOntopBindingSet.InvalidConstantTypeInResultException(constant + "is a DB constant. But a binding cannot have a DB constant as value")
            }
            //              throw new InvalidConstantTypeInResultException("Unexpected constant type for " + constant);
          }
        case _ =>
        //            throw new SQLOntopBindingSet.InvalidTermAsResultException(simplifiedTerm)
      }
    })

    // and finally, we also have to ensure the original order of the projection vars
    result = result.select(signature.asScala.map(v => v.getName).map(col): _*)

    result
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
   * Executes the given SPARQL query on the provided dataset partitions.
   *
   * @param query the SPARQL query
   * @return a DataFrame with the resulting bindings as columns
   * @throws org.apache.spark.sql.AnalysisException if the query execution fails
   */
  def execute(query: String): DataFrame = {
    val (resultRaw, queryRewrite) = executeDebug(query)
    var result = resultRaw

    if (queryRewrite.nonEmpty) {
      result = postProcess(resultRaw, queryRewrite.get)
    }

    result
  }

  /**
   * Executes a SELECT query on the provided dataset partitions and returns a DataFrame.
   *
   * @param query the SPARQL query
   * @return an RDD of solution bindings
   * @throws org.apache.spark.sql.AnalysisException if the query execution fails
   */
  def execSelect(query: String): RDD[Binding] = {
    checkQueryType(query, QueryType.SELECT)

    val df = executeDebug(query)._1

    val sparqlQueryBC = spark.sparkContext.broadcast(query)
    val mappingsBC = spark.sparkContext.broadcast(sparql2sql.mappings)
    val propertiesBC = spark.sparkContext.broadcast(sparql2sql.ontopProperties)
    val partitionerBC = spark.sparkContext.broadcast(partitioner)
    val partitionsBC = spark.sparkContext.broadcast(partitions)
    val ontologyBC = spark.sparkContext.broadcast(ontology)

    implicit val bindingEncoder: Encoder[Binding] = org.apache.spark.sql.Encoders.kryo[Binding]
    df.coalesce(20).mapPartitions(iterator => {
//      println("mapping partition")
      val mapper = new OntopRowMapper(mappingsBC.value, propertiesBC.value, partitionerBC.value, partitionsBC.value, sparqlQueryBC.value, ontologyBC.value)
      val it = iterator.map(mapper.map)
//      mapper.close()
      it
    }).rdd

  }

  /**
   * Executes an ASK query on the provided dataset partitions.
   *
   * @param query the SPARQL query
   * @return `true` or `false` depending on the result of the ASK query execution
   * @throws org.apache.spark.sql.AnalysisException if the query execution fails
   */
  def execAsk(query: String): Boolean = {
    checkQueryType(query, QueryType.ASK)
    val df = executeDebug(query)._1
    !df.isEmpty
  }

  /**
   * Executes a CONSTRUCT query on the provided dataset partitions.
   *
   * @param query the SPARQL query
   * @return an RDD of triples
   * @throws org.apache.spark.sql.AnalysisException if the query execution fails
   */
  def execConstruct(query: String): RDD[org.apache.jena.graph.Triple] = {
    checkQueryType(query, QueryType.CONSTRUCT)

    val df = executeDebug(query)._1

    val sparqlQueryBC = spark.sparkContext.broadcast(query)
    val mappingsBC = spark.sparkContext.broadcast(sparql2sql.mappings)
    val propertiesBC = spark.sparkContext.broadcast(sparql2sql.ontopProperties)
    val partitionerBC = spark.sparkContext.broadcast(partitioner)
    val partitionsBC = spark.sparkContext.broadcast(partitions)
    val ontologyBC = spark.sparkContext.broadcast(ontology)

    implicit val tripleEncoder: Encoder[Triple] = org.apache.spark.sql.Encoders.kryo[Triple]
    df.mapPartitions(iterator => {
      val mapper = new OntopRowMapper(mappingsBC.value, propertiesBC.value, partitionerBC.value, partitionsBC.value, sparqlQueryBC.value, ontologyBC.value)
      val it = mapper.toTriples(iterator)
      mapper.close()
      it
    }).rdd
  }

  private def checkQueryType(query: String, queryType: QueryType) = {
    val q = QueryFactory.create(query)
    if (q.queryType() != queryType) throw new RuntimeException(s"Wrong query type. Expected ${queryType.toString} query," +
      s" got ${q.queryType().toString}")
  }


}

object OntopSPARQLEngine {

  def main(args: Array[String]): Unit = {
    new OntopCLI().run(args)
  }

  def apply(spark: SparkSession,
            databaseName: String,
            partitioner: RdfPartitioner[RdfPartitionStateDefault],
            partitions: Set[RdfPartitionStateDefault],
            ontology: Option[OWLOntology]): OntopSPARQLEngine
  = new OntopSPARQLEngine(spark, databaseName, partitioner, partitions, ontology)

  def apply(spark: SparkSession,
            partitioner: RdfPartitioner[RdfPartitionStateDefault],
            partitions: Map[RdfPartitionStateDefault, RDD[Row]],
            ontology: Option[OWLOntology]): OntopSPARQLEngine = {
    // create and register Spark tables
    SparkTableGenerator(spark).createAndRegisterSparkTables(partitioner, partitions)

    new OntopSPARQLEngine(spark, null, partitioner, partitions.keySet, ontology)
  }

}
