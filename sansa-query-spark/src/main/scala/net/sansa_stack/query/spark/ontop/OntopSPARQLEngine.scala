package net.sansa_stack.query.spark.ontop

import java.io.{File, StringReader}
import java.net.{URI, URLEncoder}
import java.nio.charset.StandardCharsets
import java.nio.file.Paths
import java.sql.{Connection, DriverManager, SQLException}
import java.util
import java.util.Properties

import scala.collection.JavaConverters._

import com.google.common.collect.{ImmutableMap, ImmutableSortedSet, Sets}
import it.unibz.inf.ontop.answering.reformulation.QueryReformulator
import it.unibz.inf.ontop.answering.reformulation.input.SPARQLQuery
import it.unibz.inf.ontop.answering.resultset.OBDAResultSet
import it.unibz.inf.ontop.exception.{MinorOntopInternalBugException, OBDASpecificationException, OntopInternalBugException, OntopReformulationException}
import it.unibz.inf.ontop.injection.{OntopMappingSQLAllConfiguration, OntopReformulationSQLConfiguration}
import it.unibz.inf.ontop.iq.exception.EmptyQueryException
import it.unibz.inf.ontop.iq.node.{ConstructionNode, NativeNode}
import it.unibz.inf.ontop.iq.{IQ, IQTree, UnaryIQTree}
import it.unibz.inf.ontop.model.`type`.{DBTermType, TypeFactory}
import it.unibz.inf.ontop.model.atom.DistinctVariableOnlyDataAtom
import it.unibz.inf.ontop.model.term._
import it.unibz.inf.ontop.model.vocabulary.XSD
import it.unibz.inf.ontop.substitution.{ImmutableSubstitution, SubstitutionFactory}
import org.apache.jena.query.{QueryFactory, QueryType}
import org.apache.jena.sparql.engine.binding.Binding
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.model.{HasDataPropertiesInSignature, HasObjectPropertiesInSignature, OWLOntology}
import scopt.OParser

import net.sansa_stack.query.spark.ontop.VerticalPartitioner.{Config, parser, run, showTables}
import net.sansa_stack.rdf.common.partition.core.{RdfPartitionComplex, RdfPartitionerComplex}
import net.sansa_stack.rdf.spark.partition.core.RdfPartitionUtilsSpark

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
class OntopSPARQL2SQLRewriter(val partitions: Set[RdfPartitionComplex], blankNodeStrategy: BlankNodeStrategy.Value)
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
    DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASSWORD)
  } catch {
    case e: SQLException =>
      logger.error("Error occurred when creating in-memory H2 database", e)
      throw e
  }
  JDBCDatabaseGenerator.generateTables(connection, partitions, blankNodeStrategy)

  // create OBDA mappings
  val mappings = OntopMappingGenerator.createOBDAMappingsForPartitions(partitions)
  println(mappings)

  // the Ontop core
  val (queryReformulator, termFactory, typeFactory, substitutionFactory) = createReformulator(mappings, ontopProperties)
  val inputQueryFactory = queryReformulator.getInputQueryFactory

  // mapping from RDF datatype to Spark SQL datatype
  import org.apache.spark.sql.types.DataTypes._
  val rdfDatatype2SQLCastName = Map(
    typeFactory.getXsdStringDatatype -> StringType,
    typeFactory.getXsdIntegerDatatype -> IntegerType,
    typeFactory.getXsdDecimalDatatype -> createDecimalType(),
    typeFactory.getXsdDoubleDatatype -> DoubleType,
    typeFactory.getXsdBooleanDatatype -> BooleanType,
    typeFactory.getXsdFloatDatatype -> FloatType,
    typeFactory.getDatatype(XSD.SHORT) -> ShortType,
    typeFactory.getDatatype(XSD.DATE) -> DateType,
    typeFactory.getDatatype(XSD.BYTE) -> ByteType,
    typeFactory.getDatatype(XSD.LONG) -> LongType
  )



  @throws[OBDASpecificationException]
  @throws[OntopReformulationException]
  def createSQLQuery(sparqlQuery: String): OntopQueryRewrite = {
    val inputQuery = inputQueryFactory.createSPARQLQuery(sparqlQuery)

    val executableQuery = queryReformulator.reformulateIntoNativeQuery(inputQuery, queryReformulator.getQueryLoggerFactory.create())

    val sqlQuery = extractSQLQuery(executableQuery)
    val constructionNode = extractRootConstructionNode(executableQuery)
    val nativeNode = extractNativeNode(executableQuery)
    val signature = nativeNode.getVariables
    val typeMap = nativeNode.getTypeMap

    OntopQueryRewrite(sparqlQuery, inputQuery, sqlQuery, signature, typeMap, constructionNode,
      executableQuery.getProjectionAtom, constructionNode.getSubstitution, termFactory, typeFactory, substitutionFactory)
  }

  @throws[EmptyQueryException]
  @throws[OntopInternalBugException]
  private def extractSQLQuery(executableQuery: IQ): String = {
    val tree = executableQuery.getTree
    if (tree.isDeclaredAsEmpty) throw new EmptyQueryException
    val queryString = Option(tree)
      .filter((t: IQTree) => t.isInstanceOf[UnaryIQTree])
      .map((t: IQTree) => t.asInstanceOf[UnaryIQTree].getChild.getRootNode)
      .filter(n => n.isInstanceOf[NativeNode])
      .map(n => n.asInstanceOf[NativeNode])
      .map(_.getNativeQueryString)
      .getOrElse(throw new MinorOntopInternalBugException("The query does not have the expected structure " +
        "of an executable query\n" + executableQuery))
    if (queryString == "") throw new EmptyQueryException
    queryString
  }

  @throws[EmptyQueryException]
  private def extractNativeNode(executableQuery: IQ): NativeNode = {
    val tree = executableQuery.getTree
    if (tree.isDeclaredAsEmpty) throw new EmptyQueryException
    Option(tree)
      .filter(t => t.isInstanceOf[UnaryIQTree])
      .map(t => t.asInstanceOf[UnaryIQTree].getChild.getRootNode)
      .filter(n => n.isInstanceOf[NativeNode])
      .map(n => n.asInstanceOf[NativeNode])
      .getOrElse(throw new MinorOntopInternalBugException("The query does not have the expected structure " +
        "for an executable query\n" + executableQuery))
  }

  @throws[EmptyQueryException]
  @throws[OntopInternalBugException]
  private def extractRootConstructionNode(executableQuery: IQ): ConstructionNode = {
    val tree = executableQuery.getTree
    if (tree.isDeclaredAsEmpty) throw new EmptyQueryException
    Option(tree.getRootNode)
      .filter(n => n.isInstanceOf[ConstructionNode])
      .map(n => n.asInstanceOf[ConstructionNode])
      .getOrElse(throw new MinorOntopInternalBugException(
        "The \"executable\" query is not starting with a construction node\n" + executableQuery))
  }

  /**
   * Instantiation of the query reformulator
   */
  @throws[OBDASpecificationException]
  def createReformulator(obdaMappings: String, properties: Properties): (QueryReformulator, TermFactory, TypeFactory, SubstitutionFactory) = {
    val obdaSpecification = loadOBDASpecification(obdaMappings, properties)
    val reformulationConfiguration = OntopReformulationSQLConfiguration.defaultBuilder
      .obdaSpecification(obdaSpecification)
      .jdbcUrl(JDBC_URL)
      .properties(properties)
      .enableTestMode
      .build

    val termFactory = reformulationConfiguration.getTermFactory
    val typeFactory = reformulationConfiguration.getTypeFactory
    val queryReformulator = reformulationConfiguration.loadQueryReformulator
    val substitutionFactory = reformulationConfiguration.getInjector.getInstance(classOf[SubstitutionFactory])

    (queryReformulator, termFactory, typeFactory, substitutionFactory)
  }

  @throws[OBDASpecificationException]
  private def loadOBDASpecification(obdaMappings: String, properties: Properties) = {
    val mappingConfiguration = OntopMappingSQLAllConfiguration.defaultBuilder
      .nativeOntopMappingReader(new StringReader(obdaMappings))
      .jdbcUrl(JDBC_URL)
      .jdbcUser(JDBC_USER)
      .jdbcPassword(JDBC_PASSWORD)
      .properties(properties)
      .enableTestMode
      .build
    mappingConfiguration.loadSpecification
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
  def apply(partitions: Set[RdfPartitionComplex], blankNodeStrategy: BlankNodeStrategy.Value): OntopSPARQL2SQLRewriter = {
    new OntopSPARQL2SQLRewriter(partitions, blankNodeStrategy)
  }
}


class OntopSPARQLEngine(val spark: SparkSession,
                        databaseName: String,
                        partitions: Set[RdfPartitionComplex]) {

  /**
   * Partition RDDs will be registered as Spark tables.
   * @param spark
   * @param partitions
   */
  def this(spark: SparkSession, partitions: Map[RdfPartitionComplex, RDD[Row]]) {
    this(spark, null, partitions.keySet)

    // create and register Spark tables
    registerSparkTables(partitions)
  }

  val blankNodeStrategy: BlankNodeStrategy.Value = BlankNodeStrategy.Table

  private val sparql2sql = OntopSPARQL2SQLRewriter(partitions, blankNodeStrategy)

  private val logger = com.typesafe.scalalogging.Logger(OntopSPARQLEngine.getClass.getName)

  val typeFactory = sparql2sql.typeFactory
  // mapping from RDF datatype to Spark SQL datatype
  import org.apache.spark.sql.types.DataTypes._
  val rdfDatatype2SQLCastName = Map(
    typeFactory.getXsdStringDatatype -> StringType,
    typeFactory.getXsdIntegerDatatype -> IntegerType,
    typeFactory.getXsdDecimalDatatype -> createDecimalType(),
    typeFactory.getXsdDoubleDatatype -> DoubleType,
    typeFactory.getXsdBooleanDatatype -> BooleanType,
    typeFactory.getXsdFloatDatatype -> FloatType,
    typeFactory.getDatatype(XSD.SHORT) -> ShortType,
    typeFactory.getDatatype(XSD.DATE) -> DateType,
    typeFactory.getDatatype(XSD.BYTE) -> ByteType,
    typeFactory.getDatatype(XSD.LONG) -> LongType
  )


  val useHive: Boolean = false
  val useStatistics: Boolean = true

  if (databaseName != null && databaseName.trim.nonEmpty) {
    spark.sql(s"USE $databaseName")
  }

  implicit val o: Ordering[RdfPartitionComplex] = Ordering.by(e => (e.predicate, e.subjectType, e.objectType, e.langTagPresent, e.lang, e.datatype))

  private def registerSparkTables(partitions: Map[RdfPartitionComplex, RDD[Row]]): Unit = {

//    val partitions = Map(partitionsMap.toArray: _*) // scala.collection.immutable.TreeMap(partitionsMap.toArray: _*)

    // register the lang tagged RDDs as a single table:
    // we have to merge the RDDs of all languages per property first, otherwise we would always replace it by another
    // language
    partitions
      .filter(_._1.lang.nonEmpty)
      .map { case (p, rdd) => (p.predicate, p, rdd) }
      .groupBy(_._1)
      .map { case (k, v) =>
        val rdd = spark.sparkContext.union(v.map(_._3).toSeq)
        val p = v.head._2
        (p, rdd)
      }
      .foreach {case (p, rdd) => createSparkTable(p, rdd)}

    // register the non-lang tagged RDDs as table
    partitions
      .filter(_._1.lang.isEmpty)
      .foreach {
        case (p, rdd) => createSparkTable(p, rdd)
      }
  }

  /**
   * creates a Spark table for each RDF partition
   */
  private def createSparkTable(p: RdfPartitionComplex, rdd: RDD[Row]) = {

    val name = SQLUtils.createTableName(p, blankNodeStrategy)
    logger.debug(s"creating Spark table ${escapeTablename(name)}")
    println(s"creating Spark table ${escapeTablename(name)}")

    val scalaSchema = p.layout.schema
    val sparkSchema = ScalaReflection.schemaFor(scalaSchema).dataType.asInstanceOf[StructType]
    val df = spark.createDataFrame(rdd, sparkSchema).persist()
//    df.show(false)

    if (useHive) {
      df.createOrReplaceTempView("`" + escapeTablename(name) + "_tmp`")

      val schemaDDL = spark.createDataFrame(rdd, sparkSchema).schema.toDDL
      spark.sql(s"DROP TABLE IF EXISTS `${escapeTablename(name)}`")
      val query =
        s"""
           |CREATE TABLE IF NOT EXISTS `${escapeTablename(name)}`
           |
           |USING PARQUET
           |PARTITIONED BY (`s`)
           |AS SELECT * FROM `${escapeTablename(name)}_tmp`
           |""".stripMargin
      spark.sql(query)
      if (useStatistics) {
        spark.sql(s"ANALYZE TABLE `${escapeTablename(name)}` COMPUTE STATISTICS FOR COLUMNS s, o")
      }
    } else {
      df.createOrReplaceTempView("`" + escapeTablename(name) + "`")
      //          df.write.partitionBy("s").format("parquet").saveAsTable(escapeTablename(name))
    }
  }

  private def escapeTablename(path: String): String =
    URLEncoder.encode(path, StandardCharsets.UTF_8.toString)
      .toLowerCase
      .replace('%', 'P')
      .replace('.', 'C')
      .replace("-", "dash")


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
////              map.get(new DBConstantImpl())
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
      println(sql)

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
    val q = QueryFactory.create(query)
    if (!q.isSelectType) throw new RuntimeException(s"Wrong query type. Expected SELECT query," +
      s" got ${q.queryType().toString}")
    val df = executeDebug(query)._1
//    df.show(false)
//
//    var input = ""
//    while (input != "q") {
//      println("enter SQL query (press 'q' to quit): ")
//      input = scala.io.StdIn.readLine()
//      try {
//        spark.sql(input).show(false)
//      } catch {
//        case e: Exception => e.printStackTrace()
//      }
//
//    }


    val sparqlQueryBC = spark.sparkContext.broadcast(query)
    val mappingsBC = spark.sparkContext.broadcast(sparql2sql.mappings)
    val propertiesBC = spark.sparkContext.broadcast(sparql2sql.ontopProperties)
    val partitionsBC = spark.sparkContext.broadcast(partitions)

    implicit val myObjEncoder = org.apache.spark.sql.Encoders.kryo[Binding]
    df.coalesce(20).mapPartitions(iterator => {
      println("mapping partition")
      val mapper = new OntopRowMapper(mappingsBC.value, propertiesBC.value, partitionsBC.value, sparqlQueryBC.value)
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
    val q = QueryFactory.create(query)
    if (!q.isAskType) throw new RuntimeException(s"Wrong query type. Expected ASK query," +
      s" got ${q.queryType().toString}")
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
    val q = QueryFactory.create(query)
    if (!q.isConstructType) throw new RuntimeException(s"Wrong query type. Expected CONSTRUCT query," +
      s" got ${q.queryType().toString}")

    val df = executeDebug(query)._1

    val sparqlQueryBC = spark.sparkContext.broadcast(query)
    val mappingsBC = spark.sparkContext.broadcast(sparql2sql.mappings)
    val propertiesBC = spark.sparkContext.broadcast(sparql2sql.ontopProperties)
    val partitionsBC = spark.sparkContext.broadcast(partitions)

    implicit val myObjEncoder = org.apache.spark.sql.Encoders.kryo[org.apache.jena.graph.Triple]
    df.mapPartitions(iterator => {
      val mapper = new OntopRowMapper(mappingsBC.value, propertiesBC.value, partitionsBC.value, sparqlQueryBC.value)
      val it = mapper.toTriples(iterator)
      mapper.close()
      it
    }).rdd
  }

}

object OntopSPARQLEngine {

  val warehouseLocation = new File("spark-warehouse").getAbsolutePath

  def main(args: Array[String]): Unit = {
    OParser.parse(parser, args, Config()) match {
      case Some(config) =>
        run(config)
      case _ =>
      // arguments are bad, error message will have been displayed
    }
  }

  private def run(config: Config): Unit = {
    import net.sansa_stack.rdf.spark.io._

    val spark = SparkSession.builder
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // .config("spark.kryo.registrationRequired", "true")
      // .config("spark.eventLog.enabled", "true")
      .config("spark.kryo.registrator", String.join(", ",
        "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator",
        "net.sansa_stack.query.spark.ontop.OntopKryoRegistrator"))
      //      .config("spark.default.parallelism", "4")
      //      .config("spark.sql.shuffle.partitions", "4")
      //      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("spark.sql.cbo.enabled", true)
      .config("spark.sql.statistics.histogram.enabled", true)
      .config("spark.sql.crossJoin.enabled", "true")
      .enableHiveSupport()
      .getOrCreate()

    val sparqlEngine =
      if (config.inputPath != null) {
        // read triples as RDD[Triple]
        var triplesRDD = spark.ntriples()(config.inputPath.toString).cache()

        // load optional schema file and filter properties used for VP
        var ont: OWLOntology = null

        if (config.schemaPath != null) {
          val owlFile = new File(config.schemaPath)
          val man = OWLManager.createOWLOntologyManager()
          ont = man.loadOntologyFromOntologyDocument(owlFile)
          //    val cleanOnt = man.createOntology()
          //    man.addAxioms(cleanOnt, ont.asInstanceOf[HasLogicalAxioms].getLogicalAxioms)
          //
          //    owlFile = "/tmp/clean-dbo.nt"
          //    man.saveOntology(cleanOnt, new FileOutputStream(owlFile))

          // get all object properties in schema file
          val objectProperties = ont.asInstanceOf[HasObjectPropertiesInSignature].getObjectPropertiesInSignature.iterator().asScala.map(_.toStringID).toSet

          // get all object properties in schema file
          val dataProperties = ont.asInstanceOf[HasDataPropertiesInSignature].getDataPropertiesInSignature.iterator().asScala.map(_.toStringID).toSet

          var schemaProperties = objectProperties ++ dataProperties
          schemaProperties = Set("http://dbpedia.org/ontology/birthPlace", "http://dbpedia.org/ontology/birthDate")

          // filter triples RDD
          triplesRDD = triplesRDD.filter(t => schemaProperties.contains(t.getPredicate.getURI))
        }

        // do partitioning here
        val partitions: Map[RdfPartitionComplex, RDD[Row]] = RdfPartitionUtilsSpark.partitionGraph(triplesRDD, partitioner = RdfPartitionerComplex(false))
        println(s"num partitions: ${partitions.keySet.size}")

        // create the SPARQL engine
        new OntopSPARQLEngine(spark, partitions)
      } else {
        // load partitioning metadata
        val partitions = PartitionSerDe.deserializeFrom(Paths.get(config.metaDataPath))
        // create the SPARQL engine
        new OntopSPARQLEngine(spark, config.databaseName, partitions)
      }

    var input = config.initialQuery

    def runQuery(query: String) = {
      try {
        val q = QueryFactory.create(query)

        q.queryType() match {
          case QueryType.SELECT =>
            val res = sparqlEngine.execSelect(query)
            println(res.collect().mkString("\n"))
          case QueryType.ASK =>
            val res = sparqlEngine.execAsk(query)
            println(res)
          case QueryType.CONSTRUCT =>
            val res = sparqlEngine.execConstruct(query)
            println(res.collect().mkString("\n"))
        }
      } catch {
        case e: Exception => Console.err.println("failed to execute query")
          e.printStackTrace()
      }
    }

    if (input != null) {
      runQuery(input)
    }

    while (input != "q") {
      println("enter SPARQL query (press 'q' to quit): ")
      input = scala.io.StdIn.readLine()

      runQuery(input)
    }

    sparqlEngine.stop()

    spark.stop()
  }

  case class Config(
                     inputPath: URI = null,
                     metaDataPath: URI = null,
                     schemaPath: URI = null,
                     databaseName: String = null,
                     initialQuery: String = null)

  import scopt.OParser
  val builder = OParser.builder[Config]
  val parser = {
    import builder._
    OParser.sequence(
      programName("Ontop SPARQL Engine"),
      head("Ontop SPARQL Engine", "0.1.0"),
      opt[URI]('i', "input")
        .action((x, c) => c.copy(inputPath = x))
        .text("path to input data"),
      opt[URI]('m', "metadata")
        .action((x, c) => c.copy(metaDataPath = x))
        .text("path to partitioning metadata"),
      opt[String]("database")
        .abbr("db")
        .action((x, c) => c.copy(databaseName = x))
        .text("the name of the Spark databases used as KB"),
      opt[URI]('s', "schema")
        .optional()
        .action((x, c) => c.copy(schemaPath = x))
        .text("an optional file containing the OWL schema to process only object and data properties"),
      opt[String]('q', "query")
        .optional()
        .action((x, c) => c.copy(initialQuery = x))
        .text("an initial SPARQL query that will be executed"),
      checkConfig( c =>
        if (c.databaseName != null && c.inputPath != null) failure("either specify path to data or an already created database")
        else success ),
        checkConfig( c =>
        if (c.databaseName != null && c.metaDataPath == null) failure("If database is used the path to the partitioning " +
          "metadata has to be provided as well")
        else success )
    )
  }


}
