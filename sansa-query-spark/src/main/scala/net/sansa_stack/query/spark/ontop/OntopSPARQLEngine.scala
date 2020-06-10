package net.sansa_stack.query.spark.ontop

import java.io.{File, StringReader}
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.sql.{Connection, DatabaseMetaData, DriverManager, ResultSet, SQLException}
import java.util
import java.util.Properties

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe.typeOf
import com.esotericsoftware.kryo
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import com.google.common.collect.{ImmutableList, ImmutableMap, ImmutableSortedSet, Sets}
import it.unibz.inf.ontop.answering.reformulation.QueryReformulator
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
import org.apache.jena.datatypes.TypeMapper
import org.apache.jena.graph.NodeFactory
import org.apache.jena.query.{QuerySolution, QuerySolutionMap}
import org.apache.jena.rdf.model.{RDFNode, ResourceFactory}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.model.{HasDataPropertiesInSignature, HasObjectPropertiesInSignature, OWLOntology}
import net.sansa_stack.rdf.common.partition.core.{RdfPartitionComplex, RdfPartitionerComplex}
import net.sansa_stack.rdf.common.partition.schema._
import net.sansa_stack.rdf.spark.io.JenaKryoRegistrator
import net.sansa_stack.rdf.spark.partition.core.RdfPartitionUtilsSpark

case class OntopQueryRewrite(sparqlQuery: String,
                             sqlQuery: String,
                             sqlSignature: ImmutableSortedSet[Variable],
                             sqlTypeMap: ImmutableMap[Variable, DBTermType],
                             constructionNode: ConstructionNode,
                             answerAtom: DistinctVariableOnlyDataAtom,
                             sparqlVar2Term: ImmutableSubstitution[ImmutableTerm],
                             termFactory: TermFactory,
                             typeFactory: TypeFactory,
                             substitutionFactory: SubstitutionFactory
                            ) {

}

class RowMapper(queryRewrite: OntopQueryRewrite,
                termFactory: TermFactory,
                substitutionFactory: SubstitutionFactory)
  extends (Row => Int)
    with Serializable {

  override def apply(row: Row): Int = {
    val qs = new QuerySolutionMapSer()

    val builder: ImmutableMap.Builder[Variable, Constant] = ImmutableMap.builder()
    queryRewrite.sqlSignature.asScala
      .foreach(v => builder.put(v,
        convertToConstant(row.getAs[String](v.getName),
          queryRewrite.sqlTypeMap.get(v))
      ))

    val sqlVar2Constant = substitutionFactory.getSubstitution(builder.build())

    val composition = sqlVar2Constant.composeWith(queryRewrite.constructionNode.getSubstitution)

    queryRewrite.answerAtom.getArguments.asScala.foreach(v => {
      val rdfNode = evaluate(composition.apply(v))
      if (rdfNode.isDefined) {
        qs.add(v.getName, rdfNode.get)
      }
    })

    qs

    4
  }

  /**
   * Convert term to Apache Jena node object
   */
  private def evaluate(term: ImmutableTerm): Option[RDFNode] = {
    val simplifiedTerm = term.simplify()
    if (simplifiedTerm.isInstanceOf[Constant]) {
      if (simplifiedTerm.isInstanceOf[RDFConstant]) {
        simplifiedTerm match {
          case iri: IRIConstant => Some(ResourceFactory.createResource(iri.getIRI.getIRIString))
          case l: RDFLiteralConstant => // literals casted to its corresponding type, otherwise String
            val lexicalValue = l.getValue
            val lang = l.getType.getLanguageTag
            val dt = TypeMapper.getInstance().getSafeTypeByName(l.getType.getIRI.getIRIString)
            if (lang.isPresent) {
              Some(ResourceFactory.createLangLiteral(lexicalValue, lang.get().getFullString))
            } else {
              Some(ResourceFactory.createTypedLiteral(lexicalValue, dt))
            }
          case _ => None
        }
      } else {
        None
      }
    } else {
      None
    }
  }

  private def convertToConstant(jdbcValue: String, termType: DBTermType): Constant = {
    if (jdbcValue == null) return termFactory.getNullConstant
    termFactory.getDBConstant(jdbcValue, termType)
  }
}

class OntopSPARQL2SQLRewriter(val partitions: Seq[RdfPartitionComplex])
  extends Serializable {

  // load Ontop properties
  val ontopProperties = new Properties()
  ontopProperties.load(getClass.getClassLoader.getResourceAsStream("ontop-spark.properties"))

  // mapping from partition type to H2 database type
  private val partitionType2DatabaseType = Map(
    typeOf[SchemaStringLong] -> "LONG",
    typeOf[SchemaStringDouble] -> "DOUBLE",
    typeOf[SchemaStringFloat] -> "FLOAT",
    typeOf[SchemaStringDecimal] -> "DECIMAL",
    typeOf[SchemaStringBoolean] -> "BOOLEAN",
    typeOf[SchemaStringString] -> "VARCHAR(255)",
    typeOf[SchemaStringDate] -> "DATE"
  ) // .map(e => (typeOf[e._1.type], e._2))

  private val JDBC_URL = "jdbc:h2:mem:sansaontopdb;DATABASE_TO_UPPER=FALSE"
  private val JDBC_USER = "sa"
  private val JDBC_PASSWORD = ""

  createTempDB(partitions)

  val md: DatabaseMetaData = connection.getMetaData
  val rs: ResultSet = md.getTables(null, null, "%", null)
  while (rs.next) println(rs.getString(3))

  // create OBDA mappings
  val mappings = createOBDAMappingsForPartitions(partitions)

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

  /*
   * DB connection (keeps it alive)
   */
  var connection: Connection = null

  private def createTempDB(partitions: Seq[RdfPartitionComplex]): Unit = {

    try {
      connection = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASSWORD)

      val stmt = connection.createStatement()

      stmt.executeUpdate("DROP ALL OBJECTS")

      partitions.foreach {
        case p: RdfPartitionComplex =>

          val name = createTableName(p)

          val sparkSchema = ScalaReflection.schemaFor(p.layout.schema).dataType.asInstanceOf[StructType]
          println(p.predicate + "\t" + sparkSchema + "\t" + p.asInstanceOf[RdfPartitionComplex].layout.schema)

          p match {
            case RdfPartitionComplex(subjectType, predicate, objectType, datatype, langTagPresent) =>
              objectType match {
                case 1 => stmt.addBatch(s"CREATE TABLE IF NOT EXISTS ${escapeTablename(name)} (" +
                  "s varchar(255) NOT NULL," +
                  "o varchar(255) NOT NULL" +
                  ")")
                case 2 => if (langTagPresent) {
                  stmt.addBatch(s"CREATE TABLE IF NOT EXISTS ${escapeTablename(name)} (" +
                    "s varchar(255) NOT NULL," +
                    "o varchar(255) NOT NULL," +
                    "l varchar(10) NOT NULL" +
                    ")")
                } else {
                  println(s"datatype: $datatype")
                  if (p.layout.schema == typeOf[SchemaStringStringType]) {
                    stmt.addBatch(s"CREATE TABLE IF NOT EXISTS ${escapeTablename(name)} (" +
                      "s varchar(255) NOT NULL," +
                      "o varchar(255) NOT NULL," +
                      "t varchar(255) NOT NULL" +
                      ")")
                  } else {
                    val colType = partitionType2DatabaseType.get(p.layout.schema)

                    if (colType.isDefined) {
                      stmt.addBatch(
                        s"""
                           |CREATE TABLE IF NOT EXISTS ${escapeTablename(name)} (
                           |s varchar(255) NOT NULL,
                           |o ${colType.get} NOT NULL)
                           |""".stripMargin)
                    } else {
                      println(s"couldn't create table for schema ${p.layout.schema}")
                    }
                  }
                }
                case _ => println("TODO: bnode SQL table for Ontop mappings")
              }
            case _ => println("wrong partition type")
          }
      }
      //            stmt.addBatch(s"CREATE TABLE IF NOT EXISTS triples (" +
      //              "s varchar(255) NOT NULL," +
      //              "p varchar(255) NOT NULL," +
      //              "o varchar(255) NOT NULL" +
      //              ")")
      val numTables = stmt.executeBatch().length
      println(s"created $numTables tables")
    } catch {
      case e: SQLException => e.printStackTrace()
    }
    connection.commit()
    //    connection.close()
  }

  private def createOBDAMappingsForPartitions(partitions: Seq[RdfPartitionComplex]): String = {

    def createMapping(id: String, tableName: String, property: String): String = {
      s"""
         |mappingId     $id
         |source        SELECT "s", "o" FROM ${escapeTablename(tableName)}
         |target        <{s}> <$property> <{o}> .
         |""".stripMargin
    }

    def createMappingLit(id: String, tableName: String, property: String, datatypeURI: String): String = {
      s"""
         |mappingId     $id
         |source        SELECT "s", "o" FROM ${escapeTablename(tableName)}
         |target        <{s}> <$property> "{o}"^^<$datatypeURI> .
         |""".stripMargin
    }

    def createMappingLiteralWithType(id: String, tableName: String, property: String): String = {
      s"""
         |mappingId     $id
         |source        SELECT "s", "o", "t" FROM ${escapeTablename(tableName)}
         |target        <{s}> <$property> "{o}"^^<{t}> .
         |""".stripMargin
    }

    def createMappingLang(id: String, tableName: String, property: String): String = {
      s"""
         |mappingId     $id
         |source        SELECT "s", "o", "l" FROM ${escapeTablename(tableName)}
         |target        <{s}> <$property> "{o}@{l}" .
         |""".stripMargin
    }

    val triplesMapping =
      s"""
         |mappingId     triples
         |source        SELECT `s`, `p`, `o` FROM `triples`
         |target        <{s}> <http://sansa.net/ontology/triples> "{o}" .
         |""".stripMargin

    "[MappingDeclaration] @collection [[" +
      partitions
        .map {
          case p@RdfPartitionComplex(subjectType, predicate, objectType, datatype, langTagPresent) =>
            val tableName = createTableName(p)
            val id = escapeTablename(tableName)
            objectType match {
              case 1 => createMapping(id, tableName, predicate)
              case 2 => if (langTagPresent) createMappingLang(id, tableName, predicate)
              else createMappingLit(id, tableName, predicate, datatype)
              case _ => println("TODO: bnode Ontop mapping")
                ""
            }
        }
        .mkString("\n\n") + "]]"
  }

  @throws[OBDASpecificationException]
  @throws[OntopReformulationException]
  def createSQLQuery(sparqlQuery: String): OntopQueryRewrite = {
    val query = inputQueryFactory.createSPARQLQuery(sparqlQuery)

    val executableQuery = queryReformulator.reformulateIntoNativeQuery(query)

    val sqlQuery = extractSQLQuery(executableQuery)
    val constructionNode = extractRootConstructionNode(executableQuery)
    val nativeNode = extractNativeNode(executableQuery)
    val signature = nativeNode.getVariables
    val typeMap = nativeNode.getTypeMap

    OntopQueryRewrite(sparqlQuery, sqlQuery, signature, typeMap, constructionNode,
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
      //      .properties(properties)
      .enableTestMode
      .build
    mappingConfiguration.loadSpecification
  }

  private def escapeTablename(path: String): String =
  "\"" +
    URLEncoder.encode(path, StandardCharsets.UTF_8.toString)
      .toLowerCase
      .replace('%', 'P')
      .replace('.', 'C')
      .replace("-", "dash") +
    "\""


  private def createTableName(p: RdfPartitionComplex): String = {
    val pred = p.predicate

    // For now let's just use the full predicate as the uri
    // val predPart = pred.substring(pred.lastIndexOf("/") + 1)
    val predPart = pred
    val pn = NodeFactory.createURI(p.predicate)

    val dt = p.datatype
    val dtPart = if (dt != null && !dt.isEmpty) "_" + dt.substring(dt.lastIndexOf("/") + 1) else ""
    val langPart = if (p.langTagPresent) "_lang" else ""

    val tableName = predPart + dtPart + langPart // .replace("#", "__").replace("-", "_")

    tableName
  }

  def mapRow(row: Row,
                     signature: ImmutableList[Variable],
                     sqlSignature: ImmutableSortedSet[Variable],
                     sparqlVar2Term: ImmutableSubstitution[ImmutableTerm],
                     sqlTypeMap: ImmutableMap[Variable, DBTermType]): QuerySolution = {

    val qs = new QuerySolutionMapSer()

    val builder: ImmutableMap.Builder[Variable, Constant] = ImmutableMap.builder()
    sqlSignature.asScala
      .foreach(v => builder.put(v,
        convertToConstant(row.getAs[String](v.getName),
          sqlTypeMap.get(v))
      ))

    val sqlVar2Constant = substitutionFactory.getSubstitution(builder.build())

    val composition = sqlVar2Constant.composeWith(sparqlVar2Term)

    signature.asScala.foreach(v => {
      val rdfNode = evaluate(composition.apply(v))
      if (rdfNode.isDefined) {
        qs.add(v.getName, rdfNode.get)
      }
    })

    qs
  }

  /**
   * Convert term to Apache Jena node object
   */
  private def evaluate(term: ImmutableTerm): Option[RDFNode] = {
    val simplifiedTerm = term.simplify()
    if (simplifiedTerm.isInstanceOf[Constant]) {
      if (simplifiedTerm.isInstanceOf[RDFConstant]) {
        simplifiedTerm match {
          case iri: IRIConstant => Some(ResourceFactory.createResource(iri.getIRI.getIRIString))
          case l: RDFLiteralConstant => // literals casted to its corresponding type, otherwise String
            val lexicalValue = l.getValue
            val lang = l.getType.getLanguageTag
            val dt = TypeMapper.getInstance().getSafeTypeByName(l.getType.getIRI.getIRIString)
            if (lang.isPresent) {
              Some(ResourceFactory.createLangLiteral(lexicalValue, lang.get().getFullString))
            } else {
              Some(ResourceFactory.createTypedLiteral(lexicalValue, dt))
            }
          case _ => None
        }
      } else {
        None
      }
    } else {
      None
    }
  }

  private def convertToConstant(jdbcValue: String, termType: DBTermType): Constant = {
    if (jdbcValue == null) return termFactory.getNullConstant
    termFactory.getDBConstant(jdbcValue, termType)
  }
}

object OntopSPARQL2SQLRewriter {
  def apply(partitions: Seq[RdfPartitionComplex]): OntopSPARQL2SQLRewriter = {
    new OntopSPARQL2SQLRewriter(partitions)
  }
}



//object RowMapper extends RowMapper(null) {}


class OntopSPARQLEngine(val spark: SparkSession,
                        val partitions: Map[RdfPartitionComplex, RDD[Row]]) {


  val sparql2sql = OntopSPARQL2SQLRewriter(partitions.keySet.toSeq)

  private val logger = com.typesafe.scalalogging.Logger(OntopSPARQLEngine.getClass.getName)

  case class OntopQueryRewrite(sparqlQuery: String,
                               sqlQuery: String,
                               sqlSignature: ImmutableSortedSet[Variable],
                               sqlTypeMap: ImmutableMap[Variable, DBTermType],
                               constructionNode: ConstructionNode,
                               answerAtom: DistinctVariableOnlyDataAtom,
                               sparqlVar2Term: ImmutableSubstitution[ImmutableTerm],
                               termFactory: TermFactory,
                               typeFactory: TypeFactory,
                               substitutionFactory: SubstitutionFactory
                              ) {

  }

  // load Ontop properties
  val ontopProperties = new Properties()
  ontopProperties.load(getClass.getClassLoader.getResourceAsStream("ontop-spark.properties"))

  // mapping from partition type to H2 database type
  private val partitionType2DatabaseType = Map(
    typeOf[SchemaStringLong] -> "LONG",
    typeOf[SchemaStringDouble] -> "DOUBLE",
    typeOf[SchemaStringFloat] -> "FLOAT",
    typeOf[SchemaStringDecimal] -> "DECIMAL",
    typeOf[SchemaStringBoolean] -> "BOOLEAN",
    typeOf[SchemaStringString] -> "VARCHAR(255)",
    typeOf[SchemaStringDate] -> "DATE"
  ) // .map(e => (typeOf[e._1.type], e._2))


  private def init(): Unit = {

    // create and register Spark tables
    partitions.foreach {
      case (p, rdd) => createSparkTable(spark, p, rdd)
    }
  }

  init()


  val typeFactory = sparql2sql.typeFactory
  val inputQueryFactory = sparql2sql.queryReformulator.getInputQueryFactory
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



  private def createTableName(p: RdfPartitionComplex): String = {
    val pred = p.predicate

    // For now let's just use the full predicate as the uri
    // val predPart = pred.substring(pred.lastIndexOf("/") + 1)
    val predPart = pred
    val pn = NodeFactory.createURI(p.predicate)

    val dt = p.datatype
    val dtPart = if (dt != null && !dt.isEmpty) "_" + dt.substring(dt.lastIndexOf("/") + 1) else ""
    val langPart = if (p.langTagPresent) "_lang" else ""

    val tableName = predPart + dtPart + langPart // .replace("#", "__").replace("-", "_")

    tableName
  }


  val useHive: Boolean = false
  val useStatistics: Boolean = true

  /**
   * creates a Spark table for each RDF partition
   */
  private def createSparkTable(session: SparkSession, p: RdfPartitionComplex, rdd: RDD[Row]) = {

    val name = createTableName(p)
    println(s"creating Spark table ${escapeTablename(name)}")

    val scalaSchema = p.layout.schema
    val sparkSchema = ScalaReflection.schemaFor(scalaSchema).dataType.asInstanceOf[StructType]
    val df = session.createDataFrame(rdd, sparkSchema).persist()
//    df.show(false)

    if (useHive) {
      df.createOrReplaceTempView("`" + escapeTablename(name) + "_tmp`")

      val schemaDDL = session.createDataFrame(rdd, sparkSchema).schema.toDDL
      session.sql(s"DROP TABLE IF EXISTS `${escapeTablename(name)}`")
      val query =
        s"""
           |CREATE TABLE IF NOT EXISTS `${escapeTablename(name)}`
           |
           |USING PARQUET
           |PARTITIONED BY (`s`)
           |AS SELECT * FROM `${escapeTablename(name)}_tmp`
           |""".stripMargin
      session.sql(query)
      if (useStatistics) {
        session.sql(s"ANALYZE TABLE `${escapeTablename(name)}` COMPUTE STATISTICS FOR COLUMNS s, o")
      }
    } else {
      df.createOrReplaceTempView("`" + escapeTablename(name) + "`")
      //          df.write.partitionBy("s").format("parquet").saveAsTable(escapeTablename(name))
    }

  }


  val QUOTATION_STRING = "\""
  private def escapeTablename(path: String): String =
    QUOTATION_STRING +
    URLEncoder.encode(path, StandardCharsets.UTF_8.toString)
      .toLowerCase
      .replace('%', 'P')
      .replace('.', 'C')
      .replace("-", "dash") +
      QUOTATION_STRING


  /**
   * Shutdown of the engine, i.e. all open resource will be closed.
   */
  def stop(): Unit = {
    sparql2sql.connection.close()
  }

  def genMapper(kryoWrapper: KryoSerializationWrapper[(Row => Int)]) (row: Row) : Int = {
    kryoWrapper.value.apply(row)
  }

  def executeNative(query: String): RDD[Int] = {
    logger.info(s"SPARQL query:\n$query")

    // translate to SQL query
    val queryRewrite = sparql2sql.createSQLQuery(query)
    val sql = queryRewrite.sqlQuery.replace("\"", "`")
      .replace("`PUBLIC`.", "")
    logger.info(s"SQL query:\n$sql")

    // execute SQL query
    val result = spark.sql(sql)

    // map Spark row to Jena QuerySolution object
//    implicit val qsEncoder = org.apache.spark.sql.Encoders.kryo[QuerySolution]
//    result.map(row => mapRow(row,
//      queryRewrite.answerAtom.getArguments,
//      queryRewrite.sqlSignature,
//      queryRewrite.constructionNode.getSubstitution,
//      queryRewrite.sqlTypeMap))
//      .rdd

    val mappingsLocal = sparql2sql.mappings
    val props = ontopProperties

    val (queryReformulator, termFactory, typeFactory, substitutionFactory) = sparql2sql.createReformulator(mappingsLocal, props)

    val mapper = genMapper(KryoSerializationWrapper(new RowMapper(queryRewrite, termFactory, substitutionFactory))) _
//    implicit val qsEncoder = org.apache.spark.sql.Encoders.kryo[QuerySolution]
    import spark.implicits._
    result.map(KryoSerializationWrapper(new RowMapper(queryRewrite, termFactory, substitutionFactory)).value.apply)
          .rdd
  }

  /**
   * Executes the given SPARQL query on the provided dataset partitions.
   *
   * @param query the SPARQL query
   * @return a DataFrame with the resulting bindings as columns
   */
  def execute(query: String): DataFrame = {
    logger.info(s"SPARQL query:\n$query")

    // translate to SQL query
    val queryRewrite = sparql2sql.createSQLQuery(query)
    val sql = queryRewrite.sqlQuery.replace("\"", "`")
                                  .replace("`PUBLIC`.", "")
    logger.info(s"SQL query:\n$sql")

    // execute SQL query
    var result = spark.sql(sql)
//    result.show(false)
//    result.printSchema()

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

}

class QuerySolutionMapSer() extends QuerySolutionMap with Serializable {

}

// class QuerySolutionSerializer extends kryo.Serializer[QuerySolution]{
//  override def write(kryo: Kryo, output: Output, t: QuerySolution): Unit =
//    kryo.writeClassAndObject(output, t.asInstanceOf[QuerySolutionMap].asMap())
//
//  override def read(kryo: Kryo, input: Input, aClass: Class[QuerySolution]): QuerySolution =
//    new QuerySolutionMap().kryo.readClassAndObject(input).asInstanceOf[Map[String, RDFNode]]
// }

class ExtendedJenaKryoRegistrator extends JenaKryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit = {
    super.registerClasses(kryo)
    kryo.register(classOf[QuerySolutionMapSer])
  }

}

object OntopSPARQLEngine {

  val warehouseLocation = new File("spark-warehouse").getAbsolutePath

  def main(args: Array[String]): Unit = {
    import net.sansa_stack.rdf.spark.io._

    val spark = SparkSession.builder
      .master("local")
      .appName("playground")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // .config("spark.kryo.registrationRequired", "true")
      // .config("spark.eventLog.enabled", "true")
      .config("spark.kryo.registrator", String.join(", ",
        "net.sansa_stack.query.spark.ontop.ExtendedJenaKryoRegistrator"))
//        "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator"))
      .config("spark.default.parallelism", "4")
      .config("spark.sql.shuffle.partitions", "4")
      //      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("spark.sql.cbo.enabled", true)
      .config("spark.sql.statistics.histogram.enabled", true)
      .enableHiveSupport()
      .getOrCreate()

    val data = args(0)

    // read triples as RDD[Triple]
    var triplesRDD = spark.ntriples()(data)

    // load optional schema file and filter properties used for VP
    var ont: OWLOntology = null
    if (args.length == 2) {
      val owlFile = args(1)
      val man = OWLManager.createOWLOntologyManager()
      ont = man.loadOntologyFromOntologyDocument(new File(owlFile))
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
    val partitions: Map[RdfPartitionComplex, RDD[Row]] = RdfPartitionUtilsSpark.partitionGraph(triplesRDD, partitioner = RdfPartitionerComplex)
    println(s"num partitions: ${partitions.size}")

    // create the SPARQL engine
    val sparqlEngine = new OntopSPARQLEngine(spark, partitions)


    var input = "select * where {?s <http://sansa-stack.net/ontology/someBooleanProperty> ?o; " +
      "<http://sansa-stack.net/ontology/someIntegerProperty> ?o2; " +
      "<http://sansa-stack.net/ontology/someDecimalProperty> ?o3} limit 10"
    println("enter SPARQL query (press 'q' to quit): ")
    println("select * where {?s <http://dbpedia.org/ontology/birthPlace> ?o bind(\"s\" as ?z)} limit 10")

    while (input != "q") {
      input = scala.io.StdIn.readLine()

      try {
        val result = sparqlEngine.execute(input)

        result.show(false)
        result.printSchema()

        val resultRDD = sparqlEngine.executeNative(input)
        resultRDD.count()
        resultRDD.take(10).foreach(println)
      } catch {
        case e: Exception => Console.err.println("failed to execute query")
          e.printStackTrace()
      }
    }

    sparqlEngine.stop()

    spark.stop()
  }


}
