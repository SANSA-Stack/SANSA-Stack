package net.sansa_stack.query.spark.ontop

import java.io.{File, StringReader}
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.sql.{Connection, DriverManager, SQLException}
import java.util.Properties

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe.typeOf

import com.google.common.collect.{ImmutableMap, ImmutableSortedSet}
import it.unibz.inf.ontop.exception.{MinorOntopInternalBugException, OBDASpecificationException, OntopInternalBugException, OntopReformulationException}
import it.unibz.inf.ontop.injection.{OntopMappingSQLAllConfiguration, OntopReformulationSQLConfiguration}
import it.unibz.inf.ontop.iq.exception.EmptyQueryException
import it.unibz.inf.ontop.iq.node.{ConstructionNode, NativeNode}
import it.unibz.inf.ontop.iq.{IQ, IQTree, UnaryIQTree}
import it.unibz.inf.ontop.model.`type`.{DBTermType, TypeFactory}
import it.unibz.inf.ontop.model.atom.DistinctVariableOnlyDataAtom
import it.unibz.inf.ontop.model.term.{ImmutableTerm, TermFactory, Variable}
import it.unibz.inf.ontop.substitution.{ImmutableSubstitution, SubstitutionFactory}
import org.apache.jena.graph.NodeFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession}
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.model.{HasDataPropertiesInSignature, HasObjectPropertiesInSignature, OWLOntology}

import net.sansa_stack.rdf.common.partition.core.{RdfPartitionComplex, RdfPartitionerComplex}
import net.sansa_stack.rdf.common.partition.schema._
import net.sansa_stack.rdf.spark.partition.core.RdfPartitionUtilsSpark

class OntopSPARQLEngine(val spark: SparkSession, val partitions: Map[RdfPartitionComplex, RDD[Row]]) {

  // create and register Spark tables
  partitions.foreach {
    case (p, rdd) => createSparkTable(spark, p, rdd)
  }

  // load Ontop properties
  val ontopProperties = new Properties()
  ontopProperties.load(getClass.getClassLoader.getResourceAsStream("ontop-spark.properties"))

  private val JDBC_URL = "jdbc:h2:mem:sansaontopdb"
  private val JDBC_USER = "sa"
  private val JDBC_PASSWORD = ""

  // create DB used by Ontop to extract metadata
  createTempDB(partitions)

  // create OBDA mappings
//  val baseDir = new File("/tmp/ontop-spark")
//  baseDir.mkdirs()
  val mappings = createOBDAMappingsForPartitions(partitions.keySet)
//  //        var mappingFile = File.createTempFile("ontop-mappings", "tmp", baseDir)
//  var mappingFile = new File(baseDir, "spark-mappings.obda")
//  println(s"mappings location: ${mappingFile.getAbsolutePath}");
//  Files.write(mappingFile.toPath, mappings.getBytes(StandardCharsets.UTF_8))

  val (queryReformulator, termFactory, typeFactory, substitutionFactory) = createReformulator(mappings, ontopProperties)
  val inputQueryFactory = queryReformulator.getInputQueryFactory



  private def createOBDAMappingsForPartitions(partitions: Set[RdfPartitionComplex]): String = {

    def createMapping(id: String, tableName: String, property: String): String = {
      s"""
         |mappingId     $id
         |source        SELECT `s`, `o` FROM `${escapeTablename(tableName)}`
         |target        <{s}> <$property> <{o}> .
         |""".stripMargin
    }

    def createMappingLit(id: String, tableName: String, property: String, datatypeURI: String): String = {
      s"""
         |mappingId     $id
         |source        SELECT `s`, `o` FROM `${escapeTablename(tableName)}`
         |target        <{s}> <$property> "{o}"^^<$datatypeURI> .
         |""".stripMargin
    }

    def createMappingLiteralWithType(id: String, tableName: String, property: String): String = {
      s"""
         |mappingId     $id
         |source        SELECT `s`, `o`, `t` FROM `${escapeTablename(tableName)}`
         |target        <{s}> <$property> "{o}"^^<{t}> .
         |""".stripMargin
    }

    def createMappingLang(id: String, tableName: String, property: String): String = {
      s"""
         |mappingId     $id
         |source        SELECT `s`, `o`, `l` FROM `${escapeTablename(tableName)}`
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


  private def createTableName(p: RdfPartitionComplex): String = {
    // println("Counting the dataset: " + ds.count())
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

  /*
   * DB connection (keeps it alive)
   */
  private var connection: Connection = null

  private def createTempDB(partitions: Map[RdfPartitionComplex, RDD[Row]]) = {

    try {
      connection = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASSWORD)

      val stmt = connection.createStatement()

      stmt.executeUpdate("DROP ALL OBJECTS")

      partitions.foreach {
        case (p, rdd) =>

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
                      val mapping = Map(
                        typeOf[SchemaStringLong] -> "LONG",
                        typeOf[SchemaStringDouble] -> "DOUBLE",
                        typeOf[SchemaStringFloat] -> "FLOAT",
                        typeOf[SchemaStringDecimal] -> "DECIMAL",
                        typeOf[SchemaStringBoolean] -> "BOOLEAN",
                        typeOf[SchemaStringString] -> "VARCHAR(255)",
                        typeOf[SchemaStringDate] -> "DATE"
                      ) // .map(e => (typeOf[e._1.type], e._2))
                      val colType = mapping.get(p.layout.schema)

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

  val useHive: Boolean = false
  val useStatistics: Boolean = true

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
  private def createReformulator(obdaMappings: String, properties: Properties) = {
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
      .properties(properties)
      .enableTestMode
      .build
    mappingConfiguration.loadSpecification
  }

  private def escapeTablename(path: String): String =
    URLEncoder.encode(path, StandardCharsets.UTF_8.toString)
//      .toLowerCase
      .replace('%', 'P')
      .replace('.', 'C')
      .replace("-", "dash")

  def stop(): Unit = {
    connection.close()
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
        "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator"))
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
    println("select * where {?s <http://dbpedia.org/ontology/birthPlace> ?o} limit 10")

    while (input != "q") {
      input = scala.io.StdIn.readLine()

      try {
        val queryRewrite: OntopQueryRewrite = sparqlEngine.createSQLQuery(input)
        val sql = queryRewrite.sqlQuery.replace("\"", "`")
          .replace("`PUBLIC`.", "")
        val result = spark.sql(sql)
        result.show(false)
        result.printSchema()
      } catch {
        case e: Exception => Console.err.println("failed to execute query")
          e.printStackTrace()
      }

    }

    sparqlEngine.stop()

    spark.stop()
  }


}

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

