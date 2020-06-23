package net.sansa_stack.query.spark.ontop

import java.io.{File, FileInputStream, StringReader}
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.sql.{Connection, DriverManager, ResultSet, SQLException}
import java.util.{Optional, Properties}
import java.util.stream.Collectors
import java.util.stream.Collectors.joining

import scala.collection.JavaConverters
import scala.collection.JavaConverters._
import scala.reflect.runtime.universe.typeOf

import com.google.common.collect.{ImmutableMap, ImmutableSortedSet}
import it.unibz.inf.ontop.exception.{MinorOntopInternalBugException, OBDASpecificationException, OntopInternalBugException, OntopReformulationException}
import it.unibz.inf.ontop.injection.{OntopMappingSQLAllConfiguration, OntopReformulationSQLConfiguration, OntopSQLOWLAPIConfiguration}
import it.unibz.inf.ontop.iq.exception.EmptyQueryException
import it.unibz.inf.ontop.iq.{IQ, IQTree, UnaryIQTree}
import it.unibz.inf.ontop.iq.node.{ConstructionNode, NativeNode}
import it.unibz.inf.ontop.iq.node.impl.ConstructionNodeImpl
import it.unibz.inf.ontop.model.`type`.{DBTermType, TypeFactory}
import it.unibz.inf.ontop.model.atom.DistinctVariableOnlyDataAtom
import it.unibz.inf.ontop.model.term.{ImmutableTerm, TermFactory, Variable}
import it.unibz.inf.ontop.owlapi.OntopOWLFactory
import it.unibz.inf.ontop.owlapi.connection.OntopOWLConnection
import it.unibz.inf.ontop.substitution.{ImmutableSubstitution, SubstitutionFactory}
import org.aksw.obda.domain.impl.LogicalTableTableName
import org.apache.jena.graph.{NodeFactory, Triple}
import org.apache.jena.sparql.util.FmtUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession}
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.model.{HasDataPropertiesInSignature, HasObjectPropertiesInSignature, OWLOntology}

import net.sansa_stack.rdf.common.partition.core.{RdfPartitionComplex, RdfPartitionerComplex}
import net.sansa_stack.rdf.common.partition.model.sparqlify.SparqlifyUtils2
import net.sansa_stack.rdf.common.partition.schema._
import net.sansa_stack.rdf.spark.partition.core.RdfPartitionUtilsSpark

object Sparql2Sql {

  val baseDir = new File("/tmp/ontop-spark")
  baseDir.mkdirs()

  def obtainSQL(sparqlFile: String, r2rmlFile: String, owlFile: String, propertyFile: String): String = {
    var factory = OntopOWLFactory.defaultFactory();

    var config = OntopSQLOWLAPIConfiguration.defaultBuilder()
      .r2rmlMappingFile(r2rmlFile)
      .ontologyFile(owlFile)
      .propertyFile(propertyFile)
      .enableTestMode()
      .build();

    var reasoner = factory.createReasoner(config)

    /*
     * Prepare the data connection for querying.
     */
    var sparqlQuery = Files.lines(Paths.get(sparqlFile)).collect(joining("\n"))

    var conn = reasoner.getConnection
    var st = conn.createStatement()

    var sqlExecutableQuery = st.getExecutableQuery(sparqlQuery)
    var sqlQuery = sqlExecutableQuery.toString // getSQL();

    sqlQuery
  }

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

  private def createTempDB(dbProps: Properties, partitions: Map[RdfPartitionComplex, RDD[Row]]) = {
    val driver = dbProps.getProperty("jdbc.driver")
    val url = dbProps.getProperty("jdbc.url", JDBC_URL)
    val username = dbProps.getProperty("jdbc.user", JDBC_USER)
    val password = dbProps.getProperty("jdbc.password", JDBC_PASSWORD)

    var connection: Connection = null

    try {
      Class.forName(driver)
      connection = DriverManager.getConnection(url, username, password)

      val statement = connection.createStatement()

      //            val dbName = "Ontop"
      //            statement.executeUpdate(s"DROP DATABASE $dbName")
      //            statement.executeUpdate(s"CREATE DATABASE $dbName")

      statement.executeUpdate("DROP ALL OBJECTS")

      connection = DriverManager.getConnection(url, username, password)

      partitions.foreach {
        case (p, rdd) =>

          val name = createTableName(p)

          val sparkSchema = ScalaReflection.schemaFor(p.layout.schema).dataType.asInstanceOf[StructType]
          println(p.predicate + "\t" + sparkSchema + "\t" + p.asInstanceOf[RdfPartitionComplex].layout.schema)

          p match {
            case RdfPartitionComplex(subjectType, predicate, objectType, datatype, langTagPresent) =>
              objectType match {
                case 1 => statement.addBatch(s"CREATE TABLE IF NOT EXISTS ${escapeTablename(name)} (" +
                  "s varchar(255) NOT NULL," +
                  "o varchar(255) NOT NULL" +
                  ")")
                case 2 => if (langTagPresent) {
                  statement.addBatch(s"CREATE TABLE IF NOT EXISTS ${escapeTablename(name)} (" +
                    "s varchar(255) NOT NULL," +
                    "o varchar(255) NOT NULL," +
                    "l varchar(10) NOT NULL" +
                    ")")
                } else {
                  println(s"datatype: $datatype")
                  if (p.layout.schema == typeOf[SchemaStringStringType]) {
                    statement.addBatch(s"CREATE TABLE IF NOT EXISTS ${escapeTablename(name)} (" +
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
                      val stmt =
                        s"""
                           |CREATE TABLE IF NOT EXISTS ${escapeTablename(name)} (
                           |s varchar(255) NOT NULL,
                           |o ${colType.get} NOT NULL)
                           |""".stripMargin

                      statement.addBatch(stmt)
                    } else {
                      println(s"couldn't create table for schema ${p.layout.schema}")
                    }
                  }
                }
              }
            case _ => println("")
          }
      }
      //            statement.addBatch(s"CREATE TABLE IF NOT EXISTS triples (" +
      //              "s varchar(255) NOT NULL," +
      //              "p varchar(255) NOT NULL," +
      //              "o varchar(255) NOT NULL" +
      //              ")")
      val numTables = statement.executeBatch().length
      println(s"created $numTables tables")
    } catch {
      case e: SQLException => e.printStackTrace()
    }
    connection.commit()
    connection.close()
  }

  import java.sql.{Connection, DriverManager}

  private val JDBC_URL = "jdbc:h2:mem:sansaontopdb"
  private val JDBC_USER = "sa"
  private val JDBC_PASSWORD = ""

  /*
   * DB connection (keeps it alive)
   */
  private var CONN: Connection = null

  private def createDB(dbProps: Properties, properties: Set[String]) = {
    val driver = dbProps.getProperty("jdbc.driver")
    val url = dbProps.getProperty("jdbc.url", JDBC_URL)
    val username = dbProps.getProperty("jdbc.user", JDBC_USER)
    val password = dbProps.getProperty("jdbc.password", JDBC_PASSWORD)

    var connection: Connection = null

    try {
      Class.forName(driver)
      connection = DriverManager.getConnection(url, username, password)

      val statement = connection.createStatement()
      properties.foreach(p => statement.addBatch(s"CREATE TABLE IF NOT EXISTS ${escapeTablename(p)} (" +
        "s varchar(255) NOT NULL," +
        "o varchar(255) NOT NULL" +
        ")"))
      val numTables = statement.executeBatch().length
      println(s"created $numTables tables")
    } catch {
      case e: SQLException => e.printStackTrace()
    }
    connection.commit()
    connection.close()
  }

  def initOntopConnection(mappings: String,
                          mappingFile: File,
                          ontology: OWLOntology,
                          properties: Properties): OntopOWLConnection = {
    val factory = OntopOWLFactory.defaultFactory

    //    val metadata = new SimpleBasicDBMetaData("dummy",
    //      null,
    //      null,
    //      "",
    //      new SimpleQuotedIDFactory("\""))
    //    metadata.createDatabaseRelation(
    //      new RelationID(QuotedID.EMPTY_ID, new QuotedID(null, "httpP3aP2fP2fdbpediaCorgP2fontologyP2facademicadvisor")))
    //      .addAttribute(new QuotedID(null, "s"), "CHARACTER", new StringDBTermType(), false)


    //    RDBMetadataExtractionTools.loadMetadata(metadata, dbConnection, null)

    val config = OntopSQLOWLAPIConfiguration.defaultBuilder
      .nativeOntopMappingReader(new StringReader(mappings))
      //          .nativeOntopMappingFile(mappingFile)
      .ontology(ontology)
      .properties(properties)
      //      .dbMetadata(metadata)
      .enableTestMode
      .build

    val reasoner = factory.createReasoner(config)

    reasoner.getConnection
  }

  def toSQL(conn: OntopOWLConnection, sparqlQuery: String): (String, Map[Variable, Variable]) = {

    val st = conn.createStatement()

    val executableQuery = st.getExecutableQuery(sparqlQuery)
    val sqlQuery = Option(executableQuery.getTree)
      .filter(t => t.isInstanceOf[UnaryIQTree])
      .map(t => t.asInstanceOf[UnaryIQTree].getChild.getRootNode)
      .filter(n => n.isInstanceOf[NativeNode])
      .map(n => n.asInstanceOf[NativeNode].getNativeQueryString)
      .getOrElse(throw new RuntimeException("Cannot extract the SQL query from\n" + executableQuery))


    val tree = executableQuery.getTree.asInstanceOf[UnaryIQTree]
                  .getRootNode.asInstanceOf[ConstructionNodeImpl]

    val substitution = executableQuery.getTree.asInstanceOf[UnaryIQTree]
      .getRootNode.asInstanceOf[ConstructionNodeImpl]
      .getSubstitution

    val columnMappings = tree.getVariables.asScala.map(v => (v, substitution.get(v).getVariableStream.findFirst().get())).toMap


    (sqlQuery, columnMappings)
//
//
//    val sql = sqlExecutableQuery.getTree.getChildren.get(0).asInstanceOf[NativeNode].getNativeQueryString
//    val sqlQuery = sqlExecutableQuery.toString // getSQL();
//
//    println(sqlQuery)
//    println(sqlExecutableQuery.getTree.getChildren.get(0).asInstanceOf[NativeNode].getColumnNames)
//
//    st.close()
//
//    sql
  }

  val useHive: Boolean = false
  val useStatistics: Boolean = true

  private def createSparkTable(session: SparkSession, p: RdfPartitionComplex, rdd: RDD[Row]) = {

    val name = createTableName(p)
    println(s"creating Spark table ${escapeTablename(name)}")

    val scalaSchema = p.layout.schema
    val sparkSchema = ScalaReflection.schemaFor(scalaSchema).dataType.asInstanceOf[StructType]
    val df = session.createDataFrame(rdd, sparkSchema).persist()
    df.show(false)

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

  def asSQL(session: SparkSession, sparqlQuery: String, triples: RDD[Triple], properties: Properties): OntopQueryRewrite = {
    // do partitioning here
    val partitions: Map[RdfPartitionComplex, RDD[Row]] = RdfPartitionUtilsSpark.partitionGraph(triples, partitioner = RdfPartitionerComplex)
    partitions.foreach {
      case (p, rdd) => createSparkTable(session, p, rdd)
    }

    // create the mappings and write to file read by Ontop
    val mappings = createOBDAMappingsForPartitions(partitions.keySet)
    //        var mappingFile = File.createTempFile("ontop-mappings", "tmp", baseDir)
    var mappingFile = new File(baseDir, "spark-mappings.obda")
    println(s"mappings location: ${mappingFile.getAbsolutePath}");
    Files.write(mappingFile.toPath, mappings.getBytes(StandardCharsets.UTF_8))

    // create the H2 database used by Ontop
    createTempDB(properties, partitions)

    // finally, create the SQL query + some metadata used for mapping rows back to bindings
    createSQLQuery(sparqlQuery, mappingFile, properties)
  }

  @throws[OBDASpecificationException]
  @throws[OntopReformulationException]
  def createSQLQuery(sparqlQuery: String, obda_file: File, properties: Properties): OntopQueryRewrite = {
    val (queryReformulator, termFactory, typeFactory, substitutionFactory) = createReformulator(obda_file, properties)
    val inputQueryFactory = queryReformulator.getInputQueryFactory

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
  private def createReformulator(obda_file: File, properties: Properties) = {
    val obdaSpecification = loadOBDASpecification(obda_file, properties)
    val reformulationConfiguration = OntopReformulationSQLConfiguration.defaultBuilder
      .obdaSpecification(obdaSpecification)
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
  private def loadOBDASpecification(obda_file: File, properties: Properties) = {
    val mappingConfiguration = OntopMappingSQLAllConfiguration.defaultBuilder
      .nativeOntopMappingFile(obda_file)
      .properties(properties)
      .enableTestMode
      .build
    mappingConfiguration.loadSpecification
  }

  private def escapeTablename(path: String): String =
    URLEncoder.encode(path, StandardCharsets.UTF_8.toString)
      .toLowerCase
      .replace('%', 'P')
      .replace('.', 'C')
      .replace("-", "dash")


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
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("spark.sql.cbo.enabled", true)
      .config("spark.sql.statistics.histogram.enabled", true)
      .enableHiveSupport()
      .getOrCreate()

    val data = args(0)

    // read triples as RDD[Triple]
    var triplesRDD = spark.ntriples()(data)

    // load optional schema file and filter properties used for VP
    var ont: OWLOntology = null
    if (args.size == 2) {
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
    partitions.foreach {
      case (p, rdd) => createSparkTable(spark, p, rdd)
    }
    println(s"num partitions: ${partitions.size}")

    // load Ontop properties
    val propertiesInputStream = if (args.length == 3) {
      new FileInputStream(args(2))
    } else {
      getClass.getClassLoader.getResourceAsStream("ontop-spark.properties")
    }
    val ontopProperties = new Properties()
    ontopProperties.load(propertiesInputStream)

    // create the mappings and write to file read by Ontop
    val mappings = createOBDAMappingsForPartitions(partitions.keySet)
    //        var mappingFile = File.createTempFile("ontop-mappings", "tmp", baseDir)
    var mappingFile = new File(baseDir, "spark-mappings.obda")
    println(s"mappings location: ${mappingFile.getAbsolutePath}");
    //        Files.write(mappingFile.toPath, mappings.getBytes(StandardCharsets.UTF_8))

    // create DB used by Ontop to extract metadata
    createTempDB(ontopProperties, partitions)

    // setup Ontop connection
    val conn: OntopOWLConnection =
      if (ont != null) {
        initOntopConnection(mappings, mappingFile, ont, ontopProperties)
      } else {
        null
      }
    var input = "select * where {?s <http://sansa-stack.net/ontology/someBooleanProperty> ?o; " +
      "<http://sansa-stack.net/ontology/someIntegerProperty> ?o2; " +
      "<http://sansa-stack.net/ontology/someDecimalProperty> ?o3} limit 10"
    println("enter SPARQL query (press 'q' to quit): ")
    println("select * where {?s <http://dbpedia.org/ontology/birthPlace> ?o} limit 10")

    while (input != "q") {
      input = scala.io.StdIn.readLine()

      try {
        if (ont != null) {
          runQuery(conn, input)
        } else {
          val sql =
            if (input.startsWith("sql:")) {
              input.substring(4)
            } else {
              val queryRewrite: OntopQueryRewrite = Sparql2Sql.asSQL(spark, input, triplesRDD, ontopProperties)
              val sql = queryRewrite.sqlQuery.replace("\"", "`")
                .replace("`PUBLIC`.", "")
              sql
            }
          val result = spark.sql(sql)
          result.show(false)
          result.printSchema()

        }
      } catch {
        case e: Exception => Console.err.println("failed to execute query")
          e.printStackTrace()
      }

    }

    def runQuery(conn: OntopOWLConnection, query: String) = {
      val result =
        if (query.startsWith("sql:")) {
          spark.sql(query.substring(4))
        } else {
          // create SQL query
          val (sql, columnMapping) = toSQL(conn, query)

          val sqlQuery = sql.replace("\"", "`").replace("`PUBLIC`.", "")
          println(s"SQL query:\n $sqlQuery")

          // run query
          var result = spark.sql(sqlQuery)

          println(s"var mapping: $columnMapping")
          columnMapping.foreach {
            case (sparqlVar, sqlVar) => result = result.withColumnRenamed(sqlVar.getName, sparqlVar.getName)
          }
          result
        }

      result.show(false)
      result.printSchema()
    }

    conn.close()

    spark.stop()
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

}

