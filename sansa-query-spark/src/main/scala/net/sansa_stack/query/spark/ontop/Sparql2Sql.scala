package net.sansa_stack.query.spark.ontop

import java.io.{File, FileInputStream, StringReader}
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.sql.{Connection, DriverManager, SQLException}
import java.util.Properties
import java.util.stream.Collectors
import java.util.stream.Collectors.joining

import scala.collection.JavaConverters
import scala.collection.JavaConverters._
import scala.reflect.runtime.universe.typeOf

import com.google.common.collect.ImmutableMap
import it.unibz.inf.ontop.exception.{OBDASpecificationException, OntopReformulationException}
import it.unibz.inf.ontop.injection.{OntopMappingSQLAllConfiguration, OntopReformulationSQLConfiguration, OntopSQLOWLAPIConfiguration}
import it.unibz.inf.ontop.iq.UnaryIQTree
import it.unibz.inf.ontop.iq.node.NativeNode
import it.unibz.inf.ontop.iq.node.impl.ConstructionNodeImpl
import it.unibz.inf.ontop.model.term.Variable
import it.unibz.inf.ontop.owlapi.OntopOWLFactory
import it.unibz.inf.ontop.owlapi.connection.OntopOWLConnection
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

  private val JDBC_URL = "jdbc:h2:mem:questjunitdb"
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
    val scalaSchema = p.layout.schema
    val sparkSchema = ScalaReflection.schemaFor(scalaSchema).dataType.asInstanceOf[StructType]
    val df = session.createDataFrame(rdd, sparkSchema).persist()
    df.show(false)

    println(s"creating Spark table ${escapeTablename(name)}")

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

  def asSQL(session: SparkSession, sparqlQuery: String, triples: RDD[Triple], properties: Properties): (String, Map[Variable, Variable]) = {
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

    // create the database the sl
    createTempDB(properties, partitions)

    createSQLQuery(sparqlQuery, mappingFile, properties)
  }

  @throws[OBDASpecificationException]
  @throws[OntopReformulationException]
  def createSQLQuery(sparqlQuery: String, obda_file: File, properties: Properties): (String, Map[Variable, Variable]) = {
    val queryReformulator = createReformulator(obda_file, properties)
    val inputQueryFactory = queryReformulator.getInputQueryFactory

    val query = inputQueryFactory.createSPARQLQuery(sparqlQuery)

    val executableQuery = queryReformulator.reformulateIntoNativeQuery(query)
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
    reformulationConfiguration.loadQueryReformulator
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

    // load optional schema file
    val owlFile = args(1)
    val man = OWLManager.createOWLOntologyManager()
    val ont = man.loadOntologyFromOntologyDocument(new File(owlFile))
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
    val conn = initOntopConnection(mappings, mappingFile, ont, ontopProperties)
    var input = "select * where {?s <http://sansa-stack.net/ontology/someBooleanProperty> ?o; " +
      "<http://sansa-stack.net/ontology/someIntegerProperty> ?o2; " +
      "<http://sansa-stack.net/ontology/someDecimalProperty> ?o3} limit 10"
    println("enter SPARQL query (press 'q' to quit): ")
    println("select * where {?s <http://dbpedia.org/ontology/birthPlace> ?o} limit 10")
    while (input != "q") {
      input = scala.io.StdIn.readLine()

      try {
        runQuery(conn, input)
      } catch {
        case e: Exception => Console.err.println("failed to execute query")
          e.printStackTrace()
      }

    }

    def runQuery(conn: OntopOWLConnection, sparqlQuery: String) = {
      // create SQL query
      val (sql, columnMapping) = toSQL(conn, sparqlQuery)

      val sqlQuery = sql.replace("\"", "`").replace("`PUBLIC`.", "")
      println(s"SQL query:\n $sqlQuery")

      // run query
      var result = spark.sql(sqlQuery)

      println(s"var mapping: $columnMapping")
      columnMapping.foreach {
        case (sparqlVar, sqlVar) => result = result.withColumnRenamed(sqlVar.getName, sparqlVar.getName)
      }
      result.show(false)
      result.printSchema()
    }

    conn.close()

    spark.stop()
  }

}

