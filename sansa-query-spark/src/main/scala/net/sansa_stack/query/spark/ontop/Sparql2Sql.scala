package net.sansa_stack.query.spark.sparql2sql

import java.io.{BufferedReader, File, FileInputStream, FileReader}
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.sql.{Connection, DriverManager, SQLException, Statement}
import java.util.Properties
import java.util.stream.Collectors.joining

import it.unibz.inf.ontop.injection.{OntopMappingSQLAllConfiguration, OntopReformulationSQLConfiguration, OntopSQLOWLAPIConfiguration}
import it.unibz.inf.ontop.iq.node.NativeNode
import it.unibz.inf.ontop.owlapi.OntopOWLFactory
import org.apache.spark.sql.{Row, SparkSession}
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.model.{HasDataPropertiesInSignature, HasObjectPropertiesInSignature, OWLOntology}
import scala.collection.JavaConverters._

import it.unibz.inf.ontop.answering.reformulation.QueryReformulator
import it.unibz.inf.ontop.answering.reformulation.input.InputQueryFactory
import it.unibz.inf.ontop.exception.{OBDASpecificationException, OntopReformulationException}
import it.unibz.inf.ontop.iq.UnaryIQTree
import it.unibz.inf.ontop.model.`type`.impl.H2SQLDBTypeFactory
import it.unibz.inf.ontop.owlapi.connection.OntopOWLConnection
import it.unibz.inf.ontop.spec.OBDASpecification
import org.aksw.obda.domain.impl.LogicalTableTableName
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType

import net.sansa_stack.rdf.common.partition.core.RdfPartitionDefault
import net.sansa_stack.rdf.common.partition.layout.TripleLayout
import net.sansa_stack.rdf.common.partition.model.sparqlify.SparqlifyUtils2
import net.sansa_stack.rdf.common.partition.schema.{SchemaStringDate, SchemaStringDouble, SchemaStringLong, SchemaStringString, SchemaStringStringType}
import net.sansa_stack.rdf.spark.partition.core.RdfPartitionUtilsSpark
import scala.reflect.runtime.universe.Type
import scala.reflect.runtime.universe.typeOf

object Sparql2Sql {

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

    private def createOBDAMappingsforPartitions(partitions: Set[RdfPartitionDefault]): String = {

        def createMapping(id: String, property: String): String = {
            s"""
               |mappingId     $id
               |source        SELECT `s`, `o` FROM `${escapeTablename(property)}`
               |target        <{s}> <$property> <{o}> .
               |""".stripMargin
        }

        def createMappingLit(id: String, property: String, datatypeURI: String): String = {
            s"""
               |mappingId     $id
               |source        SELECT `s`, `o` FROM `${escapeTablename(property)}`
               |target        <{s}> <$property> "{o}"^^<$datatypeURI> .
               |""".stripMargin
        }

        def createMappingLang(id: String, property: String): String = {
            s"""
               |mappingId     $id
               |source        SELECT `s`, `o`, `l` FROM `${escapeTablename(property)}`
               |target        <{s}> <$property> "{o}@{l}" .
               |""".stripMargin
        }

        "[MappingDeclaration] @collection [[" +
          partitions
            .map {
                case p@RdfPartitionDefault(subjectType, predicate, objectType, datatype, langTagPresent) =>
                    objectType match {
                        case 1 => createMapping(escapeTablename(predicate), predicate)
                        case 2 => if (langTagPresent) createMappingLang(escapeTablename(predicate), predicate)
                        else createMappingLit(escapeTablename(predicate), predicate, datatype)
                    }
            }
            .mkString("\n\n") + "]]"
    }

    val baseDir = new File("/tmp/ontop-spark")

    def main(args: Array[String]): Unit = {
        import net.sansa_stack.rdf.spark.io._
        import org.apache.jena.riot.Lang

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

        val schemaProperties = objectProperties ++ dataProperties

        // filter triples RDD
        triplesRDD = triplesRDD.filter(t => schemaProperties.contains(t.getPredicate.getURI))

        // do partitioning here
        val partitions: Map[RdfPartitionDefault, RDD[Row]] = RdfPartitionUtilsSpark.partitionGraph(triplesRDD)
        partitions.foreach {
            case (p, rdd) =>
                val vd = SparqlifyUtils2.createViewDefinition(p)

                val tableName = vd.getLogicalTable match {
                    case o: LogicalTableTableName => o.getTableName
                    case _ => throw new RuntimeException("Table name required - instead got: " + vd)
                }

                val scalaSchema = p.layout.schema
                val sparkSchema = ScalaReflection.schemaFor(scalaSchema).dataType.asInstanceOf[StructType]
                val df = spark.createDataFrame(rdd, sparkSchema).persist()

                df.createOrReplaceTempView(escapeTablename(p.predicate))
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

        // create the mappings
        var mappingFile = File.createTempFile("ontop-mappings", "tmp", baseDir)
        println(mappingFile.getAbsolutePath)
        Files.write(
            mappingFile.toPath,
            createOBDAMappingsforPartitions(partitions.keySet).getBytes(StandardCharsets.UTF_8))
        //    mappingFile = new File(baseDir, "ontop-mappings1137406897844479900tmp")

        // create DB and OBDA mappings
        createTempDB(ontopProperties, partitions)

        def createTempDB(dbProps: Properties, partitions: Map[RdfPartitionDefault, RDD[Row]]) = {
            val driver = dbProps.getProperty("jdbc.driver")
            val url = dbProps.getProperty("jdbc.url", JDBC_URL)
            val username = dbProps.getProperty("jdbc.user", JDBC_USER)
            val password = dbProps.getProperty("jdbc.password", JDBC_PASSWORD)

            var connection: Connection = null

            try {
                Class.forName(driver)
                connection = DriverManager.getConnection(url, username, password)

                val statement = connection.createStatement()
                partitions.foreach {
                    case (p, rdd) =>

                        val vd = SparqlifyUtils2.createViewDefinition(p)

                        val sparkSchema = ScalaReflection.schemaFor(p.layout.schema).dataType.asInstanceOf[StructType]
                        println(p.predicate + "\t" + sparkSchema + "\t" + p.asInstanceOf[RdfPartitionDefault].layout.schema)

                        p match {
                            case RdfPartitionDefault(subjectType, predicate, objectType, datatype, langTagPresent) =>
                               objectType match {
                                   case 1 => statement.addBatch(s"CREATE TABLE IF NOT EXISTS ${escapeTablename(predicate)} (" +
                                     "s varchar(255) NOT NULL," +
                                     "o varchar(255) NOT NULL" +
                                     ")")
                                   case 2 => if (langTagPresent) {
                                                   statement.addBatch(s"CREATE TABLE IF NOT EXISTS ${escapeTablename(predicate)} (" +
                                                     "s varchar(255) NOT NULL," +
                                                     "o varchar(255) NOT NULL," +
                                                     "l varchar(10) NOT NULL" +
                                                     ")")
                                               } else {
                                                    println(s"datatype: $datatype")
                                                   if (p.layout.schema == typeOf[SchemaStringLong]) {
                                                       statement.addBatch(s"CREATE TABLE IF NOT EXISTS ${escapeTablename(predicate)} (" +
                                                         "s varchar(255) NOT NULL," +
                                                         "o BIGINT NOT NULL" +
                                                         ")")
                                                   } else if (p.layout.schema == typeOf[SchemaStringDouble]) {
                                                       statement.addBatch(s"CREATE TABLE IF NOT EXISTS ${escapeTablename(predicate)} (" +
                                                         "s varchar(255) NOT NULL," +
                                                         "o DOUBLE NOT NULL" +
                                                         ")")
                                                   } else if (p.layout.schema == typeOf[SchemaStringString]) {
                                                       statement.addBatch(s"CREATE TABLE IF NOT EXISTS ${escapeTablename(predicate)} (" +
                                                         "s varchar(255) NOT NULL," +
                                                         "o varchar(255) NOT NULL" +
                                                         ")")
                                                   } else if (p.layout.schema == typeOf[SchemaStringStringType]) {
                                                       statement.addBatch(s"CREATE TABLE IF NOT EXISTS ${escapeTablename(predicate)} (" +
                                                         "s varchar(255) NOT NULL," +
                                                         "o varchar(255) NOT NULL," +
                                                         "t varchar(255) NOT NULL" +
                                                         ")")
                                                   } else if (p.layout.schema == typeOf[SchemaStringDate]) {
                                                       statement.addBatch(s"CREATE TABLE IF NOT EXISTS ${escapeTablename(predicate)} (" +
                                                         "s varchar(255) NOT NULL," +
                                                         "o DATE NOT NULL" +
                                                         ")")
                                                   } else {
                                                       println(s"couldn't create table for schema ${p.layout.schema}")
                                                   }

                                               }
                               }
                            case _ => println("")
                        }
                }
                val numTables = statement.executeBatch().length
                println(s"created $numTables tables")
            } catch {
                case e: SQLException => e.printStackTrace()
            }
            connection.commit()
            connection.close()
        }




        val lang = Lang.NTRIPLES
        val triples = spark
          .read
          .rdf(lang)(data)
//        triples.take(5).foreach(println(_))



        // vertical partitioning
        // get all properties in dataset
//        val properties = triples.select("p").distinct().collect().map(row => row.getString(0)).toSet
//
//        val relevantProperties = objectProperties.filter(properties.contains)

        // create the database
//        createDB(ontopProperties, relevantProperties)
//
//        // create Spark SQL tables
//        relevantProperties
//          .foreach(op =>
//              triples
//                .filter(s"p = '$op'")
//                .persist()
//                .createOrReplaceTempView(escapeTablename(op))
//          )


        // setup Ontop connection
        val conn = initOntopConnection(mappingFile, ont, ontopProperties)
        var input = ""
        println("enter SPARQL query (press 'q' to quit): ")
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
            val sql = toSQL(conn, sparqlQuery)
              .replace("\"", "`")
              //      .replace("OFFSET 0 ROWS", "")
              .replace("`PUBLIC`.", "")
            //        .replaceAll("FETCH NEXT (\\d+) ROWS ONLY", "LIMIT \\1")
            println(sql)

            // run query
            val result = spark.sql(sql)
            result.show(false)
            result.printSchema()
        }

        conn.close()

        spark.stop()
    }

    import java.sql.DriverManager
    import java.sql.Connection

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

    def initOntopConnection(mappingFile: File,
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
          .nativeOntopMappingFile(mappingFile)
          .ontology(ontology)
          .properties(properties)
          //      .dbMetadata(metadata)
          .enableTestMode
          .build

        val reasoner = factory.createReasoner(config)

        reasoner.getConnection
    }

    def toSQL(conn: OntopOWLConnection, sparqlQuery: String): String = {

        val st = conn.createStatement()

        val sqlExecutableQuery = st.getExecutableQuery(sparqlQuery)
        val sql = sqlExecutableQuery.getTree.getChildren.get(0).asInstanceOf[NativeNode].getNativeQueryString
        val sqlQuery = sqlExecutableQuery.toString // getSQL();

        println(sqlQuery)
        println(sqlExecutableQuery.getTree.getChildren.get(0).asInstanceOf[NativeNode].getColumnNames)

        st.close()

        sql
    }

    @throws[OBDASpecificationException]
    @throws[OntopReformulationException]
    private def createSQLQuery(sparqlQuery: String, obda_file: File, properties: Properties): String = {
        val queryReformulator = createReformulator(obda_file, properties)
        val inputQueryFactory = queryReformulator.getInputQueryFactory

        val query = inputQueryFactory.createSelectQuery(sparqlQuery)

        val executableQuery = queryReformulator.reformulateIntoNativeQuery(query)
        val sqlQuery = Option(executableQuery.getTree)
          .filter(t => t.isInstanceOf[UnaryIQTree])
          .map(t => t.asInstanceOf[UnaryIQTree].getChild.getRootNode)
          .filter(n => n.isInstanceOf[NativeNode])
          .map(n => n.asInstanceOf[NativeNode].getNativeQueryString)
          .getOrElse(throw new RuntimeException("Cannot extract the SQL query from\n" + executableQuery))

        sqlQuery
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
        URLEncoder.encode(path, StandardCharsets.UTF_8.toString).toLowerCase.replace('%', 'P').replace('.', 'C')


    private def createOBDAMappings(properties: Set[String]): String = {

        def createMapping(id: String, property: String): String = {
            s"""
               |mappingId     $id
               |source        SELECT `s`, `o` FROM `${escapeTablename(property)}`
               |target        <{s}> <$property> <{o}> .
               |""".stripMargin
        }

        "[MappingDeclaration] @collection [[" +
          properties
            .map(op => createMapping(escapeTablename(op), op))
            .mkString("\n\n") + "]]"
    }

}

