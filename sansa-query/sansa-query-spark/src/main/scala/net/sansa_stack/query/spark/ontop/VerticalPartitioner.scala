package net.sansa_stack.query.spark.ontop

import net.sansa_stack.rdf.common.partition.core.{RdfPartitionStateDefault, RdfPartitioner, RdfPartitionerComplex, TermType}
import net.sansa_stack.rdf.common.partition.r2rml.R2rmlUtils
import net.sansa_stack.rdf.common.partition.utils.SQLUtils
import net.sansa_stack.rdf.spark.partition.core.{BlankNodeStrategy, RdfPartitionUtilsSpark, SparkTableGenerator}
import org.aksw.commons.sql.codec.util.SqlCodecUtils
import org.aksw.r2rml.jena.vocab.RR
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.vocabulary.{RDF, XSD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.LegacyBehaviorPolicy
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SparkSession, SaveMode => TableSaveMode}
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.model.{HasDataPropertiesInSignature, HasObjectPropertiesInSignature, IRI}

import java.io.{File, FileOutputStream}
import java.net.URI
import java.nio.file.Paths


/**
 * A vertical partitioner implementation that can be run solely.
 * It creates SQL tables and writes the partitioned data in Parquet format to disk.
 * SQL tables will registered in a metastore, thus, on restart tables will still be available via SQL queries.
 *
 * @author Lorenz Buehmann
 */
object VerticalPartitioner {

  val warehouseLocation = new File("spark-warehouse").getAbsolutePath

  implicit val bNodeRead: scopt.Read[BlankNodeStrategy.Value] =
    scopt.Read.reads(BlankNodeStrategy.withName)

  case class Config(
                     inputPath: URI = null,
                     outputPath: URI = null,
                     schemaPath: URI = null,
                     blankNodeStrategy: BlankNodeStrategy.Value = BlankNodeStrategy.Table,
                     computeStatistics: Boolean = true,
                     databaseName: String = "Default",
                     dropDatabase: Boolean = false,
                     mappingsFile: Option[String] = None,
                     saveIgnore: Boolean = false,
                     saveOverwrite: Boolean = false,
                     saveAppend: Boolean = false,
                     usePartitioning: Boolean = false,
                     partitioningThreshold: Int = 100,
                     mode: String = "partitioner")

  val parser = new scopt.OptionParser[Config]("VerticalPartitioner") {
      head("vertical partitioner", "0.1")
      opt[URI]('i', "input")
        .required()
        .action((x, c) => c.copy(inputPath = x))
        .text("path to input data")
      opt[URI]('o', "output")
        .required()
        .action((x, c) => c.copy(outputPath = x))
        .text("path to output directory")
      opt[URI]('s', "schema")
        .optional()
        .action((x, c) => c.copy(schemaPath = x))
        .text("an optional file containing the OWL schema to process only object and data properties")
      opt[BlankNodeStrategy.Value]('b', "blanknode-strategy")
        .optional()
        .action((x, c) => c.copy(blankNodeStrategy = x))
        .text("how blank nodes are handled during partitioning (TABLE, COLUMN)")
      opt[Boolean]('s', "stats")
        .optional()
        .action((x, c) => c.copy(computeStatistics = x))
        .text("compute statistics for the Parquet tables")
      opt[String]("database")
        .optional()
        .abbr("db")
        .action((x, c) => c.copy(databaseName = x))
        .text("the database name registered in Spark metadata. Default: 'Default'")
      opt[Unit]("drop-db")
        .optional()
        .action((_, c) => c.copy(dropDatabase = true))
        .text("if to drop an existing database")
      opt[String]("mappings-file")
        .optional()
        .action((x, c) => c.copy(mappingsFile = Option(x)))
        .text("path to the generated R2RML mappings file")
      opt[Unit]("save-ignore")
        .optional()
        .action((_, c) => c.copy(saveIgnore = true))
        .text("if data/table already exists, the save operation is expected to not save the contents of the DataFrame and to not change the existing data")
      opt[Unit]("save-overwrite")
        .optional()
        .action((_, c) => c.copy(saveOverwrite = true))
        .text("if data/table already exists, existing data is expected to be overwritten")
      opt[Unit]("save-append")
        .optional()
        .action((_, c) => c.copy(saveAppend = true))
        .text("if data/table already exists, contents of the DataFrame are expected to be appended to existing data")
      opt[Unit]("partitioning")
        .optional()
        .action((_, c) => c.copy(usePartitioning = true))
        .text("if partitioning of subject/object columns should be computed")
      opt[Int]("partitioning-threshold")
        .optional()
        .action((x, c) => c.copy(partitioningThreshold = x))
        .text("the max. number of values of subject/object values for which partitioning of the table is considered")
      cmd("show")
        .action((_, c) => c.copy(mode = "show-tables"))
        .text("update is a command.")
  }

  def main(args: Array[String]): Unit = {
    parser.parse(args, Config()) match {
      case Some(config) =>
        if (config.mode == "partitioner") {
          run(config)
        } else if (config.mode == "show-tables") {
          showTables(config.databaseName)
        } else {
        }

      case _ =>
      // arguments are bad, error message will have been displayed
    }

  }

  val sqlCodec = SqlCodecUtils.createSqlCodecForApacheSpark



  private def showTables(databaseName: String): Unit = {
    val spark = SparkSession.builder()
      //      .master("local")
      .appName("vpartitioner")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // .config("spark.kryo.registrationRequired", "true")
      // .config("spark.eventLog.enabled", "true")
      .config("spark.kryo.registrator", String.join(", ",
        "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator"))
      //      .config("spark.default.parallelism", "4")
      //      .config("spark.sql.shuffle.partitions", "4")
      //      .config("spark.sql.warehouse.dir", config.outputPath.)
      .config("spark.sql.cbo.enabled", true)
      .config("spark.sql.statistics.histogram.enabled", true)
      .enableHiveSupport()
      .getOrCreate()

    spark.sql(s"use ${sqlCodec.forSchemaName.encode(databaseName)}")
    spark.sql("show tables").show(1000, false)
    spark.sql("show tables").select("tableName").collect().foreach(
      row => {
        spark.sql(s"describe extended ${row.getString(0)}").show(21, false)
        spark.sql(s"DESC FORMATTED ${row.getString(0)}").show(false)
        spark.sql(s"select * from ${row.getString(0)}").show(false)
      })

    spark.stop()
  }

  private def run(config: Config): Unit = {

    import net.sansa_stack.rdf.spark.io._

    import scala.collection.JavaConverters._

    val spark = SparkSession.builder()
      //      .master("local")
      .appName("vpartitioner")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // .config("spark.kryo.registrationRequired", "true")
      // .config("spark.eventLog.enabled", "true")
      .config("spark.kryo.registrator", String.join(", ",
        "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator"))
      //      .config("spark.default.parallelism", "4")
      //      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.warehouse.dir", config.outputPath.toString)
      .config("spark.sql.cbo.enabled", true)
      .config("spark.sql.statistics.histogram.enabled", true)
      .config("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", LegacyBehaviorPolicy.CORRECTED.toString)
      .enableHiveSupport()
      .getOrCreate()

    // sanity check for existing database
    val dbExists = spark.catalog.databaseExists(config.databaseName)

    // we do terminate here if a database exist but neither overwrite or drop first was forced
    if (dbExists && !(config.saveAppend || config.saveOverwrite || config.saveAppend || config.dropDatabase)) {
      System.err.println("Error: database already exists. Please use CLI flags --drop-db or --overwrite-db to continue")
      return
    }

    // read triples as RDD[Triple]
    var triplesRDD = spark.ntriples()(config.inputPath.toString)

    // filter properties if schema ontology was given
    if (config.schemaPath != null) {
      val man = OWLManager.createOWLOntologyManager()
      val ont = man.loadOntologyFromOntologyDocument(IRI.create(config.schemaPath))
      // get all object properties in schema file
      val objectProperties = ont.asInstanceOf[HasObjectPropertiesInSignature].getObjectPropertiesInSignature.iterator().asScala.map(_.toStringID).toSet
      // get all data properties in schema file
      val dataProperties = ont.asInstanceOf[HasDataPropertiesInSignature].getDataPropertiesInSignature.iterator().asScala.map(_.toStringID).toSet
      val schemaProperties = objectProperties ++ dataProperties ++ Set(RDF.`type`.getURI)
      // filter triples RDD
      triplesRDD = triplesRDD.filter(t => schemaProperties.contains(t.getPredicate.getURI))
    }
    triplesRDD.cache()

    // do partitioning here
    println("computing partitions ...")
    val partitioner = RdfPartitionerComplex()
    val partitions: Map[RdfPartitionStateDefault, RDD[Row]] = time(RdfPartitionUtilsSpark.partitionGraph(triplesRDD, partitioner))
    println(s"#partitions: ${partitions.size}")
    println(partitions.mkString("\n"))

    val dbName = sqlCodec.forSchemaName.encode(config.databaseName)

    // we drop the database if forced
    if (config.dropDatabase) spark.sql(s"DROP DATABASE IF EXISTS $dbName CASCADE")

    // create database in Spark
    spark.sql(s"create database if not exists $dbName")

    // set database as current
    spark.sql(s"use $dbName")

    val saveMode: TableSaveMode =
      if (config.saveIgnore) {
        TableSaveMode.Ignore
      } else if (config.saveAppend) {
        TableSaveMode.Append
      } else if (config.saveOverwrite) {
        TableSaveMode.Overwrite
      } else {
        TableSaveMode.ErrorIfExists
      }

    // create the Spark tables
    println("creating Spark tables ...")
    // we use a URL encoding on top of the default table naming to avoid file path issues when saving to disk
    val tableNameFn: RdfPartitionStateDefault => String = p => SQLUtils.encodeTablename(SQLUtils.createDefaultTableName(p))

    // generated the Spark tables (virtually, not materialized yet)
    val partitionToTableName: Seq[(RdfPartitionStateDefault, String)] = SparkTableGenerator(spark,
      database = Option(config.databaseName),
      blankNodeStrategy = config.blankNodeStrategy,
      useHive = false,
      computeStatistics = config.computeStatistics)
      .createAndRegisterSparkTables(partitioner, partitions, extractTableName = tableNameFn)

    // determine the rdf:type partition (we assume just one here, no blank nodes classes aka complex classes covered)
    val typeTableName = partitionToTableName
      .find { case (p, _) => p.predicate == RDF.`type`.getURI && p.subjectType == TermType.IRI && p.objectType == TermType.IRI }
      .map(_._2)

    val dateDatatypes = Seq(XSD.date, XSD.gYear, XSD.gMonthDay, XSD.gDay, XSD.gMonth, XSD.gYearMonth).map(_.getURI)

    // save tables to disk
    partitionToTableName.foreach { case (p, tableName) =>

      try {
        val df = spark.table(sqlCodec.forTableName.encode(tableName))
        var writer: DataFrameWriter[Row] = df.write

        // rdf:type partition will be partitioned by types
        if (typeTableName.contains(tableName)) {
          writer = df.repartition(col("o"))
            .sortWithinPartitions(col("o"), col("s"))
            .write
            .partitionBy("o")
        }

        // handle literals
        if (p.objectType == TermType.LITERAL) {
          // we do always partition by language tag
          if (p.languages.size >= 2) {
            writer = df.repartition(col("l"))
              .sortWithinPartitions(col("l"), col("o"), col("s"))
              .write
              .partitionBy("l")
          }

          // date literals are also a good value for partitioning
          if (dateDatatypes.contains(p.datatype)) {
            writer = df.repartition(col("o"))
              .sortWithinPartitions(col("o"), col("s"))
              .write
              .partitionBy("o")
          }

        }

        writer
          .mode(saveMode)
          .format("parquet")
          .saveAsTable(SQLUtils.encodeTablename(tableName))
      } catch {
        case e: Exception => System.err.println(s"failed to write table ${tableName} as Parquet to disk. Reason:")
          e.printStackTrace()
      }


      //    partitions.foreach {
      //      case (p, rdd) => createSparkTable(spark, p, rdd, saveMode,
      //                                        config.blankNodeStrategy, config.computeStatistics, config.outputPath.toString,
      //                                        config.usePartitioning, config.partitioningThreshold)
      //    }
    }

    // write the partitioning metadata as R2RML mappings to disk
    // we use double quotes as escape chars for the SQL identifiers
    val path = config.mappingsFile.getOrElse(Paths.get(config.outputPath.toString, s"${config.databaseName}-r2rml-mappings.ttl").toAbsolutePath.toString)
    val model = ModelFactory.createDefaultModel()
    R2rmlUtils.createR2rmlMappings(
      partitioner,
      partitions.keySet.toSeq,
      tableNameFn,
      Option(config.databaseName),
      SqlCodecUtils.createSqlCodecDefault(),
      model,
      explodeLanguageTags = true)

    model.write(new FileOutputStream(new File(path)), "TURTLE", RR.uri)
    println(s"R2RML mapping model written to $path")
    spark.stop()
  }

//  def getOrCreate(databaseName: String): Set[RdfPartitionDefault]: Unit {
//
//  }

  private def estimatePartitioningColumns(df: DataFrame): (Long, Long) = {
    val sCnt = df.select("s").distinct().count()
    val oCnt = df.select("o").distinct().count()

    (sCnt, oCnt)
  }

  private def createSparkTable(session: SparkSession,
                               partitioner: RdfPartitioner[RdfPartitionStateDefault],
                               p: RdfPartitionStateDefault,
                               rdd: RDD[Row],
                               saveMode: TableSaveMode,
                               blankNodeStrategy: BlankNodeStrategy.Value,
                               computeStatistics: Boolean,
                               path: String,
                               usePartitioning: Boolean,
                               partitioningThreshold: Int): Unit = {
    val tableName = sqlCodec.forTableName.encode(SQLUtils.createDefaultTableName(p))
    // val scalaSchema = p.layout.schema
    val scalaSchema = partitioner.determineLayout(p).schema
    val sparkSchema = ScalaReflection.schemaFor(scalaSchema).dataType.asInstanceOf[StructType]
    val df = session.createDataFrame(rdd, sparkSchema)

    if (session.catalog.tableExists(tableName) && saveMode == TableSaveMode.ErrorIfExists) {
      throw new RuntimeException(s"ERROR: table $tableName already exists. Please enable a save mode to handle this case.")
    } else if (!(session.catalog.tableExists(tableName) && saveMode == TableSaveMode.Ignore)) {
      println(s"creating Spark table $tableName")
      time {
        var writer = df.write.mode(saveMode).format("parquet")// .option("path", path)

        if (usePartitioning) {
          val (sCnt, oCnt) = estimatePartitioningColumns(df)
          val ratio = oCnt / sCnt.doubleValue()
          println(s"partition estimates: |s|=$sCnt |o|=$oCnt ratio o/s=$ratio")

          if (sCnt <= partitioningThreshold) writer = writer.partitionBy("s")
          else if (oCnt <= partitioningThreshold && ratio < 0.01) writer = writer.partitionBy("o")
        }

        writer.saveAsTable(tableName)
      }

      //    df.createOrReplaceTempView("`" + escapeTablename(name) + "_tmp`")
      //
      //    val schemaDDL = session.createDataFrame(rdd, sparkSchema).schema.toDDL
      //    val query =
      //      s"""
      //         |CREATE TABLE IF NOT EXISTS `${escapeTablename(name)}`
      //         |USING PARQUET
      //         |PARTITIONED BY (`s`)
      //         |AS SELECT * FROM `${escapeTablename(name)}_tmp`
      //         |""".stripMargin
      //    session.sql(query)

      if (computeStatistics) {
        println(s"computing statistics for table $tableName")
        session.sql(s"ANALYZE TABLE `$tableName` COMPUTE STATISTICS FOR COLUMNS s, o")
      }
    }
  }

  def time[R](block: => R): R = {
    val t0 = System.currentTimeMillis()
    val result = block    // call-by-name
    val t1 = System.currentTimeMillis()
    println("Elapsed time: " + (t1 - t0) + "ms")
    result
  }


}
