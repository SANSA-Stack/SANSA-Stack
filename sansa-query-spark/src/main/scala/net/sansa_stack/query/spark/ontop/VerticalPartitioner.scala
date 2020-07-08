package net.sansa_stack.query.spark.ontop

import java.io.File
import java.net.URI

import org.apache.jena.vocabulary.RDF
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.model.{HasDataPropertiesInSignature, HasObjectPropertiesInSignature, IRI}

import net.sansa_stack.rdf.common.partition.core.{RdfPartitionComplex, RdfPartitionerComplex}
import net.sansa_stack.rdf.spark.partition.core.RdfPartitionUtilsSpark


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
    scopt.Read.reads(BlankNodeStrategy withName _)

  case class Config(
                     inputPath: URI = null,
                     outputPath: URI = null,
                     schemaPath: URI = null,
                     blankNodeStrategy: BlankNodeStrategy.Value = BlankNodeStrategy.Table,
                     computeStatistics: Boolean = true,
                     databaseName: String = "Default",
                     usePartitioning: Boolean = false,
                     partitioningThreshold: Int = 100,
                     mode: String = "partitioner")

  import scopt.OParser
  val builder = OParser.builder[Config]
  val parser = {
    import builder._
    OParser.sequence(
      programName("vpartitioner"),
      head("vertical partitioner", "0.1"),
      opt[URI]('i', "input")
        .required()
        .action((x, c) => c.copy(inputPath = x))
        .text("path to input data"),
      opt[URI]('o', "output")
        .required()
        .action((x, c) => c.copy(outputPath = x))
        .text("path to output directory"),
      opt[URI]('s', "schema")
        .optional()
        .action((x, c) => c.copy(schemaPath = x))
        .text("an optional file containing the OWL schema to process only object and data properties"),
      opt[BlankNodeStrategy.Value]('b', "blanknode-strategy")
        .optional()
        .action((x, c) => c.copy(blankNodeStrategy = x))
        .text("how blank nodes are handled during partitioning (TABLE, COLUMN)"),
      opt[Boolean]('s', "stats")
        .action((x, c) => c.copy(computeStatistics = x))
        .text("compute statistics for the Parquet tables"),
      opt[String]("database")
        .optional()
        .abbr("db")
        .action((x, c) => c.copy(databaseName = x))
        .text("the database name registered in Spark metadata. Default: 'Default'"),
      opt[Boolean]("partitioning")
        .optional()
        .action((x, c) => c.copy(usePartitioning = x))
        .text("if partitioning of subject/object columns should be computed"),
      opt[Int]("partitioning-threshold")
        .optional()
        .action((x, c) => c.copy(partitioningThreshold = x))
        .text("the max. number of values of subject/object values for which partitioning of the table is considered"),
      cmd("show")
        .action((_, c) => c.copy(mode = "show-tables"))
        .text("update is a command.")
    )
  }

  def main(args: Array[String]): Unit = {
    OParser.parse(parser, args, Config()) match {
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

  private def showTables(databaseName: String): Unit = {
    val spark = SparkSession.builder
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

    spark.sql(s"use $databaseName")
    spark.sql("show tables").show(1000, false)
    spark.sql("show tables").select("tableName").collect().foreach(
      row => {
        print(row.getString(0))
        spark.sql(s"select * from ${row.getString(0)}").show(false)
      })

    spark.stop()
  }

  private def run(config: Config): Unit = {

    import scala.collection.JavaConverters._

    import net.sansa_stack.rdf.spark.io._

    val spark = SparkSession.builder
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

    // read triples as RDD[Triple]
    var triplesRDD = spark.ntriples()(config.inputPath.toString)

    // filter properties if schema ontology was given
    if(config.schemaPath != null) {
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
    val partitions: Map[RdfPartitionComplex, RDD[Row]] = time(RdfPartitionUtilsSpark.partitionGraph(triplesRDD, partitioner = RdfPartitionerComplex()))
    println(s"#partitions: ${partitions.size}")

    // create database in Spark
    spark.sql(s"create database ${config.databaseName}")

    // set database as current
    spark.sql(s"use ${config.databaseName}")

    println("creating Spark tables ...")
    partitions.foreach {
      case (p, rdd) => createSparkTable(spark, p, rdd,
                                        config.blankNodeStrategy, config.computeStatistics, config.outputPath.toString,
                                        config.usePartitioning, config.partitioningThreshold)
    }


    spark.stop()

  }

  private def estimatePartioningColumns(df: DataFrame): (Long, Long) = {
    val sCnt = df.select("s").distinct().count()
    val oCnt = df.select("o").distinct().count()

    (sCnt, oCnt)
  }

  private def createSparkTable(session: SparkSession,
                               p: RdfPartitionComplex,
                               rdd: RDD[Row],
                               blankNodeStrategy: BlankNodeStrategy.Value,
                               computeStatistics: Boolean,
                               path: String,
                               usePartitioning: Boolean,
                               partitioningThreshold: Int): Unit = {
    val tableName = SQLUtils.escapeTablename(SQLUtils.createTableName(p, blankNodeStrategy), quoted = false)
    val scalaSchema = p.layout.schema
    val sparkSchema = ScalaReflection.schemaFor(scalaSchema).dataType.asInstanceOf[StructType]
    val df = session.createDataFrame(rdd, sparkSchema)

    if (!session.catalog.tableExists(tableName)) {
      println(s"creating Spark table $tableName")
      time {
        var writer = df.write.format("parquet")// .option("path", path)

        if (usePartitioning) {
          val (sCnt, oCnt) = estimatePartioningColumns(df)
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
