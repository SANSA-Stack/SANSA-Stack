package net.sansa_stack.rdf.spark.partition.core

import org.aksw.sparqlify.core.sql.common.serialization.SqlEscaperBacktick
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import net.sansa_stack.rdf.common.partition.core.{RdfPartitionStateDefault, RdfPartitioner}
import net.sansa_stack.rdf.common.partition.utils.SQLUtils

/**
 * Creates Spark tables for given RDF partitions.
 *
 * @param spark the Spark session
 * @param database the Spark database name used to manage the tables
 * @param blankNodeStrategy if to use separate tables for bnode or columns, thus, it affects the table names
 * @param useHive
 * @param computeStatistics compute statistics for the tables which can be used by the CBO of Spark SQL engine
 * @author Lorenz Buehmann
 */
class SparkTableGenerator(spark: SparkSession,
                          database: Option[String] = None,
                          blankNodeStrategy: BlankNodeStrategy.Value = BlankNodeStrategy.Table,
                          useHive: Boolean = false,
                          computeStatistics: Boolean = false) {

  val logger = com.typesafe.scalalogging.Logger(SparkTableGenerator.getClass)

  val sqlEscaper = new SqlEscaperBacktick()

  if (database.isDefined) {
    val dbName = sqlEscaper.escapeIdentifier(database.get)
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $dbName")
    spark.sql(s"USE $dbName")
  }

  /**
   * Creates and registers a Spark table view `p(s,o)` resp. `p(s,o,l)` for each partition.
   *
   * @note partitions with string literals as object will be kept into a single table per property and a 3rd column for
   *       the (optional) language tag is used instead.
   * @param partitioner the partitioner
   * @param partitions the partitions
   * @param persistent if the tables will be saved to disk, i.e. they will be kept after the Spark session has been closed
   *
   * @return a mapping from the partition state to the Spark table name
   */
  def createAndRegisterSparkTables(partitioner: RdfPartitioner[RdfPartitionStateDefault],
                                   partitions: Map[RdfPartitionStateDefault, RDD[Row]],
                                   extractTableName: RdfPartitionStateDefault => String = p => SQLUtils.createDefaultTableName(p),
                                   persistent: Boolean = false): Seq[(RdfPartitionStateDefault, String)] = {

    // register the lang-tagged RDDs as a single table:
    // we have to merge the RDDs of all languages per property first, otherwise we would always replace it by another
    // language
    (partitions
      .filter(_._1.languages.nonEmpty)
      .map { case (p, rdd) => (p.predicate, p, rdd) }
      .groupBy(_._1)
      .map { case (k, v) =>
        val rdd = spark.sparkContext.union(v.map(_._3).toSeq)
        val p = v.head._2
        (p, rdd)
      }

      .map { case (p, rdd) => (extractTableName(p), p, rdd) }
      .groupBy(_._1)
      .map(map => map._2.head)
      .map(e => (e._2, e._3))

      .map { case (p, rdd) => p -> createSparkTable(partitioner, p, rdd, extractTableName, persistent) } ++

    // register the non-lang-tagged RDDs as table
    partitions
      .filter(_._1.languages.isEmpty)
      .map { case (p, rdd) => (extractTableName(p), p, rdd) }
      .groupBy(_._1)
      .map(map => map._2.head)
      .map(e => (e._2, e._3))

      .map {
        case (p, rdd) => p -> createSparkTable(partitioner, p, rdd, extractTableName, persistent)
      }).toSeq
  }

  /**
   * Creates a Spark table for each RDF partition.
   * Returns generated table name.
   */
  private def createSparkTable(partitioner: RdfPartitioner[RdfPartitionStateDefault],
                               p: RdfPartitionStateDefault,
                               rdd: RDD[Row],
                               extractTableName: RdfPartitionStateDefault => String,
                               persistent: Boolean): String = {

    // create table name
    val tableName = extractTableName(p)
    // escape table name for Spark/Hive
    val escapedTableName = sqlEscaper.escapeTableName(tableName)
    logger.debug(s"creating Spark table $tableName")
    val escapedQualifiedTableName = database.map(tnq => s"${sqlEscaper.escapeIdentifier(tnq)}.").getOrElse("") + escapedTableName

    // create the DataFrame out of the RDD and the schema
    val scalaSchema = partitioner.determineLayout(p).schema
    val sparkSchema = ScalaReflection.schemaFor(scalaSchema).dataType.asInstanceOf[StructType]

    // make all columns non-nullable for now as we do only consider VP tables
    def setNullableStateOfColumn(df: DataFrame, nullable: Boolean) : DataFrame = {
      // get schema
      val schema = df.schema
      // modify [[StructField] with name `cn`
      val newSchema = StructType(schema.map {
        case StructField( c, t, _, m) => StructField( c, t, nullable = nullable, m)
      })
      // apply new schema
      df.sqlContext.createDataFrame( df.rdd, newSchema )
    }
    val df = setNullableStateOfColumn(spark.createDataFrame(rdd, sparkSchema), false).persist()

    if (useHive) {
      df.createOrReplaceTempView(s"`${escapedTableName}_tmp`")

      val schemaDDL = spark.createDataFrame(rdd, sparkSchema).schema.toDDL
      spark.sql(s"DROP TABLE IF EXISTS $escapedTableName")
      val query =
        s"""
           |CREATE TABLE IF NOT EXISTS $escapedTableName
           |
           |USING PARQUET
           |AS SELECT * FROM ${escapedTableName}_tmp
           |""".stripMargin
      spark.sql(query)
    } else {
      df.createOrReplaceTempView(s"$escapedTableName") // register the dataframe as a view

      // if a database has been defined, we generate the table based on the qualified table identifier and fill it with the temp view
      if (database.isDefined) {
        spark.sql(s"DROP TABLE IF EXISTS $escapedQualifiedTableName")
        spark.sql(s"CREATE TABLE $escapedQualifiedTableName AS SELECT * FROM $escapedTableName")
      }
      if (persistent) {
        df.write
          .mode(SaveMode.Overwrite)
          .format("parquet")
          .saveAsTable(escapedTableName)
      }
    }

    // optionally, compute table statistics
    if (computeStatistics) {
      val columns = spark.table(escapedTableName).columns.mkString(",")
      spark.sql(s"ANALYZE TABLE $escapedTableName COMPUTE STATISTICS FOR COLUMNS $columns")
    }

    tableName
  }

}

object SparkTableGenerator {
  def apply(spark: SparkSession): SparkTableGenerator = new SparkTableGenerator(spark)

  def apply(spark: SparkSession,
            database: Option[String],
            blankNodeStrategy: BlankNodeStrategy.Value,
            useHive: Boolean,
            computeStatistics: Boolean): SparkTableGenerator =
    new SparkTableGenerator(spark, database, blankNodeStrategy, useHive, computeStatistics)

  def apply(spark: SparkSession,
            database: Option[String]): SparkTableGenerator =
    new SparkTableGenerator(spark, database)
}

