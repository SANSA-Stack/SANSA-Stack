package net.sansa_stack.query.spark.ontop

import java.net.URLEncoder
import java.nio.charset.StandardCharsets

import net.sansa_stack.rdf.common.partition.core.{RdfPartitionStateDefault, RdfPartitioner}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession}

/**
 * @author Lorenz Buehmann
 */
class SparkTableGenerator(spark: SparkSession,
                          blankNodeStrategy: BlankNodeStrategy.Value = BlankNodeStrategy.Table,
                          useHive: Boolean = false,
                          computeStatistics: Boolean = false) {

  val logger = com.typesafe.scalalogging.Logger(SparkTableGenerator.getClass)

  implicit val o: Ordering[RdfPartitionStateDefault] = Ordering.by(e => (e.predicate, e.subjectType, e.objectType, e.langTagPresent, e.lang, e.datatype))

  /**
   * Creates and registers a Spark table p(s,o) for each partition.
   *
   * Note: partitions with string literals as object will be kept into a single table per property and a 3rd column for the
   * (optional) language tag is used instead.
   *
   * @param partitions the partitions
   */
  def createAndRegisterSparkTables(partitioner: RdfPartitioner[RdfPartitionStateDefault], partitions: Map[RdfPartitionStateDefault, RDD[Row]]): Unit = {

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

      .map { case (p, rdd) => (SQLUtils.createTableName(p, blankNodeStrategy), p, rdd) }
      .groupBy(_._1)
      .map(map => map._2.head)
      .map(e => (e._2, e._3))

      .foreach { case (p, rdd) => createSparkTable(partitioner, p, rdd) }

    // register the non-lang tagged RDDs as table
    partitions
      .filter(_._1.lang.isEmpty)

      .map { case (p, rdd) => (SQLUtils.createTableName(p, blankNodeStrategy), p, rdd) }
      .groupBy(_._1)
      .map(map => map._2.head)
      .map(e => (e._2, e._3))

      .foreach {
        case (p, rdd) => createSparkTable(partitioner, p, rdd)
      }
  }

  /**
   * creates a Spark table for each RDF partition
   */
  private def createSparkTable(partitioner: RdfPartitioner[RdfPartitionStateDefault], p: RdfPartitionStateDefault, rdd: RDD[Row]) = {

    val name = SQLUtils.createTableName(p, blankNodeStrategy)
    logger.debug(s"creating Spark table ${escapeTablename(name)}")

    // val scalaSchema = p.layout.schema
    val scalaSchema = partitioner.determineLayout(p).schema
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
    } else {
      df.createOrReplaceTempView("`" + escapeTablename(name) + "`")
      //          df.write.partitionBy("s").format("parquet").saveAsTable(escapeTablename(name))
    }

    if (computeStatistics) {
      spark.sql(s"ANALYZE TABLE `${escapeTablename(name)}` COMPUTE STATISTICS FOR COLUMNS s, o")
    }
  }

  private def escapeTablename(path: String): String =
    URLEncoder.encode(path, StandardCharsets.UTF_8.toString)
      .toLowerCase
      .replace('%', 'P')
      .replace('.', 'C')
      .replace("-", "dash")

}

object SparkTableGenerator {
  def apply(spark: SparkSession): SparkTableGenerator = new SparkTableGenerator(spark)

  def apply(spark: SparkSession,
            blankNodeStrategy: BlankNodeStrategy.Value,
            useHive: Boolean, computeStatistics: Boolean): SparkTableGenerator =
    new SparkTableGenerator(spark, blankNodeStrategy, useHive, computeStatistics)
}
