package net.sansa_stack.query.spark.api.impl

import org.aksw.sparqlify.core.sql.common.serialization.{SqlEscaper, SqlEscaperBacktick}
import org.apache.jena.graph
import org.apache.jena.rdf.model.ModelFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import net.sansa_stack.query.spark.api.domain.{QueryEngineFactory, QueryExecutionFactorySpark}
import net.sansa_stack.rdf.common.partition.core.{RdfPartitionStateDefault, RdfPartitioner, RdfPartitionerDefault}
import net.sansa_stack.rdf.common.partition.r2rml.R2rmlUtils
import net.sansa_stack.rdf.common.partition.utils.SQLUtils
import net.sansa_stack.rdf.spark.partition.core.{RdfPartitionUtilsSpark, SparkTableGenerator}

/**
 * @author Lorenz Buehmann
 */
abstract class QueryEngineFactoryBase(spark: SparkSession)
  extends QueryEngineFactory {

  val dbNameFn: RDD[graph.Triple] => String = triplesRDD => s"sansa_${triplesRDD.id}"

  protected def createWithPartitioning(triples: RDD[graph.Triple],
                                       partitioner: RdfPartitioner[RdfPartitionStateDefault] = RdfPartitionerDefault,
                                       explodeLanguageTags: Boolean = false,
                                       sqlEscaper: SqlEscaper = new SqlEscaperBacktick(),
                                       escapeIdentifiers: Boolean = false): QueryExecutionFactorySpark = {
    // apply vertical partitioning
    val partitions2RDD = RdfPartitionUtilsSpark.partitionGraph(triples, partitioner)

    // we use the RDD ID as table name as this ID is guaranteed to be unique among the Spark session
    val dbName = dbNameFn(triples)

//    val tableNameFn: RdfPartitionStateDefault => (Option[String], String) = p => (Some(dbName), SQLUtils.encodeTablename(R2rmlUtils.createDefaultTableName(p)))
    val tableNameFn: RdfPartitionStateDefault => String = p => SQLUtils.encodeTablename(SQLUtils.createDefaultTableName(p))

    val database = None // Some(dbName) // TODO activate the database per RDD here

    // create the Spark tables
    SparkTableGenerator(spark, database).createAndRegisterSparkTables(
      partitioner,
      partitions2RDD,
      extractTableName = tableNameFn)

//    spark.catalog.listDatabases().show(false)
//    spark.catalog.listDatabases().collect().foreach(db => spark.catalog.listTables(db.name).show(false))

    // create the mappings model
    val mappingsModel = ModelFactory.createDefaultModel()
    R2rmlUtils.createR2rmlMappings(
      partitioner,
      partitions2RDD.keySet.toSeq,
      tableNameFn,
      database,
      sqlEscaper,
      mappingsModel,
      explodeLanguageTags,
      escapeIdentifiers)

//     mappingsModel.write(System.out, "Turtle")

    create(database, mappingsModel)

  }

  override def create(triples: RDD[graph.Triple]): QueryExecutionFactorySpark = {
    createWithPartitioning(triples)
  }

}
