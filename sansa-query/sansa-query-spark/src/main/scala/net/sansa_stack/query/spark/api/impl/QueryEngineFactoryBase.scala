package net.sansa_stack.query.spark.api.impl

import org.aksw.sparqlify.core.sql.common.serialization.{SqlEscaper, SqlEscaperBacktick, SqlEscaperDoubleQuote}
import org.apache.jena.graph
import org.apache.jena.rdf.model.ModelFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import net.sansa_stack.query.spark.api.domain.{QueryEngineFactory, QueryExecutionFactorySpark}
import net.sansa_stack.rdf.common.partition.core.{RdfPartitionStateDefault, RdfPartitioner, RdfPartitionerComplex, RdfPartitionerDefault}
import net.sansa_stack.rdf.common.partition.r2rml.R2rmlUtils
import net.sansa_stack.rdf.spark.partition.core.{RdfPartitionUtilsSpark, SQLUtils, SparkTableGenerator}

/**
 * @author Lorenz Buehmann
 */
abstract class QueryEngineFactoryBase(spark: SparkSession) extends QueryEngineFactory {

  protected def createWithPartitioning(triples: RDD[graph.Triple],
                       partitioner: RdfPartitioner[RdfPartitionStateDefault] = RdfPartitionerDefault,
                       explodeLanguageTags: Boolean = false,
                       sqlEscaper: SqlEscaper = new SqlEscaperBacktick(),
                       escapeIdentifiers: Boolean = false): QueryExecutionFactorySpark = {
    // apply vertical partitioning
    val partitions2RDD = RdfPartitionUtilsSpark.partitionGraph(triples, partitioner)

    val tableNameFn: RdfPartitionStateDefault => String = p => SQLUtils.escapeTablename(R2rmlUtils.createDefaultTableName(p))

//    partitions2RDD.foreach {
//      case (p, rdd) =>
//        println(p)
//        rdd.collect().foreach(println)
//    }

    // create the Spark tables
    SparkTableGenerator(spark).createAndRegisterSparkTables(partitioner,
      partitions2RDD,
      extractTableName = tableNameFn)

    // create the mappings model
    val mappingsModel = ModelFactory.createDefaultModel()
    R2rmlUtils.createR2rmlMappings(
      partitioner,
      partitions2RDD.keySet.toSeq,
      tableNameFn,
      sqlEscaper,
      mappingsModel,
      explodeLanguageTags,
      escapeIdentifiers)

    mappingsModel.write(System.out, "Turtle")

    create(null, mappingsModel)

  }

  override def create(triples: RDD[graph.Triple]): QueryExecutionFactorySpark = {
    createWithPartitioning(triples)
  }

}
