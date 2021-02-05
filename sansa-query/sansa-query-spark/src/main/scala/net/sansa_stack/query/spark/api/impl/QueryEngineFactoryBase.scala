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

  protected def createWithPartitioning(triples: RDD[graph.Triple],
                                       partitioner: RdfPartitioner[RdfPartitionStateDefault] = RdfPartitionerDefault,
                                       explodeLanguageTags: Boolean = false,
                                       sqlEscaper: SqlEscaper = new SqlEscaperBacktick(),
                                       escapeIdentifiers: Boolean = false): QueryExecutionFactorySpark = {
    // apply vertical partitioning
    import net.sansa_stack.rdf.spark.partition._
    // Pass the table name from the outside?
    // val tableNameFn: RdfPartitionStateDefault => String = p => SQLUtils.escapeTablename(R2rmlUtils.createDefaultTableName(p))

    val r2rmlMappedSparkSession = triples.verticalPartition(partitioner, explodeLanguageTags, sqlEscaper, escapeIdentifiers)

    val mappingsModel = r2rmlMappedSparkSession.r2rmlModel
    create(None, mappingsModel)
  }

  override def create(triples: RDD[graph.Triple]): QueryExecutionFactorySpark = {
    createWithPartitioning(triples)
  }

}
