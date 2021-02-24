package net.sansa_stack.query.spark.api.impl

import net.sansa_stack.query.spark.api.domain.{QueryEngineFactory, QueryExecutionFactorySpark}
import net.sansa_stack.rdf.common.partition.core.{RdfPartitionStateDefault, RdfPartitioner, RdfPartitionerDefault}
import org.aksw.commons.sql.codec.api.SqlCodec
import org.aksw.commons.sql.codec.util.SqlCodecUtils
import org.apache.jena.graph
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * @author Lorenz Buehmann
 */
abstract class QueryEngineFactoryBase(spark: SparkSession)
  extends QueryEngineFactory {

  protected def createWithPartitioning(triples: RDD[graph.Triple],
                                       partitioner: RdfPartitioner[RdfPartitionStateDefault] = RdfPartitionerDefault,
                                       explodeLanguageTags: Boolean = false,
                                       sqlCodec: SqlCodec = SqlCodecUtils.createSqlCodecForApacheSpark,
                                       escapeIdentifiers: Boolean = false): QueryExecutionFactorySpark = {
    // apply vertical partitioning
    import net.sansa_stack.rdf.spark.partition._
    // Pass the table name from the outside?
    // val tableNameFn: RdfPartitionStateDefault => String = p => SQLUtils.escapeTablename(R2rmlUtils.createDefaultTableName(p))

    val r2rmlMappedSparkSession = triples.verticalPartition(partitioner, explodeLanguageTags, sqlCodec, escapeIdentifiers)

    val mappingsModel = r2rmlMappedSparkSession.r2rmlModel
    create(None, mappingsModel)
  }

  override def create(triples: RDD[graph.Triple]): QueryExecutionFactorySpark = {
    createWithPartitioning(triples)
  }

}
