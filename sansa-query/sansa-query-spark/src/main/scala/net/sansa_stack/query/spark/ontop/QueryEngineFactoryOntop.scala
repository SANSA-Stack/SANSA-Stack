package net.sansa_stack.query.spark.ontop

import net.sansa_stack.query.spark.api.domain.QueryExecutionFactorySpark
import net.sansa_stack.query.spark.api.impl.QueryEngineFactoryBase
import net.sansa_stack.rdf.common.partition.core.{RdfPartitionStateDefault, RdfPartitioner, RdfPartitionerComplex}
import org.aksw.commons.sql.codec.api.SqlCodec
import org.aksw.commons.sql.codec.util.SqlCodecUtils
import org.apache.jena.graph
import org.apache.jena.rdf.model.Model
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.semanticweb.owlapi.model.OWLOntology

/**
 * A query engine factory with Ontop as SPARQL to SQL rewriter.
 *
 * @author Lorenz Buehmann
 */
class QueryEngineFactoryOntop(spark: SparkSession)
  extends QueryEngineFactoryBase(spark, new RdfPartitionerComplex()) {

  // partitioner: RdfPartitioner[RdfPartitionStateDefault],
  override protected def createWithPartitioning(triples: RDD[graph.Triple],
                                                explodeLanguageTags: Boolean,
                                                sqlCodec: SqlCodec,
                                                escapeIdentifiers: Boolean): QueryExecutionFactorySpark = {
    super.createWithPartitioning(triples,
      explodeLanguageTags = true,
      SqlCodecUtils.createSqlCodecDefault,
      escapeIdentifiers = true)
  }

  override def create(database: Option[String], mappingModel: Model): QueryExecutionFactorySpark = {
    create(database, mappingModel, null)
  }

  def create(database: Option[String], mappingModel: Model, ontology: OWLOntology): QueryExecutionFactorySpark = {
    require(database != null, "database must non be null. Use None for absence.")
    require(mappingModel != null, "mappings must not be null.")

    val ontop: QueryEngineOntop = QueryEngineOntop(spark, database, mappingModel, Option(ontology))

    new QueryExecutionFactorySparkOntop(spark, database, ontop)
  }
}
