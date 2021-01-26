package net.sansa_stack.query.spark.ontop

import org.aksw.sparqlify.core.sql.common.serialization.{SqlEscaper, SqlEscaperDoubleQuote}
import org.apache.jena.graph
import org.apache.jena.rdf.model.Model
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.semanticweb.owlapi.model.OWLOntology

import net.sansa_stack.query.spark.api.domain.QueryExecutionFactorySpark
import net.sansa_stack.query.spark.api.impl.QueryEngineFactoryBase
import net.sansa_stack.rdf.common.partition.core.{RdfPartitionStateDefault, RdfPartitioner, RdfPartitionerComplex}

/**
 * A query engine factory with Ontop as SPARQL to SQL rewriter.
 *
 * @author Lorenz Buehmann
 */
class QueryEngineFactoryOntop(spark: SparkSession) extends QueryEngineFactoryBase(spark) {

  override protected def createWithPartitioning(triples: RDD[graph.Triple],
                                                partitioner: RdfPartitioner[RdfPartitionStateDefault],
                                                explodeLanguageTags: Boolean,
                                                sqlEscaper: SqlEscaper,
                                                escapeIdentifiers: Boolean): QueryExecutionFactorySpark = {
    super.createWithPartitioning(triples,
      new RdfPartitionerComplex(),
      explodeLanguageTags = true,
      new SqlEscaperDoubleQuote(),
      escapeIdentifiers = true)
  }

  override def create(database: String, mappingModel: Model): QueryExecutionFactorySpark = {
    create(database, mappingModel, null)
  }

  def create(database: String, mappingModel: Model, ontology: OWLOntology): QueryExecutionFactorySpark = {
    val ontop: QueryEngineOntop = QueryEngineOntop(spark, database, mappingModel, Option(ontology))

    new QueryExecutionFactorySparkOntop(spark, ontop)
  }
}
