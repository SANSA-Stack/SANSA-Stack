package net.sansa_stack.rdf.spark

import net.sansa_stack.rdf.common.partition.core.{RdfPartitionStateDefault, RdfPartitioner, RdfPartitionerDefault}
import net.sansa_stack.rdf.common.partition.r2rml.R2rmlUtils.createR2rmlMappings
import net.sansa_stack.rdf.common.partition.r2rml.{R2rmlModel, R2rmlUtils}
import net.sansa_stack.rdf.spark.mappings.R2rmlMappedSparkSession
import net.sansa_stack.rdf.spark.partition.core.{RdfPartitionUtilsSpark, SparkTableGenerator}
import net.sansa_stack.rdf.spark.partition.semantic.SemanticRdfPartitionUtilsSpark
import net.sansa_stack.rdf.spark.utils.Logging
import org.aksw.sparqlify.core.sql.common.serialization.SqlEscaperBacktick
import org.apache.jena.graph.Triple
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.apache.jena.riot.{RDFDataMgr, RDFFormat}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType

/**
 * Wrap up implicit classes/methods to partition RDF data from N-Triples
 * files into either [[Sparqlify]] or [[Semantic]] partition strategies.
 *
 * @author Gezim Sejdiu
 */

package object partition extends Logging {

  object Strategy extends Enumeration {
    val CORE, SEMANTIC = Value
  }

  implicit class RDFPartition(rddOfTriples: RDD[Triple]) extends Serializable {

    /**
     * Default partition - using VP.
     */
    def partitionGraph(): Map[RdfPartitionStateDefault, RDD[Row]] = {
      RdfPartitionUtilsSpark.partitionGraph(rddOfTriples, RdfPartitionerDefault)
    }

    /**
     * Default partition - using VP.
     */
    def verticalPartition(partitioner: RdfPartitioner[RdfPartitionStateDefault],
                          explodeLanguageTags: Boolean = false,
                          escapeIdentifiers: Boolean = false): R2rmlMappedSparkSession = {
      val partitioning: Map[RdfPartitionStateDefault, RDD[Row]] =
        RdfPartitionUtilsSpark.partitionGraph(rddOfTriples, partitioner)

      val model: Model = ModelFactory.createDefaultModel
      // R2rmlUtils.createR2rmlMappings(partitioner, partitioning.keys.toSeq, model, explodeLanguageTags)

      val sparkSession = SparkSession.builder.config(rddOfTriples.sparkContext.getConf).getOrCreate()

      val tableNaming = R2rmlUtils.createDefaultTableName(_)
      val sqlEscaper = new SqlEscaperBacktick

      partitioning.foreach { case(p, rdd) =>

        // val scalaSchema = partitioner.determineLayout(p).schema
        // val sparkSchema = ScalaReflection.schemaFor(scalaSchema).dataType.asInstanceOf[StructType]
        // val df = sparkSession.createDataFrame(rdd, sparkSchema).persist()

        val tableName = tableNaming(p)

        val triplesMaps = R2rmlUtils.createR2rmlMappings(
          partitioner,
          p,
          tableNaming,
          sqlEscaper,
          model,
          explodeLanguageTags,
          escapeIdentifiers)

//        triplesMaps.foreach(tm =>
//          RDFDataMgr.write(System.err, model, RDFFormat.TURTLE_PRETTY)
//        )

        log.info(s"Partitioning: Created table ${tableName} with R2RML model of ${triplesMaps.size} triples maps")
        // val tableName = tableNaming.apply(p)
        // df.createOrReplaceTempView(sqlEscaper.escapeTableName(tableName))
      }

//      RDFDataMgr.write(System.out, model, RDFFormat.TURTLE_PRETTY)

      new SparkTableGenerator(sparkSession).createAndRegisterSparkTables(partitioner, partitioning, tableNaming)

      new R2rmlMappedSparkSession(sparkSession, model)
    }


    /**
     * semantic partition of and RDF graph
     */
    def partitionGraphAsSemantic(): RDD[String] = {
      SemanticRdfPartitionUtilsSpark.partitionGraph(rddOfTriples)
    }

  }
}
