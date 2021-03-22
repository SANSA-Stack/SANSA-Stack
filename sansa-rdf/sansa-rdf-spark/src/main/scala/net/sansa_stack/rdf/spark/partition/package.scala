package net.sansa_stack.rdf.spark

import net.sansa_stack.rdf.common.partition.core.{RdfPartitionStateDefault, RdfPartitioner, RdfPartitionerDefault}
import net.sansa_stack.rdf.common.partition.r2rml.R2rmlUtils
import net.sansa_stack.rdf.common.partition.utils.SQLUtils
import net.sansa_stack.rdf.spark.mappings.R2rmlMappedSparkSession
import net.sansa_stack.rdf.spark.partition.core.{BlankNodeStrategy, RdfPartitionUtilsSpark, SparkTableGenerator}
import net.sansa_stack.rdf.spark.partition.semantic.SemanticRdfPartitionUtilsSpark
import net.sansa_stack.rdf.spark.utils.{Logging, SparkSessionUtils}
import org.aksw.commons.sql.codec.api.SqlCodec
import org.aksw.commons.sql.codec.util.SqlCodecUtils
import org.apache.jena.graph.Triple
import org.apache.jena.rdf.model.ModelFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

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

    val dbNameFn: RDD[Triple] => String = triplesRDD => s"sansa_${triplesRDD.id}"

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
                          sqlCodec: SqlCodec = SqlCodecUtils.createSqlCodecDefault(),
                          escapeIdentifiers: Boolean = false): R2rmlMappedSparkSession = {
      val partitioning: Map[RdfPartitionStateDefault, RDD[Row]] =
        RdfPartitionUtilsSpark.partitionGraph(rddOfTriples, partitioner)

      val mappingsModel = ModelFactory.createDefaultModel
      // R2rmlUtils.createR2rmlMappings(partitioner, partitioning.keys.toSeq, model, explodeLanguageTags)

      val sparkSession = SparkSessionUtils.getSessionFromRdd(rddOfTriples)

      // Create a table prefix for each partitioning of RDDs
      // TODO Encode partitioner hash into the name
      val rddSysHash = System.identityHashCode(rddOfTriples)
      val rddId = if (rddSysHash < 0) "_" + -rddSysHash else "" + rddSysHash
      val tableNaming: RdfPartitionStateDefault => String =
        partitionState => "rdd" + rddId + "_" + SQLUtils.encodeTablename(SQLUtils.createDefaultTableName(partitionState))

      val tableNameFn: RdfPartitionStateDefault => String = p => SQLUtils.encodeTablename(SQLUtils.createDefaultTableName(p))

      // we use the RDD ID as table name as this ID is guaranteed to be unique among the Spark session
      val dbName = dbNameFn(rddOfTriples)
      val database = None // Some(dbName) // TODO activate the database per RDD here

      // val sqlEscaper = new SqlEscaperBacktick

      R2rmlUtils.createR2rmlMappings(partitioner,
        partitioning.keySet.toSeq,
        tableNaming,
        database,
        sqlCodec,
        mappingsModel,
        explodeLanguageTags
      )

//      partitioning.foreach { case(p, rdd) =>
//
//        // val scalaSchema = partitioner.determineLayout(p).schema
//        // val sparkSchema = ScalaReflection.schemaFor(scalaSchema).dataType.asInstanceOf[StructType]
//        // val df = sparkSession.createDataFrame(rdd, sparkSchema).persist()
//
//        val tableName = tableNaming(p)
//
//        val triplesMaps = R2rmlUtils.createR2rmlMappings(
//          partitioner,
//          p,
//          tableNaming,
//          database,
//          sqlEscaper,
//          model,
//          explodeLanguageTags,
//          escapeIdentifiers
//        )
//
// //        triplesMaps.foreach(tm =>
// //          RDFDataMgr.write(System.err, model, RDFFormat.TURTLE_PRETTY)
// //        )
//
//        log.info(s"Partitioning: Created table ${tableName} with R2RML model of ${triplesMaps.size} triples maps")
//        // val tableName = tableNaming.apply(p)
//        // df.createOrReplaceTempView(sqlEscaper.escapeTableName(tableName))
//      }

//      RDFDataMgr.write(System.out, model, RDFFormat.TURTLE_PRETTY)

      // TODO Refactor this method to return an object that gives access to the SparkTableGenerator
      //   such that its settings can be post-processed

      val targetDatabase = if (sparkSession.catalog.currentDatabase == "default") None
        else Option(sparkSession.catalog.currentDatabase)

      val useHive: Boolean = targetDatabase.isDefined

      new SparkTableGenerator(sparkSession, targetDatabase, BlankNodeStrategy.Table, useHive)
        .createAndRegisterSparkTables(partitioner, partitioning, tableNaming)

      R2rmlMappedSparkSession(sparkSession, mappingsModel)
    }


    /**
     * semantic partition of and RDF graph
     */
    def partitionGraphAsSemantic(): RDD[String] = {
      SemanticRdfPartitionUtilsSpark.partitionGraph(rddOfTriples)
    }

  }
}
