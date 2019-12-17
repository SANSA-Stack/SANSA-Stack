package net.sansa_stack.query.spark.sparqlify

import net.sansa_stack.rdf.common.partition.core.RdfPartitionDefault
import net.sansa_stack.rdf.common.partition.model.sparqlify.SparqlifyUtils2
import org.aksw.obda.domain.impl.LogicalTableTableName
import org.aksw.sparqlify.algebra.sql.nodes.SqlOpTable
import org.aksw.sparqlify.backend.postgres.DatatypeToStringCast
import org.aksw.sparqlify.config.syntax.Config
import org.aksw.sparqlify.core.algorithms.CandidateViewSelectorSparqlify
import org.aksw.sparqlify.core.algorithms.ViewDefinitionNormalizerImpl
import org.aksw.sparqlify.core.interfaces.SparqlSqlStringRewriter
import org.aksw.sparqlify.core.sql.common.serialization.SqlEscaperBase
import org.aksw.sparqlify.util.{SparqlifyCoreInit, SparqlifyUtils, SqlBackendConfig}
import org.aksw.sparqlify.validation.LoggerCount
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType

object SparqlifyUtils3 // extends StrictLogging
{
  def createSparqlSqlRewriter(sparkSession: SparkSession, partitions: Map[RdfPartitionDefault, RDD[Row]]): SparqlSqlStringRewriter = {
    val config = new Config()
    // val loggerCount = new LoggerCount(logger.underlying)

    val backendConfig = new SqlBackendConfig(new DatatypeToStringCast(), new SqlEscaperBase("`", "`")) // new SqlEscaperBacktick())
    val sqlEscaper = backendConfig.getSqlEscaper()
    val typeSerializer = backendConfig.getTypeSerializer()
    val sqlFunctionMapping = SparqlifyCoreInit.loadSqlFunctionDefinitions("functions-spark.xml")

    val ers = SparqlifyUtils.createDefaultExprRewriteSystem()
    val mappingOps = SparqlifyUtils.createDefaultMappingOps(ers)

    val candidateViewSelector = new CandidateViewSelectorSparqlify(mappingOps, new ViewDefinitionNormalizerImpl());

    val views = partitions.map {
      case (p, rdd) =>
        //
        //        logger.debug("Processing RdfPartition: " + p)

        val vd = SparqlifyUtils2.createViewDefinition(p)
        //       logger.debug("Created view definition: " + vd)

        val tableName = vd.getLogicalTable match {
          case o: LogicalTableTableName => o.getTableName
          case _ => throw new RuntimeException("Table name required - instead got: " + vd)
        }

        val scalaSchema = p.layout.schema
        val sparkSchema = ScalaReflection.schemaFor(scalaSchema).dataType.asInstanceOf[StructType]
        val df = sparkSession.createDataFrame(rdd, sparkSchema)

        df.createOrReplaceTempView(sqlEscaper.escapeTableName(tableName))
        config.getViewDefinitions.add(vd)
    }

    val basicTableInfoProvider = new BasicTableInfoProviderSpark(sparkSession)

    val rewriter = SparqlifyUtils.createDefaultSparqlSqlStringRewriter(basicTableInfoProvider, null, config, typeSerializer, sqlEscaper, sqlFunctionMapping)
    //   val rewrite = rewriter.rewrite(QueryFactory.create("Select * { <http://dbpedia.org/resource/Guy_de_Maupassant> ?p ?o }"))

    //    val rewrite = rewriter.rewrite(QueryFactory.create("Select * { ?s <http://xmlns.com/foaf/0.1/givenName> ?o ; <http://dbpedia.org/ontology/deathPlace> ?d }"))
    rewriter
  }

}
