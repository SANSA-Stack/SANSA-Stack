package net.sansa_stack.query.flink.sparqlify

import com.typesafe.scalalogging.StrictLogging
import net.sansa_stack.rdf.partition.core.RdfPartitionDefault
import net.sansa_stack.rdf.partition.schema._
import net.sansa_stack.rdf.partition.sparqlify.SparqlifyUtils2
import org.aksw.sparqlify.algebra.sql.nodes.SqlOpTable
import org.aksw.sparqlify.backend.postgres.DatatypeToStringCast
import org.aksw.sparqlify.config.syntax.Config
import org.aksw.sparqlify.core.algorithms.{CandidateViewSelectorSparqlify, ViewDefinitionNormalizerImpl}
import org.aksw.sparqlify.core.interfaces.SparqlSqlStringRewriter
import org.aksw.sparqlify.core.sql.common.serialization.SqlEscaperBase
import org.aksw.sparqlify.util.{SparqlifyUtils, SqlBackendConfig}
import org.aksw.sparqlify.validation.LoggerCount
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}
import org.apache.flink.table.api.scala.BatchTableEnvironment

import scala.reflect.runtime.universe.typeOf

object SparqlifyUtils3
  extends StrictLogging
{
  def createSparqlSqlRewriter(flinkEnv: ExecutionEnvironment, flinkTable: BatchTableEnvironment, partitions: Map[RdfPartitionDefault, DataSet[_<:Product]]): SparqlSqlStringRewriter = {
    val config = new Config()
    val loggerCount = new LoggerCount(logger.underlying)


    val backendConfig = new SqlBackendConfig(new DatatypeToStringCast(), new SqlEscaperBase("", "")) //new SqlEscaperBacktick())
    val sqlEscaper = backendConfig.getSqlEscaper()
    val typeSerializer = backendConfig.getTypeSerializer()


    val ers = SparqlifyUtils.createDefaultExprRewriteSystem()
    val mappingOps = SparqlifyUtils.createDefaultMappingOps(ers)


    val candidateViewSelector = new CandidateViewSelectorSparqlify(mappingOps, new ViewDefinitionNormalizerImpl());

    partitions.map { case (p, ds) => {
      logger.debug("Processing RdfPartition: " + p)
      val vd = SparqlifyUtils2.createViewDefinition(p)
      logger.debug("Created view definition: " + vd)
      val tableName = vd.getRelation match {
        case o: SqlOpTable => o.getTableName
        case _ => throw new RuntimeException("Table name required - instead got: " + vd)
      }
      val q = p.layout.schema
      q match {
        case q if q =:= typeOf[SchemaStringLong] =>
          flinkTable.registerDataSet(tableName, ds.map { r => r.asInstanceOf[SchemaStringLong] })
        case q if q =:= typeOf[SchemaStringString] =>
          flinkTable.registerDataSet(tableName, ds.map { r => r.asInstanceOf[SchemaStringString] })
        case q if q =:= typeOf[SchemaStringStringType] =>
          flinkTable.registerDataSet(tableName, ds.map { r => r.asInstanceOf[SchemaStringStringType] })
        case q if q =:= typeOf[SchemaStringDouble] =>
          flinkTable.registerDataSet(tableName, ds.map { r => r.asInstanceOf[SchemaStringDouble] })
        case q if q =:= typeOf[SchemaStringStringLang] =>
          flinkTable.registerDataSet(tableName, ds.map { r => r.asInstanceOf[SchemaStringStringLang] })
      }
      config.getViewDefinitions.add(vd)
      (p, vd.getName)
    }
    }

    val basicTableInfoProvider = new BasicTableInfoProviderFlink(flinkTable)

    val rewriter = SparqlifyUtils.createDefaultSparqlSqlStringRewriter(basicTableInfoProvider, null, config, typeSerializer, sqlEscaper)
    rewriter
  }

}