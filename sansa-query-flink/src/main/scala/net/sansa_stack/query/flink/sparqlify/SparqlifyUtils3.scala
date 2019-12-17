package net.sansa_stack.query.flink.sparqlify

import scala.reflect.runtime.universe.typeOf
import com.typesafe.scalalogging.StrictLogging
import net.sansa_stack.rdf.common.partition.core.RdfPartitionDefault
import net.sansa_stack.rdf.common.partition.model.sparqlify.SparqlifyUtils2
import net.sansa_stack.rdf.common.partition.schema._
import org.aksw.obda.domain.impl.LogicalTableTableName
import org.aksw.sparqlify.config.syntax.Config
import org.aksw.sparqlify.core.algorithms.{CandidateViewSelectorSparqlify, ViewDefinitionNormalizerImpl}
import org.aksw.sparqlify.core.interfaces.SparqlSqlStringRewriter
import org.aksw.sparqlify.core.sql.common.serialization.{SqlEscaperBacktick, SqlEscaperBase, SqlEscaperDoubleQuote}
import org.aksw.sparqlify.util.{SparqlifyCoreInit, SparqlifyUtils, SqlBackendConfig}
import org.aksw.sparqlify.validation.LoggerCount
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}
import org.apache.flink.table.api.scala.BatchTableEnvironment

object SparqlifyUtils3
  extends StrictLogging {
  def createSparqlSqlRewriter(flinkEnv: ExecutionEnvironment, flinkTable: BatchTableEnvironment, partitions: Map[RdfPartitionDefault, DataSet[_ <: Product]]): SparqlSqlStringRewriter = {
    val config = new Config()
    val loggerCount = new LoggerCount(logger.underlying)

    val backendConfig = new SqlBackendConfig(new DatatypeToStringFlink(), new SqlEscaperBacktick())
    val sqlEscaper = backendConfig.getSqlEscaper()
    val typeSerializer = backendConfig.getTypeSerializer()
    val sqlFunctionMapping = SparqlifyCoreInit.loadSqlFunctionDefinitions("functions-spark.xml")

    val ers = SparqlifyUtils.createDefaultExprRewriteSystem()
    val mappingOps = SparqlifyUtils.createDefaultMappingOps(ers)

    val candidateViewSelector = new CandidateViewSelectorSparqlify(mappingOps, new ViewDefinitionNormalizerImpl());

    partitions.map {
      case (p, ds) =>
        logger.debug("Processing RdfPartition: " + p)
        val vd = SparqlifyUtils2.createViewDefinition(p)
        logger.debug("Created view definition: " + vd)
        val tableName = vd.getLogicalTable match {
          case o: LogicalTableTableName => o.getTableName
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
          case q if q =:= typeOf[SchemaStringDate] =>
            flinkTable.registerDataSet(tableName, ds.map { r => r.asInstanceOf[SchemaStringDate] })
          case _ =>
            throw new RuntimeException("Unhandled schema type: " + q)
        }
        config.getViewDefinitions.add(vd)
        (p, vd.getName)

    }

    val basicTableInfoProvider = new BasicTableInfoProviderFlink(flinkTable)

    val rewriter = SparqlifyUtils.createDefaultSparqlSqlStringRewriter(basicTableInfoProvider, null, config, typeSerializer, sqlEscaper, sqlFunctionMapping)
    rewriter
  }

}
