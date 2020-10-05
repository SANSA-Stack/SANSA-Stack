package net.sansa_stack.query.flink.sparqlify

import scala.reflect.runtime.universe.typeOf

import com.typesafe.scalalogging.StrictLogging
import org.aksw.obda.domain.impl.LogicalTableTableName
import org.aksw.sparqlify.config.syntax.Config
import org.aksw.sparqlify.core.algorithms.{CandidateViewSelectorSparqlify, ViewDefinitionNormalizerImpl}
import org.aksw.sparqlify.core.interfaces.SparqlSqlStringRewriter
import org.aksw.sparqlify.core.sql.common.serialization.SqlEscaperBacktick
import org.aksw.sparqlify.util.{SparqlifyCoreInit, SparqlifyUtils, SqlBackendConfig}
import org.aksw.sparqlify.validation.LoggerCount
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}
import org.apache.flink.table.api.bridge.scala.BatchTableEnvironment

import net.sansa_stack.rdf.common.partition.core.RdfPartitionDefault
import net.sansa_stack.rdf.common.partition.model.sparqlify.SparqlifyUtils2
import net.sansa_stack.rdf.common.partition.schema._

object SparqlifyUtils3
  extends StrictLogging {

  def createSparqlSqlRewriter(flinkEnv: ExecutionEnvironment, flinkTable: BatchTableEnvironment,
                              partitions: Map[RdfPartitionDefault, DataSet[_ <: Product]]): SparqlSqlStringRewriter = {
    val config = new Config()
    val loggerCount = new LoggerCount(logger.underlying)

    val backendConfig = new SqlBackendConfig(new DatatypeToStringFlink(), new SqlEscaperBacktick())
    val sqlEscaper = backendConfig.getSqlEscaper
    val typeSerializer = backendConfig.getTypeSerializer
    val sqlFunctionMapping = SparqlifyCoreInit.loadSqlFunctionDefinitions("functions-spark.xml")

    val ers = SparqlifyUtils.createDefaultExprRewriteSystem()
    val mappingOps = SparqlifyUtils.createDefaultMappingOps(ers)

    val candidateViewSelector = new CandidateViewSelectorSparqlify(mappingOps, new ViewDefinitionNormalizerImpl());

    partitions.map {
      case (p, ds: DataSet[Product]) =>
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
            var fn = (r: Product) => r.asInstanceOf[SchemaStringLong]
            flinkTable.createTemporaryView(tableName, ds.map (fn))
          case q if q =:= typeOf[SchemaStringString] =>
            var fn = (r: Product) => r.asInstanceOf[SchemaStringString]
            flinkTable.createTemporaryView(tableName, ds.map (fn))
          case q if q =:= typeOf[SchemaStringStringType] =>
            var fn = (r: Product) => r.asInstanceOf[SchemaStringStringType]
            flinkTable.createTemporaryView(tableName, ds.map (fn))
          case q if q =:= typeOf[SchemaStringDouble] =>
            var fn = (r: Product) => r.asInstanceOf[SchemaStringDouble]
            flinkTable.createTemporaryView(tableName, ds.map (fn))
          case q if q =:= typeOf[SchemaStringStringLang] =>
            var fn = (r: Product) => r.asInstanceOf[SchemaStringStringLang]
            flinkTable.createTemporaryView(tableName, ds.map (fn))
          case q if q =:= typeOf[SchemaStringDate] =>
            var fn = (r: Product) => r.asInstanceOf[SchemaStringDate]
            flinkTable.createTemporaryView(tableName, ds.map (fn))
          case _ =>
            throw new RuntimeException("Unhandled schema type: " + q)
        }


/*
        val fn: Product => Product = q match {
          case q if q =:= typeOf[SchemaStringLong] =>
            (r: Product) => r.asInstanceOf[SchemaStringLong]
          case q if q =:= typeOf[SchemaStringString] =>
            (r: Product) => r.asInstanceOf[SchemaStringString]
          case q if q =:= typeOf[SchemaStringStringType] =>
            (r: Product) => r.asInstanceOf[SchemaStringStringType]
          case q if q =:= typeOf[SchemaStringDouble] =>
            (r: Product) => r.asInstanceOf[SchemaStringDouble]
          case q if q =:= typeOf[SchemaStringStringLang] =>
            (r: Product) => r.asInstanceOf[SchemaStringStringLang]
          case q if q =:= typeOf[SchemaStringDate] =>
            (r: Product) => r.asInstanceOf[SchemaStringDate]
          case _ =>
            throw new RuntimeException("Unhandled schema type: " + q)
        }
        import org.apache.flink.api.scala._
        // implicit val typeInfo = TypeInformation.of(classOf[(Product, Product)])
        flinkTable.registerDataSet(tableName, ds.map (fn))
*/

//        val newDS = classOf[DataSet[Product]].getMethod("map", classOf[MapFunction[Product, _ <: Product]]).invoke(ds, fn).asInstanceOf[DataSet[Product]]
//        flinkTable.registerDataSet(tableName, newDS)

        config.getViewDefinitions.add(vd)
        (p, vd.getName)

    }

    val basicTableInfoProvider = new BasicTableInfoProviderFlink(flinkTable)

    val rewriter = SparqlifyUtils.createDefaultSparqlSqlStringRewriter(basicTableInfoProvider, null, config, typeSerializer, sqlEscaper, sqlFunctionMapping)
    rewriter
  }

}
