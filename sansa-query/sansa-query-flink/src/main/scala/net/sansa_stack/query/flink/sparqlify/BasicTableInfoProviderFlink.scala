package net.sansa_stack.query.flink.sparqlify

import java.util.Collections

import org.aksw.sparqlify.config.v0_2.bridge.{BasicTableInfo, BasicTableInfoProvider}
import org.apache.flink.table.api.bridge.scala.BatchTableEnvironment

import scala.jdk.CollectionConverters._

/**
 * Created by Simon Bin on 12/06/17.
 */
class BasicTableInfoProviderFlink(flinkTable: BatchTableEnvironment)
  extends BasicTableInfoProvider {
  override def getBasicTableInfo(queryString: String): BasicTableInfo = {
    val table = flinkTable.sqlQuery(queryString)
    val schema = table.getSchema
    val types = schema.getFieldDataTypes
    val names = schema.getFieldNames
    val map = (0 until types.length).map { i => {
      (names(i), types(i).toString.toLowerCase.capitalize)
    }
    } toMap

    println(map)
    val nullableColumns = Collections.emptySet[String] // Set[String]().asJava
    new BasicTableInfo(map.asJava, nullableColumns)
  }
}
