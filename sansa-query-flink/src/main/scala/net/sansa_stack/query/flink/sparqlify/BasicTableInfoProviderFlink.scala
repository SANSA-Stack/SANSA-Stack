package net.sansa_stack.query.flink.sparqlify

import java.util.Collections

import collection.JavaConverters._
import org.aksw.sparqlify.config.v0_2.bridge.{ BasicTableInfo, BasicTableInfoProvider }
import org.apache.flink.table.api.scala.BatchTableEnvironment

/**
 * Created by Simon Bin on 12/06/17.
 */
class BasicTableInfoProviderFlink(flinkTable: BatchTableEnvironment)
  extends BasicTableInfoProvider {
  override def getBasicTableInfo(queryString: String): BasicTableInfo = {
    val table = flinkTable.sql(queryString)
    val schema = table.getSchema
    val types = schema.getTypes
    val names = schema.getColumnNames
    val map = (0 until types.length).map { i =>
      (names(i), types(i).toString)
    } toMap

    println(map)
    val nullableColumns = Collections.emptySet[String] // Set[String]().asJava
    new BasicTableInfo(map.asJava, nullableColumns)
  }
}
