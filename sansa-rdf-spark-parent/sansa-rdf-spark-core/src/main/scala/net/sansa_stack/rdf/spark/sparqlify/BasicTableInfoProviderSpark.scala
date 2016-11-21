package net.sansa_stack.rdf.spark.sparqlify

import scala.collection.JavaConverters._

import org.aksw.sparqlify.config.v0_2.bridge.BasicTableInfo
import org.aksw.sparqlify.config.v0_2.bridge.BasicTableInfoProvider
import org.apache.spark.sql.SparkSession

import net.sansa_stack.utils.spark.DatasetUtils
import java.util.Collections


class BasicTableInfoProviderSpark(val sparkSession: SparkSession)
  extends BasicTableInfoProvider
{
  def getBasicTableInfo(queryStr: String) : BasicTableInfo = {
    val dataFrame = sparkSession.sql(queryStr)
    val flatSchema = DatasetUtils.flattenSchema(dataFrame.schema)

    // TODO Handle nullable columns

    val rawTypeMap = flatSchema.asJava
    val nullableColumns = Collections.emptySet[String]//Set[String]().asJava

    new BasicTableInfo(rawTypeMap, nullableColumns)
  }
}