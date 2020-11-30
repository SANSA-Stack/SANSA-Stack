package net.sansa_stack.rdf.spark.io

import org.apache.jena.riot.Lang
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, PrunedScan, TableScan}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
 * @author Lorenz Buehmann
 */
class RDFRelation (location: String, lang: Lang, userSchema: StructType)
                  (@transient val sqlContext: SQLContext)
  extends BaseRelation
    with TableScan
    with PrunedScan
    with Serializable {

  override def schema: StructType = {
    if (this.userSchema != null) {
      this.userSchema
    }
    else {
      StructType(
        Seq(
          StructField("s", StringType, nullable = false),
          StructField("p", StringType, nullable = false),
          StructField("o", StringType, nullable = false)
        ))
    }
  }

  override def buildScan(): RDD[Row] = {
    // parse the RDF file into an RDD[Triple]
    val rdd = sqlContext.sparkSession.rdf(lang)(location)

    // map to Row
    val rows = rdd.map(toRow)

    rows
  }

  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    // parse the RDF file into an RDD[Triple]
    val rdd = sqlContext.sparkSession.rdf(lang)(location)

    // map to Row
    val rows = rdd.map { t =>
      val nodes = for (col <- requiredColumns) yield {
        col match {
          case "s" => t.getSubject
          case "p" => t.getPredicate
          case "o" => t.getObject
          case other => throw new RuntimeException(s"unsupported column name '$other''")
        }
      }

      toRow(nodes)
    }

    rows
  }
}
