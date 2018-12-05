package net.sansa_stack.rdf.spark

import net.sansa_stack.rdf.spark.mappings.R2RMLMappings
import net.sansa_stack.rdf.spark.utils.Logging
import org.apache.jena.graph.{ Node, Triple }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

package object mappings {

  implicit class R2rml(spark: SparkSession) extends Serializable {
    /**
      * Loading (in memory the triples into tables.
      * Generating the associated r2rml mappings.
      */

    // val spark = SparkSession.builder().getOrCreate()

    def loadIntoTable(tripleFilePath: String): Iterable[String] = {
      R2RMLMappings.loadSQLTables(tripleFilePath, spark)
    }

    def insertIntoTable(tripleFilePath: String): RDD[String] = {
      R2RMLMappings.insertSQLTables(tripleFilePath, spark)
    }

    def obtainMappings(tripleFilePath: String): Iterable[String] = {
      R2RMLMappings.generateR2RMLMappings(tripleFilePath, spark)
    }

  }
}
