package net.sansa_stack.rdf.spark

import org.apache.jena.graph.Triple
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

package object mappings {

  implicit class R2rml(spark: SparkSession) extends Serializable {
    /**
      * Generating:
      *  1. The statements to create SQL tables;
      *  2. The commands to insert the triples into them;
      *  3. The associated R2RML mappings.
      */

    def loadIntoTable(triples: RDD[Triple]): Iterable[String] = {
      R2RMLMappings.loadSQLTables(triples, spark)
    }

    def insertIntoTable(triples: RDD[Triple]): RDD[String] = {
      R2RMLMappings.insertSQLTables(triples, spark)
    }

    def obtainMappings(triples: RDD[Triple]): Iterable[String] = {
      R2RMLMappings.generateR2RMLMappings(triples, spark)
    }

  }
}
