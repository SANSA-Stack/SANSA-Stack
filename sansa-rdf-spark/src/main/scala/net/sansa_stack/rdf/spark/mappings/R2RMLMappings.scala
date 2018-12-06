package net.sansa_stack.rdf.spark.mappings

import java.io.{ File, StringWriter }
import java.net.URI

import scala.collection.mutable

import net.sansa_stack.rdf.common.partition.core.RdfPartitionerDefault
import net.sansa_stack.rdf.spark.io._
import net.sansa_stack.rdf.spark.model._
import net.sansa_stack.rdf.spark.partition._
import org.apache.jena.graph.{ Node, Triple }
import org.apache.jena.riot.Lang
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * Provide a set of functions to deal with SQL tables and R2RML mappings.
 *
 * @author dgraux
 */
object R2RMLMappings extends Serializable {

  @transient val spark: SparkSession = SparkSession.builder().getOrCreate()

  /**
   * Return instructions to create SQL tables
   * @param Path to the triple file
   * @param Current SparkSession
   */
  def loadSQLTables(triples: RDD[Triple], spark: SparkSession): Iterable[String] = {
    val partitions = triples.partitionGraph()
    // Generating commands to create SQL tables.
    val schemaSQLTable = partitions.map {
      case (p, rdd) =>
        /* Since data can be wrongly formatted,
       * just skip those details for the moment…
      p.layout.schema.toString match {
        case "net.sansa_stack.rdf.common.partition.schema.SchemaStringDouble" => "CREATE TABLE " + p.predicate.replaceAll("[^A-Za-z0-9]", "-") + " (s STRING, o FLOAT);" // No 'double' in SQL
        case "net.sansa_stack.rdf.common.partition.schema.SchemaStringLong" => "CREATE TABLE " + p.predicate.replaceAll("[^A-Za-z0-9]", "-") + " (s STRING, o BIGINT);" // No 'long' in SQL
        case "net.sansa_stack.rdf.common.partition.schema.SchemaStringString" => "CREATE TABLE " + p.predicate.replaceAll("[^A-Za-z0-9]", "-") + " (s STRING, o STRING);"
        case "net.sansa_stack.rdf.common.partition.schema.SchemaStringStringLang" => "CREATE TABLE " + p.predicate.replaceAll("[^A-Za-z0-9]", "-") + " (s STRING, o STRING, l STRING);"
        case "net.sansa_stack.rdf.common.partition.schema.SchemaStringStringType" => "CREATE TABLE " + p.predicate.replaceAll("[^A-Za-z0-9]", "-") + " (s STRING, o STRING, t STRING);"
        case _ => println("error: schema unknown")
      }
      */
        "CREATE TABLE IF NOT EXISTS " + p.predicate.replaceAll("[^A-Za-z0-9]", "_") + " (s STRING, o STRING, l STRING)"
    }
    schemaSQLTable
  }

  /**
   * Return instructions to fill SQL tables
   * @param Path to the triple file
   * @param Current SparkSession
   */
  def insertSQLTables(triples: RDD[Triple], spark: SparkSession): RDD[String] = {
    val insertSQL = triples.map {      // .getTriples.map {
      case t =>
        var tablename = t.getPredicate.toString.replaceAll("[^A-Za-z0-9]", "_");
        var subj = RdfPartitionerDefault.getUriOrBNodeString(t.getSubject);
        var complement = if (t.getObject.isLiteral) {
          if (t.getObject.getLiteralLanguage != "") {
            "\"" + t.getObject.getLiteralLexicalForm + "\" , \"" + t.getObject.getLiteralLanguage + "\""
          } else {
            "\"" + t.getObject.getLiteralLexicalForm + "\" , \"" + t.getObject.getLiteralDatatypeURI + "\""
          }
        } else {
          "\"" + RdfPartitionerDefault.getUriOrBNodeString(t.getObject) + "\" , \"\""
        };
        var statement = "INSERT INTO " + tablename + " VALUES ( \"" + subj + "\" , " + complement + " ) ";
        statement;
    }
    insertSQL
  }

  /**
   * Return R2RML mappings
   * @param Path to the triple file
   * @param Current SparkSession
   */
  def generateR2RMLMappings(triples: RDD[Triple], spark: SparkSession): Iterable[String] = {
    var mappingNumber = 1
    val partitions = triples.partitionGraph()
    val r2rmlMappings = partitions.map {
      case (p, rdd) =>
        p.layout.schema.toString match {
          case _ =>
            var mapping = "<TriplesMap" + mappingNumber.toString + "> a rr:TriplesMapClass ; "
            mapping += "rr:logicalTable [rr:SQLQuery \"\"\"SELECT s , o , l FROM " + p.predicate.replaceAll("[^A-Za-z0-9]", "_") + " \"\"\"] ; "
            mapping += "rr:subjectMap [ rr:column \"s\"] ; "
            mapping += "rr:predicateObjectMap [ rr:predicate " + p.predicate + " ; rr:objectMap [ rr:column \"o\"] ] . "
            mappingNumber += 1;
            mapping;
        }
      // Since data can be bad-formatted we still have to be a bit prudent.
    }
    r2rmlMappings
  }

}
