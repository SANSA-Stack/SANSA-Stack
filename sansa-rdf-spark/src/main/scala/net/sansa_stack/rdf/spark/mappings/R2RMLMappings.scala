package net.sansa_stack.rdf.spark.mappings

import java.io.{ File, StringWriter }
import java.net.URI

import scala.collection.mutable

import net.sansa_stack.rdf.common.partition.core.RdfPartitionerDefault
import net.sansa_stack.rdf.spark.io.NTripleReader
import net.sansa_stack.rdf.spark.model.rdd.TripleOps
import net.sansa_stack.rdf.spark.partition.core.RdfPartitionUtilsSpark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


/**
 *
 *
 * @author dgraux
 */
object R2RMLMappings extends Serializable {

  @transient val spark: SparkSession = SparkSession.builder().getOrCreate()

  /**
   * Compute SQL tables
   * @param Path to the triple file
   */
  def loadSQLTables(tripleFile: String, spark: SparkSession): Iterable[String] = {
    // Reading the NTriple file from its path.
    val graphRdd = NTripleReader.load(spark, URI.create(tripleFile))
    val partitions = RdfPartitionUtilsSpark.partitionGraph(graphRdd)
    // Generating commands to create SQL tables.
    val schemaSQLTable = partitions.map{ case ( p, rdd) =>
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

  def insertSQLTables(tripleFile: String, spark: SparkSession): RDD[String] = {
    // Reading the NTriple file from its path.
    val graphRdd = NTripleReader.load(spark, URI.create(tripleFile))
    val partitions = RdfPartitionUtilsSpark.partitionGraph(graphRdd)
    val insertSQL = TripleOps.getTriples(graphRdd).map{ case t =>
      var tablename = t.getPredicate.toString.replaceAll("[^A-Za-z0-9]", "_") ;
      var subj = RdfPartitionerDefault.getUriOrBNodeString(t.getSubject);
      var complement = if (t.getObject.isLiteral) {
                         if ( t.getObject.getLiteralLanguage != "") {
                           "\"" + t.getObject.getLiteralLexicalForm + "\" , \"" + t.getObject.getLiteralLanguage + "\""
                         } else {
                           "\"" + t.getObject.getLiteralLexicalForm + "\" , \"" + t.getObject.getLiteralDatatypeURI + "\""
                         }
                       } else {
                          "\"" + RdfPartitionerDefault.getUriOrBNodeString(t.getObject) + "\" , \"\""
                       } ;
    var statement = "INSERT INTO " + tablename + " VALUES ( \"" +  subj + "\" , " + complement + " ) " ;
      statement ;
    }
    insertSQL
  }


  def generateR2RMLMappings(tripleFile: String, spark: SparkSession): Iterable[String] = {
    var mappingNumber = 1
    // Reading the NTriple file from its path.
    val graphRdd = NTripleReader.load(spark, URI.create(tripleFile))
    val partitions = RdfPartitionUtilsSpark.partitionGraph(graphRdd)
    val r2rmlMappings = partitions.map{ case ( p, rdd) =>
      p.layout.schema.toString match { case _ =>
        var mapping = "<TriplesMap" + mappingNumber.toString + "> a rr:TriplesMapClass ; "
        mapping += "rr:logicalTable [rr:SQLQuery \"\"\"SELECT s , o , l FROM " + p.predicate.replaceAll("[^A-Za-z0-9]", "_") + " \"\"\"] ; "
        mapping += "rr:subjectMap [ rr:column \"s\"] ; "
        mapping += "rr:predicateObjectMap [ rr:predicate " + p.predicate + " ; rr:objectMap [ rr:column \"o\"] ] . "
        mappingNumber+=1;
        mapping;
      }
      // Since data can be bad-formatted we still have to be a bit prudent.
    }
    r2rmlMappings
  }




    // schemaSQLTable.map{case nt => spark.sql(nt.toString)}
    // insertSQL.collect().map{case insert => spark.sql(insert.toString)}



}



