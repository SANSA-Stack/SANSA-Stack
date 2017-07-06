package net.sansa_stack.inference.spark.data.loader.rdd

import net.sansa_stack.inference.utils.NTriplesStringToJenaTriple
import org.apache.jena.graph.Triple
import org.apache.jena.riot.Lang
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrameWriter

package object rdf {

  /**
    * Adds methods, `ntriples` and `turtle`, to SparkContext that allows to write N-Triples and Turtle files
    */
  implicit class RDFDataFrameWriter[T](writer: DataFrameWriter[T]) {
    def ntriples: String => Unit = writer.format("ntriples").save
  }

  /**
    * Adds methods, `rdf`, `ntriples` and `turtle`, to SparkContext that allows to read N-Triples and Turtle files
    */
  implicit class RDFReader(sc: SparkContext) {
    /**
      * Load RDF data into an `RDD[Triple]`. Currently, only N-Triples and Turtle syntax are supported
      * @param lang the RDF language (Turtle or N-Triples)
      * @return the RDD
      */
    def rdf(lang: Lang): String => RDD[Triple] = lang match {
      case i if lang == Lang.NTRIPLES => ntriples
      case j if lang == Lang.TURTLE => turtle
      case _ => throw new IllegalArgumentException(s"${lang.getLabel} syntax not supported yet!")
    }

    /**
      * Load RDF data in N-Triples syntax into an `RDD[Triple]`
      * @return the RDD
      */
    def ntriples: String => RDD[Triple] = path =>
      sc
      .textFile(path, 4) // read the text file
      .map(new NTriplesStringToJenaTriple())

    /**
      * Load RDF data in Turtle syntax into an `RDD[Triple]`
      * @return the RDD
      */
    def turtle: String => RDD[Triple] = path =>
      sc
      .textFile(path, 4) // read the text file
      .map(new NTriplesStringToJenaTriple())
  }
}