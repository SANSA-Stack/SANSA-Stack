package net.sansa_stack.inference.spark.data.loader.sql

import com.typesafe.config.{Config, ConfigFactory}
import net.sansa_stack.inference.utils.Logging
import org.apache.jena.riot.Lang
import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter}

package object rdf {

  /**
    * Adds methods, `ntriples` and `turtle`, to DataFrameWriter that allows to write N-Triples and Turtle files using
    * the `DataFrameWriter`
    */
  implicit class RDFDataFrameWriter[T](writer: DataFrameWriter[T]) {
    def rdf: String => Unit = writer.format("ntriples").save
    def ntriples: String => Unit = writer.format("ntriples").save
  }

  /**
    * Adds methods, `rdf`, `ntriples` and `turtle`, to DataFrameReader that allows to read N-Triples and Turtle files using
    * the `DataFrameReader`
    */
  implicit class RDFDataFrameReader(reader: DataFrameReader) extends Logging {
    @transient lazy val conf: Config = ConfigFactory.load("rdf_loader")
    /**
      * Load RDF data into a `DataFrame`. Currently, only N-Triples and Turtle syntax are supported
      * @param lang the RDF language (Turtle or N-Triples)
      * @return a `DataFrame[(String, String, String)]`
      */
    def rdf(lang: Lang): String => DataFrame = lang match {
      case i if lang == Lang.NTRIPLES => ntriples
      case j if lang == Lang.TURTLE => turtle
      case _ => throw new IllegalArgumentException(s"${lang.getLabel} syntax not supported yet!")
    }
    /**
      * Load RDF data in N-Triples syntax into a `DataFrame` with columns `s`, `p`, and `o`.
      * @return a `DataFrame[(String, String, String)]`
      */
    def ntriples: String => DataFrame = {
      log.debug(s"Parsing N-Triples with ${conf.getString("rdf.ntriples.parser")} ...")
      reader.format("ntriples").load
    }
    /**
      * Load RDF data in Turtle syntax into a `DataFrame` with columns `s`, `p`, and `o`.
      * @return a `DataFrame[(String, String, String)]`
      */
    def turtle: String => DataFrame = reader.format("turtle").load
  }
}