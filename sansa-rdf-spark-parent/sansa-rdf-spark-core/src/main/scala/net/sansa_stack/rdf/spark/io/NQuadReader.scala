package net.sansa_stack.rdf.spark.io

import java.io.ByteArrayInputStream
import java.net.URI

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.jena.sparql.core.Quad
import org.apache.jena.riot.{ Lang, RDFDataMgr }

/**
 * N-Quad reader
 * An N-Quad rdf data format reader.
 */
object NQuadReader {

  /**
   * Loads an N-Quads file into an RDD.
   *
   * @param session the Spark session
   * @param path    the path to the N-Quads file(s)
   * @return the RDD of quads
   */
  def load(session: SparkSession, path: URI): RDD[Quad] = {
    load(session, path.toString)
  }

  /**
   * Loads an N-Quads  file into an RDD.
   *
   * @param session the Spark session
   * @param path    the path to the N-Quads file(s)
   * @return the RDD of quads
   */
  def load(session: SparkSession, path: String): RDD[Quad] = {
    session.sparkContext.textFile(path)
      .filter(line => !line.trim().isEmpty & !line.startsWith("#"))
      .map(line =>
        RDFDataMgr.createIteratorQuads(new ByteArrayInputStream(line.getBytes), Lang.NQUADS, null).next())
  }

}