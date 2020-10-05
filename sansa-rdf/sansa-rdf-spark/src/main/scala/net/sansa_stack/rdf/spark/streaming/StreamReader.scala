package net.sansa_stack.rdf.spark.streaming

import org.apache.jena.graph.Triple
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

/**
 * Abstract class for loading a DStream of Triples.
 *
 * @author Gezim Sejdiu
 */
abstract class StreamReader {

  /**
   * Load a stream of triples.
   *
   * @param ssc a Spark Streaming context
   * @return a stream of Triples
   */
  def load(ssc: StreamingContext): DStream[Triple]

}
