package net.sansa_stack.rdf.spark.streaming

import org.apache.spark.streaming.dstream.DStream

/**
 * Abstract class that save the output from a given DStream.
 *
 * @author Gezim Sejdiu
 */
abstract class StreamWriter {
  /**
   * Save the output.
   *
   * @param stream a DStream of Strings
   */
  def save(stream: DStream[String]): Unit
}
