package net.sansa_stack.rdf.spark.streaming

import java.io.ByteArrayInputStream

import scala.collection.mutable.ArrayBuffer

import org.apache.jena.graph.Triple
import org.apache.jena.riot.{ Lang, RDFDataMgr }
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{ Duration, StreamingContext, Time }
import org.apache.spark.streaming.dstream.{ DStream, InputDStream }

/**
 * @author Gezim Sejdiu
 */
class FileReader(path: String) extends StreamReader {

  /**
   * Load a stream of triples.
   *
   * @param ssc a Spark Streaming context
   * @return a stream of Triples
   */
  override def load(ssc: StreamingContext): DStream[Triple] = {

    val slideDurationOption = 1000
    val chunkdSizeOption = 1000

    new InputDStream[Triple](ssc) {
      override def start(): Unit = {}

      override def stop(): Unit = {}

      override def compute(validTime: Time): Option[RDD[Triple]] = {
        val arr = new ArrayBuffer[Triple]();

        val it = ssc.textFileStream(path).map(line =>
          RDFDataMgr.createIteratorTriples(new ByteArrayInputStream(line.getBytes), Lang.NTRIPLES, null).next())

        it.foreachRDD(arr ++= _.collect())

        Some(ssc.sparkContext.parallelize(arr))
      }

      override def slideDuration = {
        new Duration(slideDurationOption)
      }
    }

  }
}
