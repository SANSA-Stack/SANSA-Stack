package net.sansa_stack.rdf.spark.streaming

import java.io.ByteArrayInputStream

import org.apache.jena.graph.Triple
import org.apache.jena.riot.{ Lang, RDFDataMgr }
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

/**
 * @author Gezim Sejdiu
 */
class KafkaReader extends StreamReader {

  /**
   * Load a stream of triples.
   *
   * @param ssc a Spark Streaming context
   * @return a stream of Triples
   */
  override def load(ssc: StreamingContext): DStream[Triple] = {
    val brokers = "kafka brokers"
    val topics = "topics name"

    assert(!(brokers + topics).contains("unset"), "brokers or topics should be set")

    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val topicMap = topics.split(",").toSet

    KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent,
      Subscribe[String, String](topicMap, kafkaParams))
      .map(line => RDFDataMgr.createIteratorTriples(new ByteArrayInputStream(line.value().getBytes), Lang.NTRIPLES, null).next())

  }

}
