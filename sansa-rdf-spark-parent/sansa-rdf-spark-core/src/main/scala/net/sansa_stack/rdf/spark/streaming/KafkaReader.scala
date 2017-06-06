package net.sansa_stack.rdf.spark.streaming

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.jena.graph.Triple
import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.serializer.StringDecoder
import org.apache.jena.riot.{Lang, RDFDataMgr}
import java.io.ByteArrayInputStream

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

    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicMap)
      .map(line =>RDFDataMgr.createIteratorTriples(new ByteArrayInputStream(line._2.getBytes), Lang.NTRIPLES, null).next())

  }

}