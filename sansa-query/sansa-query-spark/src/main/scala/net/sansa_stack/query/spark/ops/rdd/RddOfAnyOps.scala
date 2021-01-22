package net.sansa_stack.query.spark.ops.rdd

import java.util.stream.Collector

import org.aksw.jena_sparql_api.mapper.{Accumulator, Aggregators}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag


object RddOfAnyOps {

  /**
   * TODO Remove this method; the Java collector version should be sufficient as our aggregators support agg.toCollector()
   *
   * Aggregation based on a supplier of our Accumulator interface
   * In constrast to {@link #aggregateUsingJavaCollector} this method only requires the
   * accumulator instances to be serializable (and the function that supplies them).
   */
//  def aggregate[T: ClassTag, C <: java.util.Collection[T]](rdd: RDD[T], aggregator: () => Accumulator[T, C]): C = {
//    var unfinishedResult = rdd
//      .mapPartitions(it => {
//        val result = aggregator()
//        it.foreach(result.accumulate(_))
//        Iterator.single(result)
//      })
//      .reduce((a, b) => Aggregators.mergeAccumulators[T, T, C](a, b, (x: Accumulator[T, C]) => x, (y: T) => y))
//
//    val finishedResult = unfinishedResult.getValue
//    finishedResult
//  }


  /**
   * Aggregate over an RDD using a Java Collector. The collector and its attributes
   * must be serializable.
   * FIXME Probably the requirement for serialization makes this method rather useless
   *
   * @param rdd
   * @param collector
   * @return
   */
  def aggregateUsingJavaCollector[T: ClassTag, A: ClassTag, C: ClassTag](rdd: RDD[T], collector: Collector[T, A, C]): C = {
    var unfinishedResult = rdd
      .mapPartitions(it => {
        val result = collector.supplier.get
        val accumulator = collector.accumulator
        it.foreach(accumulator.accept(result, _))
        Iterator.single(result)
      })
      .reduce(collector.combiner.apply)

    val finishedResult = collector.finisher.apply(unfinishedResult)
    finishedResult
  }

}
