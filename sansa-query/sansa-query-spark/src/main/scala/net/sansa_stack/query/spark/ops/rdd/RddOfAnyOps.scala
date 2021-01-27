package net.sansa_stack.query.spark.ops.rdd

import java.util.stream.Collector

import org.aksw.jena_sparql_api.mapper.{Accumulator, Aggregators}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Operations that can be applied to any RDD
 *
 */
object RddOfAnyOps {

  /**
   * Aggregate over an RDD using a Java Collector. The collector and its attributes
   * must be serializable.
   *
   * IMPORTANT The collector must be serializable - Standard Java collectors are not!
   * Our {@link org.aksw.commons.collector.core.AggBuilder} framework however
   * produces such collectors that can be used both in java8 streams and spark.
   *
   * @param rdd
   * @param collector
   * @return
   */
  def aggregateUsingJavaCollector[T: ClassTag, A: ClassTag, R: ClassTag](rdd: RDD[_ <: T], collector: Collector[_ >: T, A, R]): R = {
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
