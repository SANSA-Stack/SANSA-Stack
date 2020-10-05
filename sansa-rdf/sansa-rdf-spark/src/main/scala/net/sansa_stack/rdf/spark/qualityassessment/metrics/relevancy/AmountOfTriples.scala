package net.sansa_stack.rdf.spark.qualityassessment.metrics.relevancy

import org.apache.jena.graph.{ Node, Triple }
import org.apache.spark.rdd.RDD

/**
 * @author Gezim Sejdiu
 */
object AmountOfTriples {

  /*
   * 1   : > 1,000,000,000                 triples
   * 0.75:      10,000,000 - 1,000,000,000 triples
   * 0.5 :         500,000 -    10,000,000 triples
   * 0.25:          10,000 -       500,000 triples
   * 0   :                        < 10,000 triples
   */
  def assessAmountOfTriples(dataset: RDD[Triple]): Double = {

    val high = 1000000000
    val mediumHigh = 10000000
    val mediumLow = 500000
    val low = 10000

    val triples = dataset.count().toDouble

    val predicates = dataset.map(_.getPredicate).distinct().count().toDouble

    val value = if (triples >= high) 1
    else if (triples < high && triples >= mediumHigh) 0.75;
    else if (triples < mediumHigh && triples >= mediumLow) 0.5;
    else if (triples < mediumLow && triples >= low) 0.25;
    else 0

    value
  }
}
