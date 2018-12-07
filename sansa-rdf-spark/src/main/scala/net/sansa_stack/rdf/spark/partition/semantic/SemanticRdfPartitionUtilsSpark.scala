package net.sansa_stack.rdf.spark.partition.semantic

import java.util.concurrent.TimeUnit

import net.sansa_stack.rdf.common.partition.utils.Symbols
import org.apache.jena.graph.Triple
import org.apache.spark.rdd._

/**
 * Semantic partition of and RDF graph
 */
object SemanticRdfPartitionUtilsSpark extends Serializable {

  /**
   * Apply semantic partitioning for a given RDF graph
   * @param triples an RDD of triples.
   * @return semantic partition data.
   */
  def partitionGraph(triples: RDD[Triple]): RDD[String] = {
    val symbol = Symbols.symbol
    // partition the data
    val partitionedData = triples
      .distinct
      .filter(_.getSubject.getURI.nonEmpty) // ignore SUBJECT with empty URI
      .map(triple => {
        var filteredPredicate: Any = triple.getPredicate
        var filteredObject: Any = triple.getObject

        // set: PREDICATE
        if (triple.getPredicate.isURI && triple.getPredicate.getURI.contains(symbol("hash"))) {
          filteredPredicate = triple.getPredicate.getURI.split(symbol("hash"))(1)

          // set: OBJECT where PREDICATE is a "type"
          if (filteredPredicate.equals("type") && triple.getObject.isURI && triple.getObject.getURI.contains(symbol("hash"))) {
            filteredObject = symbol("colon") + triple.getObject.getURI.split(symbol("hash"))(1)
          } else if (triple.getObject.isURI) {
            filteredObject = symbol("less-than") + triple.getObject + symbol("greater-than")
          } else {
            filteredObject = triple.getObject
          }

          // add colon at the start
          filteredPredicate = symbol("colon") + filteredPredicate
        } else {
          // PREDICATE
          if (triple.getPredicate.isURI) {
            filteredPredicate = symbol("less-than") + triple.getPredicate + symbol("greater-than")
          }

          // OBJECT
          if (triple.getObject.isURI) {
            filteredObject = symbol("less-than") + triple.getObject + symbol("greater-than")
          }
        }

        // (K,V) pair
        (
          symbol("less-than") + triple.getSubject + symbol("greater-than"),
          filteredPredicate + symbol("space") + filteredObject + symbol("space"))
      })
      .reduceByKey(_ + _) // group based on key
      .sortBy(x => x._1) // sort by key
      .map(x => x._1 + symbol("space") + x._2) // output format

    partitionedData
  }

}
