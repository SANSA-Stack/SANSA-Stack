package net.sansa_stack.rdf.flink.partition.semantic

import net.sansa_stack.rdf.common.partition.utils.Symbols
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.DataSet
import org.apache.flink.streaming.api.scala._
import org.apache.jena.graph.Triple

import scala.reflect.ClassTag

/**
  * Semantic partition of and RDF graph
  *
  * @author Gezim Sejdiu
  */
object SemanticRdfPartitionUtilsFlink extends Serializable {

  /**
    * Apply semantic partitioning for a given RDF graph
    *
    * @param triples an DataSet of triples.
    * @return semantic partition data.
    */
  implicit def partitionGraph[T <: DataSet[Triple] : TypeInformation : ClassTag](
                                                                                  triples: DataSet[Triple]): DataSet[String] = {
    val symbol = Symbols.symbol
    // partition the data
    val partitionedData = triples
    // .distinct(_.hashCode())
      .filter(_.getSubject.getURI.nonEmpty)
      .map(triple => {

        var filteredPredicate: Any = triple.getPredicate
        var filteredObject: Any = ()

        // filter out PREDICATE
        if (triple.getPredicate.isURI && triple.getPredicate.getURI.contains(symbol("hash"))) {
          filteredPredicate = triple.getPredicate.getURI.split(symbol("hash"))(1)

          // filter out OBJECT where PREDICATE is a "type"
          if (filteredPredicate.equals("type") && triple.getObject.isURI && triple.getObject.getURI.contains(symbol("hash"))) {
            filteredObject = symbol("colon") + triple.getObject.getURI.split(symbol("hash"))(1)
          } else if (!triple.getObject.isURI) {
            filteredObject = triple.getObject
          } else {
            filteredObject = symbol("less-than") + triple.getObject + symbol("greater-than")
          }
        }

        // (K,V) pair
        (
          symbol("less-than") + triple.getSubject + symbol("greater-than"),
          symbol("colon") + filteredPredicate + symbol("space") + filteredObject + symbol("space"))
      }).groupBy(0).reduce { (v1, v2) => (v1._1 + v2._1, ((v1._2) ++ v2._2)) } // group based on key
      .sortPartition(0, Order.ASCENDING)
      .map(x => x._1 + symbol("space") + x._2) // output format

    partitionedData

  }

}
