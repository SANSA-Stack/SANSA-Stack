package net.sansa_stack.rdf.flink.partition.semantic

import scala.reflect.ClassTag

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.DataSet
import org.apache.flink.streaming.api.scala._
import org.apache.jena.graph.Triple

/*
 * RdfPartition - semantic partition of and RDF graph
 * @symbol - list of symbols.
 * @nTroplesRDD - a DataSet of n-triples.
 * @return - semantic partition data.
 */
object RdfPartition extends Serializable {

  implicit def partitionGraph[T <: DataSet[Triple]: TypeInformation: ClassTag](
    symbol: Map[String, String],
    triples: DataSet[Triple]): DataSet[String] = {
    // partition the data
    val partitionedData = triples
      .distinct
      .filter(
        line => {
          // ignore SUBJECT with empty URI
          line.getSubject.getURI.nonEmpty
        }).map(line => {
          // SUBJECT, PREDICATE and OBJECT
          val getSubject = line.getSubject
          val getPredicate = line.getPredicate
          val getObject = line.getObject

          var filteredPredicate: Any = getPredicate
          var filteredObject: Any = ()

          // filter out PREDICATE
          if (getPredicate.isURI && getPredicate.getURI.contains(symbol("hash"))) {
            filteredPredicate = getPredicate.getURI.split(symbol("hash"))(1)

            // filter out OBJECT where PREDICATE is a "type"
            if (filteredPredicate.equals("type") && getObject.isURI && getObject.getURI.contains(symbol("hash"))) {
              filteredObject = symbol("colon") + getObject.getURI.split(symbol("hash"))(1)
            } else if (!getObject.isURI) {
              filteredObject = getObject
            } else {
              filteredObject = symbol("less-than") + getObject + symbol("greater-than")
            }
          }

          // (K,V) pair
          (
            symbol("less-than") + getSubject + symbol("greater-than"),
            symbol("colon") + filteredPredicate + symbol("space") + filteredObject + symbol("space"))
        }).groupBy(0).reduce { (v1, v2) => (v1._1 + v2._1, ((v1._2) ++ v2._2)) } // group based on key
      .sortPartition(0, Order.ASCENDING)
      .map(x => x._1 + symbol("space") + x._2) // output format

    partitionedData

  }

}
