package net.sansa_stack.rdf.spark

import org.apache.jena.graph.{Node, Triple}
import org.apache.spark.rdd.RDD
import org.apache.jena.graph.NodeFactory

object GraphRDDUtils extends Serializable {

  implicit def partitionGraphByPredicates(graphRdd : RDD[Triple]) : Map[Node, RDD[(Node, Node)]] = {
    val map = Map(partitionGraphByPredicatesArray(graphRdd) :_*)
    map
  }

//  def equalsPredicate(a : Triple, b : Node) : Boolean = {
//    a.getPredicate == b
//  }

  implicit def partitionGraphByPredicatesArray(graphRdd : RDD[Triple]) : Array[(Node, RDD[(Node, Node)])] = {
    val predicates = graphRdd.map(_.getPredicate).distinct.map( _.getURI).collect

    val array = predicates map { p => (
          NodeFactory.createURI(p),
          graphRdd
            .filter(_.getPredicate.getURI == p)
            .map(t => t.getSubject -> t.getObject)
            .persist())
          }
    array
//
//    val predicates = graphRdd.map(_.getPredicate).distinct.collect
//
//    val array = predicates map { p => (
//          p,
//          graphRdd
//            .filter(equalsPredicate(_, p))
//            .map(t => t.getSubject -> t.getObject)
//            .persist())
//          }
//    array
  }
}