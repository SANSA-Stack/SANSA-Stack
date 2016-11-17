package net.sansa_stack.rdf.spark

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.typeOf

import org.apache.jena.graph.Triple
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import net.sansa_stack.rdf.common.partition.core.RdfPartitionDefault
import net.sansa_stack.rdf.common.partition.core.RdfPartitionerDefault


object GraphRDDUtils extends Serializable {

  implicit def partitionGraphByPredicates(graphRdd : RDD[Triple]) : Map[RdfPartitionDefault, RDD[Row]] = {
    val map = Map(partitionGraphByPredicatesArray(graphRdd) :_*)
    map
  }

//  def equalsPredicate(a : Triple, b : Node) : Boolean = {
//    a.getPredicate == b
//  }

  implicit def partitionGraphByPredicatesArray(graphRdd : RDD[Triple]) : Array[(RdfPartitionDefault, RDD[Row])] = {
    //val predicates = graphRdd.map(_.getPredicate).distinct.map( _.getURI).collect
    val partitionKeys = graphRdd.map(RdfPartitionerDefault.fromTriple).distinct.collect

    // TODO Collect an RDD of distinct with entries of structure (predicate, datatype, language tag)
    val array = partitionKeys map { p => (
          p,
          graphRdd
            .filter(p.matches) //_.getPredicate.getURI == p)
            .map(t => Row(RdfPartitionerDefault.determineLayout(p).fromTriple(t).productIterator.toList :_* ))
            .persist())
          }
    array

  }
}
