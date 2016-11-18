package net.sansa_stack.rdf.spark.partition.core

import org.apache.jena.graph.Triple
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import net.sansa_stack.rdf.common.partition.core.RdfPartition
import net.sansa_stack.rdf.common.partition.core.RdfPartitioner
import net.sansa_stack.rdf.common.partition.core.RdfPartitionerDefault
import org.aksw.jena_sparql_api.utils.Vars

import scala.reflect.ClassTag
//import scala.reflect.runtime.universe.TypeTag



object RdfPartitionUtilsSpark extends Serializable {

  implicit def partitionGraphByPredicates[P <: RdfPartition : ClassTag](graphRdd : RDD[Triple], partitioner: RdfPartitioner[P] = RdfPartitionerDefault) : Map[P, RDD[Row]] = {
    val map = Map(partitionGraphByPredicatesArray(graphRdd, partitioner) :_*)
    map
  }

  implicit def partitionGraphByPredicatesArray[P <: RdfPartition : ClassTag](graphRdd: RDD[Triple], partitioner: RdfPartitioner[P] = RdfPartitionerDefault) : Array[(P, RDD[Row])] = {
    val partitions = graphRdd.map(x => partitioner.fromTriple(x)).distinct.collect

    // TODO Collect an RDD of distinct with entries of structure (predicate, datatype, language tag)
    val array = partitions map { p => (
          p,
          graphRdd
            .filter(p.matches)
            .map(t => Row(p.layout.fromTriple(t).productIterator.toList :_* ))
            .persist())
          }
    array
  }
}
