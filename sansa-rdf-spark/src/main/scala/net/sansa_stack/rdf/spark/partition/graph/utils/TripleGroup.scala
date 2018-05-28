package net.sansa_stack.rdf.spark.partition.graph.utils

import net.sansa_stack.rdf.spark.partition.graph.utils.TripleGroupType.TripleGroupType
import org.apache.spark.graphx._

import scala.reflect.ClassTag

/**
  * Construct triple groups for input vertices
  *
  * Subject-based triples groups: s-TG of vertex v∈V is a set of triples in which their subject is v
  * denoted by s-TG(v)= {(u,w)\(u,w)∈E, u = v}
  *
  * Object-based triples groups: o-TG of vertex v∈V is a set of triples in which their object is v
  * denoted by s-TG(v)= {(u,w)\(u,w)∈E, w = v}
  *
  * Subject-object-based triple groups: so-TG of vertex v∈V is a set of triples in which their object is v
  * denoted by s-TG(v)= {(u,w)\(u,w)∈E, v∈{u,w}}
  *
  * @author Zhe Wang
  */
class TripleGroup[VD,ED: ClassTag](graph: Graph[VD,ED], tgt:TripleGroupType) extends Serializable {

  graph.cache()
  private val ops = graph.ops
  val direction: Option[EdgeDirection] = determineType()
  val verticesGroupSet: VertexRDD[Array[(VertexId,VD)]] = setVerticesGroupSet()
  val edgesGroupSet: VertexRDD[Array[Edge[ED]]] = setEdgesGroupSet()

  private def determineType(): Option[EdgeDirection] = {
    tgt match {
      case TripleGroupType.s => Some(EdgeDirection.Out)
      case TripleGroupType.o => Some(EdgeDirection.In)
      case TripleGroupType.so => Some(EdgeDirection.Either)
    }
  }

  private def setVerticesGroupSet(): VertexRDD[Array[(VertexId,VD)]] = {
    ops.collectNeighbors(direction.get)
  }

  private def setEdgesGroupSet(): VertexRDD[Array[Edge[ED]]] = {
    ops.collectEdges(direction.get)
  }
}

