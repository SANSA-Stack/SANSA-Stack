package net.sansa_stack.rdf.spark.partition.graph.utils

import org.apache.jena.graph.Node
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.lib.ShortestPaths

/**
  * Determine the distance from every vertex in a graph to the farstest edge
  * In the case of directed, we consider to pick up the vertex with largest distance and return the distance
  *
  * @author Zhe Wang
  */
object DistanceOfFarthestEdge {

  def apply(graph: Graph[Node,Node]):Int = {
    graph.cache()
    val landsmark = graph.vertices.map(v => v._1).collect().toSeq
    val result = ShortestPaths.run[Node,Node](graph,landsmark).vertices.filter{ case(id,map) => map.nonEmpty}
    val distance = result.map(v=>v._2.valuesIterator.max).filter(_>0).max
    distance
  }
}
