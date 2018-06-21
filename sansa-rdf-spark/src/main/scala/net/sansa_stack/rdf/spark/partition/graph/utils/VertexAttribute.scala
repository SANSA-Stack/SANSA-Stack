package net.sansa_stack.rdf.spark.partition.graph.utils

import scala.reflect.ClassTag

import net.sansa_stack.rdf.spark.partition.graph.utils.EndToEndPaths.Path
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{ Graph, VertexId, VertexRDD }

/**
 * Vertex Attribute denotes the weight and start vertices list for each vertex v
 * Vertex Weight is the number of paths that contain v.
 * Start vertices list is a list of start vertices S(v) for each vertex v,
 * where S(v) contains all the start vertices that can reach v.
 *
 * @author Zhe Wang
 */
object VertexAttribute {

  type attribute = (Int, List[VertexId])

  def apply[VD: ClassTag, ED: ClassTag](
    graph: Graph[VD, ED],
    paths: VertexRDD[List[Path[ED]]],
    sc: SparkContext): VertexRDD[attribute] = {
    val broadcast = sc.broadcast(paths.collect())
    graph.vertices.mapValues { (vid, _) =>
      val attr = broadcast.value.map {
        case (src, list) =>
          (src, list.count(path => path.count(e => e.dstId == vid) == 1))
      }
      val weight = attr.map { case (_, num) => num }.sum
      val srcList = attr.filter { case (_, num) => num > 0 }.map { case (src, _) => src }
      (weight, srcList.toList)
    }.filter { case (_, attr) => attr._1 > 0 }
  }
}
