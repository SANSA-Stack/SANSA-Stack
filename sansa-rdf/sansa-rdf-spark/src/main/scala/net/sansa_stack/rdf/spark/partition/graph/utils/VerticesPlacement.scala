package net.sansa_stack.rdf.spark.partition.graph.utils

import org.apache.spark.graphx.{PartitionID, VertexId}
import scala.collection.mutable

/**
  * Assign each vertex to one partition according to its one attribute.
  */
object VerticesPlacement {

  def placeByIndex(vertices: Array[VertexId], numParts: PartitionID): Map[VertexId, PartitionID] = {
    val mixingPrime: VertexId = 1125899906842597L
    val sort = vertices.sortBy(vid => vid)
    val results = mutable.HashMap.empty[VertexId, PartitionID]
    for (index <- sort.indices) {
      val part: PartitionID = (math.abs(index * mixingPrime) % numParts).toInt
      results.put(sort(index), part)
    }
    results.toMap
  }

  def placeById(vertices: Array[VertexId], numParts: PartitionID): Map[PartitionID, Array[VertexId]] = {
    val mixingPrime: VertexId = 1125899906842597L
    val results = mutable.HashMap.empty[PartitionID, mutable.ArrayBuffer[VertexId]]
    vertices.foreach{vid =>
      val part: PartitionID = (math.abs(vid * mixingPrime) % numParts).toInt
      if (results.get(part).isEmpty) {
        results.put(part, mutable.ArrayBuffer(vid))
      } else {
        results(part) += vid
      }
    }
    results.mapValues(_.toArray).toMap
  }

  def placeById(vid: VertexId, numParts: PartitionID): PartitionID = {
    val mixingPrime: VertexId = 1125899906842597L
    val part: PartitionID = (math.abs(vid * mixingPrime) % numParts).toInt
    part
  }

}
