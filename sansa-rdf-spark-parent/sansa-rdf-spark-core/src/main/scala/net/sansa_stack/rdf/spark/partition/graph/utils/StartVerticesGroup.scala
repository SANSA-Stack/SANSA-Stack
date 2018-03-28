package net.sansa_stack.rdf.spark.partition.graph.utils

import net.sansa_stack.rdf.spark.partition.graph.utils.VertexAttribute.attribute
import org.apache.spark.graphx.{Graph, PartitionID, VertexId, VertexRDD}

import scala.reflect.ClassTag

/**
  * Merge a vertex by assigning all end to end paths into a path group that pass through this vertex
  * It divides all start vertices into k nonempty and disjoint sets (k: number of partitions)
  *
  * @author Zhe Wang
  */
object StartVerticesGroup extends Serializable {

  /**
    * Generate start vertices groups.
    *
    * @param graph the input graph.
    * @param vertices VertexRDD of vertex attribute.
    * @param numPartitions number of partitions.
    * @return Array of start vertices sets, each set means put all paths from those start vertices into one partition
    */
  def apply[VD: ClassTag,ED:ClassTag](
      graph: Graph[VD,ED],
      vertices: VertexRDD[attribute],
      numPartitions: PartitionID): Array[Array[VertexId]] = {

    var mergeVertices = vertices.filter{ case(_,attr) => attr._1 > 1 && attr._2.lengthCompare(1) > 0}
      .sortBy{ case(_,attr) => (attr._1.toInt,attr._2.length) }.collect()
    var startVerticesGroup = EndToEndPaths.setSrcVertices(graph).map(vid=>Array(vid))

    //Reset the start vertices group if its length is more than num of partitions and the set of
    //vertices to merge is non-empty
    while(startVerticesGroup.lengthCompare(numPartitions)>0 && mergeVertices.length !=0) {
      val mergeVertexId = mergeVertices.head._1
      val startVerticesToMerge = mergeVertices.filter(_._1==mergeVertexId).map(_._2._2).head
      val groupToMerge = startVerticesGroup.filter{group=>
        startVerticesToMerge.exists(sv=>group.contains(sv))
      }
      if(groupToMerge.lengthCompare(1) > 0){
        val newGroup = groupToMerge.reduce((group1,group2)=>group1.++(group2))
        //To keep every partitions balance, number of vertices in each partition will be no more than upper bound of average
        if(newGroup.lengthCompare(
          Math.ceil(EndToEndPaths.setSrcVertices(graph).length.toDouble / numPartitions.toDouble).toInt) < 0){
          startVerticesGroup = startVerticesGroup.diff(groupToMerge).:+(newGroup)
        }
      }
      mergeVertices = mergeVertices.drop(1)
    }

    //Further set start vertices groups if its length still more than num of partitions after steps above, in this step,
    //we sort the groups by num of vertices, then merge groups with fewest num of vertices
    while(startVerticesGroup.lengthCompare(numPartitions)>0){
      startVerticesGroup = startVerticesGroup.sortBy(array=>array.length)(Ordering[Int].reverse)
      val lastTwo = startVerticesGroup.slice(startVerticesGroup.length-2,startVerticesGroup.length)
      startVerticesGroup = startVerticesGroup.dropRight(2) :+ lastTwo(0).++(lastTwo(1))
    }
    startVerticesGroup
  }
}
