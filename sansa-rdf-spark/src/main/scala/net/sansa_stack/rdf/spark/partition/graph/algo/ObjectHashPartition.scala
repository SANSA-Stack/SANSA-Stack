package net.sansa_stack.rdf.spark.partition.graph.algo

import net.sansa_stack.rdf.spark.partition.graph.utils.{TripleGroup, TripleGroupType}
import org.apache.spark.graphx.{Edge, Graph, PartitionID, PartitionStrategy}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

/**
  * Expanding the partitions by assigning object triple group of each vertex to partitions
  *
  * @param graph target graph to be partitioned
  * @param session spark session
  * @param numPartitions number of partitions
  * @param numIterations number of iterations
  * @tparam VD the vertex attribute associated with each vertex in the set.
  * @tparam ED the edge attribute associated with each edge in the set.
  *
  * @author Zhe Wang
  */
class ObjectHashPartition[VD: ClassTag,ED: ClassTag](
    override val graph: Graph[VD,ED],
    override val session: SparkSession,
    override val numPartitions: PartitionID,
    private var numIterations: Int)
  extends PartitionAlgo(graph, session, numPartitions) with Serializable {

  /**
    * Constructs a default instance with default parameters {numPartitions: graph partitions, numIterations: 2}
    *
    */
  def this(graph: Graph[VD,ED], session: SparkSession) = {
    this(graph, session, graph.edges.partitions.length, 2)
  }

  /**
    * Constructs a default instance with default parameters {numIterations: 2}
    *
    */
  def this(graph: Graph[VD,ED], session: SparkSession, numPartitions: Int) = {
    this(graph, session, numPartitions, 2)
  }

  /**
    * Sets the number of iterations (default: 2)
    *
    */
  def setNumIterations(numIterations: Int): this.type = {
    require(numIterations > 0,
      s"Number of iterations must be positive but got $numIterations")
    this.numIterations = numIterations
    this
  }

  override def partitionBy(): Graph[VD,ED] = {
    val stg = new TripleGroup(graph,TripleGroupType.o)
    val edgesBroadcast = session.sparkContext.broadcast(stg.edgesGroupSet.collectAsMap)
    val e = new Array[RDD[Edge[ED]]](numIterations)
    val bhp = graph.reverse.partitionBy(PartitionStrategy.EdgePartition1D,numPartitions).reverse.cache()
    for(i<-0 until numIterations){
      if(i==0){
        e(i) = bhp.edges
      }
      else{
        e(i) = e(i-1).mapPartitions{ iter =>
          val initialEdges = iter.toArray
          val expandEdges = initialEdges.map(e => e.srcId).distinct.flatMap(vid =>
            edgesBroadcast.value.get(vid)).flatten
          initialEdges.++(expandEdges).distinct.toIterator
        }
      }
    }
    Graph[VD,ED](bhp.vertices,e(numIterations-1))
  }
}
