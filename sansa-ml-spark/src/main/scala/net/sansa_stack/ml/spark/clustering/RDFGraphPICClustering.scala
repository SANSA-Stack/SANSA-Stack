package net.sansa_stack.ml.spark.clustering

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.clustering.{ PowerIterationClusteringModel, PowerIterationClustering }
import org.apache.spark.graphx.{ Graph, EdgeDirection }
import scala.math.BigDecimal
import org.apache.commons.math3.util.MathUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._

class RDFGraphPICClustering(@transient val sparkSession: SparkSession,
                            val graph: Graph[String, String],
                            private val k: Int,
                            private val maxIterations: Int
                            ) extends Serializable {
  /*
	 * undirected graph : orient =0
	 * directed graph : orient =1.
	 * 
	 * Jaccard similarity measure : selectYourSimilarity = 0
	 * Batet similarity measure : selectYourSimilarity = 1
	 * Rodríguez and Egenhofer similarity measure : selectYourSimilarity = 2
	 * The Contrast model similarity : selectYourSimilarity = 3
	 * The Ratio model similarity : selectYourSimilarity = 4
	 */
   val orient = 0
   val selectYourSimilarity = 0

  def clusterRdd(): RDD[(Long, Long, Double)] = {
    SimilaritesInPIC(orient,selectYourSimilarity)
  }
   

  /*
	 * Computes different similarities function for a given graph @graph.
	 */
  def SimilaritesInPIC(e: Int, f:Int): RDD[(Long, Long, Double)] = {
    /*
	 * Collect all the edges of the graph
	*/
    val edge = graph.edges.collect()
    /*
	 * Collect neighbor IDs of all the vertices
	 */
    def neighbor(d: Int): VertexRDD[Array[VertexId]]={
      var neighbor: VertexRDD[Array[VertexId]] = graph.collectNeighborIds(EdgeDirection.Either)
      
       if(d==1){
        neighbor = graph.collectNeighborIds(EdgeDirection.Out)
      }
       neighbor
    }
    val neighbors = neighbor(e)
    /*
	 * Collect distinct vertices of the graph
	 */
    val vertices = graph.vertices.distinct()
    /*
	 * Difference between two set of vertices, used in different similarity measures
	 */
    def difference(a: Long, b: Long): Double = {
      val ansec = neighbors.lookup(a).distinct.head.toSet
      val ansec1 = neighbors.lookup(b).distinct.head.toSet
      if (ansec.isEmpty) { return 0.0 }
      val differ = ansec.diff(ansec1)
      if (differ.isEmpty) { return 0.0 }

      differ.size.toDouble
    }

    /*
	 * Intersection of two set of vertices, used in different similarity measures
	 */
    def intersection(a: Long, b: Long): Double = {
      val inters = neighbors.lookup(a).distinct.head.toSet
      val inters1 = neighbors.lookup(b).distinct.head.toSet
      if (inters.isEmpty || inters1.isEmpty) { return 0.0 }
      val rst = inters.intersect(inters1).toArray
      if (rst.isEmpty) { return 0.0 }
      rst.size.toDouble
    }

    /*
			 * Union of two set of vertices, used in different similarity measures
			 */
    def union(a: Long, b: Long): Double = {
      val inters = neighbors.lookup(a).distinct.head.toSet
      val inters1 = neighbors.lookup(b).distinct.head.toSet
      val rst = inters.union(inters1).toArray
      if (rst.isEmpty) { return 0.0 }

      rst.size.toDouble
    }
   /*
			 * computing algorithm based 2 
			 */
    val LOG2 = math.log(2)
    val log2 = { x: Double => math.log(x) / LOG2 }

    def selectSimilarity(a: Long, b: Long, c: Int): Double ={
      var s = 0.0
      if(c ==0){

    /*
			 * Jaccard similarity measure 
			 */
    
     s = intersection(a, b) / union(a, b).toDouble

    }
      if(c ==1){
    /*
			 * Batet similarity measure 
			 */
    
      val cal = 1 + ((difference(a, b) + difference(b, a)) / (difference(a, b) + difference(b, a) + intersection(a, b))).abs
      s = log2(cal.toDouble)
    }
      
      if(c ==2){
    /*
			 * Rodríguez and Egenhofer similarity measure
			 */

    var g = 0.8
    
      s = (intersection(a, b) / ((g * difference(a, b)) + ((1 - g) * difference(b, a)) + intersection(a, b))).toDouble.abs
    }
      if(c ==3){
    /*
			 * The Contrast model similarity
			 */
    var gamma = 0.3
    var alpha = 0.3
    var beta = 0.3
   
     s = ((gamma * intersection(a, b)) - (alpha * difference(a, b)) - (beta * difference(b, a))).toDouble.abs
    }
      if(c ==4){
    /*
			 * The Ratio model similarity
			 */
    var alph = 0.5
    var beth = 0.5
    
     s = ((intersection(a, b)) / ((alph * difference(a, b)) + (beth * difference(b, a)) + intersection(a, b))).toDouble.abs
    
      }
      s
    }
    /*
			 * Calculate similarities between different pair of vertices in the given graph
			 */

    val weightedGraph = edge.map { x =>
      {
        val x2 = x.dstId.toLong
        val x1 = x.srcId.toLong
        
        (x1, x2, selectSimilarity(x1, x2, f))
      }
    }

    weightedGraph.foreach(x => println(x))
    sparkSession.sparkContext.parallelize(weightedGraph)
  }

  def pic() = {
    val pic = new PowerIterationClustering()
      .setK(k)
      .setMaxIterations(maxIterations)
    pic
  }

  def model = pic.run(clusterRdd())

  /*
			 * Cluster the graph data into two classes using PowerIterationClustering
			 */
  def run() = model

  /*
			 * Save the model.
			 * @path - path for a model.
			 */
  def save(path: String) = model.save(sparkSession.sparkContext, path)

  /*
			 * Load the model.
			 * @path - the given model.
			 */
  def load(path: String) = PowerIterationClusteringModel.load(sparkSession.sparkContext, path)

}

object RDFGraphPICClustering {
  def apply(sparkSession: SparkSession, graph: Graph[String, String], k: Int, maxIterations: Int) = new RDFGraphPICClustering(sparkSession, graph, k, maxIterations)
}
