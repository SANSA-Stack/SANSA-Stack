package net.sansa_stack.ml.spark.clustering.algorithms

import com.typesafe.config.ConfigFactory
import org.apache.jena.graph.{ NodeFactory, Triple }
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.mllib.clustering.PowerIterationClustering
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.{ ArrayBuffer, HashMap }

import net.sansa_stack.ml.spark.clustering.datatypes.Distance
import net.sansa_stack.ml.spark.clustering.utils._

class PIC(input: RDD[Triple]) extends Serializable with ClusterAlgo {
  val prefixID = "<http://clustering.slipo.eu/poi/"
  val prefixCategory = "<http://clustering.slipo.eu/hasCategories>"
  val prefixCoordinate = "<http://clustering.slipo.eu/hasCoordinate>"

  var noofcluster = 0
  var noOfIter = 0
  var oneHotClusters = Map[Int, Array[Long]]()
  val spark = SparkSession.builder.getOrCreate()
  import spark.implicits._

  val conf = ConfigFactory.load()

  val dataProcessing = new DataProcessing(spark, conf, input)
  val pois = dataProcessing.pois

  def setK(K: Int): this.type = {
    noofcluster = K
    this
  }
  /**
   * set maximum iterations for PIC
   * @param iter
   */
  def setMaxIterations(iter: Int): this.type = {
    noOfIter = iter
    this
  }

  def run(): RDD[(Int, List[Triple])] = {
    val spark = SparkSession.builder.getOrCreate()
    import spark.implicits._

    val poiCategorySetVienna = pois.map(poi => (poi.poi_id, poi.categories.categories.toSet)).persist()
    val poiCartesian = poiCategorySetVienna.cartesian(poiCategorySetVienna)
    val pairwisePOICategorySet = poiCartesian.filter {
      case (a, b) =>
        a._1 < b._1
    }
    val pairwisePOISimilarity = pairwisePOICategorySet.map(x => (x._1._1.toLong, x._2._1.toLong,
      new Distances().jaccardSimilarity(x._1._2, x._2._2))).persist()
    val picDistanceMatrix = pairwisePOISimilarity.map(x => Distance(x._1, x._2, 1 - x._3)).collect()
    val clustersPIC = picSparkML(
      pairwisePOISimilarity,
      noofcluster,
      noOfIter,
      spark)
    val picClusters = Common.seralizeToNT(spark.sparkContext, clustersPIC, pois)
    picClusters
  }
  /*
   * Power Iteration clustering algorithm from Spark standard library
   * */
  def picSparkML(pairwisePOISimilarity: RDD[(Long, Long, Double)], numCentroids: Int, numIterations: Int, sparkSession: SparkSession): Map[Int, Array[Long]] = {
    val model = new PowerIterationClustering().setK(numCentroids).setMaxIterations(numIterations).setInitializationMode("degree").run(pairwisePOISimilarity)
    val clusters = model.assignments.collect().groupBy(_.cluster).mapValues(_.map(_.id))
    clusters
  }
  /*
   * Power Iteration using implementation from SANSA
   * */
  def picSANSA(pairwisePOISimilarity: RDD[(Long, Long, Double)], numCentroids: Int, numIterations: Int, sparkSession: SparkSession) {
    val verticeS = pairwisePOISimilarity.map(f => f._1)
    val verticeD = pairwisePOISimilarity.map(f => f._2)
    val indexedMap = verticeS.union(verticeD).distinct().zipWithIndex()
    val vertices = indexedMap.map(f => (f._2, f._1))
    val edges = pairwisePOISimilarity.map(f => Edge(f._1, f._2, f._3)) // from similarity to int
    val similarityGraph = Graph(vertices, edges)
  }

}
object PIC {
  def apply(input: RDD[Triple]): PIC = new PIC(input)
}



