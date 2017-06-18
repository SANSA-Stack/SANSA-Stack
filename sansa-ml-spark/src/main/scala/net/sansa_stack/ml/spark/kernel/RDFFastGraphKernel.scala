package net.sansa_stack.ml.spark.kernel

import net.sansa_stack.rdf.spark.model.TripleRDD
import org.apache.jena.graph.Node
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.SparkSession

class RDFFastGraphKernel (
                           val tripleRDD: TripleRDD,
                           val instances: RDD[Node],
                           val maxDepth: Int

                         ) extends Serializable {

  var uri2int : Map[Node, Int] = Map.empty[Node, Int]
  var int2uri : Map[Int, Node] = Map.empty[Int, Node]

  var path2index : Map[List[Int], Int] = Map.empty[List[Int], Int]
  var index2path : Map[Int, List[Int]] = Map.empty[Int, List[Int]]

  def getPathIndexOrSet (path: List[Int]) : Int = {
    var index : Int = 0
    if (path2index.keys.exists(_ == path)) {
      index = path2index(path)
    } else {
      index = path2index.size + 1
      path2index = path2index + (path -> index)
      index2path = index2path + (index -> path)
    }
    index
  }
  def getUriIndexOrSet (uri: Node) : Int = {
    var index : Int = 0
    if (uri2int.keys.exists(_ == uri)) {
      index = uri2int(uri)
    } else {
      index = uri2int.size + 1
      uri2int = uri2int + (uri -> index)
      int2uri = int2uri + (index -> uri)
    }
    index
  }

  def processVertex(root: Node, tripleRDD1: TripleRDD, instances1: RDD[Node]): Vector = {

//    println("1")
//    println(root.toString)

//    instances1.foreach(println(_))
//    tripleRDD.getTriples.foreach(println(_))

    // Recursive ProcessVertex
/* - TODO:: Commented because of Error from Nested RDD operations - need to find a solution
    def processVertexRec(vertex: Node, root: Node, path: List[Int], featureMap: Map[Int, Double], depth: Int ): ( Map[Int, Double], List[Int]) = {
      // get path index from pathMap, add if not registered
      val pathIdx = getPathIndexOrSet(path)

      var feature = 1.0
      if (featureMap.keys.exists(_ == pathIdx)) {
        feature = featureMap(pathIdx) + 1
      }
      var updatedFeatureMap = featureMap + (pathIdx -> feature)

      //
      if (depth > 0) {

        val filteredTriples = tripleRDD.getTriples
          .filter(_.getSubject == vertex)
          .filter(_.getObject != root)

        filteredTriples.foreach(f => {
          val predicateIdx = getUriIndexOrSet(f.getPredicate)
          val ObjectIdx = getUriIndexOrSet(f.getObject)
          val newPath = path :+ predicateIdx :+ ObjectIdx

          val result = processVertexRec(f.getSubject, root, newPath, updatedFeatureMap, depth-1)
          updatedFeatureMap = result._1
        })

        (updatedFeatureMap, path)
      } else {
        (updatedFeatureMap, path)
      }
    }

    val path: List[Int] = List[Int]()
    val featureMap: Map[Int, Double] = Map.empty[Int, Double]


    val result = processVertexRec(root, root, path, featureMap, maxDepth)

    val features = Vectors.sparse(result._1.size, result._1.keys.toArray, result._1.values.toArray)
*/
    val features = Vectors.dense(1.3, 2.3, 3.3)
    features
  }



  def kernelCompute(): RDD[(Node, Vector)] = {
//    tripleRDD.getTriples.foreach(println(_))
//    instances.map(f => f).foreach(println(_))

//    var featuresSet = new HashingTF().setInputCol("instances").setOutputCol("features")

    val featureSet = instances.map(f => (f, processVertex(f, tripleRDD, instances)))

    featureSet
  }
}


object RDFFastGraphKernel {

  def apply(
            tripleRDD: TripleRDD,
            instances: RDD[Node],
            maxDepth: Int): RDFFastGraphKernel = new RDFFastGraphKernel(tripleRDD, instances, maxDepth)
}
