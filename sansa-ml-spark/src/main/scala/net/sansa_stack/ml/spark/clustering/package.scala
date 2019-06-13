package net.sansa_stack.ml.spark

import org.apache.jena.graph.Triple
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import net.sansa_stack.ml.spark.clustering.algorithms._
import net.sansa_stack.rdf.spark.io._
import net.sansa_stack.rdf.spark.model._

package object clustering {
  object ClusteringAlgorithm extends Enumeration {
    type ClusteringAlgorithm = Value

    val KMeans = Value("KMeans")
    val PIC = Value("PIC")
    val DBSCAN = Value("DBSCAN")
    val BorderFlow = Value("BorderFlow")
    val RDFGraphPowerIterationClustering = Value("RDFGraphPowerIterationClustering")
    val SilviaClustering = Value("SilviaClustering")
    val RDFByModularityClustering = Value("RDFByModularityClustering")
  }
  implicit class clusterT(input: RDD[Triple]) extends Enumeration {
    def cluster(name: ClusteringAlgorithm.Value): ClusterAlgo = name match {
      case ClusteringAlgorithm.KMeans => new Kmeans(input)
      case ClusteringAlgorithm.DBSCAN => new DBSCAN(input)
      case ClusteringAlgorithm.PIC => new PIC(input)
      case ClusteringAlgorithm.BorderFlow => new BorderFlow(input.asGraph())
      case ClusteringAlgorithm.SilviaClustering => new SilviaClustering(input.asStringGraph())
      case ClusteringAlgorithm.RDFGraphPowerIterationClustering => new RDFGraphPowerIterationClustering(input.asStringGraph())
    }
  }
}
