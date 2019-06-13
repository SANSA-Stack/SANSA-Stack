package net.sansa_stack.ml.spark.clustering.algorithms

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.typesafe.config.ConfigFactory
import net.sansa_stack.rdf.spark.io._
import net.sansa_stack.rdf.spark.model._
import org.apache.jena.riot.Lang
import org.scalatest.FunSuite

import net.sansa_stack.ml.spark.clustering._

class RdfPicTest extends FunSuite with DataFrameSuiteBase {

  test("Clustering RDF data") {
    val conf = ConfigFactory.load()
    val lang = Lang.NTRIPLES
    val path = getClass.getResource("/Cluster/Clustering_sampledata.txt").getPath
    val triples = spark.rdf(lang)(path)
    val cluster = triples.cluster(ClusteringAlgorithm.RDFGraphPowerIterationClustering).asInstanceOf[RDFGraphPowerIterationClustering]
    val runTest = cluster.setK(conf.getInt("sansa.clustering.pic.number_clusters")).
      setMaxIterations(conf.getInt("sansa.clustering.pic.iterations")).run()
    assert(true)
  }
}
