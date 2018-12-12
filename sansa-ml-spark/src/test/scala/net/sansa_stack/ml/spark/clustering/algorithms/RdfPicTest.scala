package net.sansa_stack.ml.spark.clustering.algorithms

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import net.sansa_stack.rdf.spark.io._
import net.sansa_stack.rdf.spark.model.graph._
import org.apache.jena.riot.Lang
import org.scalatest.FunSuite

class RdfPicTest extends FunSuite with DataFrameSuiteBase {

  test("Clustering RDF data") {

    val lang = Lang.NTRIPLES
    val path = getClass.getResource("/Cluster/Clustering_sampledata.txt").getPath
    val triples = spark.rdf(lang)(path)

    val graph = triples.asStringGraph()
    val cluster = RDFGraphPowerIterationClustering(spark, graph, "/Cluster/output", 4, 10)

    cluster.collect().foreach(println)
    assert(true)
  }
}
