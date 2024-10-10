package net.sansa_stack.rdf.spark.ops

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.jena.graph.{Node, NodeFactory}
import org.apache.jena.query.{DatasetFactory, QueryFactory, Syntax}
import org.apache.jena.vocabulary.RDF
import org.apache.spark.SparkConf
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.JavaConverters._

class RddOfDatasetsOpsTests extends AnyFunSuite with DataFrameSuiteBase {

  override def conf(): SparkConf = {
    val conf = super.conf
    conf
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", String.join(", ",
        "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator"))
    conf
  }


  test("using a query as a mapper for datasets should succeed") {

    // val path = getClass.getResource("/loader/data.nq").getPath
    // val quads = spark.nquads()(path)
    // val ds = RDFDataMgr.loadDataset("loader/data.nq")
    val ds = DatasetFactory.create
    ds.asDatasetGraph().add(RDF.Nodes.`type`, RDF.Nodes.`type`, RDF.Nodes.`type`, RDF.Nodes.`type`)

    val in = sc.parallelize(Seq(ds))

    assert(in.count() == 1)

    val query = QueryFactory.create("""
      CONSTRUCT {
        GRAPH ?g {
          ?s a <http://suc.ce/ss> ; ?p ?o
        }
      } {
        GRAPH ?g { ?s ?p ?o }
      }
      """, Syntax.syntaxARQ)
    // ops.datasetOps.flatMapQuery(in, query)
    // val out = in.flatMapQuery(query)
    // val out = DatasetOps.flatMapQueryCore(in, query)
    val out = in.sparqlFlatMap(query)
    val tmp = out.collect()
    // tmp.foreach(item => RDFDataMgr.write(System.out, item, RDFFormat.TRIG_PRETTY))
    val dataset = tmp(0)

    val successTriples = dataset.asDatasetGraph()
      .find(Node.ANY, Node.ANY, Node.ANY, NodeFactory.createURI("http://suc.ce/ss"))
      .asScala

    assert(successTriples.length == 1)
  }

}
