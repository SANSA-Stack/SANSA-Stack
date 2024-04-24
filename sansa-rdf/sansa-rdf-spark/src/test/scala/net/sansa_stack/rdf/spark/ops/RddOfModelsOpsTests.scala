package net.sansa_stack.rdf.spark.ops

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.jena.graph.{Node, NodeFactory, Triple}
import org.apache.jena.query.{QueryFactory, Syntax}
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.apache.jena.vocabulary.RDF
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite

import scala.collection.JavaConverters._

class RddOfModelsOpsTests extends FunSuite with DataFrameSuiteBase {

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
    // val model = RDFDataMgr.loadDataset("loader/data.nq")
    val m = ModelFactory.createDefaultModel()
    m.getGraph.add(Triple.create(RDF.Nodes.`type`, RDF.Nodes.`type`, RDF.Nodes.`type`))

    val in: RDD[Model] = sc.parallelize(Seq(m))

    assert(in.count() == 1)

    val query = QueryFactory.create("""
      CONSTRUCT {
        ?s a <http://suc.ce/ss> ; ?p ?o
      } {
        ?s ?p ?o
      }
      """, Syntax.syntaxARQ)
    // ops.datasetOps.flatMapQuery(in, query)
    // val out = in.flatMapQuery(query)
    // val out = DatasetOps.flatMapQueryCore(in, query)
    val out = in.sparqlMap(query)
    val tmp = out.collect()
    // tmp.foreach(item => RDFDataMgr.write(System.out, item, RDFFormat.TRIG_PRETTY))
    val model = tmp(0)

    val successTriples = model.getGraph
      .find(Node.ANY, Node.ANY, NodeFactory.createURI("http://suc.ce/ss"))
      .asScala

    assert(successTriples.length == 1)
  }

}
