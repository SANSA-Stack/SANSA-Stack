package net.sansa_stack.examples.spark.query

import org.scalatest.FunSuite

import org.aksw.jena_sparql_api.server.utils.FactoryBeanSparqlServer
import org.aksw.jena_sparql_api.core.FluentQueryExecutionFactory

import org.apache.jena.rdf.model.ModelFactory

import org.apache.jena.rdfconnection.{RDFConnectionFactory, RDFConnection}

class SparqlServerTests extends FunSuite {
  test("starting the default SPARQL server should succeed") {

    val server = new FactoryBeanSparqlServer()
      .setSparqlServiceFactory(FluentQueryExecutionFactory
        .from(ModelFactory.createDefaultModel())
        .create())
      .create();

    while(server.isStarting) {
      Thread.sleep(1000)
    }

    val conn = RDFConnectionFactory.connect("http://localhost:7531");
    val model = conn.queryConstruct("CONSTRUCT WHERE { ?s ?p ?o }")

    server.stop()
    server.join()

    assert(true)
  }
}

