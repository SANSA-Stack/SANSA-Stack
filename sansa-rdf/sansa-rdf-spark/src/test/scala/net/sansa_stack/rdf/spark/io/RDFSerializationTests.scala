package net.sansa_stack.rdf.spark.io

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.jena.query.{QueryExecutionFactory, ResultSetFactory, ResultSetFormatter}
import org.apache.jena.rdf.model.Model
import org.apache.jena.riot.RDFDataMgr
import org.apache.jena.sparql.resultset.ResultSetCompare
import org.apache.spark.storage.StorageLevel
import org.scalatest.funsuite.AnyFunSuite

/**
 * Tests for whether serializer round trips work as expected
 *
 * @author Claus Stadler
 */
class RDFSerializationTests
  extends AnyFunSuite with DataFrameSuiteBase {

  val logger = com.typesafe.scalalogging.Logger(classOf[RDFLoadingTests].getName)

  test("serializing and deserializing triples should return the same model") {

    import net.sansa_stack.rdf.spark.model._

    val expectedModel = RDFDataMgr.loadModel("rdf.nt")
    val triples = spark.rdf(expectedModel)
    triples.persist(StorageLevel.MEMORY_AND_DISK_SER)
    val actualModel = triples.toModel

    val isIsomorphic = actualModel.isIsomorphicWith(expectedModel)
    assert(isIsomorphic)
  }

  // This method should go into a common ResultSetCompareUtils method
  def isIsomorphic(expected: Model, actual: Model): Boolean = {
    var result = false
    val everything = "SELECT ?s ?p ?o { ?s ?p ?o }"
    try {
      val qea = QueryExecutionFactory.create(everything, expected)
      val qeb = QueryExecutionFactory.create(everything, actual)
      try {
        val rsa = ResultSetFactory.copyResults(qea.execSelect)
        val rsb = ResultSetFactory.copyResults(qeb.execSelect)
        result = ResultSetCompare.equalsByTerm(rsa, rsb)
        if (!result) {
          rsa.reset()
          rsb.reset()
          System.err.println("Expected:")
          ResultSetFormatter.out(rsa)
          System.err.println("Actual:")
          ResultSetFormatter.out(rsb)
        }
      } finally {
        if (qea != null) qea.close()
        if (qeb != null) qeb.close()
      }
    }
    result
  }
}
