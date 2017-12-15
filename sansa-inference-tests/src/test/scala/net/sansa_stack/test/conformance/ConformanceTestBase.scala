package net.sansa_stack.test.conformance

import java.io.File

import net.sansa_stack.inference.data.{RDF, RDFOps}
import org.apache.jena.rdf.model.Model
import org.apache.jena.shared.PrefixMapping
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

import scala.collection.mutable

/**
  * The class is to test the conformance of each materialization rule of RDFS(simple) entailment.
  *
  * @author Lorenz Buehmann
  *
  */
@RunWith(classOf[JUnitRunner])
abstract class ConformanceTestBase[Rdf <: RDF](val rdfOps: RDFOps[Rdf]) extends FlatSpec with BeforeAndAfterAll {

  behavior of ""

  // the test case IDs
  def testCaseIds: Set[String]

  // the base directory of the test cases
  def testsCasesFolder: File = {
    new File(this.getClass.getClassLoader.getResource(testCasesPath).getPath)
  }

  def testCasesPath: String

  private val pm = PrefixMapping.Factory.create()
    .setNsPrefix("ex", "http://www.example.org#")
    .setNsPrefix("", "http://www.example.org#")
    .withDefaultMappings(PrefixMapping.Standard)

  // load the test cases
  private val testCases = TestCases.loadTestCases(testsCasesFolder, testCaseIds)

  // scalastyle:off println
  testCases.foreach { testCase =>
    println(testCase.id)

    testCase.id should "produce the same graph" in {
      val triples = new mutable.HashSet[Rdf#Triple]()

      // convert to internal triples
      val iterator = testCase.inputGraph.listStatements()
      while (iterator.hasNext) {
        val st = iterator.next()
        triples.add(
          rdfOps.makeTriple(
            rdfOps.makeUri(st.getSubject.toString),
            rdfOps.makeUri(st.getPredicate.toString),
            if (st.getObject.isLiteral) {
              rdfOps.makeLiteral(st.getObject.asLiteral().getLexicalForm, rdfOps.makeUri(st.getObject.asLiteral().getDatatypeURI))
            } else {
              rdfOps.makeUri(st.getObject.toString)
            }
          )
        )
      }

      // compute inferred graph
      val inferredModel = computeInferredModel(triples)
      inferredModel.setNsPrefixes(pm)

      // remove the input triples such that we can compare only the conclusion graph
      inferredModel.remove(testCase.inputGraph)

      println("#" * 80 + "\ninput:")
      testCase.inputGraph.write(System.out, "TURTLE")

      println("#" * 80 + "\nexpected output:")
      testCase.outputGraph.write(System.out, "TURTLE")

      println("#" * 80 + "\ngot output:")
      inferredModel.write(System.out, "TURTLE")

      // compare models, i.e. the inferred model should contain exactly the triples of the conclusion graph
      val correctOutput = inferredModel.containsAll(testCase.outputGraph)
      assert(correctOutput, "contains all expected triples")

      val isomorph = inferredModel.isIsomorphicWith(testCase.outputGraph)
      assert(isomorph)
    }
  }

  def computeInferredModel(triples: mutable.HashSet[Rdf#Triple]): Model
}
