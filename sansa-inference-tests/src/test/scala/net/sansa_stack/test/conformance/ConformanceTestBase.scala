package net.sansa_stack.test.conformance

import java.io.File

import org.apache.jena.rdf.model.Model
import org.junit.runner.RunWith
import net.sansa_stack.inference.data.RDFTriple
import org.apache.jena.shared.PrefixMapping
import org.apache.jena.sparql.util.{FmtUtils, PrefixMapping2}
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
abstract class ConformanceTestBase extends FlatSpec with BeforeAndAfterAll {

  behavior of ""

  // the test case IDs
  def testCaseIds: Set[String]

  // the base directory of the test cases
  def testsCasesFolder: File

  val pm = PrefixMapping.Factory.create()
    .setNsPrefix("ex", "http://www.example.org#")
    .setNsPrefix("", "http://www.example.org#")
    .withDefaultMappings(PrefixMapping.Standard)

  // load the test cases
  val testCases = TestCases.loadTestCases(testsCasesFolder).filter(t => testCaseIds.contains(t.id))

  testCases.foreach{testCase =>
    println(testCase.id)

    testCase.id should "produce the same graph" in {
      val triples = new mutable.HashSet[RDFTriple]()

      // convert to internal triples
      val iterator = testCase.inputGraph.listStatements()
      while(iterator.hasNext) {
        val st = iterator.next()
        triples.add(RDFTriple(st.getSubject.toString, st.getPredicate.toString,
          if(st.getObject.isLiteral) FmtUtils.stringForNode(st.getObject.asNode()) else st.getObject.toString))
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

  def computeInferredModel(triples: mutable.HashSet[RDFTriple]): Model

}
