package net.sansa_stack.inference.test.conformance

import java.io.ByteArrayOutputStream

import scala.collection.mutable

import com.typesafe.scalalogging.LazyLogging
import org.apache.jena.rdf.model.Model
import org.apache.jena.shared.PrefixMapping
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

import net.sansa_stack.inference.data.{RDF, RDFOps}

/**
  * The class is to test the conformance of each materialization rule of RDFS(simple) entailment.
  *
  * @author Lorenz Buehmann
  *
  */
@RunWith(classOf[JUnitRunner])
abstract class ConformanceTestBase[Rdf <: RDF](val rdfOps: RDFOps[Rdf])
  extends FlatSpec
    with BeforeAndAfterAll
    with LazyLogging {

  behavior of ""

  // the test case IDs
  def testCaseIds: Set[String]

  def testsCasesFolder: String = testCasesPath // this.getClass.getClassLoader.getResource(testCasesPath).getPath
//  def testsCasesFolder: File = null // new File(this.getClass.getClassLoader.getResource(testCasesPath).getPath)

  def testCasesPath: String

  private val pm = PrefixMapping.Factory.create()
    .setNsPrefix("ex", "http://www.example.org#")
    .setNsPrefix("", "http://www.example.org#")
    .withDefaultMappings(PrefixMapping.Standard)

  // load the test cases
  lazy val testCases = TestCases.loadTestCasesJar(testsCasesFolder, testCaseIds)

  // scalastyle:off println
  testCases.foreach { testCase =>
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

      logger.whenDebugEnabled {
        println("#" * 80 + "\ninput:")
        testCase.inputGraph.write(System.out, "TURTLE")
      }

      logger.whenDebugEnabled {
        println("#" * 80 + "\nexpected output:")
        testCase.outputGraph.write(System.out, "TURTLE")
      }

      logger.whenDebugEnabled {
        println("#" * 80 + "\ngot output:")
        inferredModel.write(System.out, "TURTLE")
      }

      // compare models, i.e. the inferred model should contain exactly the triples of the conclusion graph
      val correctOutput = inferredModel.containsAll(testCase.outputGraph)
      if(!correctOutput) {
        logger.whenErrorEnabled {
          logger.error("Missing triples in inferred graph:\n {}", toNTriplesString(testCase.outputGraph.difference(inferredModel)))
        }
      }
      assert(correctOutput, "contains all expected triples")


      val isomorph = inferredModel.isIsomorphicWith(testCase.outputGraph)
      if(!isomorph) {
        logger.whenErrorEnabled {
          logger.error(s"Inferred graph not isomorph to target graph. Inferred triples:\n{}", toNTriplesString(inferredModel))
        }

      }
      assert(isomorph, "input and output are isomorph")
    }
  }

  private def toNTriplesString(model: Model): String = {
    val baos = new ByteArrayOutputStream()
    model.write(baos, "N-Triples")
    val s = new String(baos.toByteArray)
    s
  }

  def computeInferredModel(triples: mutable.HashSet[Rdf#Triple]): Model
}
