package net.sansa_stack.test.conformance

import java.io.File

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
  * The class is to test the conformance of each materialization rule of OWL Horst entailment.
  *
  * @author Lorenz Buehmann
  *
  */
@RunWith(classOf[JUnitRunner])
abstract class OWLHorstConformanceTestBase extends ConformanceTestBase {

  behavior of "conformance of OWL Horst entailment rules"

  override def testCaseIds: Set[String] = Set(
    "rdfbased-sem-rdfs-domain-cond",
    "rdfbased-sem-rdfs-range-cond",
    "rdfbased-sem-rdfs-subclass-cond",
    "rdfbased-sem-rdfs-subclass-trans",
    "rdfbased-sem-rdfs-subprop-cond",
    "rdfbased-sem-rdfs-subprop-trans",

    "rdfbased-sem-char-functional-inst",
    "rdfbased-sem-char-inversefunc-data",
    "rdfbased-sem-char-symmetric-inst",
    "rdfbased-sem-char-transitive-inst",
    "rdfbased-sem-inv-inst",
    "rdfbased-sem-eqdis-eqclass-subclass-1", // the test works but returns more correct triples than specified
    "rdfbased-sem-eqdis-eqclass-subclass-2",
    "rdfbased-sem-eqdis-eqprop-subprop-1", // the test works but returns more correct triples than specified
    "rdfbased-sem-eqdis-eqprop-subprop-2",
    "rdfbased-sem-restrict-hasvalue-inst-obj",
    "rdfbased-sem-restrict-hasvalue-inst-subj",
    "rdfbased-sem-restrict-somevalues-inst-subj",
    "rdfbased-sem-restrict-allvalues-inst-obj"
  )

  override def testsCasesFolder: File = new File(this.getClass.getClassLoader.getResource("data/conformance/owl2rl").getPath)
}
