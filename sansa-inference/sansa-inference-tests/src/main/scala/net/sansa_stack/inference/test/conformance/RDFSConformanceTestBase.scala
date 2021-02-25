package net.sansa_stack.inference.test.conformance

import net.sansa_stack.inference.data.{RDF, RDFOps}
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner


/**
  * The base class to test the conformance of each materialization rule of RDFS(simple) entailment.
  *
  * @author Lorenz Buehmann
  *
  */
@RunWith(classOf[JUnitRunner])
abstract class RDFSConformanceTestBase[Rdf <: RDF](override val rdfOps: RDFOps[Rdf])
  extends ConformanceTestBase[Rdf](rdfOps) {

  behavior of "conformance of RDFS(simple) entailment rules"

  override def testCasesPath: String = "data/conformance/rdfs"

  override def testCaseIds: Set[String] = Set(
    "rdfbased-sem-rdfs-domain-cond",
    "rdfbased-sem-rdfs-range-cond",
    "rdfbased-sem-rdfs-subclass-cond",
    "rdfbased-sem-rdfs-subclass-trans",
    "rdfbased-sem-rdfs-subprop-cond",
    "rdfbased-sem-rdfs-subprop-trans")
}
