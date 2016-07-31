package org.dissect.inference.rules.conformance

import org.apache.jena.rdf.model.Model

/**
  * A test case to test the conformance of a particular forward chaining reasoner rule.
  *
  * @author Lorenz Buehmann
  */
case class TestCase (id: String,  description: String, testCaseType: String, inputGraph: Model, outputGraph: Model){

}
