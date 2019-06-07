package net.sansa_stack.rdf.common.qualityassessment.utils

import org.apache.jena.graph.{ NodeFactory, Triple}
import org.scalatest.FunSuite

class QANodeUtilsTests extends FunSuite {

  val triple = Triple.create(
    NodeFactory.createURI("http://dbpedia.org/resource/Guy_de_Maupassant"),
    NodeFactory.createURI("http://xmlns.com/foaf/0.1/givenName"),
    NodeFactory.createLiteral("Guy De"))

  test("checking if resource is external should match") {

    val isExternal = NodeUtils.isExternal(triple.getSubject)

    assert(isExternal == false)
  }

  test("checking if resource is internal should match") {

    val isInternal = NodeUtils.isInternal(triple.getSubject)

    assert(isInternal == true)
  }

  test("checking if resource is lexical form compatible with datatype should match") {

    val isLexicalFormCompatibleWithDatatype = NodeUtils.isLexicalFormCompatibleWithDatatype(triple.getObject)

    assert(isLexicalFormCompatibleWithDatatype == true)
  }

  test("checking if resource is license statement should match") {

    val isLicenseStatement = NodeUtils.isLicenseStatement(triple.getObject)

    assert(isLicenseStatement == false)
  }

  test("checking if resource has licence indications should match") {

    val hasLicenceIndications = NodeUtils.hasLicenceIndications(triple.getPredicate)

    assert(hasLicenceIndications == false)
  }

  test("checking if resource has licence associated should match") {

    val hasLicenceAssociated = NodeUtils.hasLicenceAssociated(triple.getPredicate)

    assert(hasLicenceAssociated == false)
  }

  test("checking if resource has broken link should match") {

    val isBroken = NodeUtils.isBroken(triple.getPredicate)

    assert(isBroken == false)
  }

  test("checking if resource is hash URI should match") {

    val isHashUri = NodeUtils.isHashUri(triple.getPredicate)

    assert(isHashUri == false)
  }

  test("getting the parent URI of the resource should match") {

    val getParentURI = NodeUtils.getParentURI(triple.getPredicate)

    val parentURI = NodeFactory.createURI("http://xmlns.com/foaf/0.1")

    assert(getParentURI.matches(parentURI.toString()))
  }

  test("getting the lexical form of the resource should match") {

    val checkLiteral = NodeUtils.checkLiteral(triple.getObject)

    val literal = NodeFactory.createLiteral("Guy De").getLiteralLexicalForm

    assert(checkLiteral.matches(literal))
  }

  test("checking if the resource is labeled should match") {

    val isLabeled = NodeUtils.isLabeled(triple.getObject)

    assert(isLabeled == false)
  }

  test("checking if the resource is an RDFS class should match") {

    val isRDFSClass = NodeUtils.isRDFSClass(triple.getPredicate)

    assert(isRDFSClass == false)
  }

  test("checking if the resource is an OWL class should match") {

    val isOWLClass = NodeUtils.isOWLClass(triple.getPredicate)

    assert(isOWLClass == false)
  }

  test("checking if the resource is too long should match") {

    val resourceTooLong = NodeUtils.resourceTooLong(triple.getSubject)

    assert(resourceTooLong == false)
  }

  test("checking if the resource has query sting should match") {

    val hasQueryString = NodeUtils.hasQueryString(triple.getSubject)

    assert(hasQueryString == false)
  }

}
