package org.dissect.inference.utils

import org.apache.jena.graph.{Node, NodeFactory, Triple}
import org.apache.jena.reasoner.TriplePattern
import org.apache.jena.vocabulary.OWL2._
import org.apache.jena.vocabulary.RDFS._
import org.apache.jena.vocabulary.{OWL2, RDF, RDFS}

/**
  * Utility class for triples.
  *
  * @author Lorenz Buehmann
  */
object TripleUtils {

  // set of properties that indicate terminological triples
  val properties = List(
    subClassOf, equivalentClass, disjointWith,
    intersectionOf, unionOf, complementOf, someValuesFrom, allValuesFrom, hasValue,
    maxCardinality, minCardinality, cardinality,
    subPropertyOf, equivalentProperty, propertyDisjointWith, domain, range, inverseOf).map(t => t.asNode())

  // set of types that indicate terminological triples
  val types = Set(
    ObjectProperty, DatatypeProperty,
    FunctionalProperty, InverseFunctionalProperty,
    SymmetricProperty, AsymmetricProperty,
    ReflexiveProperty, IrreflexiveProperty, TransitiveProperty,
    OWL2.Class , RDFS.Class, Restriction
  ).map(t => t.asNode())

  /**
    * Checks whether a triple is terminological.
    * <p>
    * A terminological triple denotes information about the TBox of the ontology (class and property axioms),
    * such as subclass relationships, class equivalence, property types, etc.
    * </p>
    *
    * @param triple the triple to check
    */
  def isTerminological(triple: Triple) : Boolean = {
    properties.contains(triple.getPredicate) ||
      (triple.getPredicate.equals(RDF.`type`.asNode()) && types.contains(triple.getObject))
  }

  /**
    * Checks whether a triple is assertional.
    * <p>
    * Intuitively, an assertional triple denotes information about the ABox of the ontology, such as
    * instance class membership or instance equality/inequality (owl:sameAs/owl:differentFrom)
    * (we consider the oneOf construct as simple C(a) assertions).
    * </p>
    * @param triple the triple to check
    */
  def isAssertional(triple: Triple) : Boolean = {triple.getObject
    !isTerminological(triple)
  }

  /**
    * Returns the position of the node in the triple. If the node does not occur, `-1` will be returned.
    * @param node the node
    * @param triple the triple
    */
  def position(node: Node, triple: Triple): Int = {
    val ret = if(triple.subjectMatches(node)) {
      1
    } else if(triple.predicateMatches(node)) {
      2
    } else if(triple.objectMatches(node)) {
      3
    } else {
      -1
    }
    ret
  }

  def nodes(tp: org.apache.jena.graph.Triple): List[Node] = List[Node](tp.getSubject, tp.getPredicate, tp.getObject)


  implicit class TriplePatternExtension(val tp: TriplePattern) {
    def toTriple = {
      val s = alignVarNode(tp.getSubject)
      val p = alignVarNode(tp.getPredicate)
      val o = alignVarNode(tp.getObject)

      Triple.create(s, p, o)
    }

    def alignVarNode(node: Node) = {
      if(node.isVariable) {
        var name = node.getName
        if(name.startsWith("?")) {
          name = name.substring(1)
        }
        NodeFactory.createVariable(name)
      }
      node
    }
  }

}
