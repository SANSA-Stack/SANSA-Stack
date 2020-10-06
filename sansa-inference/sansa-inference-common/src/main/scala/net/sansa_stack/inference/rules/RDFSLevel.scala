package net.sansa_stack.inference.rules

/**
  * The ForwardRuleReasonerRDFS can be configured to work at three different compliance levels:
  *
  * <p>
  * <b>Full</b><br/>
  * This implements all of the RDFS axioms and closure rules with the exception of bNode entailments and datatypes
  * (rdfD 1). See above for comments on these. This is an expensive mode because all statements in the data graph
  * need to be checked for possible use of container membership properties. It also generates type assertions
  * for all resources and properties mentioned in the data (rdf1, rdfs4a, rdfs4b).
  *</p>
  *
  * <p>
  * <b>Default</b><br/>
  * This omits the expensive checks for container membership properties and the "everything is a resource" and
  * "everything used as a property is one" rules (rdf1, rdfs4a, rdfs4b).
  * This mode does include all the axiomatic rules. Thus, for example, even materializing an "empty" RDF graph will
  * return triples such as [rdf:type rdfs:range rdfs:Class].
  * </p>
  *
  * <p>
  * <b>Simple</b><br/>
  * This implements just the transitive closure of subPropertyOf and subClassOf relations, the domain and range
  * entailments and the implications of subPropertyOf and subClassOf. It omits all of the axioms. This is probably
  * the most useful mode but is not the default because it is a less complete implementation of the standard.
  * </p>
  *
  * @author Lorenz Buehmann
  */
object RDFSLevel extends Enumeration {
  type RDFSLevel = Value
  val FULL, DEFAULT, SIMPLE = Value
}
