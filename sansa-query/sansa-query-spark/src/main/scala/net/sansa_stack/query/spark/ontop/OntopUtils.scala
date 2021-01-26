package net.sansa_stack.query.spark.ontop

import java.util.Properties

import it.unibz.inf.ontop.exception.{MinorOntopInternalBugException, OBDASpecificationException, OntopInternalBugException}
import it.unibz.inf.ontop.injection.{OntopMappingSQLAllConfiguration, OntopMappingSQLAllOWLAPIConfiguration, OntopReformulationSQLConfiguration, OntopSQLOWLAPIConfiguration}
import it.unibz.inf.ontop.iq.exception.EmptyQueryException
import it.unibz.inf.ontop.iq.node.{ConstructionNode, NativeNode}
import it.unibz.inf.ontop.iq.{IQ, IQTree, UnaryIQTree}
import it.unibz.inf.ontop.model.`type`.TypeFactory
import it.unibz.inf.ontop.model.term._
import org.apache.commons.rdf.jena.JenaRDF
import org.apache.jena.datatypes.TypeMapper
import org.apache.jena.graph.{Node, NodeFactory}
import org.apache.jena.rdf.model.Model
import org.semanticweb.owlapi.model.OWLOntology

/**
 * @author Lorenz Buehmann
 */
object OntopUtils extends Serializable {

  def toNode(constant: RDFConstant, typeFactory: TypeFactory): Node = {
    val termType = constant.getType
    if (termType.isA(typeFactory.getIRITermType)) {
      NodeFactory.createURI(constant.asInstanceOf[IRIConstant].getIRI.getIRIString)
    } else if (termType.isA(typeFactory.getAbstractRDFSLiteral)) {
      val lit = constant.asInstanceOf[RDFLiteralConstant]
      val dt = TypeMapper.getInstance().getTypeByName(lit.getType.getIRI.getIRIString)
      NodeFactory.createLiteral(lit.getValue, dt)
    } else {
      null.asInstanceOf[Node]
    }
  }

  def evaluate(term: ImmutableTerm): Option[RDFConstant] = {
    val simplifiedTerm = term.simplify
    simplifiedTerm match {
      case constant: Constant =>
        if (constant.isInstanceOf[RDFConstant]) return Some(constant.asInstanceOf[RDFConstant])
        if (constant.isNull) return None
        if (constant.isInstanceOf[DBConstant]) throw new InvalidConstantTypeInResultException(constant +
          "is a DB constant. But a binding cannot have a DB constant as value")
        throw new InvalidConstantTypeInResultException("Unexpected constant type for " + constant)
      case _ =>
    }
    throw new InvalidTermAsResultException(simplifiedTerm)
  }

  @throws[EmptyQueryException]
  @throws[OntopInternalBugException]
  def extractSQLQuery(executableQuery: IQ): String = {
    val tree = executableQuery.getTree
    if (tree.isDeclaredAsEmpty) throw new EmptyQueryException
    val queryString = Option(tree)
      .filter((t: IQTree) => t.isInstanceOf[UnaryIQTree])
      .map((t: IQTree) => t.asInstanceOf[UnaryIQTree].getChild.getRootNode)
      .filter(n => n.isInstanceOf[NativeNode])
      .map(n => n.asInstanceOf[NativeNode])
      .map(_.getNativeQueryString)
      .getOrElse(throw new MinorOntopInternalBugException("The query does not have the expected structure " +
        "of an executable query\n" + executableQuery))
    if (queryString == "") throw new EmptyQueryException
    queryString
  }

  @throws[EmptyQueryException]
  def extractNativeNode(executableQuery: IQ): NativeNode = {
    val tree = executableQuery.getTree
    if (tree.isDeclaredAsEmpty) throw new EmptyQueryException
    Option(tree)
      .filter(t => t.isInstanceOf[UnaryIQTree])
      .map(t => t.asInstanceOf[UnaryIQTree].getChild.getRootNode)
      .filter(n => n.isInstanceOf[NativeNode])
      .map(n => n.asInstanceOf[NativeNode])
      .getOrElse(throw new MinorOntopInternalBugException("The query does not have the expected structure " +
        "for an executable query\n" + executableQuery))
  }

  @throws[EmptyQueryException]
  @throws[OntopInternalBugException]
  def extractRootConstructionNode(executableQuery: IQ): ConstructionNode = {
    val tree = executableQuery.getTree
    if (tree.isDeclaredAsEmpty) throw new EmptyQueryException
    Option(tree.getRootNode)
      .filter(n => n.isInstanceOf[ConstructionNode])
      .map(n => n.asInstanceOf[ConstructionNode])
      .getOrElse(throw new MinorOntopInternalBugException(
        "The \"executable\" query is not starting with a construction node\n" + executableQuery))
  }

  import scala.language.existentials
  @throws[OBDASpecificationException]
  private def loadOBDASpecification(obdaMappings: Model, properties: Properties, ontology: Option[OWLOntology]) = {
    val builder = if (ontology.nonEmpty) OntopMappingSQLAllOWLAPIConfiguration.defaultBuilder.ontology(ontology.get)
                  else OntopMappingSQLAllConfiguration.defaultBuilder

    val mappingConfiguration = builder
      .r2rmlMappingGraph(new JenaRDF().asGraph(obdaMappings))
      .jdbcUrl(OntopConnection.JDBC_URL)
      .jdbcUser(OntopConnection.JDBC_USER)
      .jdbcPassword(OntopConnection.JDBC_PASSWORD)
      .properties(properties)
      .enableTestMode
      .build
    mappingConfiguration.loadSpecification
  }

  @throws[OBDASpecificationException]
  def createReformulationConfig(obdaMappings: Model, properties: Properties, ontology: Option[OWLOntology] = None): OntopReformulationSQLConfiguration = {
    val obdaSpecification = loadOBDASpecification(obdaMappings, properties, ontology)

    val builder = if (ontology.nonEmpty) OntopSQLOWLAPIConfiguration.defaultBuilder
                                              .ontology(ontology.get)
                                              .properties(properties)
                                              .jdbcUser(OntopConnection.JDBC_USER)
                                              .jdbcPassword(OntopConnection.JDBC_PASSWORD)
                  else OntopReformulationSQLConfiguration.defaultBuilder

    builder
      .obdaSpecification(obdaSpecification)
      .properties(properties)
      .jdbcUrl(OntopConnection.JDBC_URL)
      .enableTestMode
      .build
  }

}

class InvalidTermAsResultException(term: ImmutableTerm) extends OntopInternalBugException(s"Term $term does not evaluate to a constant")
class InvalidConstantTypeInResultException(message: String) extends OntopInternalBugException(message)
