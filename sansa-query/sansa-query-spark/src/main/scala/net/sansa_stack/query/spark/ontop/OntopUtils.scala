package net.sansa_stack.query.spark.ontop

import it.unibz.inf.ontop.exception.{MinorOntopInternalBugException, OBDASpecificationException, OntopInternalBugException}
import it.unibz.inf.ontop.injection._
import it.unibz.inf.ontop.iq.exception.EmptyQueryException
import it.unibz.inf.ontop.iq.node.{ConstructionNode, NativeNode}
import it.unibz.inf.ontop.iq.{IQ, IQTree, UnaryIQTree}
import it.unibz.inf.ontop.model.`type`.TypeFactory
import it.unibz.inf.ontop.model.term._
import org.apache.commons.rdf.jena.JenaRDF
import org.apache.jena.datatypes.TypeMapper
import org.apache.jena.datatypes.xsd.XSDDatatype
import org.apache.jena.graph.{Node, NodeFactory}
import org.apache.jena.rdf.model.Model
import org.semanticweb.owlapi.model.OWLOntology

import java.io.StringReader
import java.util.Properties

/**
 * @author Lorenz Buehmann
 */
object OntopUtils extends Serializable {

  val fixScientificNotation: Boolean = true
  import java.text.NumberFormat
  val formatter = NumberFormat.getInstance()
  formatter.setMaximumIntegerDigits(Integer.MAX_VALUE)
  formatter.setMaximumFractionDigits(Integer.MAX_VALUE)


  /**
   * Convert a constant to a Jena [[Node]]
   * @param constant the constant
   * @param typeFactory a type factory
   * @return the Jena [[Node]]
   */
  def toNode(constant: RDFConstant, typeFactory: TypeFactory): Node = {
    val termType = constant.getType
    if (termType.isA(typeFactory.getIRITermType)) {
      NodeFactory.createURI(constant.asInstanceOf[IRIConstant].getIRI.getIRIString)
    } else if (termType.isA(typeFactory.getAbstractRDFSLiteral)) {
      val lit = constant.asInstanceOf[RDFLiteralConstant]
      // parse datatype
      val litType = lit.getType
      val dt = TypeMapper.getInstance().getTypeByName(litType.getIRI.getIRIString)

      // workaround scientific notation in decimal values
      var lexicalForm = lit.getValue
      if (fixScientificNotation && dt == XSDDatatype.XSDdecimal) {
        val d = java.lang.Double.parseDouble(lexicalForm)
        lexicalForm = formatter.format(d)
      }
      // parse lang tag
      val lang = if (litType.getLanguageTag.isPresent) litType.getLanguageTag.get().getFullString else null

      NodeFactory.createLiteral(lexicalForm, lang, dt)
    } else if (termType.isA(typeFactory.getBlankNodeType)) {
      NodeFactory.createBlankNode(constant.asInstanceOf[BNode].getInternalLabel)
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
  private def loadOBDASpecification[C <: OntopMappingSQLAllConfiguration.Builder[C]](database: Option[String],
                                                                                     obdaMappings: Model,
                                                                                     properties: Properties,
                                                                                     ontology: Option[OWLOntology]) = {
    val builder = (if (ontology.nonEmpty) OntopMappingSQLAllOWLAPIConfiguration.defaultBuilder.ontology(ontology.get)
                    else OntopMappingSQLAllConfiguration.defaultBuilder)
      .asInstanceOf[C]

    val mappingConfiguration = builder
      .r2rmlMappingGraph(new JenaRDF().asGraph(obdaMappings))
      .jdbcUrl(OntopConnection.getConnectionURL(database))
      .jdbcUser(OntopConnection.JDBC_USER)
      .jdbcPassword(OntopConnection.JDBC_PASSWORD)
      .properties(properties)
      .enableTestMode
      .build
    mappingConfiguration.loadSpecification
  }

  @throws[OBDASpecificationException]
  private def loadOBDASpecification[C <: OntopMappingSQLAllConfiguration.Builder[C]](dbMetadata: String,
                                                                                     obdaMappings: Model,
                                                                                     properties: Properties,
                                                                                     ontology: Option[OWLOntology]) = {
    val builder = (if (ontology.nonEmpty) OntopMappingSQLAllOWLAPIConfiguration.defaultBuilder.ontology(ontology.get)
    else OntopMappingSQLAllConfiguration.defaultBuilder)
      .asInstanceOf[C]

    val mappingConfiguration = builder
      .r2rmlMappingGraph(new JenaRDF().asGraph(obdaMappings))
      .dbMetadataReader(new StringReader(dbMetadata))
      .properties(properties)
      .enableTestMode
      .build
    mappingConfiguration.loadSpecification
  }

  @throws[OBDASpecificationException]
  def createReformulationConfig[B <: OntopSQLOWLAPIConfiguration.Builder[B], C <: OntopReformulationSQLConfiguration.Builder[C]](database: Option[String],
                                                                                                                                 obdaMappings: Model,
                                                                                                                                 properties: Properties,
                                                                                                                                 ontology: Option[OWLOntology]): OntopReformulationSQLConfiguration = {
    val obdaSpecification = loadOBDASpecification(database, obdaMappings, properties, ontology)

    val builder = (if (ontology.nonEmpty) OntopSQLOWLAPIConfiguration.defaultBuilder.asInstanceOf[B]
      .ontology(ontology.get)
      .properties(properties)
      .jdbcUser(OntopConnection.JDBC_USER)
      .jdbcPassword(OntopConnection.JDBC_PASSWORD)
    else OntopReformulationSQLConfiguration.defaultBuilder).asInstanceOf[C]

    builder
      .obdaSpecification(obdaSpecification)
      .properties(properties)
      .jdbcUrl(OntopConnection.getConnectionURL(database))
      .enableTestMode
      .build
  }

  @throws[OBDASpecificationException]
  def createReformulationConfig[B <: OntopSQLOWLAPIConfiguration.Builder[B], C <: OntopReformulationSQLConfiguration.Builder[C]](dbMetadata: String,
                                                                                                                                 obdaMappings: Model,
                                                                                                                                 properties: Properties,
                                                                                                                                 ontology: Option[OWLOntology]): OntopReformulationSQLConfiguration = {
    val obdaSpecification = loadOBDASpecification(dbMetadata, obdaMappings, properties, ontology)

    val builder = OntopSQLOWLAPIConfiguration.defaultBuilder.asInstanceOf[B]
      .properties(properties)
      .obdaSpecification(obdaSpecification)
      .enableTestMode
    if (ontology.isDefined) builder.ontology(ontology.get)

    builder.build
  }

  def createMappingConfig[B <: OntopMappingSQLConfiguration.Builder[B]](properties: Properties,
                                                                        database: Option[String]): OntopMappingSQLConfiguration = {
    OntopMappingSQLConfiguration.defaultBuilder.asInstanceOf[B]
      .properties(properties)
      .jdbcUrl(OntopConnection.getConnectionURL(database))
      .jdbcUser(OntopConnection.JDBC_USER)
      .jdbcPassword(OntopConnection.JDBC_PASSWORD)
      .build
  }

}

class InvalidTermAsResultException(term: ImmutableTerm) extends OntopInternalBugException(s"Term $term does not evaluate to a constant")
class InvalidConstantTypeInResultException(message: String) extends OntopInternalBugException(message)
