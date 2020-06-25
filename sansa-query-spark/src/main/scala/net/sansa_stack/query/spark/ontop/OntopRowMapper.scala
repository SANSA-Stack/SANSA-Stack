package net.sansa_stack.query.spark.ontop

import java.io.StringReader
import java.util.Properties

import scala.collection.JavaConverters._

import com.google.common.collect.ImmutableMap
import it.unibz.inf.ontop.exception.{MinorOntopInternalBugException, OntopInternalBugException}
import it.unibz.inf.ontop.injection.{OntopMappingSQLAllConfiguration, OntopModelConfiguration, OntopReformulationSQLConfiguration}
import it.unibz.inf.ontop.iq.exception.EmptyQueryException
import it.unibz.inf.ontop.iq.node.{ConstructionNode, NativeNode}
import it.unibz.inf.ontop.iq.{IQ, UnaryIQTree}
import it.unibz.inf.ontop.model.`type`.TypeFactory
import it.unibz.inf.ontop.model.term.impl.DBConstantImpl
import it.unibz.inf.ontop.model.term._
import it.unibz.inf.ontop.substitution.SubstitutionFactory
import org.apache.jena.datatypes.TypeMapper
import org.apache.jena.graph.{Node, NodeFactory}
import org.apache.jena.sparql.core.Var
import org.apache.jena.sparql.engine.binding.{Binding, BindingFactory}
import org.apache.spark.sql.Row

import net.sansa_stack.rdf.common.partition.core.{RdfPartitionComplex, RdfPartitionerComplex}

/**
 * @author Lorenz Buehmann
 */
class OntopRowMapper(obdaMappings: String,
                     properties: Properties,
                     partitions: Seq[RdfPartitionComplex],
                     sparqlQuery: String) {

  val metatdata = new MetadataProviderH2(OntopModelConfiguration.defaultBuilder.build()).generate(partitions)

  val mappingConfiguration = OntopMappingSQLAllConfiguration.defaultBuilder
    .nativeOntopMappingReader(new StringReader(obdaMappings))
    .properties(properties)
    .enableTestMode
    .build

  val obdaSpecification = mappingConfiguration.loadSpecification

  val reformulationConfiguration = OntopReformulationSQLConfiguration.defaultBuilder
    .obdaSpecification(obdaSpecification)
    .properties(properties)
    .enableTestMode
    .build

  val termFactory = reformulationConfiguration.getTermFactory
  val typeFactory = reformulationConfiguration.getTypeFactory
  val queryReformulator = reformulationConfiguration.loadQueryReformulator
  val substitutionFactory = reformulationConfiguration.getInjector.getInstance(classOf[SubstitutionFactory])
  val inputQueryFactory = queryReformulator.getInputQueryFactory

  val inputQuery = inputQueryFactory.createSPARQLQuery(sparqlQuery)

  val executableQuery = queryReformulator.reformulateIntoNativeQuery(inputQuery, queryReformulator.getQueryLoggerFactory.create())

  val constructionNode = extractRootConstructionNode(executableQuery)
  val nativeNode = extractNativeNode(executableQuery)
  val sqlSignature = nativeNode.getVariables
  val sqlTypeMap = nativeNode.getTypeMap
  val sparqlVar2Term = constructionNode.getSubstitution
  val answerAtom = executableQuery.getProjectionAtom

  @throws[EmptyQueryException]
  private def extractNativeNode(executableQuery: IQ): NativeNode = {
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
  private def extractRootConstructionNode(executableQuery: IQ): ConstructionNode = {
    val tree = executableQuery.getTree
    if (tree.isDeclaredAsEmpty) throw new EmptyQueryException
    Option(tree.getRootNode)
      .filter(n => n.isInstanceOf[ConstructionNode])
      .map(n => n.asInstanceOf[ConstructionNode])
      .getOrElse(throw new MinorOntopInternalBugException(
        "The \"executable\" query is not starting with a construction node\n" + executableQuery))
  }

  def map(row: Row): Binding = {
    toBinding(row)
  }

  def toBinding(row: Row): Binding = {
    println(row)
    val binding = BindingFactory.create()

    val builder = ImmutableMap.builder[Variable, Constant]

    val it = sqlSignature.iterator()
    for (i <- 0 until sqlSignature.size()) {
      val variable = it.next()
      val value: String = if (row.get(i) != null) row.get(i).toString else null
      val sqlType = sqlTypeMap.get(variable)
      builder.put(variable, new DBConstantImpl(value, sqlType))
    }
    val sub = substitutionFactory.getSubstitution(builder.build)

    val composition = sub.composeWith(sparqlVar2Term)
    val ontopBindings = answerAtom.getArguments.asScala.map(v => {
      (v, evaluate(composition.apply(v)))
    })

    ontopBindings.foreach {
      case (v, Some(term)) => binding.add(Var.alloc(v.getName), toNode(term, typeFactory))
      case _ =>
    }
    binding
  }

  private def toNode(constant: RDFConstant, typeFactory: TypeFactory): Node = {
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

  private def evaluate(term: ImmutableTerm): Option[RDFConstant] = {
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

  class InvalidTermAsResultException(term: ImmutableTerm) extends OntopInternalBugException("Term " + term + " does not evaluate to a constant")
  class InvalidConstantTypeInResultException(message: String) extends OntopInternalBugException(message)

}
