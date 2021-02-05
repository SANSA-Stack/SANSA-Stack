package net.sansa_stack.query.spark.ontop

import java.util.Properties

import scala.collection.JavaConverters._
import scala.collection.mutable

import it.unibz.inf.ontop.answering.reformulation.input.{ConstructQuery, ConstructTemplate}
import it.unibz.inf.ontop.com.google.common.collect.ImmutableMap
import it.unibz.inf.ontop.exception.OntopInternalBugException
import it.unibz.inf.ontop.iq.IQ
import it.unibz.inf.ontop.model.`type`.TypeFactory
import it.unibz.inf.ontop.model.term._
import it.unibz.inf.ontop.substitution.SubstitutionFactory
import org.apache.jena.datatypes.TypeMapper
import org.apache.jena.graph.{Node, NodeFactory, Triple}
import org.apache.jena.rdf.model.Model
import org.apache.jena.sparql.core.Var
import org.apache.jena.sparql.engine.binding.{Binding, BindingFactory}
import org.apache.spark.sql.Row
import org.eclipse.rdf4j.model.{IRI, Literal}
import org.eclipse.rdf4j.query.algebra.{ProjectionElem, ValueConstant, ValueExpr}
import org.semanticweb.owlapi.model.OWLOntology

import net.sansa_stack.rdf.common.partition.core.{RdfPartitionStateDefault, RdfPartitioner}

/**
 * Mapper of Spark DataFrame rows to other entities, e.g. binding, triple, ...
 *
 * @author Lorenz Buehmann
 */
class OntopRowMapper2(sessionId: String,
                      database: Option[String],
                      obdaMappings: Model,
                      properties: Properties,
                      jdbcMetaData: Map[String, String],
                      sparqlQuery: String,
                      ontology: Option[OWLOntology],

                     ) {



  val reformulationConfiguration = OntopConnection(sessionId, database, obdaMappings, properties, jdbcMetaData, ontology)

  val termFactory = reformulationConfiguration.getTermFactory
  val typeFactory = reformulationConfiguration.getTypeFactory
  val queryReformulator = reformulationConfiguration.loadQueryReformulator
  val substitutionFactory = reformulationConfiguration.getInjector.getInstance(classOf[SubstitutionFactory])
  val inputQueryFactory = queryReformulator.getInputQueryFactory

  val inputQuery = inputQueryFactory.createSPARQLQuery(sparqlQuery)

  val executableQuery = queryReformulator.reformulateIntoNativeQuery(inputQuery,
    queryReformulator.getQueryLoggerFactory.create(it.unibz.inf.ontop.com.google.common.collect.ImmutableMultimap.of()))

  val constructionNode = OntopUtils.extractRootConstructionNode(executableQuery)
  val nativeNode = OntopUtils.extractNativeNode(executableQuery)
  val sqlSignature = nativeNode.getVariables
  val sqlTypeMap = nativeNode.getTypeMap
  val sparqlVar2Term = constructionNode.getSubstitution
  val answerAtom = executableQuery.getProjectionAtom

  println(executableQuery)



  def map(row: Row): Binding = {
    toBinding(row)
  }

  def toBinding(row: Row): Binding = { println(row)
    val binding = BindingFactory.create()

    val builder = ImmutableMap.builder[Variable, Constant]

    val it = sqlSignature.iterator()
    for (i <- 0 until sqlSignature.size()) {
      val variable = it.next()
      val value = row.get(i)
      val sqlType = sqlTypeMap.get(variable)
      val constant = if (value == null) termFactory.getNullConstant else termFactory.getDBConstant(value.toString, sqlType)
      builder.put(variable, constant)
    }
    val sub = substitutionFactory.getSubstitution(builder.build)

    val composition = sub.composeWith(sparqlVar2Term)
    val ontopBindings = answerAtom.getArguments.asScala.map(v => {
      (v, OntopUtils.evaluate(composition.apply(v)))
    })

    ontopBindings.foreach {
      case (v, Some(term)) => binding.add(Var.alloc(v.getName), toNode(term, typeFactory))
      case _ =>
    }
//    println(s"row: $row --- binding: $binding")
    binding
  }

  def toTriples(rows: Iterator[Row]): Iterator[Triple] = {
    val constructTemplate = inputQuery.asInstanceOf[ConstructQuery].getConstructTemplate

    val ex = constructTemplate.getExtension
    var extMap: Map[String, ValueExpr] = null
    if (ex != null) {
      extMap = ex.getElements.asScala.map(e => (e.getName, e.getExpr)).toMap
    }

    val bindings = rows.map(toBinding)
    bindings.flatMap (binding => toTriples(binding, constructTemplate, extMap))
  }

  /**
   * Convert a single binding to a set of triples.
   */
  private def toTriples(binding: Binding, constructTemplate: ConstructTemplate, extMap: Map[String, ValueExpr]): mutable.Buffer[Triple] = {
    val l = constructTemplate.getProjectionElemList.asScala
    l.flatMap { peList =>
      val size = peList.getElements.size()

      var triples = scala.collection.mutable.Set[Triple]()

      for (i <- 0 until (size/3)) {

        val s = getConstant(peList.getElements.get(i * 3), binding, extMap)
        val p = getConstant(peList.getElements.get(i * 3 + 1), binding, extMap)
        val o = getConstant(peList.getElements.get(i * 3 + 2), binding, extMap)

        // A triple can only be constructed when none of bindings is missing
        if (s == null || p == null || o == null) {

        } else {
          triples += Triple.create(s, p, o)
        }
      }
      triples
    }
  }

  /**
   * Convert each node in a CONSTRUCT template to an RDF node.
   */
  private def getConstant(node: ProjectionElem, binding: Binding, extMap: Map[String, ValueExpr]): Node = {
    var constant: Node = null

    val node_name = node.getSourceName

    val ve: Option[ValueExpr] = if (extMap != null) extMap.get(node_name) else None

    // for constant terms in the template
    if (ve.isDefined && ve.get.isInstanceOf[ValueConstant]) {
      val vc = ve.get.asInstanceOf[ValueConstant]
      vc.getValue match {
        case _: IRI =>
          constant = NodeFactory.createURI(vc.getValue.stringValue)
        case lit: Literal =>
          val dt = TypeMapper.getInstance().getTypeByName(lit.getDatatype.toString)
          constant = NodeFactory.createLiteral(vc.getValue.stringValue, dt)
        case _ =>
          constant = NodeFactory.createBlankNode(vc.getValue.stringValue)
      }
    } else { // for variable bindings
      constant = binding.get(Var.alloc(node_name))
    }

    constant
  }

  private def toNode(constant: RDFConstant, typeFactory: TypeFactory): Node = {
    val termType = constant.getType
    if (termType.isA(typeFactory.getIRITermType)) {
      NodeFactory.createURI(constant.asInstanceOf[IRIConstant].getIRI.getIRIString)
    } else if (termType.isA(typeFactory.getAbstractRDFSLiteral)) {
      val lit = constant.asInstanceOf[RDFLiteralConstant]
      val litType = lit.getType
      val dt = TypeMapper.getInstance().getTypeByName(litType.getIRI.getIRIString)
      val lang = if (litType.getLanguageTag.isPresent) litType.getLanguageTag.get().getFullString else null
      NodeFactory.createLiteral(lit.getValue, lang, dt)
    } else if (termType.isA(typeFactory.getBlankNodeType)) {
      NodeFactory.createBlankNode(constant.asInstanceOf[BNode].getInternalLabel)
    } else {
      null.asInstanceOf[Node]
    }
  }

  def close(): Unit = {

  }

  class InvalidTermAsResultException(term: ImmutableTerm) extends OntopInternalBugException("Term " + term + " does not evaluate to a constant")
  class InvalidConstantTypeInResultException(message: String) extends OntopInternalBugException(message)

}
