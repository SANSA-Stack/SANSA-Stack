package net.sansa_stack.query.spark.ontop

import java.io.StringReader
import java.sql.{Connection, DriverManager, SQLException}
import java.util.Properties

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.google.common.collect.ImmutableMap
import it.unibz.inf.ontop.answering.reformulation.input.{ConstructQuery, ConstructTemplate}
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
import org.apache.jena.graph.{Node, NodeFactory, Triple}
import org.apache.jena.sparql.core.Var
import org.apache.jena.sparql.engine.binding.{Binding, BindingFactory}
import org.apache.spark.sql.Row
import org.eclipse.rdf4j.model.{IRI, Literal}
import org.eclipse.rdf4j.query.algebra.{ProjectionElem, ValueConstant, ValueExpr}

import net.sansa_stack.rdf.common.partition.core.{RdfPartitionComplex, RdfPartitionerComplex}

/**
 * Mapper of Spark DataFrame rows to other entities, e.g. binding, triple, ...
 *
 * @author Lorenz Buehmann
 */
class OntopRowMapper(
                     obdaMappings: String,
                     properties: Properties,
                     partitions: Set[RdfPartitionComplex],
                     sparqlQuery: String,
                     id: String = "id") {

//  val metatdata = new MetadataProviderH2(OntopModelConfiguration.defaultBuilder.build()).generate(partitions)

  // create the tmp DB needed for Ontop
  private val JDBC_URL = "jdbc:h2:mem:sansaontopdb;DATABASE_TO_UPPER=FALSE;DB_CLOSE_DELAY=-1"
  private val JDBC_USER = "sa"
  private val JDBC_PASSWORD = ""

  private val connection: Connection = try {
    DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASSWORD)
  } catch {
    case e: SQLException =>
      throw e
  }

  val reformulationConfiguration = {
    JDBCDatabaseGenerator.generateTables(connection, partitions)

    val mappingConfiguration = {
      OntopMappingSQLAllConfiguration.defaultBuilder
        .nativeOntopMappingReader(new StringReader(obdaMappings))
        .properties(properties)
        .jdbcUrl(JDBC_URL)
        .jdbcUser(JDBC_USER)
        .jdbcPassword(JDBC_PASSWORD)
        .enableTestMode
        .build
    }

    val obdaSpecification = mappingConfiguration.loadSpecification

    OntopReformulationSQLConfiguration.defaultBuilder
      .obdaSpecification(obdaSpecification)
      .properties(properties)
      .jdbcUrl(JDBC_URL)
      .enableTestMode
      .build
  }

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
    println(s"row: $row")
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
      if (vc.getValue.isInstanceOf[IRI]) {
        constant = NodeFactory.createURI(vc.getValue.stringValue)
      } else if (vc.getValue.isInstanceOf[Literal]) {
        val lit = vc.getValue.asInstanceOf[Literal]
        val dt = TypeMapper.getInstance().getTypeByName(lit.getDatatype.toString)
        constant = NodeFactory.createLiteral(vc.getValue.stringValue, dt)
      } else {
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

  def close(): Unit = {
    try {
      connection.close()
    } catch {
      case e: SQLException => throw e
    }
  }

  class InvalidTermAsResultException(term: ImmutableTerm) extends OntopInternalBugException("Term " + term + " does not evaluate to a constant")
  class InvalidConstantTypeInResultException(message: String) extends OntopInternalBugException(message)

}
