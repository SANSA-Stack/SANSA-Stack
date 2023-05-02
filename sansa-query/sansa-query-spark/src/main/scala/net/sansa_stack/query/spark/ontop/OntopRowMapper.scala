package net.sansa_stack.query.spark.ontop

import it.unibz.inf.ontop.com.google.common.collect.ImmutableMap
import it.unibz.inf.ontop.exception.OntopInternalBugException
import it.unibz.inf.ontop.model.term._
import it.unibz.inf.ontop.substitution.SubstitutionFactory
import org.apache.jena.rdf.model.Model
import org.apache.jena.sparql.core.Var
import org.apache.jena.sparql.engine.binding.{Binding, BindingFactory}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructField
import org.semanticweb.owlapi.model.OWLOntology

import java.util.Properties
import scala.collection.JavaConverters._

/**
 * Mapper of Spark DataFrame rows to other entities, e.g. binding, triple, ...
 *
 * @author Lorenz Buehmann
 */
class OntopRowMapper(sessionId: String,
                     database: Option[String],
                     obdaMappings: Model,
                     properties: Properties,
                     jdbcMetaData: Map[String, String],
                     sparqlQuery: String,
                     ontology: Option[OWLOntology],
                     rewriteInstruction: RewriteInstruction,
                     dbMetadata: String = ""
                    ) {

  val startTime = System.currentTimeMillis()

  // val reformulationConfiguration = OntopConnection(sessionId, database, obdaMappings, properties, jdbcMetaData, ontology)
  val reformulationConfiguration = OntopConnection(sessionId, dbMetadata, obdaMappings, properties, ontology)


  val termFactory = reformulationConfiguration.getTermFactory
  val typeFactory = reformulationConfiguration.getTypeFactory

  val substitutionFactory = reformulationConfiguration.getInjector.getInstance(classOf[SubstitutionFactory])

  val sqlSignature = rewriteInstruction.sqlSignature
  val sqlTypeMap = rewriteInstruction.sqlTypeMap
  val sparqlVar2Term = rewriteInstruction.sparqlVar2Term
  val answerAtom = rewriteInstruction.answerAtom

  val substitution = substitutionFactory.getSubstitution(sparqlVar2Term)

  import org.apache.spark.TaskContext
  val ctx = Option(TaskContext.get)
  val stageId = if (ctx.isDefined) ctx.get.stageId
  val partId = if (ctx.isDefined) ctx.get.partitionId()
  val taskId = if (ctx.isDefined) ctx.get.taskAttemptId()
  val hostname = if (ctx.isDefined) java.net.InetAddress.getLocalHost.getHostName

  // if (ctx.isDefined) println(s"row mapper setup at { Stage: $stageId, Partition: $partId, Host: $hostname, Task: $taskId } in ${System.currentTimeMillis() - startTime} ms")

  def map(row: Row): Binding = {
    toBinding(row)
  }

  def toBinding(row: Row): Binding = { // println(row)
    val startTime = System.currentTimeMillis()
    val bb = BindingFactory.builder

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

    val composition = sub.composeWith(substitution)
    val ontopBindings = answerAtom.getArguments.asScala.map(v => {
      (v, OntopUtils.evaluate(composition.apply(v)))
    })

    ontopBindings.foreach {
      case (v, Some(term)) => bb.add(Var.alloc(v.getName), OntopUtils.toNode(term, typeFactory))
      case _ =>
    }
    // if (ctx.isDefined) println(s"binding generated at { Stage: $stageId, Partition: $partId, Host: $hostname, Task: $taskId } in ${System.currentTimeMillis() - startTime} ms")

//    println(s"row: $row --- binding: $binding")
    bb.build()
  }

  val datatypeMappings = DatatypeMappings(typeFactory)
  def map(row: InternalRow, schema: Array[StructField]): Binding = {
    toBinding(row, schema)
  }

  def toBinding(row: InternalRow, schema: Array[StructField]): Binding = { // println(row)
    val startTime = System.currentTimeMillis()
    val bb = BindingFactory.builder

    val builder = ImmutableMap.builder[Variable, Constant]

    val it = sqlSignature.iterator()
    for (i <- 0 until sqlSignature.size()) {
      val variable = it.next()
      val sqlType = sqlTypeMap.get(variable)
      val value = row.get(i, schema(i).dataType)
      val constant = if (value == null) termFactory.getNullConstant else termFactory.getDBConstant(value.toString, sqlType)
      builder.put(variable, constant)
    }
    val sub = substitutionFactory.getSubstitution(builder.build)

    val composition = sub.composeWith(substitution)
    val ontopBindings = answerAtom.getArguments.asScala.map(v => {
      (v, OntopUtils.evaluate(composition.apply(v)))
    })

    ontopBindings.foreach {
      case (v, Some(term)) => bb.add(Var.alloc(v.getName), OntopUtils.toNode(term, typeFactory))
      case _ =>
    }
    if (ctx.isDefined) {
      println(s"binding generated at { Stage: $stageId, Partition: $partId, Host: $hostname, Task: $taskId } in ${System.currentTimeMillis() - startTime} ms")
    }

    //    println(s"row: $row --- binding: $binding")
    bb.build
  }

  def close(): Unit = {}

  class InvalidTermAsResultException(term: ImmutableTerm) extends OntopInternalBugException("Term " + term + " does not evaluate to a constant")
  class InvalidConstantTypeInResultException(message: String) extends OntopInternalBugException(message)

}
