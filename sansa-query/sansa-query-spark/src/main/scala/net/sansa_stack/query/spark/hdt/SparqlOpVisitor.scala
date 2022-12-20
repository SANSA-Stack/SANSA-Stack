package net.sansa_stack.query.spark.hdt

import java.util

import org.apache.jena.graph.{Node, Triple}
import org.apache.jena.sparql.algebra.OpVisitor
import org.apache.jena.sparql.algebra.op._
import org.apache.jena.sparql.core.{Quad, Var}
import org.apache.jena.sparql.expr.ExprAggregator

/**
  * @author David Ibhaluobe, Gezim Sejdiu
  */
object SparqlOpVisitor extends OpVisitor {

  val whereCondition = new util.ArrayList[Triple]()
  val triples = new util.ArrayList[Quad]()
  val subjects = new util.ArrayList[Node]()
  val predicates = new util.ArrayList[Node]()
  val objects = new util.ArrayList[Node]()
  val varList = new util.ArrayList[Var]()
  val filters = new util.ArrayList[org.apache.jena.sparql.expr.Expr]()
  val aggregatorList = new util.ArrayList[ExprAggregator]()
  var isDistinctEnabled = false
  val optional = new util.ArrayList[Boolean]()
  var indexOptional = 0

  def reset(): Unit = {
    whereCondition.clear()
    triples.clear()
    subjects.clear()
    predicates.clear()
    objects.clear()
    varList.clear()
    filters.clear()
    isDistinctEnabled = false
  }

  override def visit(opBGP: OpBGP): Unit = {
    whereCondition.addAll(opBGP.getPattern.getList)
    indexOptional += 1
    for (i <- 0 until whereCondition.size()) {
      subjects.add(i, whereCondition.get(i).getSubject)
      objects.add(i, whereCondition.get(i).getObject)
      predicates.add(i, whereCondition.get(i).getPredicate)
      optional.add(false)
    }

  }

  override def visit(opQuadPattern: OpQuadPattern): Unit = {
    triples.addAll(opQuadPattern.getPattern.getList)
  }

  override def visit(opQuadBlock: OpQuadBlock): Unit = {
  }

  override def visit(opTriple: OpTriple): Unit = {
  }

  override def visit(opQuad: OpQuad): Unit = {
  }

  override def visit(opPath: OpPath): Unit = {
  }

  override def visit(opTable: OpTable): Unit = {
  }

  override def visit(opNull: OpNull): Unit = {
  }

  override def visit(opProcedure: OpProcedure): Unit = {
  }

  override def visit(opPropFunc: OpPropFunc): Unit = {
  }

  override def visit(opFilter: OpFilter): Unit = {
    filters.addAll(opFilter.getExprs.getList)
  }

  override def visit(opGraph: OpGraph): Unit = {
  }

  override def visit(opService: OpService): Unit = {
  }

  override def visit(opDatasetNames: OpDatasetNames): Unit = {
  }

  override def visit(opLabel: OpLabel): Unit = {
  }

  override def visit(opAssign: OpAssign): Unit = {
  }

  override def visit(opExtend: OpExtend): Unit = {
  }

  override def visit(opJoin: OpJoin): Unit = {
  }

  override def visit(opLeftJoin: OpLeftJoin): Unit = {
    optional.set(indexOptional - 1, true)
  }

  override def visit(opUnion: OpUnion): Unit = {
  }

  override def visit(opDiff: OpDiff): Unit = {
  }

  override def visit(opMinus: OpMinus): Unit = {
  }

  override def visit(opConditional: OpConditional): Unit = {
  }

  override def visit(opSequence: OpSequence): Unit = {
  }

  override def visit(opDisjunction: OpDisjunction): Unit = {
  }

  override def visit(opExt: OpExt): Unit = {
  }

  override def visit(opList: OpList): Unit = {
  }

  override def visit(opOrder: OpOrder): Unit = {
  }

  override def visit(opProject: OpProject): Unit = {
    varList.addAll(opProject.getVars)
  }

  override def visit(opReduced: OpReduced): Unit = {
  }

  override def visit(opDistinct: OpDistinct): Unit = {
    if (opDistinct.getName.equals("distinct")) {
      isDistinctEnabled = true
    }
  }

  override def visit(opSlice: OpSlice): Unit = {
  }

  override def visit(opGroup: OpGroup): Unit = {
    aggregatorList.addAll(opGroup.getAggregators)
  }

  override def visit(opTopN: OpTopN): Unit = {
  }

  override def visit(op: OpLateral): Unit = {
  }

}
