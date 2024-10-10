package net.sansa_stack.query.spark.graph.jena

import net.sansa_stack.query.spark.graph.jena.patternOp._
import net.sansa_stack.query.spark.graph.jena.resultOp._
import org.apache.jena.query.QueryFactory
import org.apache.jena.sparql.algebra.op._
import org.apache.jena.sparql.algebra.{Algebra, Op, OpVisitorBase, OpWalker}

import scala.collection.mutable
import scala.io.Source

/**
 * Read sparql query from a file and convert to Op expressions.
 * @param path path to sparql query file
 *
 * @author Zhe Wang
 */
class SparqlParser(path: String, op: Op) extends OpVisitorBase with Serializable {

  def this(path: String) = {
    this(path, Algebra.compile(QueryFactory.create(Source.fromFile(path).mkString)))
  }

  def this(op: Op) = {
    this("", op)
  }

  private val ops = new mutable.Queue[Ops]()

  OpWalker.walk(op, this)

  override def visit(opBGP: OpBGP): Unit = {
    ops.enqueue(new PatternBgp(opBGP))
  }

  override def visit(opDistinct: OpDistinct): Unit = {
    ops.enqueue(new ResultDistinct(opDistinct))
  }

  override def visit(opExtend: OpExtend): Unit = {
    ops.enqueue(new ResultExtend(opExtend))
  }

  override def visit(opFilter: OpFilter): Unit = {
    ops.enqueue(new ResultFilter(opFilter))
  }

  override def visit(opGroup: OpGroup): Unit = {
    ops.enqueue(new ResultGroup(opGroup))
  }

  override def visit(opLeftJoin: OpLeftJoin): Unit = {
    ops.enqueue(new PatternOptional(opLeftJoin))
  }

  override def visit(opMinus: OpMinus): Unit = {
    ops.enqueue(new PatternMinus(opMinus))
  }

  override def visit(opOrder: OpOrder): Unit = {
    ops.enqueue(new ResultOrder(opOrder))
  }

  override def visit(opProject: OpProject): Unit = {
    ops.enqueue(new ResultProject(opProject))
  }

  override def visit(opReduced: OpReduced): Unit = {
    ops.enqueue(new ResultReduced(opReduced))
  }

  override def visit(opSlice: OpSlice): Unit = {
    ops.enqueue(new ResultSlice(opSlice))
  }

  override def visit(opUnion: OpUnion): Unit = {
    ops.enqueue(new PatternUnion(opUnion))
  }

  def getOp: Op = {
    op
  }

  def getOps: mutable.Queue[Ops] = {
    ops
    // reOrderOps(ops)
  }

  private def reOrderOps(ops: mutable.Queue[Ops]): mutable.Queue[Ops] = {
    val tags = ops.toIterator.map(op => op.getTag)
    if (tags.contains("ORDER BY")) {
      val order = ops.dequeueFirst(op => op.getTag.equals("ORDER BY"))
      ops.enqueue(order.get)
      ops
    } else {
      ops
    }
  }
}
