package net.sansa_stack.query.spark.graph.jena

import net.sansa_stack.query.spark.graph.jena.patternOp._
import net.sansa_stack.query.spark.graph.jena.resultOp._
import org.apache.jena.query.QueryFactory
import org.apache.jena.graph.Triple
import org.apache.jena.sparql.algebra.{Algebra, Op, OpVisitorBase, OpWalker}
import org.apache.jena.sparql.algebra.op._
import org.apache.jena.sparql.expr.{E_Exists, E_NotExists}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
  * Read sparql query from a file and convert to Op expressions.
  *
  * @param path path to sparql query file
  *
  * @author Zhe Wang
  */
class SparqlParser(path: String, op: Op) extends OpVisitorBase with Serializable {

  def this(path: String) {
    this(path, Algebra.compile(QueryFactory.create(Source.fromFile(path).mkString)))
  }

  def this(op: Op) {
    this("", op)
  }

  private val elementTriples = ArrayBuffer[Triple]()
  private val ops = new mutable.Queue[Ops]()

  OpWalker.walk(op, this)

  override def visit(opBGP: OpBGP): Unit = {
    val triples = opBGP.getPattern.toList
    for (triple <- triples) {
      elementTriples += triple
    }
    //if(ops.isEmpty){ ops.enqueue(new PatternBgp(elementTriples.toIterator)) }
    ops.enqueue(new PatternBgp(opBGP))
  }

  override def visit(opDistinct: OpDistinct): Unit = {
    ops.enqueue(new ResultDistinct(opDistinct))
    //ops.enqueue(new NewDistinct)
  }

  override def visit(opExtend: OpExtend): Unit = {
    ops.enqueue(new ResultExtend(opExtend))
  }

  override def visit(opFilter: OpFilter): Unit = {
    opFilter.getExprs.foreach{
      // Add triple pattern in filter expression EXISTS to elementTriples
      case e: E_Exists => val triples = e.getGraphPattern.asInstanceOf[OpBGP].getPattern
        for(triple <- triples) {
          elementTriples += triple
        }
        //ops.head.asInstanceOf[PatternBgp].setBgp(elementTriples.toIterator)
      case e: E_NotExists => val triples = e.getGraphPattern.asInstanceOf[OpBGP].getPattern
        for(triple <- elementTriples){
          triples.add(triple)
        }
        ops.enqueue(new PatternNegate(triples.toIterator))
      case other => ops.enqueue(new ResultFilter(other))
    }
  }

  override def visit(opGroup: OpGroup): Unit = {
    ops.enqueue(new ResultGroup(opGroup))
  }

  override def visit(opLeftJoin: OpLeftJoin): Unit = {
    /*val sp = new SparqlParser(opLeftJoin.getRight)
    ops.enqueue(new PatternOptional(sp.getElementTriples.toIterator, opLeftJoin.getExprs))*/
    ops.enqueue(new PatternOptional(opLeftJoin))
  }

  override def visit(opMinus: OpMinus): Unit = {
    val triples = opMinus.getRight.asInstanceOf[OpBGP].getPattern
    ops.enqueue(new PatternNegate(triples.toIterator))
  }

  override def visit(opOrder: OpOrder): Unit = {
    ops.enqueue(new ResultOrder(opOrder))
  }

  override def visit(opProject: OpProject): Unit = {
    ops.enqueue(new ResultProject(opProject))
    //ops.enqueue(new NewProject(opProject))
  }

  override def visit(opReduced: OpReduced): Unit = {
    ops.enqueue(new ResultReduced)
  }

  override def visit(opSlice: OpSlice): Unit = {
    ops.enqueue(new ResultSlice(opSlice))
  }

  override def visit(opUnion: OpUnion): Unit = {
    val sp = new SparqlParser(opUnion.getRight)
    sp.getOps.dequeue()
    sp.getOps.foreach(op => ops.dequeueFirst {
      case e: ResultFilter => e.getExpr.equals(op.asInstanceOf[ResultFilter].getExpr)
      case _               => false
    })
    ops.enqueue(new PatternUnion(opUnion))
  }

  def getOp: Op = {
    op
  }

  def getOps: mutable.Queue[Ops] = {
    ops
  }

  def getElementTriples: ArrayBuffer[Triple] = {
    elementTriples
  }
}
