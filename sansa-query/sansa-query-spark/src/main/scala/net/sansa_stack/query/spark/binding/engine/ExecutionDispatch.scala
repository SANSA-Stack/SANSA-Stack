package net.sansa_stack.query.spark.binding.engine

import java.util

import org.apache.jena.sparql.algebra.op._
import org.apache.jena.sparql.algebra.{Op, OpVisitor}
import org.apache.jena.sparql.engine.binding.Binding
import org.apache.spark.rdd.RDD

class ExecutionDispatch(opExecutor: OpExecutor)
  extends OpVisitor
{
  val stack = new util.ArrayDeque[RDD[Binding]]()

  def exec(op: Op, input: RDD[Binding]): RDD[Binding] = {
    stack.push(input)
    val x = stack.size
    op.visit(this)
    val y = stack.size
    if (x != y) throw new RuntimeException("Possible stack misalignment")
    val result = stack.pop()
    result
  }
  override def visit(opBGP: OpBGP): Unit = ???

  override def visit(quadPattern: OpQuadPattern): Unit = ???

  override def visit(quadBlock: OpQuadBlock): Unit = ???

  override def visit(opTriple: OpTriple): Unit = ???

  override def visit(opQuad: OpQuad): Unit = ???

  override def visit(opPath: OpPath): Unit = ???

  override def visit(opFind: OpFind): Unit = ???

  override def visit(opTable: OpTable): Unit = ???

  override def visit(opNull: OpNull): Unit = ???

  override def visit(opProc: OpProcedure): Unit = ???

  override def visit(opPropFunc: OpPropFunc): Unit = ???

  override def visit(opFilter: OpFilter): Unit =
    stack.push(opExecutor.execute(opFilter, stack.pop()))

  override def visit(opGraph: OpGraph): Unit = ???

  override def visit(opService: OpService): Unit =
    stack.push(opExecutor.execute(opService, stack.pop()))

  override def visit(dsNames: OpDatasetNames): Unit = ???

  override def visit(opLabel: OpLabel): Unit = ???

  override def visit(opAssign: OpAssign): Unit = ???

  override def visit(opExtend: OpExtend): Unit =
    stack.push(opExecutor.execute(opExtend, stack.pop()))

  override def visit(opJoin: OpJoin): Unit = ???

  override def visit(opLeftJoin: OpLeftJoin): Unit = ???

  override def visit(opUnion: OpUnion): Unit =
    stack.push(opExecutor.execute(opUnion, stack.pop()))

  override def visit(opDiff: OpDiff): Unit = ???

  override def visit(opMinus: OpMinus): Unit = ???

  override def visit(opCondition: OpConditional): Unit = ???

  override def visit(opSequence: OpSequence): Unit = ???

  override def visit(opDisjunction: OpDisjunction): Unit = ???

  override def visit(opList: OpList): Unit = ???

  override def visit(opOrder: OpOrder): Unit =
    stack.push(opExecutor.execute(opOrder, stack.pop))

  override def visit(opProject: OpProject): Unit =
    stack.push(opExecutor.execute(opProject, stack.pop))

  override def visit(opReduced: OpReduced): Unit =
    stack.push(opExecutor.execute(opReduced, stack.pop))

  override def visit(opDistinct: OpDistinct): Unit =
    stack.push(opExecutor.execute(opDistinct, stack.pop))


  override def visit(opSlice: OpSlice): Unit =
    stack.push(opExecutor.execute(opSlice, stack.pop))


  override def visit(opGroup: OpGroup): Unit =
    stack.push(opExecutor.execute(opGroup, stack.pop))


  override def visit(opTop: OpTopN): Unit = ???
}
