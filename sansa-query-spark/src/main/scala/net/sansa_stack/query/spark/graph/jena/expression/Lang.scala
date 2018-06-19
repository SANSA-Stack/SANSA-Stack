package net.sansa_stack.query.spark.graph.jena.expression
import net.sansa_stack.query.spark.graph.jena.util.Result
import org.apache.jena.graph.Node

class Lang(expr:Expression) extends FunctionOne(expr) {

  private val tag = "Lang"

  override def getValue(result: Map[Node, Node]): Node = {
    // compiler here
    throw new UnsupportedOperationException
  }

  override def getValue(result: Result[Node]): Node = {
    var lang: Node = null
    expr match {
      case e: NodeVar => lang = result.getValue(e.getNode)
      case e: NodeVal => lang = e.getNode
      case _          => throw new TypeNotPresentException("Variable or Value", new Throwable)
    }
    lang
  }

  override def getTag: String = { tag }
}
