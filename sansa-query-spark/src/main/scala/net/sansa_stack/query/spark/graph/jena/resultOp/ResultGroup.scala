package net.sansa_stack.query.spark.graph.jena.resultOp

import org.apache.jena.graph.{Node, NodeFactory}
import org.apache.jena.sparql.algebra.op.OpGroup
import org.apache.jena.sparql.expr.ExprAggregator

import scala.collection.JavaConversions._

/**
  * Class that execute SPARQL GROUP BY operation.
  * @param op Group By operator
  */
class ResultGroup(op: OpGroup) extends ResultOp {

  private val tag = "GROUP BY"

  override def execute(input: Array[Map[Node, Node]]): Array[Map[Node, Node]] = {
    val vars = op.getGroupVars.getVars.toList   // e.g. List(?user)
    val aggregates = op.getAggregators.toList   // e.g. List((AGG ?.0 AVG(?age)), (AGG ?.1 MAX(?age)), (AGG ?.2 MIN(?age)))
    var intermediate = input
    vars.length match {
      case 1 =>
        aggregates.foreach{ aggr =>
          input.groupBy(map => map(vars.head)).foreach{ case(node, array) =>
            intermediate = intermediate.map{ mapping =>
              if(mapping(vars.head).equals(node)){
                val c = mapping ++ aggregateOp(array, aggr: ExprAggregator)
                c
              } else{ mapping }
            }
          }
        }
    }
    val output = intermediate
    output
  }

  override def getTag: String = { tag }

  private def aggregateOp(input: Array[Map[Node, Node]], aggr: ExprAggregator): Map[Node, Node] = {
    val key = aggr.getAggregator.getExprList.head.getExprVar.getAsNode    // e.g. ?age
    val seq = input.map(_(key).getLiteralValue.toString.toDouble)
    val result = seq.aggregate((0.0, 0))(
      (acc, value) => (acc._1 + value, acc._2 + 1),
      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
    if(aggr.getAggregator.key().contains("sum")) {                          // e.g. (sum, ?age)
      Map(aggr.getVar.asNode() -> NodeFactory.createLiteral(result._1.toString))
    }
    else if(aggr.getAggregator.key().contains("avg")) {                    // e.g. (avg, ?age)
      Map(aggr.getVar.asNode() -> NodeFactory.createLiteral((result._1 / result._2).toString))
    }
    else if(aggr.getAggregator.key().contains("count")) {                 // e.g. (count, ?age)
      Map(aggr.getVar.asNode() -> NodeFactory.createLiteral(result._2.toString))
    }
    else if(aggr.getAggregator.key().contains("max")) {                   // e.g. (max, ?age)
      Map(aggr.getVar.asNode() -> NodeFactory.createLiteral(seq.max.toString))
    }
    else if(aggr.getAggregator.key().contains("min")) {                   // e.g. (min, ?age)
      Map(aggr.getVar.asNode() -> NodeFactory.createLiteral(seq.min.toString))
    }
    else {
      Map(aggr.getVar.asNode() -> NodeFactory.createBlankNode())
    }
  }
}
