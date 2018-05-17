package net.sansa_stack.query.spark.graph.jena.patternOp

import net.sansa_stack.query.spark.graph.jena.util.{BasicGraphPattern, ResultMapping}
import org.apache.jena.graph.Node
import org.apache.jena.graph.Triple
import org.apache.spark.graphx.Graph
import org.apache.spark.sql.SparkSession

import scala.util.control.Breaks._

/**
  * Class that execute SPARQL MINUS and FILTER NOT operations
  */
class PatternNegate(triples: Iterator[Triple]) extends PatternOp {

  private val tag = "FILTER NOT EXISTS / MINUS"

  override def execute(input: Array[Map[Node, Node]],
                       graph: Graph[Node, Node],
                       session: SparkSession): Array[Map[Node, Node]] = {
    val negation = ResultMapping.run(graph, new BasicGraphPattern(triples), session)
    val intVar = input.head.keySet.intersect(negation.head.keySet).toList
    input.filterNot{ i =>
      var neg = false
      breakable {
        negation.foreach { n =>
          intVar.length match {
            case 0 => neg = false
            case _ => if (mapEquals(i, n, intVar)) { neg = true
              break()
            }
          }
        }
      }
      neg
    }
  }

  override def getTag: String = { tag }

  private def mapEquals(a: Map[Node, Node], b: Map[Node, Node], intVar: List[Node]): Boolean = {
    var eq = true
    for( v <- intVar){
      eq = eq && a(v).equals(b(v))
    }
    eq
  }
}
