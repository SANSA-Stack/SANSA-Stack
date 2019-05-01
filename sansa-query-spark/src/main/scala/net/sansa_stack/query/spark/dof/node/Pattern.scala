package net.sansa_stack.query.spark.dof.node

import java.util.Iterator

import net.sansa_stack.query.spark.dof.bindings.Dof
import org.apache.jena.sparql.core.{ TriplePath, Var }
import org.apache.jena.sparql.syntax.{ ElementPathBlock, ElementUnion }
import scala.collection.JavaConverters._

object Pattern {
  def traverse(el: ElementPathBlock): Constraints = {
    var dofs = DofTripleList
    var vars = Vars
    // var vars = Variables
    val triples: Iterator[TriplePath] = el.patternElts

    while (triples.hasNext()) {
      val t = triples.next()
      val triple = t.asTriple()
      dofs = (Dof.dof(triple) -> triple) :: dofs
      Helper.getNodes(triple).foreach(node => if (node.isVariable) {
        vars += node.asInstanceOf[Var]
      })
    }

    new Constraints(vars, dofs)
  }
}

class Pattern {

  private var triplesMap = Triples
  private var variables = Set[Var]()
  private var index = 0
  def getTriples: Triples = triplesMap

  def getVariables: Set[Var] = variables

  def build(el: ElementPathBlock): Unit = {
    var constr = DofTripleList
    // var vars = Variables
    val triples: Iterator[TriplePath] = el.patternElts

    while (triples.hasNext()) {
      val t = triples.next()
      Helper.log(t)
      val triple = t.asTriple()
      constr = (Dof.dof(triple) -> triple) :: constr
      Helper.getNodes(triple).foreach(node => if (node.isVariable) {
        this.variables = this.variables + node.asInstanceOf[Var]
      })
    }

    this.triplesMap = this.triplesMap + (index -> constr)
    // this.variables = this.variables + vars
    this.index += 1 // for union
  }

  def build(el: ElementUnion): Unit = {
    println(el.getElements.size)
    println(el.getElements)
    val l = el.getElements.asScala

    for (e <- l)
      println(e)
  }

  def print: Unit = {
    Helper.log("-----------\nQuery Triples:")
    if (Helper.DEBUG) getTriples.toList.foreach(_._2.foreach(a1 => {
      val aux1 = a1._2
      Helper.log("DOF=" + a1._1 + ": " + aux1)
    }))
    Helper.log(getVariables)
    Helper.log("----------")
  }
}
