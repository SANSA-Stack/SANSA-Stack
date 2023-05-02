package net.sansa_stack.query.spark.dof.node

import net.sansa_stack.query.spark.dof.bindings.Dof
import org.apache.jena.sparql.core.{TriplePath, Var}
import org.apache.jena.sparql.syntax.ElementPathBlock

import java.util.Iterator

object Pattern {
  def traverse(el: ElementPathBlock): Constraints = {
    var dofs = DofTripleList
    var vars = Vars

    val triples: Iterator[TriplePath] = el.patternElts

    while (triples.hasNext) {
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
