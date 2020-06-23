package net.sansa_stack.query.spark.ontop

import it.unibz.inf.ontop.exception.OntopInternalBugException
import it.unibz.inf.ontop.model.`type`.TypeFactory
import it.unibz.inf.ontop.model.term.{Constant, DBConstant, IRIConstant, ImmutableTerm, RDFConstant, RDFLiteralConstant}
import org.apache.jena.datatypes.TypeMapper
import org.apache.jena.graph.{Node, NodeFactory}

/**
 * @author Lorenz Buehmann
 */
object OntopUtils extends Serializable {

  def toNode(constant: RDFConstant, typeFactory: TypeFactory): Node = {
    val termType = constant.getType
    if (termType.isA(typeFactory.getIRITermType)) {
      NodeFactory.createURI(constant.asInstanceOf[IRIConstant].getIRI.getIRIString)
    } else if (termType.isA(typeFactory.getAbstractRDFSLiteral)) {
      val lit = constant.asInstanceOf[RDFLiteralConstant]
      val dt = TypeMapper.getInstance().getTypeByName(lit.getType.getIRI.getIRIString)
      NodeFactory.createLiteral(lit.getValue, dt)
    } else {
      null.asInstanceOf[Node]
    }
  }

  def evaluate(term: ImmutableTerm): Option[RDFConstant] = {
    val simplifiedTerm = term.simplify
    simplifiedTerm match {
      case constant: Constant =>
        if (constant.isInstanceOf[RDFConstant]) return Some(constant.asInstanceOf[RDFConstant])
        if (constant.isNull) return None
        if (constant.isInstanceOf[DBConstant]) throw new InvalidConstantTypeInResultException(constant +
          "is a DB constant. But a binding cannot have a DB constant as value")
        throw new InvalidConstantTypeInResultException("Unexpected constant type for " + constant)
      case _ =>
    }
    throw new InvalidTermAsResultException(simplifiedTerm)
  }

}

class InvalidTermAsResultException(term: ImmutableTerm) extends OntopInternalBugException("Term " + term + " does not evaluate to a constant")
class InvalidConstantTypeInResultException(message: String) extends OntopInternalBugException(message)
