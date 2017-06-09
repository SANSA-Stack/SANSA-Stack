package net.sansa_stack.rdf.partition.layout

import scala.reflect.runtime.universe.Type
import scala.reflect.runtime.universe.typeOf


import org.apache.jena.graph.Triple
import net.sansa_stack.rdf.partition.core.RdfPartitionerDefault


// Layout which can be used for blank nodes, IRIs, and plain iterals without language tag
object TripleLayoutString
  extends TripleLayout
{
  def schema = typeOf[(String, String)]
  def cc = StringString

  def fromTriple(t: Triple): (String, String) = {
    val s = t.getSubject
    val o = t.getObject

    val sStr = RdfPartitionerDefault.getUriOrBNodeString(s)

    val result = if(o.isLiteral()) {
      (sStr, o.getLiteralLexicalForm)
    } else {
      val oStr = RdfPartitionerDefault.getUriOrBNodeString(o)
      (sStr, oStr)
    }

    result
  }

  override def fromTripleToC(triple: Triple): StringString = {
    cc.tupled(fromTriple(triple))
  }
}
