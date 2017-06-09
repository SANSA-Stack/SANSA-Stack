package net.sansa_stack.rdf.partition.layout

import scala.reflect.runtime.universe.Type
import scala.reflect.runtime.universe.typeOf


import org.apache.jena.graph.Triple
import net.sansa_stack.rdf.partition.core.RdfPartitionerDefault


object TripleLayoutLong
  extends TripleLayout
{
  def schema = typeOf[StringLong]
  def cc = StringLong

  def fromTriple(t: Triple): (String, Long) = {
    val s = t.getSubject
    val o = t.getObject
    val v = if(o.isLiteral() && o.getLiteralValue.isInstanceOf[Number])
       o.getLiteralValue.asInstanceOf[Number] else throw new RuntimeException("Layout only for doubles" + t)

    val sStr = RdfPartitionerDefault.getUriOrBNodeString(s)

    (sStr, v.longValue)
  }

  def fromTripleToC(t: Triple): StringLong = {
    cc.tupled(fromTriple(t))
  }
}