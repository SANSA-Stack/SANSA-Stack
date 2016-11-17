package net.sansa_stack.rdf.common.partition.layout

import scala.reflect.runtime.universe.Type
import scala.reflect.runtime.universe.typeOf


import org.apache.jena.graph.Triple
import net.sansa_stack.rdf.common.partition.core.RdfPartitionerDefault


object TripleLayoutDouble
  extends TripleLayout
{
  def schema = typeOf[(String, Double)]
  //val foo = universe

  def fromTriple(t: Triple): (String, Double) = {
    val s = t.getSubject
    val o = t.getObject
    val v = if(o.isLiteral() && o.getLiteralValue.isInstanceOf[Number])
       o.getLiteralValue.asInstanceOf[Number] else throw new RuntimeException("Layout only for doubles" + t)

    val sStr = RdfPartitionerDefault.getUriOrBNodeString(s)

    (sStr, v.doubleValue)
  }
}

