package net.sansa_stack.rdf.common.partition.layout

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import scala.reflect.runtime.universe.{Type, typeOf}

import org.apache.jena.graph.Triple

import net.sansa_stack.rdf.common.partition.core.RdfPartitionerDefault
import net.sansa_stack.rdf.common.partition.schema.{SchemaStringBoolean, SchemaStringDate}


/**
 * Triple layout for s[String], o[Boolean] columns.
 *
 * @author Lorenz Buehmann
 */
object TripleLayoutBoolean
  extends TripleLayout {
  override def schema: Type = typeOf[SchemaStringBoolean]

  // this is a more error tolerant date parser also allowing 1999-5-5 etc.
//  private val formatter = DateTimeFormatter.ofPattern("y-M-d")

  override def fromTriple(t: Triple): SchemaStringBoolean = {
    val s = t.getSubject
    val o = t.getObject
    val v = if (o.isLiteral && o.getLiteralValue.isInstanceOf[Boolean]) o.getLiteralValue.asInstanceOf[Boolean]
     else throw new RuntimeException(s"Layout only for boolean literals: $t")

    val sStr = RdfPartitionerDefault.getUriOrBNodeString(s)

    SchemaStringBoolean(sStr, v)
  }
}


