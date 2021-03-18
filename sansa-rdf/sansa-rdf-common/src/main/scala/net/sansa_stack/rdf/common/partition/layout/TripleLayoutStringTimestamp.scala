package net.sansa_stack.rdf.common.partition.layout

import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter

import scala.reflect.runtime.universe.{Type, typeOf}

import org.apache.jena.graph.Triple

import net.sansa_stack.rdf.common.partition.core.RdfPartitionerDefault
import net.sansa_stack.rdf.common.partition.schema.{SchemaStringDate, SchemaStringTimestamp}


/**
 * Triple layout for s[String], o[Timestamp] columns.
 *
 * @author Lorenz Buehmann
 */
object TripleLayoutStringTimestamp
  extends TripleLayout {
  override def schema: Type = typeOf[SchemaStringTimestamp]

  // this is a more error tolerant date parser also allowing 1999-5-5 etc.
  private val formatter = DateTimeFormatter.ISO_DATE_TIME

  override def fromTriple(t: Triple): SchemaStringTimestamp = {
    val s = t.getSubject
    val o = t.getObject
    val v = if (o.isLiteral) {
      java.sql.Timestamp.valueOf(LocalDateTime.parse(o.getLiteralLexicalForm, formatter))
    } else throw new RuntimeException(s"Layout only for datetime literals: $t")

    val sStr = RdfPartitionerDefault.getUriOrBNodeString(s)

    SchemaStringTimestamp(sStr, v)
  }
}
