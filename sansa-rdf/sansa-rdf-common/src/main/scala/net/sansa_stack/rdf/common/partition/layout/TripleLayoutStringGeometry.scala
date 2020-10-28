package net.sansa_stack.rdf.common.partition.layout

import scala.reflect.runtime.universe.{Type, typeOf}

import com.vividsolutions.jts.io.WKTReader
import org.apache.jena.graph.Triple

import net.sansa_stack.rdf.common.partition.core.RdfPartitionerDefault
import net.sansa_stack.rdf.common.partition.schema.SchemaStringGeometry

/**
 * @author Lorenz Buehmann
 */
object TripleLayoutStringGeometry
  extends TripleLayout {
  override def schema: Type = typeOf[SchemaStringGeometry]

  private val wktReader = new WKTReader()

  override def fromTriple(t: Triple): SchemaStringGeometry = {
    val s = t.getSubject
    val o = t.getObject
    val v = if (o.isLiteral) {
      wktReader.read(o.getLiteralLexicalForm)
    } else throw new RuntimeException(s"Layout only for date literals: $t")

    val sStr = RdfPartitionerDefault.getUriOrBNodeString(s)

    SchemaStringGeometry(sStr, v)
  }
}
