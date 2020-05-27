package net.sansa_stack.query.spark.graph.jena.serialization

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.charset.StandardCharsets

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import org.apache.jena.query.{Dataset, DatasetFactory, Query, QueryFactory, Syntax}
import org.apache.jena.riot.{Lang, RDFDataMgr, RDFFormat}

/**
 * Serializers for the sansa query layer
 *
 */
object JenaKryoSerializers {

  class QuerySerializer extends Serializer[Query] {
    override def write(kryo: Kryo, output: Output, obj: Query) {
      // Do we need to write the String class?
      // kryo.writeClassAndObject(output, obj.toString)
      output.writeString(obj.toString)
    }

    override def read(kryo: Kryo, input: Input, objClass: Class[Query]): Query = {
      val queryStr = input.readString() // kryo.readClass(input).asInstanceOf[String]

      // We use syntaxARQ as for all practical purposes it is a superset of
      // standard SPARQL
      val result = QueryFactory.create(queryStr, Syntax.syntaxARQ)
      result
    }
  }

  /**
   * TODO This is just a preliminary serializer implementation:
   * Main tasks: use a more compact format than NQUADS and ensure that bnodes are preserved
   *
   */
  class DatasetSerializer extends Serializer[Dataset] {
    override def write(kryo: Kryo, output: Output, obj: Dataset) {
      val tmp = new ByteArrayOutputStream()
      RDFDataMgr.write(tmp, obj, RDFFormat.NQUADS)

      output.writeString(tmp.toString)
    }

    override def read(kryo: Kryo, input: Input, objClass: Class[Dataset]): Dataset = {
      val str = input.readString()
      val tmp = new ByteArrayInputStream(str.getBytes(StandardCharsets.UTF_8))
      val result = DatasetFactory.create
      RDFDataMgr.read(result, tmp, Lang.NQUADS)

      result
    }
  }
}
