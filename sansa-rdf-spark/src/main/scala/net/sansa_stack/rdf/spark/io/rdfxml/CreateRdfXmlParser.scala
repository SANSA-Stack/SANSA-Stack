package net.sansa_stack.rdf.spark.io.rdfxml

import java.io.{ByteArrayInputStream, InputStream}
import java.nio.ByteBuffer

import com.fasterxml.jackson.core.{JsonFactory, JsonParser}
import org.apache.hadoop.io.Text
import org.apache.jena.riot.{RDFLanguages, RDFParser, RDFParserBuilder}
import org.apache.jena.riot.system.ErrorHandlerFactory
import org.apache.spark.unsafe.types.UTF8String

private[rdfxml] object CreateRdfXmlParser extends Serializable {

  def string(parserBuilder: RDFParserBuilder, record: String): RDFParser = {
    RDFParser.create()
      .source(record)
      .lang(RDFLanguages.RDFXML)
      .errorHandler(ErrorHandlerFactory.errorHandlerStrict)
      .build()
  }

  def utf8String(parserBuilder: RDFParserBuilder, record: UTF8String): RDFParser = {
    val bb = record.getByteBuffer
    assert(bb.hasArray)

    inputStream(parserBuilder, new ByteBufferBackedInputStream(bb))
  }

  def text(parserBuilder: RDFParserBuilder, record: Text): RDFParser = {
    inputStream(parserBuilder, new ByteArrayInputStream(record.getBytes))
  }

  def inputStream(parserBuilder: RDFParserBuilder, in: InputStream): RDFParser = {
    RDFParser.create()
      .source(in)
      .lang(RDFLanguages.RDFXML)
      .errorHandler(ErrorHandlerFactory.errorHandlerStrict)
      .build()
  }


  import java.io.IOException
  class ByteBufferBackedInputStream(var buf: ByteBuffer) extends InputStream {
    @throws[IOException]
    override def read: Int = {
      if (!buf.hasRemaining) return -1
      buf.get & 0xFF
    }

    @throws[IOException]
    override def read(bytes: Array[Byte], off: Int, len: Int): Int = {
      if (!buf.hasRemaining) return -1
      val newLen = Math.min(len, buf.remaining)
      buf.get(bytes, off, newLen)
      newLen
    }
  }
}
