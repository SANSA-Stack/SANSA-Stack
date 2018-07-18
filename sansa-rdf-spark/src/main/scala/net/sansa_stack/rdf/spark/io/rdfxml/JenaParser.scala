package net.sansa_stack.rdf.spark.io.rdfxml

import org.apache.jena.riot.{RDFParser, RDFParserBuilder, RiotException}
import org.apache.jena.riot.lang.CollectorStreamRDF
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.BadRecordException
import org.apache.spark.unsafe.types.UTF8String

class JenaParser(val options: RdfXmlOptions) {
  import scala.collection.JavaConverters._

  val parserBuilder: RDFParserBuilder = RDFParser.create().base(options.baseURI)

  def parse[T](record: T,
               createParser: (RDFParserBuilder, T) => RDFParser,
               recordLiteral: T => UTF8String)
  : Seq[InternalRow] = {
    try {
      val triples = new CollectorStreamRDF()
      createParser(parserBuilder, record).parse(triples)
      triples.getTriples.asScala.map(convertTriple)
    } catch {
      case e@(_: RuntimeException | _: RiotException) =>
        throw BadRecordException(() => recordLiteral(record), () => None, e)
    }

  }

  def convertTriple(triple: org.apache.jena.graph.Triple): InternalRow = {
    val row = new GenericInternalRow(3)
    row.update(0, UTF8String.fromBytes(triple.getSubject.toString.getBytes()))
    row.update(1, UTF8String.fromBytes(triple.getPredicate.toString.getBytes()))
    row.update(2, UTF8String.fromBytes(triple.getObject.toString.getBytes()))
    row
  }
}
