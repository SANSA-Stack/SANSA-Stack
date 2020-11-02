package net.sansa_stack.rdf.common.kryo.jena

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.charset.StandardCharsets
import java.util.UUID

import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.esotericsoftware.kryo.io.{Input, Output}
import org.apache.jena.atlas.io.IndentedLineBuffer
import org.apache.jena.graph.{Node => JenaNode, Triple => JenaTriple, _}
import org.apache.jena.query.{Dataset, DatasetFactory, Query, QueryFactory, Syntax}
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.apache.jena.riot.lang.LabelToNode
import org.apache.jena.riot.lang.LabelToNode.createScopeByDocumentHash
import org.apache.jena.riot.out.{NodeFmtLib, NodeFormatterTTL, NodeToLabel}
import org.apache.jena.riot.{Lang, RDFDataMgr, RDFFormat, RIOT}
import org.apache.jena.riot.system.{ErrorHandlerFactory, IRIResolver, ParserProfile, ParserProfileStd, PrefixMapExtended, RiotLib}
import org.apache.jena.riot.tokens.{TokenizerFactory, TokenizerText}
import org.apache.jena.sparql.ARQConstants
import org.apache.jena.sparql.core.{Quad => JenaQuad}
import org.apache.jena.sparql.core.Var
import org.apache.jena.sparql.expr.Expr
import org.apache.jena.sparql.util.{ExprUtils, FmtUtils, NodeFactoryExtra}

/**
 * @author Nilesh Chakraborty <nilesh@nileshc.com>
 */
object JenaKryoSerializers {


  /**
    * Kryo Serializer for Node
    */
  class NodeSerializer extends Serializer[JenaNode]
  {

    import org.apache.jena.riot.system.PrefixMap
    import org.apache.jena.riot.system.PrefixMapFactory
    import org.apache.jena.sparql.ARQConstants

    private val profile = setupInternalParserProfile

    val pmap: PrefixMap = PrefixMapFactory.createForInput
    pmap.add("rdf", ARQConstants.rdfPrefix)
    pmap.add("rdfs", ARQConstants.rdfsPrefix)
    pmap.add("xsd", ARQConstants.xsdPrefix)
    pmap.add("owl", ARQConstants.owlPrefix)

    val nodeFormatter = new NodeFormatterTTL(null, pmap, NodeToLabel.createBNodeByLabelEncoded())
    val writer = new IndentedLineBuffer()

    override def write(kryo: Kryo, output: Output, obj: JenaNode) {
//      println(s"serializing node $obj   => ${FmtUtils.stringForNode(obj)}")
      nodeFormatter.format(writer, obj)
      output.writeString(writer.toString)
      writer.clear()
    }

    def read(kryo: Kryo, input: Input, objClass: Class[JenaNode]): JenaNode = {
      val s = input.readString()
      val n = parse(s)
//      println(s"deserializing string $s   => $n")
      n
    }

    def parse(string: String): JenaNode = {
      val tokenizer = TokenizerText.create().fromString(string).build()
      val n: JenaNode = if (!tokenizer.hasNext) {
        null
      } else {
        val t = tokenizer.next()
        profile.create(null, t)
      }
      n
    }

    private def setupInternalParserProfile = {
      val pmap = PrefixMapFactory.createForInput
      pmap.add("rdf", ARQConstants.rdfPrefix)
      pmap.add("rdfs", ARQConstants.rdfsPrefix)
      pmap.add("xsd", ARQConstants.xsdPrefix)
      pmap.add("owl", ARQConstants.owlPrefix)
      val labelToNode = LabelToNode.createUseLabelEncoded()
      val factoryRDF = RiotLib.factoryRDF(labelToNode)
      new ParserProfileStd(factoryRDF, ErrorHandlerFactory.errorHandlerStd, IRIResolver.create, pmap, RIOT.getContext.copy, true, false)
    }
  }

  /**
    * Kryo Serializer for Array[Node]
    */
  class NodeArraySerializer extends Serializer[Array[JenaNode]]
  {
    override def write(kryo: Kryo, output: Output, obj: Array[JenaNode]) {
      output.writeInt(obj.length, true)
      for (node <- obj) {
        kryo.writeClassAndObject(output, node)
      }
    }

    def read(kryo: Kryo, input: Input, objClass: Class[Array[JenaNode]]): Array[JenaNode] = {
      val nodes = new Array[JenaNode](input.readInt(true))
      var i = 0
      while(i < nodes.length) {
        nodes(i) = kryo.readClassAndObject(input).asInstanceOf[JenaNode]
        i += 1
      }
      nodes
    }
  }

  /**
    * Kryo Serializer for Node_Blank
    */
//  class BlankNodeSerializer extends Serializer[Node_Blank] {
//    override def write(kryo: Kryo, output: Output, obj: Node_Blank) {
//      output.writeString(obj.toString)
//    }
//
//    override def read(kryo: Kryo, input: Input, objClass: Class[Node_Blank]): Node_Blank = {
//      NodeFactory.createBlankNode(input.readString()).asInstanceOf[Node_Blank]
//    }
//  }

  /**
    * Kryo Serializer for Node_ANY
    */
  class ANYNodeSerializer extends Serializer[Node_ANY] {
    override def write(kryo: Kryo, output: Output, obj: Node_ANY) {

    }

    def read(kryo: Kryo, input: Input, objClass: Class[Node_ANY]): Node_ANY = {
      JenaNode.ANY.asInstanceOf[Node_ANY]
    }
  }

  /**
    * Kryo Serializer for Node_Variable
    */
  class VariableNodeSerializer extends Serializer[Node_Variable] {
    override def write(kryo: Kryo, output: Output, obj: Node_Variable) {
      output.writeString(obj.toString)
    }

    def read(kryo: Kryo, input: Input, objClass: Class[Node_Variable]): Node_Variable = {
      NodeFactory.createVariable(input.readString).asInstanceOf[Node_Variable]
    }
  }

  class ExprSerializer extends Serializer[Expr] {
    override def write(kryo: Kryo, output: Output, obj: Expr) {
      val str = ExprUtils.fmtSPARQL(obj)
      output.writeString(str)
    }

    def read(kryo: Kryo, input: Input, objClass: Class[Expr]): Expr = {
      val str = input.readString()
      ExprUtils.parse(str)
    }
  }


   class VarSerializer extends Serializer[Var] {
    override def write(kryo: Kryo, output: Output, obj: Var) {
      output.writeString(obj.getName)
    }

     def read(kryo: Kryo, input: Input, objClass: Class[Var]): Var = {
      Var.alloc(input.readString)
    }
   }

  /**
    * Kryo Serializer for Node_URI
    */
//  class URINodeSerializer extends Serializer[Node_URI] {
//    override def write(kryo: Kryo, output: Output, obj: Node_URI) {
//      output.writeString(obj.toString())
//    }
//
//    override def read(kryo: Kryo, input: Input, objClass: Class[Node_URI]): Node_URI = {
//      NodeFactory.createURI(input.readString()).asInstanceOf[Node_URI]
//    }
//  }

  /**
    * Kryo Serializer for Node_Literal
    */
//  class LiteralNodeSerializer extends Serializer[Node_Literal] {
//    override def write(kryo: Kryo, output: Output, obj: Node_Literal) {
//      output.writeString(obj.toString())
//    }
//
//    override def read(kryo: Kryo, input: Input, objClass: Class[Node_Literal]): Node_Literal = {
//      NodeFactory.createLiteral(input.readString()).asInstanceOf[Node_Literal]
//    }
//  }

  /**
    * Kryo Serializer for Triple
    */
  class TripleSerializer extends Serializer[JenaTriple] {
    override def write(kryo: Kryo, output: Output, obj: JenaTriple) {
      kryo.writeClassAndObject(output, obj.getSubject)
      kryo.writeClassAndObject(output, obj.getPredicate)
      kryo.writeClassAndObject(output, obj.getObject)
    }

    def read(kryo: Kryo, input: Input, objClass: Class[JenaTriple]): JenaTriple = {
      val s = kryo.readClassAndObject(input).asInstanceOf[JenaNode]
      val p = kryo.readClassAndObject(input).asInstanceOf[JenaNode]
      val o = kryo.readClassAndObject(input).asInstanceOf[JenaNode]
      new JenaTriple(s, p, o)
    }
  }

  abstract class QuadSerializer extends Serializer[JenaQuad] {
    override def write(kryo: Kryo, output: Output, obj: JenaQuad) {
      kryo.writeClassAndObject(output, obj.getGraph)
      kryo.writeClassAndObject(output, obj.getSubject)
      kryo.writeClassAndObject(output, obj.getPredicate)
      kryo.writeClassAndObject(output, obj.getObject)
    }

    def read(kryo: Kryo, input: Input, objClass: Class[JenaQuad]): JenaQuad = {
      val g = kryo.readClassAndObject(input).asInstanceOf[JenaNode]
      val s = kryo.readClassAndObject(input).asInstanceOf[JenaNode]
      val p = kryo.readClassAndObject(input).asInstanceOf[JenaNode]
      val o = kryo.readClassAndObject(input).asInstanceOf[JenaNode]
      new JenaQuad(g, s, p, o)
    }
  }


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

  class ModelSerializer extends Serializer[Model] {
    override def write(kryo: Kryo, output: Output, obj: Model) {
      val tmp = new ByteArrayOutputStream()
      RDFDataMgr.write(tmp, obj, RDFFormat.NTRIPLES)

      output.writeString(tmp.toString)
    }

    override def read(kryo: Kryo, input: Input, objClass: Class[Model]): Model = {
      val str = input.readString()
      val tmp = new ByteArrayInputStream(str.getBytes(StandardCharsets.UTF_8))
      val result = ModelFactory.createDefaultModel
      RDFDataMgr.read(result, tmp, Lang.NTRIPLES)

      result
    }
  }
}
