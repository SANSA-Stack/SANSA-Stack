package net.sansa_stack.rdf.spark.utils.kryo.jena

import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.esotericsoftware.kryo.io.{Input, Output}
import org.apache.jena.graph.{Node => JenaNode, Triple => JenaTriple, _}
import org.apache.jena.riot.system.RiotLib
import org.apache.jena.sparql.core.{Quad => JenaQuad}
import org.apache.jena.sparql.core.Var
import org.apache.jena.sparql.expr.Expr
import org.apache.jena.sparql.util.{ExprUtils, FmtUtils}

/**
 * @author Nilesh Chakraborty <nilesh@nileshc.com>
 */
object JenaKryoSerializers {


  /**
    * Kryo Serializer for Node
    */
  class NodeSerializer extends Serializer[JenaNode]
  {
    override def write(kryo: Kryo, output: Output, obj: JenaNode) {
        output.writeString(FmtUtils.stringForNode(obj))
    }

    override def read(kryo: Kryo, input: Input, objClass: Class[JenaNode]): JenaNode = {
      val s = input.readString()
      println(s"parsing:$s")
        RiotLib.parse(s)
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

    override def read(kryo: Kryo, input: Input, objClass: Class[Array[JenaNode]]): Array[JenaNode] = {
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

    override def read(kryo: Kryo, input: Input, objClass: Class[Node_ANY]): Node_ANY = {
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

    override def read(kryo: Kryo, input: Input, objClass: Class[Node_Variable]): Node_Variable = {
      NodeFactory.createVariable(input.readString).asInstanceOf[Node_Variable]
    }
  }

  class ExprSerializer extends Serializer[Expr] {
    override def write(kryo: Kryo, output: Output, obj: Expr) {
      val str = ExprUtils.fmtSPARQL(obj)
      output.writeString(str)
    }

    override def read(kryo: Kryo, input: Input, objClass: Class[Expr]): Expr = {
      val str = input.readString()
      ExprUtils.parse(str)
    }
  }


  class VarSerializer extends Serializer[Var] {
    override def write(kryo: Kryo, output: Output, obj: Var) {
      output.writeString(obj.getName)
    }

    override def read(kryo: Kryo, input: Input, objClass: Class[Var]): Var = {
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

    override def read(kryo: Kryo, input: Input, objClass: Class[JenaTriple]): JenaTriple = {
      val s = kryo.readClassAndObject(input).asInstanceOf[JenaNode]
      val p = kryo.readClassAndObject(input).asInstanceOf[JenaNode]
      val o = kryo.readClassAndObject(input).asInstanceOf[JenaNode]
      new JenaTriple(s, p, o)
    }
  }

  class QuadSerializer extends Serializer[JenaQuad] {
    override def write(kryo: Kryo, output: Output, obj: JenaQuad) {
      kryo.writeClassAndObject(output, obj.getGraph)
      kryo.writeClassAndObject(output, obj.getSubject)
      kryo.writeClassAndObject(output, obj.getPredicate)
      kryo.writeClassAndObject(output, obj.getObject)
    }

    override def read(kryo: Kryo, input: Input, objClass: Class[JenaQuad]): JenaQuad = {
      val g = kryo.readClassAndObject(input).asInstanceOf[JenaNode]
      println(s"g:$g")
      val s = kryo.readClassAndObject(input).asInstanceOf[JenaNode]
      println(s"s:$s")
      val p = kryo.readClassAndObject(input).asInstanceOf[JenaNode]
      println(s"p:$p")
      val o = kryo.readClassAndObject(input).asInstanceOf[JenaNode]
      println(s"o:$o")
      new JenaQuad(g, s, p, o)
    }
  }
}
