package net.sansa_stack.rdf.spark.io

import org.apache.jena.graph.{ Node => JenaNode, Triple => JenaTriple, _ }
import org.apache.jena.riot.system.RiotLib
import org.apache.jena.sparql.util.FmtUtils

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.Serializer
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import org.apache.jena.rdf.model.impl.NTripleReader
import org.apache.jena.sparql.core.Var
import org.aksw.jena_sparql_api.views.RestrictedExpr
import org.apache.jena.sparql.expr.Expr
import org.apache.jena.sparql.util.ExprUtils


object KryoSerializers {

  class RestrictedExprSerializer extends Serializer[RestrictedExpr] {
    override def write(kryo: Kryo, output: Output, obj: RestrictedExpr) {
      //output.writeString("true")
      //output.writeString("" + obj.getExpr)
      kryo.writeClassAndObject(output, obj.getExpr)
    }

    override def read(kryo: Kryo, input: Input, objClass: Class[RestrictedExpr]): RestrictedExpr = {
      val expr = kryo.readClassAndObject(input).asInstanceOf[Expr]
      new RestrictedExpr(expr)
       //val restrictions = input.readString()
//      val expr = ExprUtils.parse(input.readString())
//
//      new RestrictedExpr(expr)
    }
  }
}