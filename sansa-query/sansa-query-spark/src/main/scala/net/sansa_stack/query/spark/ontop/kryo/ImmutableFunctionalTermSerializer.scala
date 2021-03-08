package net.sansa_stack.query.spark.ontop.kryo

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import it.unibz.inf.ontop.com.google.common.collect.ImmutableList
import it.unibz.inf.ontop.model.term.ImmutableFunctionalTerm
import it.unibz.inf.ontop.model.term.functionsymbol.FunctionSymbol
import it.unibz.inf.ontop.model.term.impl._
import net.sansa_stack.query.spark.ontop.OntopConnection

import scala.util.{Failure, Success, Try}

class ImmutableFunctionalTermSerializer(ontopSessionID: String)
  extends Serializer[ImmutableFunctionalTerm](false, true) {


  override def write(kryo: Kryo, output: Output, obj: ImmutableFunctionalTerm): Unit = {
//    println(obj.getFunctionSymbol + "::" + obj.getFunctionSymbol.getClass + "::" + obj.getTerms.getClass)
    kryo.writeClassAndObject(output, obj.getFunctionSymbol)
    kryo.writeClassAndObject(output, obj.getTerms)
  }

  override def read(kryo: Kryo, input: Input, `type`: Class[ImmutableFunctionalTerm]): ImmutableFunctionalTerm = {

    val functionSymbol = kryo.readClassAndObject(input).asInstanceOf[FunctionSymbol]
//    println(s"read ${functionSymbol}")
    val terms = Try(kryo.readClassAndObject(input).asInstanceOf[ImmutableList[it.unibz.inf.ontop.model.term.ImmutableTerm]]) match {
      case Success(value) => value
      case Failure(exception) => throw new Exception(s"failed to read $functionSymbol", exception)
    }

    val termFactory = OntopConnection.configs.get(Option(ontopSessionID).getOrElse(OntopConnection.configs.head._1)).get.getTermFactory
    val term = termFactory.getImmutableFunctionalTerm(functionSymbol, terms)

    term
  }
}

object ImmutableFunctionalTermSerializer {
  def registerSerializers(kryo: Kryo, ontopSessionID: String): Unit = {
    val serializer = new ImmutableFunctionalTermSerializer(ontopSessionID)

    kryo.register(classOf[ImmutableFunctionalTermImpl], serializer)
    kryo.register(classOf[NonGroundFunctionalTermImpl], serializer)
    kryo.register(classOf[GroundFunctionalTermImpl], serializer)
    kryo.register(classOf[NonGroundExpressionImpl], serializer)
    kryo.register(classOf[GroundExpressionImpl], serializer)

    kryo.register(classOf[ImmutableFunctionalTerm], serializer)
  }
}