package net.sansa_stack.query.spark.ontop

import scala.util.{Failure, Success, Try}

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import it.unibz.inf.ontop.com.google.common.collect.ImmutableList
import it.unibz.inf.ontop.model.term.ImmutableFunctionalTerm
import it.unibz.inf.ontop.model.term.functionsymbol.{FunctionSymbol, FunctionSymbolFactory, RDFTermTypeFunctionSymbol}

class RDFTermTypeFunctionSerializer(ontopSessionID: String)
  extends Serializer[RDFTermTypeFunctionSymbol](false, true) {


  override def write(kryo: Kryo, output: Output, obj: RDFTermTypeFunctionSymbol): Unit = {
    println(obj)
    kryo.writeClassAndObject(output, obj.getDictionary)
    kryo.writeClassAndObject(output, obj.getConversionMap)
    kryo.writeClassAndObject(output, obj.getSimplifiableVariant)
  }

  override def read(kryo: Kryo, input: Input, `type`: Class[RDFTermTypeFunctionSymbol]): RDFTermTypeFunctionSymbol = {

    val functionSymbol = kryo.readClassAndObject(input).asInstanceOf[FunctionSymbol]
    println(s"read ${functionSymbol}")
    val terms = Try(kryo.readClassAndObject(input).asInstanceOf[ImmutableList[it.unibz.inf.ontop.model.term.ImmutableTerm]]) match {
      case Success(value) => value
      case Failure(exception) => throw new Exception(s"failed to read $functionSymbol", exception)
    }

    val functionSymbolFactory = OntopConnection.configs(ontopSessionID).getInjector.getInstance(classOf[FunctionSymbolFactory])
//    val term = functionSymbolFactory.getRDFTermTypeFunctionSymbol()

    null.asInstanceOf[RDFTermTypeFunctionSymbol]
  }
}

