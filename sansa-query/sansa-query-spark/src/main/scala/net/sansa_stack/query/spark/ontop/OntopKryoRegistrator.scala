package net.sansa_stack.query.spark.ontop

import com.esotericsoftware.kryo.Kryo.DefaultInstantiatorStrategy
import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.serializers.ClosureSerializer.Closure
import com.esotericsoftware.kryo.serializers.{ClosureSerializer, JavaSerializer}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import de.javakaffee.kryoserializers.JdkProxySerializer
import de.javakaffee.kryoserializers.guava.HashMultimapSerializer
import it.unibz.inf.ontop.com.google.common.collect.{ImmutableMap, ImmutableSortedSet}
import it.unibz.inf.ontop.model.`type`.impl.TypeFactoryImpl
import it.unibz.inf.ontop.model.`type`.{DBTermType, TypeFactory}
import it.unibz.inf.ontop.model.atom.DistinctVariableOnlyDataAtom
import it.unibz.inf.ontop.model.term.functionsymbol.db.impl.{AbstractSQLDBFunctionSymbolFactory, DefaultSQLTimestampISONormFunctionSymbol}
import it.unibz.inf.ontop.model.term.impl.TermFactoryImpl
import it.unibz.inf.ontop.model.term.{ImmutableTerm, TermFactory, Variable}
import net.sansa_stack.query.spark.ontop.OntopKryoRegistrator.register
import net.sansa_stack.query.spark.ontop.kryo._
import org.apache.jena.sparql.engine.binding.Binding
import org.apache.spark.serializer.KryoRegistrator
import org.objenesis.strategy.StdInstantiatorStrategy
import uk.ac.manchester.cs.owl.owlapi.OWLOntologyImpl
import uk.ac.manchester.cs.owl.owlapi.concurrent.ConcurrentOWLOntologyImpl

import java.lang.invoke.SerializedLambda
import java.lang.reflect.InvocationHandler

object OntopKryoRegistrator {
  def register(kryo: Kryo): Unit = {
    HashMultimapSerializer.registerSerializers(kryo)

    // kryo.register(classOf[scala.collection.immutable.Map[_, _]])
    // kryo.register(classOf[HashMap[_, _]])

    // Partitioning
    kryo.register(classOf[net.sansa_stack.rdf.common.partition.core.RdfPartitionStateDefault])
    kryo.register(classOf[Array[net.sansa_stack.rdf.common.partition.core.RdfPartitionStateDefault]])

    kryo.register(classOf[Array[Binding]])

    kryo.register(classOf[ImmutableTerm])
    kryo.register(classOf[it.unibz.inf.ontop.substitution.impl.ImmutableSubstitutionImpl[_]])
    kryo.register(classOf[it.unibz.inf.ontop.utils.CoreUtilsFactory])
    kryo.register(classOf[DefaultSQLTimestampISONormFunctionSymbol])
    kryo.register(classOf[AbstractSQLDBFunctionSymbolFactory])

    kryo.register(classOf[RewriteInstruction], new RewriteInstructionSerializer())

    // OWLOntology
    kryo.register(classOf[OWLOntologyImpl], new JavaSerializer())
    kryo.register(classOf[ConcurrentOWLOntologyImpl], new JavaSerializer())


    kryo.setInstantiatorStrategy(new DefaultInstantiatorStrategy(new StdInstantiatorStrategy()))

    ShadedImmutableListSerializer.registerSerializers(kryo)
    ShadedImmutableSortedSetSerializer.registerSerializers(kryo)
    ShadedImmutableMapSerializer.registerSerializers(kryo)
    ShadedImmutableBiMapSerializer.registerSerializers(kryo)
    ShadedBiMapSerializer.registerSerializers(kryo)
    ShadedImmutableTableSerializer.registerSerializers(kryo)

    kryo.register(classOf[Array[AnyRef]])
    kryo.register(classOf[Class[_]])
    kryo.register(classOf[RewriteInstruction])
    kryo.register(classOf[SerializedLambda])
    kryo.register(classOf[Closure], new ClosureSerializer())
    kryo.register(classOf[InvocationHandler], new JdkProxySerializer)

    ImmutableFunctionalTermSerializer.registerSerializers(kryo, null)
    kryo.register(classOf[TermFactory], new TermFactorySerializer(null))
    kryo.register(classOf[TermFactoryImpl], new TermFactorySerializer(null))
    kryo.register(classOf[TypeFactory], new TypeFactorySerializer(null))
    kryo.register(classOf[TypeFactoryImpl], new TypeFactorySerializer(null))
  }

  class RewriteInstructionSerializer extends Serializer[RewriteInstruction](false, true) {
    override def write(kryo: Kryo, output: Output, rwi: RewriteInstruction): Unit = {
      kryo.writeClassAndObject(output, rwi.sqlSignature)
      kryo.writeClassAndObject(output, rwi.sqlTypeMap)
      kryo.writeClassAndObject(output, rwi.answerAtom)
      kryo.writeClassAndObject(output, rwi.sparqlVar2Term)
    }

    override def read(kryo: Kryo, input: Input, `type`: Class[RewriteInstruction]): RewriteInstruction = {
      RewriteInstruction(
        kryo.readClassAndObject(input).asInstanceOf[ImmutableSortedSet[Variable]],
        kryo.readClassAndObject(input).asInstanceOf[ImmutableMap[Variable, DBTermType]],
        kryo.readClassAndObject(input).asInstanceOf[DistinctVariableOnlyDataAtom],
        kryo.readClassAndObject(input).asInstanceOf[ImmutableMap[Variable, ImmutableTerm]])
    }
  }
}

/**
 * The Spark Kryo registrator for Ontop related objects.
 *
 * @author Lorenz Buehmann
 */
class OntopKryoRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    register(kryo)
  }
}
