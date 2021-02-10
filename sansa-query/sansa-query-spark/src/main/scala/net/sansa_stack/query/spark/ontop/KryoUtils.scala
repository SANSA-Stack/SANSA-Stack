package net.sansa_stack.query.spark.ontop

import java.lang.invoke.SerializedLambda
import java.lang.reflect.InvocationHandler
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.Kryo.DefaultInstantiatorStrategy
import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.pool.{KryoFactory, KryoPool}
import com.esotericsoftware.kryo.serializers.{ClosureSerializer, JavaSerializer}
import com.esotericsoftware.kryo.serializers.ClosureSerializer.Closure
import it.unibz.inf.ontop.model.`type`.TypeFactory
import it.unibz.inf.ontop.model.`type`.impl.TypeFactoryImpl
import it.unibz.inf.ontop.model.term.TermFactory
import it.unibz.inf.ontop.model.term.impl.TermFactoryImpl
import org.objenesis.strategy.StdInstantiatorStrategy
import net.sansa_stack.query.spark.ontop.kryo.{ShadedBiMapSerializer, ShadedImmutableBiMapSerializer, ShadedImmutableListSerializer, ShadedImmutableMapSerializer, ShadedImmutableSortedSetSerializer, ShadedImmutableTableSerializer}

import de.javakaffee.kryoserializers.JdkProxySerializer

/**
 * Utility class to (de)serialize the Ontop rewrite instructions.
 *
 * @author Lorenz Buehmann
 */
object KryoUtils {

  val kryoPool = new KryoPool.Builder(new KryoFactory {
    override def create(): Kryo = {
      val kryo = new Kryo()
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

      kryo
    }
  }).build()

  kryoPool.borrow()

  def serialize(rewriteInstruction: RewriteInstruction, ontopSessionId: String): Output = {
    val kryo = new Kryo() // ReflectionFactorySupport()

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

    ImmutableFunctionalTermSerializer.registerSerializers(kryo, ontopSessionId)
    kryo.register(classOf[TermFactory], new TermFactorySerializer(ontopSessionId))
    kryo.register(classOf[TermFactoryImpl], new TermFactorySerializer(ontopSessionId))
    kryo.register(classOf[TypeFactory], new TypeFactorySerializer(ontopSessionId))
    kryo.register(classOf[TypeFactoryImpl], new TypeFactorySerializer(ontopSessionId))

    val output = new Output(1024, -1)
    kryo.writeObject(output, rewriteInstruction)

    output
  }

  def deserialize(output: Output, ontopSessionId: String): RewriteInstruction = {
    val kryo = new Kryo() // ReflectionFactorySupport()

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

    // we need the current class loader which hopefully is the Spark classloader to get all the Ontop packages in
    // the Kryo classpath
    val classLoader = Thread.currentThread.getContextClassLoader
    kryo.setClassLoader(classLoader)

    ImmutableFunctionalTermSerializer.registerSerializers(kryo, ontopSessionId)
    kryo.register(classOf[TermFactory], new TermFactorySerializer(ontopSessionId))
    kryo.register(classOf[TermFactoryImpl], new TermFactorySerializer(ontopSessionId))
    kryo.register(classOf[TypeFactory], new TypeFactorySerializer(ontopSessionId))
    kryo.register(classOf[TypeFactoryImpl], new TypeFactorySerializer(ontopSessionId))

    val input = new Input(output.getBuffer, 0, output.position)
    val rewriteInstruction = kryo.readObject(input, classOf[RewriteInstruction])

    rewriteInstruction
  }

}
