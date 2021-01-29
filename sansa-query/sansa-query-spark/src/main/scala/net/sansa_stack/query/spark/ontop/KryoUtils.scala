package net.sansa_stack.query.spark.ontop

import java.lang.invoke.SerializedLambda
import java.lang.reflect.InvocationHandler

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.Kryo.DefaultInstantiatorStrategy
import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.serializers.ClosureSerializer
import com.esotericsoftware.kryo.serializers.ClosureSerializer.Closure
import org.objenesis.strategy.StdInstantiatorStrategy

import net.sansa_stack.query.spark.ontop.kryo.{ImmutableFunctionalTermSerializer, ImmutableListSerializer, ImmutableMapSerializer, ImmutableSortedSetSerializer}

/**
 * @author Lorenz Buehmann
 */
object KryoUtils {

  def serialize(rewriteInstruction: RewriteInstruction, ontopSessionId: String): Output = {
    val kryo = new Kryo() // ReflectionFactorySupport()

    kryo.setInstantiatorStrategy(new DefaultInstantiatorStrategy(new StdInstantiatorStrategy()))
    ImmutableListSerializer.registerSerializers(kryo)
    ImmutableSortedSetSerializer.registerSerializers(kryo)
    ImmutableMapSerializer.registerSerializers(kryo)
    ImmutableFunctionalTermSerializer.registerSerializers(kryo, ontopSessionId)
    kryo.register(classOf[Array[AnyRef]])
    kryo.register(classOf[Class[_]])
    kryo.register(classOf[RewriteInstruction])
    kryo.register(classOf[SerializedLambda])
    kryo.register(classOf[Closure], new ClosureSerializer())
    import de.javakaffee.kryoserializers.JdkProxySerializer
    kryo.register(classOf[InvocationHandler], new JdkProxySerializer)


    val output = new Output(1024, -1)
    kryo.writeObject(output, rewriteInstruction)

    output
  }

  def deserialize(output: Output, ontopSessionId: String): RewriteInstruction = {
    val kryo = new Kryo() // ReflectionFactorySupport()

    kryo.setInstantiatorStrategy(new DefaultInstantiatorStrategy(new StdInstantiatorStrategy()))
    ImmutableListSerializer.registerSerializers(kryo)
    ImmutableSortedSetSerializer.registerSerializers(kryo)
    ImmutableMapSerializer.registerSerializers(kryo)
    ImmutableFunctionalTermSerializer.registerSerializers(kryo, ontopSessionId)
    kryo.register(classOf[Array[AnyRef]])
    kryo.register(classOf[Class[_]])
    kryo.register(classOf[RewriteInstruction])
    kryo.register(classOf[SerializedLambda])
    kryo.register(classOf[Closure], new ClosureSerializer())
    import de.javakaffee.kryoserializers.JdkProxySerializer
    kryo.register(classOf[InvocationHandler], new JdkProxySerializer)

    val input = new Input(output.getBuffer, 0, output.position)
    val rewriteInstruction = kryo.readObject(input, classOf[RewriteInstruction])

    rewriteInstruction
  }

}
