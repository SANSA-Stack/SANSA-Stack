package net.sansa_stack.query.spark.ontop.kryo

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.Kryo.DefaultInstantiatorStrategy
import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.serializers.ClosureSerializer
import com.esotericsoftware.kryo.serializers.ClosureSerializer.Closure
import com.esotericsoftware.minlog.Log
import de.javakaffee.kryoserializers.JdkProxySerializer
import it.unibz.inf.ontop.model.`type`.TypeFactory
import it.unibz.inf.ontop.model.`type`.impl.TypeFactoryImpl
import it.unibz.inf.ontop.model.term.TermFactory
import it.unibz.inf.ontop.model.term.impl.TermFactoryImpl
import net.sansa_stack.query.spark.ontop.RewriteInstruction
import org.objenesis.strategy.StdInstantiatorStrategy

import java.io._
import java.lang.invoke.SerializedLambda
import java.lang.reflect.InvocationHandler
import java.util.Date

/**
 * Utility class to (de)serialize the Ontop rewrite instructions.
 *
 * @author Lorenz Buehmann
 */
object KryoUtils {

  //  val kryoPool = new KryoPool.Builder(new KryoFactory {
  //    override def create(): Kryo = {
  //      val kryo = new Kryo()
  //      kryo.setInstantiatorStrategy(new DefaultInstantiatorStrategy(new StdInstantiatorStrategy()))
  //      ShadedImmutableListSerializer.registerSerializers(kryo)
  //      ShadedImmutableSortedSetSerializer.registerSerializers(kryo)
  //      ShadedImmutableMapSerializer.registerSerializers(kryo)
  //      ShadedImmutableBiMapSerializer.registerSerializers(kryo)
  //      ShadedBiMapSerializer.registerSerializers(kryo)
  //      ShadedImmutableTableSerializer.registerSerializers(kryo)
  //
  //      kryo.register(classOf[Array[AnyRef]])
  //      kryo.register(classOf[Class[_]])
  //      kryo.register(classOf[RewriteInstruction])
  //      kryo.register(classOf[SerializedLambda])
  //      kryo.register(classOf[Closure], new ClosureSerializer())
  //      kryo.register(classOf[InvocationHandler], new JdkProxySerializer)
  //
  //      kryo
  //    }
  //  }).build()

  val kryoPool = new Pool[Kryo](true, false, 8) {
    protected def create: Kryo = {
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
  }

  var kryoLoggingEnabled = true

  def serialize(rewriteInstruction: RewriteInstruction, ontopSessionId: String): Output = {
    if (kryoLoggingEnabled) {
      com.esotericsoftware.minlog.Log.TRACE()
      com.esotericsoftware.minlog.Log.info("SERIALIZE")
    }

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


    //    val kryo = kryoPool.obtain()
    ImmutableFunctionalTermSerializer.registerSerializers(kryo, ontopSessionId)
    kryo.register(classOf[TermFactory], new TermFactorySerializer(ontopSessionId))
    kryo.register(classOf[TermFactoryImpl], new TermFactorySerializer(ontopSessionId))
    kryo.register(classOf[TypeFactory], new TypeFactorySerializer(ontopSessionId))
    kryo.register(classOf[TypeFactoryImpl], new TypeFactorySerializer(ontopSessionId))

    val output = new Output(1024, -1)
    kryo.writeObject(output, rewriteInstruction)
    //    kryoPool.free(kryo)
    //    com.esotericsoftware.minlog.Log.INFO
    output
  }

  def deserialize(output: Output, ontopSessionId: String): RewriteInstruction = {
    if (kryoLoggingEnabled) com.esotericsoftware.minlog.Log.info("DESERIALIZE")
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

    //    val kryo = kryoPool.obtain()

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

    //    kryoPool.free(kryo)
    rewriteInstruction
  }

  class MinLogFileLogger(pathOutLog: String) extends com.esotericsoftware.minlog.Log.Logger {
    private val firstLogTime = new Date().getTime

    val threadLocalLogger: ThreadLocal[PrintStream] = newThreadLocalLogger("myJobId")

    private def newThreadLocalLogger(myJobID: String) = new ThreadLocal[PrintStream]() {
      override protected def initialValue: PrintStream = {
        val ps: PrintStream = try {
          val bufferedOutputStream = new BufferedOutputStream(new FileOutputStream(pathOutLog + "." + Thread.currentThread().getId))
          new PrintStream(bufferedOutputStream)
        }
        catch {
          case e1: FileNotFoundException =>
            e1.printStackTrace()
            null
        }
        ps
      }
    }

    override def print(message: String): Unit = {
      threadLocalLogger.get().println(message)
      threadLocalLogger.get().flush()
    }

    override def log(level: Int, category: String, message: String, ex: Throwable): Unit = {
      val builder = new StringBuilder(256)

      //      builder.append(s"Thread: ${Thread.currentThread().getId} - ")

      val time = new Date().getTime - firstLogTime
      val minutes = time / (1000 * 60)
      val seconds = time / 1000 % 60
      if (minutes <= 9) builder.append('0')
      builder.append(minutes)
      builder.append(':')
      if (seconds <= 9) builder.append('0')
      builder.append(seconds)

      level match {
        case Log.LEVEL_ERROR =>
          builder.append(" ERROR: ")

        case Log.LEVEL_WARN =>
          builder.append("  WARN: ")

        case Log.LEVEL_INFO =>
          builder.append("  INFO: ")

        case Log.LEVEL_DEBUG =>
          builder.append(" DEBUG: ")

        case Log.LEVEL_TRACE =>
          builder.append(" TRACE: ")

      }

      if (category != null) {
        builder.append('[')
        builder.append(category)
        builder.append("] ")
      }

      builder.append(message)

      if (ex != null) {
        val writer = new StringWriter(256)
        ex.printStackTrace(new PrintWriter(writer))
        builder.append('\n')
        builder.append(writer.toString.trim)
      }

      print(builder.toString)
    }
  }

  def enableLoggingToFile(pathOutLog: String): Unit = {
    com.esotericsoftware.minlog.Log.setLogger(new MinLogFileLogger(pathOutLog))
  }

}
