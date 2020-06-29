package net.sansa_stack.query.spark.ontop

import java.lang.invoke.{MethodType, SerializedLambda}

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.serializers.ClosureSerializer
import de.javakaffee.kryoserializers.guava.HashMultimapSerializer
import it.unibz.inf.ontop.model.`type`.impl.RDFTermTypeImpl
import it.unibz.inf.ontop.model.term.ImmutableTerm
import it.unibz.inf.ontop.model.term.functionsymbol.db.impl.{AbstractSQLDBFunctionSymbolFactory, DefaultSQLTimestampISONormFunctionSymbol}
import it.unibz.inf.ontop.substitution.impl.ImmutableSubstitutionImpl
import org.apache.spark.serializer.KryoRegistrator


/**
 * @author Lorenz Buehmann
 */
class OntopKryoRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {

    HashMultimapSerializer.registerSerializers(kryo);

    // Partitioning
    kryo.register(classOf[net.sansa_stack.rdf.common.partition.core.RdfPartitionComplex])
    kryo.register(classOf[Array[net.sansa_stack.rdf.common.partition.core.RdfPartitionComplex]])

    kryo.register(classOf[ImmutableTerm])
    kryo.register(classOf[it.unibz.inf.ontop.substitution.impl.ImmutableSubstitutionImpl[_]])
    kryo.register(classOf[it.unibz.inf.ontop.utils.CoreUtilsFactory])
    kryo.register(classOf[DefaultSQLTimestampISONormFunctionSymbol])
    kryo.register(classOf[AbstractSQLDBFunctionSymbolFactory])
//    kryo.register(classOf[java.lang.invoke.SerializedLambda]);
//    kryo.register(classOf[ClosureSerializer.Closure], new ClosureSerializer() {
//
//      override def read(kryo: Kryo, input: Input, t: Class) {
//        try {
//          val lambda = kryo.readObject(input, classOf[SerializedLambda])
//          import java.lang.invoke.CallSite
//          import java.lang.invoke.LambdaMetafactory
//          import java.lang.invoke.MethodHandle
//          import java.lang.invoke.MethodHandleInfo
//          import java.util
//          val capturingClass = classOf[RDFTermTypeImpl].invoke(lambda).asInstanceOf[Class[_]]
//          val cl = capturingClass.getClassLoader
//          val implClass = cl.loadClass(lambda.getImplClass.replace('/', '.'))
//          val interfaceType = cl.loadClass(lambda.getFunctionalInterfaceClass.replace('/', '.'))
//          val lookup = getLookup(implClass)
//          val implType = MethodType.fromMethodDescriptorString(lambda.getImplMethodSignature, cl)
//          val samType = MethodType.fromMethodDescriptorString(lambda.getFunctionalInterfaceMethodSignature, null)
//
//          var implMethod = null
//          var implIsInstanceMethod = true
//          lambda.getImplMethodKind match {
//            case MethodHandleInfo.REF_invokeInterface =>
//            case MethodHandleInfo.REF_invokeVirtual =>
//              implMethod = lookup.findVirtual(implClass, lambda.getImplMethodName, implType)
//
//            case MethodHandleInfo.REF_invokeSpecial =>
//              implMethod = lookup.findSpecial(implClass, lambda.getImplMethodName, implType, implClass)
//
//            case MethodHandleInfo.REF_invokeStatic =>
//              implMethod = lookup.findStatic(implClass, lambda.getImplMethodName, implType)
//              implIsInstanceMethod = false
//
//            case _ =>
//              throw new RuntimeException("Unsupported impl method kind " + lambda.getImplMethodKind)
//          }
//
//          // determine type of factory
//          var factoryType = MethodType.methodType(interfaceType, util.Arrays.copyOf(implType.parameterArray, implType.parameterCount - samType.parameterCount))
//          if (implIsInstanceMethod) factoryType = factoryType.insertParameterTypes(0, implClass)
//
//
//          // determine type of method with implements the SAM
//          var instantiatedType = implType
//          if (implType.parameterCount > samType.parameterCount) instantiatedType = implType.dropParameterTypes(0, implType.parameterCount - samType.parameterCount)
//
//          // call factory
//          val callSite = LambdaMetafactory.altMetafactory(lookup, lambda.getFunctionalInterfaceMethodName, factoryType, samType, implMethod, instantiatedType, 1)
//
//          // invoke callsite
//          val capturedArgs = new Array[AnyRef](lambda.getCapturedArgCount)
//          var i = 0
//          while ( {
//            i < lambda.getCapturedArgCount
//          }) {
//            capturedArgs(i) = lambda.getCapturedArg(i)
//
//            i += 1
//          }
//          return callSite.dynamicInvoker.invokeWithArguments(capturedArgs)
//
//        } catch {
//          e: Throwable => throw new RuntimeException(e)
//        }
//      };
//    });

  }

}
