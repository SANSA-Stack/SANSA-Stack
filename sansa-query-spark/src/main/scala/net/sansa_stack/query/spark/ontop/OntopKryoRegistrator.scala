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
import org.apache.jena.sparql.engine.binding.{Binding, BindingHashMap}
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

    kryo.register(classOf[BindingHashMap])
    kryo.register(classOf[Array[Binding]])

    kryo.register(classOf[ImmutableTerm])
    kryo.register(classOf[it.unibz.inf.ontop.substitution.impl.ImmutableSubstitutionImpl[_]])
    kryo.register(classOf[it.unibz.inf.ontop.utils.CoreUtilsFactory])
    kryo.register(classOf[DefaultSQLTimestampISONormFunctionSymbol])
    kryo.register(classOf[AbstractSQLDBFunctionSymbolFactory])
  }

}
