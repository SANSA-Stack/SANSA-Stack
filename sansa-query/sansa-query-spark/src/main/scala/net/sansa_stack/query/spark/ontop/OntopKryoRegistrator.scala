package net.sansa_stack.query.spark.ontop

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.serializers.JavaSerializer
import de.javakaffee.kryoserializers.guava.HashMultimapSerializer
import it.unibz.inf.ontop.model.term.ImmutableTerm
import it.unibz.inf.ontop.model.term.functionsymbol.db.impl.{AbstractSQLDBFunctionSymbolFactory, DefaultSQLTimestampISONormFunctionSymbol}
import org.apache.jena.sparql.engine.binding.Binding
import org.apache.spark.serializer.KryoRegistrator
import uk.ac.manchester.cs.owl.owlapi.OWLOntologyImpl
import uk.ac.manchester.cs.owl.owlapi.concurrent.ConcurrentOWLOntologyImpl


/**
 * @author Lorenz Buehmann
 */
class OntopKryoRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {

    HashMultimapSerializer.registerSerializers(kryo)

    // kryo.register(classOf[scala.collection.immutable.Map[_, _]])
    // kryo.register(classOf[HashMap[_, _]])

    // Partitioning
    kryo.register(classOf[net.sansa_stack.rdf.common.partition.core.RdfPartitionComplex])
    kryo.register(classOf[Array[net.sansa_stack.rdf.common.partition.core.RdfPartitionComplex]])

    kryo.register(classOf[Array[Binding]])

    kryo.register(classOf[ImmutableTerm])
    kryo.register(classOf[it.unibz.inf.ontop.substitution.impl.ImmutableSubstitutionImpl[_]])
    kryo.register(classOf[it.unibz.inf.ontop.utils.CoreUtilsFactory])
    kryo.register(classOf[DefaultSQLTimestampISONormFunctionSymbol])
    kryo.register(classOf[AbstractSQLDBFunctionSymbolFactory])

    // OWLOntology
    kryo.register(classOf[OWLOntologyImpl], new JavaSerializer())
    kryo.register(classOf[ConcurrentOWLOntologyImpl], new JavaSerializer())
  }

}
