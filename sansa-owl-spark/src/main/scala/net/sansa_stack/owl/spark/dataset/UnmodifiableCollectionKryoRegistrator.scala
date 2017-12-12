package net.sansa_stack.owl.spark.dataset

import com.esotericsoftware.kryo.Kryo
import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer
import org.apache.spark.serializer.KryoRegistrator

class UnmodifiableCollectionKryoRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit = {
    val cls = Class.forName("java.util.Collections$UnmodifiableCollection")
    kryo.addDefaultSerializer(cls, new UnmodifiableCollectionsSerializer)
  }
}
