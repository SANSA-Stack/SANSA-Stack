package net.sansa_stack.owl.spark.dataset

import com.esotericsoftware.kryo.Kryo
import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.util.Utils

class UnmodifiableCollectionKryoRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit = {
    // scalastyle:off classforname
    val cls = Class.forName("java.util.Collections$UnmodifiableCollection")
    // scalastyle:on classforname

    kryo.addDefaultSerializer(cls, new UnmodifiableCollectionsSerializer)
  }
}
