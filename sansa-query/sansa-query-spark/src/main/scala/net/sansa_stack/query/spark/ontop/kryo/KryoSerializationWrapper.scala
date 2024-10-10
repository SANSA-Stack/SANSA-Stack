package net.sansa_stack.query.spark.ontop.kryo

import org.apache.spark.serializer.{KryoSerializer => SparkKryoSerializer}
import org.apache.spark.{SparkConf, SparkEnv}

import java.nio.ByteBuffer
import scala.reflect.ClassTag

object KryoSerializer {

  @transient lazy val ser: SparkKryoSerializer = {
    val sparkConf = Option(SparkEnv.get).map(_.conf).getOrElse(new SparkConf())
    new SparkKryoSerializer(sparkConf)
  }

  def serialize[T: ClassTag](o: T): Array[Byte] = {
    ser.newInstance().serialize(o).array()
  }

  def deserialize[T: ClassTag](bytes: Array[Byte]): T = {
    ser.newInstance().deserialize[T](ByteBuffer.wrap(bytes))
  }
}

/**
 * A wrapper around some unserializable objects that make them both Java
 * serializable. Internally, Kryo is used for serialization.
 *
 * Use KryoSerializationWrapper(value) to create a wrapper.
 */
class KryoSerializationWrapper[T: ClassTag] extends Serializable {

  @transient var value: T = _

  private var valueSerialized: Array[Byte] = _

  // The getter and setter for valueSerialized is used for XML serialization.
  def getValueSerialized: Array[Byte] = {
    valueSerialized = KryoSerializer.serialize(value)
    valueSerialized
  }

  def setValueSerialized(bytes: Array[Byte]): Unit = {
    valueSerialized = bytes
    value = KryoSerializer.deserialize[T](valueSerialized)
  }

  // Used for Java serialization.
  private def writeObject(out: java.io.ObjectOutputStream): Unit = {
    getValueSerialized
    out.defaultWriteObject()
  }

  private def readObject(in: java.io.ObjectInputStream): Unit = {
    in.defaultReadObject()
    setValueSerialized(valueSerialized)
  }
}


object KryoSerializationWrapper {
  def apply[T: ClassTag](value: T): KryoSerializationWrapper[T] = {
    val wrapper = new KryoSerializationWrapper[T]
    wrapper.value = value
    wrapper
  }
}