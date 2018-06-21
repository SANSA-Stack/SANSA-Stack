package net.sansa_stack.rdf.spark.utils.kryo.io

import scala.reflect.ClassTag

/**
  * A wrapper around some unserializable objects that make them both Java
  * serializable. Internally, Kryo is used for serialization.
  *
  * Use KryoSerializationWrapper(value) to create a wrapper.
  */
class KryoSerializationWrapper[T: ClassTag] extends Serializable
{

  @transient var value: T = _

  private var valueSerialized: Array[Byte] = _

  // The getter and setter for valueSerialized is used for XML serialization.
  def getValueSerialized(): Array[Byte] =
  {
    valueSerialized = KryoSerializer.serialize(value)
    valueSerialized
  }

  def setValueSerialized(bytes: Array[Byte]): Unit =
  {
    valueSerialized = bytes
    value = KryoSerializer.deserialize[T](valueSerialized)
  }

  // Used for Java serialization.
  private def writeObject(out: java.io.ObjectOutputStream)
  {
    getValueSerialized()
    out.defaultWriteObject()
  }

  private def readObject(in: java.io.ObjectInputStream)
  {
    in.defaultReadObject()
    setValueSerialized(valueSerialized)
  }
}

object KryoSerializationWrapper
{
  def apply[T: ClassTag](value: T): KryoSerializationWrapper[T] =
  {
    val wrapper = new KryoSerializationWrapper[T]
    wrapper.value = value
    wrapper
  }
}
