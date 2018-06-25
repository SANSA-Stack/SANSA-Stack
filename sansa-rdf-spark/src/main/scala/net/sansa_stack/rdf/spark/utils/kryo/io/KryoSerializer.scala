package net.sansa_stack.rdf.spark.utils.kryo.io

import java.nio.ByteBuffer

import scala.reflect.ClassTag

import org.apache.spark.SparkEnv
import org.apache.spark.serializer.Serializer


/**
  * Java object serialization using Kryo. This is much more efficient, but Kryo
  * sometimes is buggy to use. We use this mainly to serialize the object
  * inspectors.
  */
object KryoSerializer
{

  @transient lazy val ser: Serializer =
  {
//    val sparkConf = Option(SparkEnv.get).map(_.conf).getOrElse(new SparkConf())
//    new SparkKryoSerializer(sparkConf)
    SparkEnv.get.serializer
  }

  def serialize[T: ClassTag](o: T): Array[Byte] =
  {
    ser.newInstance().serialize(o).array()
  }

  def deserialize[T: ClassTag](bytes: Array[Byte]): T =
  {
    ser.newInstance().deserialize[T](ByteBuffer.wrap(bytes))
  }
}
