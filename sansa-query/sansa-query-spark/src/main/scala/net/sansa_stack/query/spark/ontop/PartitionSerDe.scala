package net.sansa_stack.query.spark.ontop

import java.nio.file.{Files, Path}

import com.twitter.chill.{KryoBase, KryoPool, ScalaKryoInstantiator}

import net.sansa_stack.rdf.common.partition.core.RdfPartitionComplex

/**
 * @author Lorenz Buehmann
 */
object PartitionSerDe {



  val kryo: KryoBase = {
    val instantiator = new ScalaKryoInstantiator()
    instantiator.setRegistrationRequired(true)
    instantiator.newKryo
  }
  kryo.register(classOf[RdfPartitionComplex])
  kryo.register(classOf[scala.collection.immutable.Set[_]])
  kryo.register(classOf[scala.collection.immutable.Set[RdfPartitionComplex]])
  val pool: KryoPool = KryoPool.withByteArrayOutputStream(4, new ScalaKryoInstantiator())


//  val kryo = new Kryo
//
//  kryo.setRegistrationRequired(false)
//  kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
//  kryo.register(classOf[RdfPartitionComplex])
//  kryo.register(classOf[scala.collection.immutable.Set[_]])
//  kryo.register(classOf[scala.collection.immutable.Set[RdfPartitionComplex]])

  def serialize[T](t: T): Array[Byte] =
    ScalaKryoInstantiator.defaultPool.toBytesWithClass(t)
  def deserialize[T](bytes: Array[Byte]): T =
    ScalaKryoInstantiator.defaultPool.fromBytes(bytes).asInstanceOf[T]
  def rt[T](t: T): T = deserialize(serialize(t))


  def serializeTo(partitions: Set[RdfPartitionComplex], path: Path): Unit = {
    val bytes = serialize(partitions)
    Files.write(path, bytes)
  }

  def deserializeFrom(path: Path): Set[RdfPartitionComplex] = {
    val bytes = Files.readAllBytes(path)
    deserialize(bytes)
  }

}
