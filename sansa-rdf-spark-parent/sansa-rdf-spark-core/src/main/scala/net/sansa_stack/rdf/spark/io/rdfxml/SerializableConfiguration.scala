package net.sansa_stack.rdf.spark.io.rdfxml

import java.io.{ObjectInputStream, ObjectOutputStream}

import net.sansa_stack.rdf.spark.utils.Utils
import org.apache.hadoop.conf.Configuration

class SerializableConfiguration(@transient var value: Configuration) extends Serializable {
  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {
    out.defaultWriteObject()
    value.write(out)
  }

  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    value = new Configuration(false)
    value.readFields(in)
  }
}