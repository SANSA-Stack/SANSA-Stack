package net.sansa_stack.query.spark.graph.jena.serialization

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.charset.StandardCharsets

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import org.apache.jena.query.{Dataset, DatasetFactory, Query, QueryFactory, Syntax}
import org.apache.jena.riot.{Lang, RDFDataMgr, RDFFormat}

/**
 * Serializers for the sansa query layer
 *
 */
object JenaKryoSerializers {

  // No common query layer specific serializers so far
  // This is the place to add them in when the need arises

}
