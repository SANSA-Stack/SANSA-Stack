package net.sansa_stack.inference.flink.data

import java.net.URI

import scala.collection.JavaConverters._
import scala.language.implicitConversions

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.jena.riot.{Lang, RDFDataMgr}

import net.sansa_stack.rdf.benchmark.io.ReadableByteChannelFromIterator


/**
  * @author Lorenz Buehmann
  */
object RDFGraphLoader {

  implicit def pathURIsConverter(uris: Seq[URI]): String = uris.map(p => p.toString).mkString(",")


  def loadFromDisk(path: String, env: ExecutionEnvironment): RDFGraph = {
    loadFromDisk(URI.create(path), env)
  }

  def loadFromDisk(path: URI, env: ExecutionEnvironment): RDFGraph = {
    loadFromDisk(Seq(path), env)
  }

  def loadFromDisk(paths: Seq[URI], env: ExecutionEnvironment): RDFGraph = {
//    // create a configuration object
//    val parameters = new Configuration
//
//    // set the recursive enumeration parameter
//    parameters.setBoolean("recursive.file.enumeration", true)
//    env.readTextFile(f).withParameters(parameters)

    val tmp: List[String] = paths.map(path => path.toString).toList

    val triples = tmp
      .map(f => env.readTextFile(f)) // no support to read from multiple paths at once, thus, map + union here
      .reduce(_ union _) // TODO Flink 1.5.0 supports multiple paths via FileInputFormat
      .mapPartition(p => {
        // convert iterator to input stream
        val is = ReadableByteChannelFromIterator.toInputStream(p.asJava)

        RDFDataMgr.createIteratorTriples(is, Lang.NTRIPLES, null).asScala
      })
      .name("triples")

    RDFGraph(triples)
  }

  def main(args: Array[String]): Unit = {
    if (args.length == 0) println("Usage: RDFGraphLoader <PATH_TO_FILE>")

    val path = args(0)

    val env = ExecutionEnvironment.getExecutionEnvironment

    val ds = RDFGraphLoader.loadFromDisk(path, env).triples

    println(s"size:${ds.count}")
    println("sample data:\n" + ds.first(10).map { _.toString.replaceAll("[\\x00-\\x1f]","???")}.collect().mkString("\n"))
  }

}
