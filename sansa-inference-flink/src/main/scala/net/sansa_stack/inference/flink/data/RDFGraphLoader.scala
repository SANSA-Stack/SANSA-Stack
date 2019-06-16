package net.sansa_stack.inference.flink.data

import java.net.URI

import scala.language.implicitConversions

import net.sansa_stack.rdf.flink.io.ntriples.NTriplesReader
import org.apache.flink.api.scala.{ExecutionEnvironment, _}


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
    RDFGraph(NTriplesReader.load(env, paths))
  }

  def main(args: Array[String]): Unit = {
    if (args.length == 0) println("Usage: RDFGraphLoader <PATH_TO_FILE>")

    val path = args(0)

//    val env = ExecutionEnvironment.getExecutionEnvironment
    val env = ExecutionEnvironment.createLocalEnvironment(parallelism = 2)

    val ds = RDFGraphLoader.loadFromDisk(path, env).triples

    println(s"size:${ds.count}")
    println("sample data:\n" + ds.first(10).map { _.toString.replaceAll("[\\x00-\\x1f]","???")}.collect().mkString("\n"))
  }

}
