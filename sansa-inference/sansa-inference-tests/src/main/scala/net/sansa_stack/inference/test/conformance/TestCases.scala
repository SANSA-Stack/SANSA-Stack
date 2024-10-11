package net.sansa_stack.inference.test.conformance

import com.google.common.reflect.ClassPath

import java.io.File
import java.nio.file.{FileSystem, Path, Paths}
import org.apache.jena.riot.{Lang, RDFDataMgr}

import scala.collection.mutable.ListBuffer
import scala.xml.XML
import org.apache.commons.io.IOUtils
import org.apache.jena.rdf.model.ModelFactory
import org.scalatest.path

/**
  * Test cases loader.
  *
  * @author Lorenz Buehmann
 *  @author Claus Stadler
  */
object TestCases {

  // val logger = com.typesafe.scalalogging.Logger("TestCases")

  def loadTestCasesJar(directory: String, ids: Set[String] = Set.empty): Seq[TestCase] = {
    import scala.jdk.CollectionConverters._

    val suffix = ".metadata.properties"
    val classLoader = TestCase.getClass.getClassLoader
    val matches = ClassPath.from(classLoader).getResources.asScala
      .map(_.getResourceName)
      .filter(_.startsWith(directory))
      .filter(_.endsWith(suffix))
      .map(x => x.substring(0, x.length - suffix.length))
      .filter(x => ids.isEmpty || ids.contains(deriveTestCaseId(x)))
      .flatMap(x => loadTestCase(x, classLoader))
      .toSeq

    matches
  }

  def deriveTestCaseId(baseName: String): String = {
    val result = Paths.get(baseName).getFileName.toString
    result
  }

  def loadTestCase(baseName : String, classLoader: ClassLoader): Option[TestCase] = {
    // get the metadata file
    val metadataResource = classLoader.getResource(baseName + ".metadata.properties")
    val metadataIn = metadataResource.openStream

    // load metadata XML
    val metadata = XML.load(metadataIn)

    // id
    val id = (metadata \\ "entry").filter(n => n.attribute("key").get.text == "testcase.id").text

    // description
    val description = (metadata \\ "entry").filter(n => n.attribute("key").get.text == "testcase.description").text

    // test case type
    val entailmentType = (metadata \\ "entry").filter(n => n.attribute("key").get.text == "testcase.type").text

    metadataIn.close

    // currently we support only entailment test cases
    if (entailmentType == "POSITIVE_ENTAILMENT") {
      // load input data

      val inputGraph = RDFDataMgr.loadModel(baseName + ".premisegraph.ttl")

      // load output data
      val outputGraph = RDFDataMgr.loadModel(baseName + ".conclusiongraph.ttl")

      Some(TestCase(id, description, entailmentType, inputGraph, outputGraph))
    } else {
      None
    }
  }
}
