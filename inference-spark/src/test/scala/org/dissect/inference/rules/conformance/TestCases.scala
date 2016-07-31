package org.dissect.inference.rules.conformance

import java.io.File

import org.apache.jena.riot.RDFDataMgr

import scala.collection.mutable.ListBuffer
import scala.xml.XML

/**
  * Test cases loader.
  *
  * @author Lorenz Buehmann
  */
object TestCases {

  /**
    * Loads test cases from the given root folder.
    *
    * @param directory the root folder containing sub-folders for each test case
    * @return test cases
    */
  def loadTestCases(directory: File, ids: Set[String] = Set.empty): Seq[TestCase] = {

    val testCases = new ListBuffer[TestCase]()

    directory.listFiles().filter(f => f.isDirectory && (ids.isEmpty || ids.contains(f.getName))).foreach { subDirectory =>

      // the files in the directory
      val files = subDirectory.listFiles()

      // get the metadata file
      val metadataFile = files.filter(_.getName.endsWith(".metadata.properties")).head

      // load metadata XML
      val metadata = XML.loadFile(metadataFile)

      // id
      val id = (metadata \\ "entry").filter(n => n.attribute("key").get.text == "testcase.id").text

      // description
      val description = (metadata \\ "entry").filter(n => n.attribute("key").get.text == "testcase.description").text

      // test case type
      val entailmentType = (metadata \\ "entry").filter(n => n.attribute("key").get.text == "testcase.type").text

      // currently we support only entailment test cases
      if(entailmentType == "POSITIVE_ENTAILMENT") {
        // load input data
        val inputGraph = RDFDataMgr.loadModel(files.filter(_.getName.endsWith(".premisegraph.ttl")).head.getPath)

        // load output data
        val outputGraph = RDFDataMgr.loadModel(files.filter(_.getName.endsWith(".conclusiongraph.ttl")).head.getPath)

        testCases += TestCase(id, description, entailmentType, inputGraph, outputGraph)
      }
    }

    testCases
  }
}
