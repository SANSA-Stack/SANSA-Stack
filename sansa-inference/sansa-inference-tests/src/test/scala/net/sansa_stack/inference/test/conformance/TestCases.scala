package net.sansa_stack.inference.test.conformance

import java.io.File
import java.nio.file.{FileSystem, Path}

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
  */
object TestCases {

  val logger = com.typesafe.scalalogging.Logger("TestCases")

  var fs: FileSystem = null

  /**
    * Loads test cases from the given root folder.
    *
    * @param directory the root folder containing sub-folders for each test case
    * @return test cases
    */
  def loadTestCases(directory: File, ids: Set[String] = Set.empty): Seq[TestCase] = {
    println(s"loading test cases from ${directory.getAbsolutePath}...")

    val testCases = new ListBuffer[TestCase]()

    println(directory)

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
      if (entailmentType == "POSITIVE_ENTAILMENT") {
        // load input data
        val inputGraph = RDFDataMgr.loadModel(files.filter(_.getName.endsWith(".premisegraph.ttl")).head.getPath)

        // load output data
        val outputGraph = RDFDataMgr.loadModel(files.filter(_.getName.endsWith(".conclusiongraph.ttl")).head.getPath)

        testCases += TestCase(id, description, entailmentType, inputGraph, outputGraph)
      }

    }
    println(s"loaded ${testCases.size} test cases")

    testCases
  }

  /**
    * Loads test cases from the given root folder.
    *
    * @param directory the root folder containing sub-folders for each test case
    * @return test cases
    */
  def loadTestCasesJar(directory: String, ids: Set[String] = Set.empty): Seq[TestCase] = {
    logger.info(s"loading test cases from ${directory}...")

    val testCases = new ListBuffer[TestCase]()

    listFiles(directory).filter(f => ids.isEmpty || ids.contains(f.getFileName.toString.replace("/", ""))).map { p =>

//      println(p.toUri)
      // the files in the directory
      val files = listFiles(
        if (p.toUri.getScheme == "jar") p.toString.substring(0) else p.toString, true)

      // get the metadata file
      val metadataFile = files.filter(_.toString.endsWith(".metadata.properties")).head

      // load metadata XML
      val metadata = XML.load(metadataFile.toUri.toURL.openStream())

      // id
      val id = (metadata \\ "entry").filter(n => n.attribute("key").get.text == "testcase.id").text

      // description
      val description = (metadata \\ "entry").filter(n => n.attribute("key").get.text == "testcase.description").text

      // test case type
      val entailmentType = (metadata \\ "entry").filter(n => n.attribute("key").get.text == "testcase.type").text

      // currently we support only entailment test cases
      if (entailmentType == "POSITIVE_ENTAILMENT") {
        // load input data

        val inputGraph = ModelFactory.createDefaultModel()
        inputGraph.read(files.filter(_.toString.endsWith(".premisegraph.ttl")).head.toUri.toURL.openStream(), null, "Turtle")

        // load output data
        val outputGraph = ModelFactory.createDefaultModel()
        outputGraph.read(files.filter(_.toString.endsWith(".conclusiongraph.ttl")).head.toUri.toURL.openStream(), null, "Turtle")

        testCases += TestCase(id, description, entailmentType, inputGraph, outputGraph)
      }
    }
//    directory.listFiles().filter(f => f.isDirectory && (ids.isEmpty || ids.contains(f.getName))).foreach { subDirectory =>


    println(s"loaded ${testCases.size} test cases")

    if(fs != null) fs.close()

    testCases
  }

  private def listFiles(path: String, subDir: Boolean = false): Seq[Path] = {
    import java.nio.file.FileSystems
    import java.nio.file.Files
    import java.nio.file.Paths
    import java.util.Collections

//    println(s"path: $path")
    val uri = if (path.startsWith("/")) new File(path).toURI else classOf[TestCase].getClassLoader.getResource(path).toURI
//    println(s"uri: $uri")
    var myPath: Path = null
    if (uri.getScheme == "jar") {
      fs = FileSystems.newFileSystem(uri, Collections.emptyMap[String, Any])
      myPath = fs.getPath(path)
    }
    else myPath = Paths.get(uri)
    val walk = Files.walk(myPath, 1)
    val it = walk.iterator
    var files = Seq[Path]()
    while ({it.hasNext}) {
      val subPath = it.next()
      if(!subPath.equals(myPath)) {
        files :+= subPath
      }
    }

    if( fs != null) fs.close()

    files
  }
}
