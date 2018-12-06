package net.sansa_stack.rdf.spark.io

import java.io.{ByteArrayInputStream, File, FileInputStream, FileOutputStream}
import java.net.URL
import java.nio.file.{Files, Path, Paths}
import java.util.zip.ZipInputStream

import scala.collection.JavaConverters._

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import net.sansa_stack.rdf.spark.io.index.TriplesIndexer
import org.apache.jena.rdf.model.{ModelFactory, ResourceFactory}
import org.apache.jena.riot.Lang
import org.apache.jena.vocabulary.RDF
import org.scalatest.FunSuite

/**
  * Tests for loading triples from either N-Triples are Turtle files into a DataFrame.
  *
  * @author Lorenz Buehmann
  */
class RDFLoadingTests extends FunSuite with DataFrameSuiteBase {

  import net.sansa_stack.rdf.spark.io._

  test("loading N-Triples file into DataFrame with REGEX parsing mode should result in 10 triples") {

    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.read.rdf(lang)(path)

    val cnt = triples.count()
    assert(cnt == 10)
  }

  test("loading Turtle file into DataFrame should result in 12 triples") {

    val path = getClass.getResource("/loader/data.ttl").getPath
    val lang: Lang = Lang.TURTLE

    val triples = spark.read.rdf(lang)(path)

    val cnt = triples.count()
    assert(cnt == 12)
  }

  test("loading RDF/XML file into DataFrame should result in 12 triples") {

    val path = getClass.getResource("/loader/data.rdf").getPath
    val lang: Lang = Lang.TURTLE

    // This test only works when "wholeFile" is set to true
    val triples = spark.read.option("wholeFile", true).rdfxml(path)

    val cnt = triples.count()
    assert(cnt == 9)
  }

  def extractZipFile(zis: ZipInputStream, destination: Path): Unit = {
    Stream.continually(zis.getNextEntry).takeWhile(_ != null).foreach { file =>
      if (!file.isDirectory) {
        val outPath = destination.resolve(file.getName)
        val outPathParent = outPath.getParent
        if (!outPathParent.toFile.exists()) {
          outPathParent.toFile.mkdirs()
        }

        val outFile = outPath.toFile
        val out = new FileOutputStream(outFile)
        val buffer = new Array[Byte](4096)
        Stream.continually(zis.read(buffer)).takeWhile(_ != -1).foreach(out.write(buffer, 0, _))
      }
    }
  }

  test("RDF 1.1 Turtle test suites must be parsed correctly") {

    // load test suite from URL
    val url = new URL("https://www.w3.org/2013/TurtleTests/TESTS.zip")
    // zip file content
    val zis: ZipInputStream = new ZipInputStream(url.openStream())

    val tmpFolder = Files.createTempDirectory("sansa-turtle-tests")

    // extract the zip file
    extractZipFile(zis, tmpFolder)

    val lang: Lang = Lang.TURTLE

    val path = tmpFolder.resolve("TurtleTests/")

    val sourceProp = ResourceFactory.createProperty("http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#action")
    val targetProp = ResourceFactory.createProperty("http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#result")

    val relativeIRI = "http://www.w3.org/2013/TurtleTests/"

    val manifest = ModelFactory.createDefaultModel()
    manifest.read(new FileInputStream(path.resolve("manifest.ttl").toString), path.toFile.toURI.toString, "TURTLE")

    val tests = manifest.listSubjectsWithProperty(
      RDF.`type`,
      ResourceFactory.createResource("http://www.w3.org/ns/rdftest#TestTurtleEval")).asScala
    val files = tests.map(test =>
      (test.getPropertyResourceValue(sourceProp).getURI, test.getPropertyResourceValue(targetProp).getURI)
    )

    files.foreach(file => {
      try {
        println(s"loading file ${file}")
        // load the triples into the DataFrame
        val triples = spark.read.rdf(lang)(file._1)

        // write triples to a string and load it into a Jena model
        val ntriplesString = triples.collect().map(row => {
          var s = row.getString(0)
          s = if (s.startsWith("http")) s"<$s>" else s"_:$s"
          val p = s"<${row.getString(1)}>"
          var o = row.getString(2)
          if (o.startsWith("http")) o = s"<$o>"
          s"$s $p $o ."
        })
        .mkString("\n")
        println(ntriplesString)

        // write the triple as N-Triples and load into Jena model
        val sourceModel = ModelFactory.createDefaultModel()
        sourceModel.read(new ByteArrayInputStream(ntriplesString.getBytes), null, "TURTLE")

        // load the reference data into a Jena model
        val targetModel = ModelFactory.createDefaultModel()
        targetModel.read(new ByteArrayInputStream(ntriplesString.getBytes), relativeIRI, "TURTLE")

        // check for isomorphism
        val isomorph = sourceModel.isIsomorphicWith(targetModel)
        println(s"isomorph: $isomorph")
      } catch {
        case e: Exception => println(e.getMessage) // e.printStackTrace()
      }

    })

  }

  test("bla") {
    val sourceModel = ModelFactory.createDefaultModel()
    val turtleFile = new File(getClass.getResource("/loader/data.ttl").getPath)
    val fileInputStream = new FileInputStream(turtleFile)
    sourceModel.read(fileInputStream, null, "TURTLE")

    sourceModel.listStatements().asScala.toSeq.foreach(st => {
      val s = st.getSubject
      if (s.isAnon) println(s.getId.getLabelString)
    })
  }
}
