package net.sansa_stack.rdf.spark.io

import java.nio.file.{Files, Path, Paths}

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.jena.riot.Lang
import org.apache.spark.sql.SaveMode
import org.scalatest.FunSuite

/**
  * Tests for loading triples from either N-Triples are Turtle files into a DataFrame.
  *
  * @author Lorenz Buehmann
  */
class RDFWritingTests extends FunSuite with DataFrameSuiteBase {

  import net.sansa_stack.rdf.spark.io._

  test("writing N-Triples file from DataFrame to disk should result in file with 10 triples") {

    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    // load the triples
    val triples = spark.read.rdf(lang)(path)

    // validate count
    val cnt1 = triples.count()
    assert(cnt1 == 10)

    // create temp dir
    val tmpDir = Files.createTempDirectory("sansa")
    tmpDir.toFile.deleteOnExit()

    // write to temp dir
    triples
      .write
      .mode(SaveMode.Overwrite)
      .ntriples(tmpDir.toString)

    // load again
    val triples2 = spark.read.rdf(lang)(path)

    // and check if count is the same
    val cnt2 = triples2.count()
    assert(cnt2 == cnt1)
  }

  def createTempDir(tmpName: String): String = {
    val tmpDir = Paths.get(System.getProperty("java.io.tmpdir"))
    val name: Path = tmpDir.getFileSystem.getPath(tmpName)
    if (name.getParent != null) throw new IllegalArgumentException("Invalid prefix or suffix")
    tmpDir.resolve(name).toString
  }

}
