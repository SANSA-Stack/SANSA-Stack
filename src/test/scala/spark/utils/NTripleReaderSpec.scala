package spark.utils

import net.sansa.rdf.spark.utils.NTripleReader
import java.io.File

import org.scalatest._
import spark.UnitSpec

class NTripleReaderSpec extends UnitSpec {

  def fixture =
    new {
      //val rdfFile = Source.fromURL(getClass.getResource("/rdf.nt"))
      val rdfFile = new File(getClass.getResource("/rdf.nt").getPath())
    }

  it should "read rdf.nt" in {
    val f = fixture
    val tripleReader = NTripleReader.fromFile(f.rdfFile)
    var count = 0
    for (quad <- tripleReader) {
      val (subject, predicate, _object, literal) = quad
      count += 1
    }
    assert(count === 100)
  }

  it should "not read the whole file in the memory" in {
    assert(false === true, "the whole RDF file should not be read completely into the memory, this will not scale!!!")
  }

  it should "use NTriplesParser for parsing" in {
    assert(false === true, "should use NTriplesParser for parsing ntriples")
  }

}
