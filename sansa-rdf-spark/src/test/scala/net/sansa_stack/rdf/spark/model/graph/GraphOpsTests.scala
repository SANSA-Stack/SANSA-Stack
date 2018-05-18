package net.sansa_stack.rdf.spark.model.graph
import org.scalatest.FunSuite
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.jena.riot.Lang
import net.sansa_stack.rdf.spark.io._

class GraphOpsTests extends FunSuite with DataFrameSuiteBase {

  import net.sansa_stack.rdf.spark.model.graph._

  test("loading N-Triples file into Graph should result in size 7") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val graph = triples.asGraph()
    val size = graph.size()

    assert(size == 7)
  }

  test("conversation of Graph into RDD should result in 9 triples") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val graph = triples.asGraph()

    val graph2rdd = graph.toRDD()

    val cnt = graph2rdd.count()
    assert(cnt == 7)
  }

}