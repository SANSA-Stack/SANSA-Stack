package net.sansa_stack.rdf.spark.model.rdd

import org.scalatest.FunSuite
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.jena.riot.Lang
import net.sansa_stack.rdf.spark.io._

class TripleOpsTests extends FunSuite with DataFrameSuiteBase {

  import net.sansa_stack.rdf.spark.model._

  test("converting RDD of triples into DataFrame should result in size 9") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val graph = triples.toDF()
    val size = graph.count()

    assert(size == 9)
  }

  test("converting RDD of triples into DataSet should result in size 9") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val graph = triples.toDS()
    val size = graph.count()

    assert(size == 9)
  }

  test("getting all the subjects should result in size 9") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val graph = triples.getSubjects()
    val size = graph.count()

    assert(size == 9)
  }

  test("getting all the predicates should result in size 9") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val graph = triples.getPredicates()
    val size = graph.count()

    assert(size == 9)
  }

  test("getting all objects should result in size 9") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val graph = triples.getObjects()
    val size = graph.count()

    assert(size == 9)
  }

  test("filtering subjects which are URI should result in size 7") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val graph = triples.filterSubjects(_.isURI())
    val size = graph.count()

    assert(size == 7)
  }

  test("filtering predicates which are variable should result in size 0") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val graph = triples.filterPredicates(_.isVariable())
    val size = graph.count()

    assert(size == 0)
  }
  
   test("filtering objects which are literalsshould result in size 9") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val graph = triples.filterObjects(_.isLiteral())
    val size = graph.count()

    assert(size == 0)
  }
   

}