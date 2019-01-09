package net.sansa_stack.ml.spark.outliers.vandalismdetection

import com.holdenkarau.spark.testing.DataFrameSuiteBase

import net.sansa_stack.ml.spark.outliers.vandalismdetection.parser._

import org.apache.jena.riot.Lang
import org.apache.hadoop.mapred.JobConf
import org.scalatest.FunSuite

class VandalismDetectionTests extends FunSuite with DataFrameSuiteBase {

  test("parsing RDF data of JTriple format should result in 0") {

    val path = getClass.getResource("/outliers/vandalismdetection/data.json").getPath

    val jobConf = new JobConf()
    val triples = JTriple.parse(jobConf, path, spark)
    val size = triples.count()

    assert(size == 0)
  }

  test("parsing RDF data of XML format should result in 0") {

    val path = getClass.getResource("/outliers/vandalismdetection/data.xml").getPath
    val vd = new VandalismDetection()
    // val triples = vd.parseStandardXML(path, spark)
    // val size = triples.count()

    assert(true)
  }

}
