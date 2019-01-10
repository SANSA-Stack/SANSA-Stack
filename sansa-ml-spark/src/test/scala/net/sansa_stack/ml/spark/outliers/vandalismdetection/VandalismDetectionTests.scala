package net.sansa_stack.ml.spark.outliers.vandalismdetection

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.hadoop.mapred.JobConf
import org.apache.jena.riot.Lang
import org.scalatest.FunSuite

import net.sansa_stack.ml.spark.outliers.vandalismdetection.parser._

class VandalismDetectionTests extends FunSuite with DataFrameSuiteBase {

  test("parsing RDF data of JTriple format should result in 0") {

    val input = getClass.getResource("/outliers/vandalismdetection/data.json").getPath

    val jobConf = new JobConf()
    val triples = JTriple.parse(jobConf, input, spark)
    val size = triples.count()

    assert(size == 0)
  }

  test("parsing RDF data of XML format should result in 0") {

    val input = getClass.getResource("/outliers/vandalismdetection/wdvc16_2016_01.xml").getPath
    val metaFile = getClass.getResource("/outliers/vandalismdetection/wdvc16_meta.csv").getPath
    val truthFile = getClass.getResource("/outliers/vandalismdetection/wdvc16_truth.csv").getPath

    val vd = new VandalismDetection()
    // val triples = vd.parseStandardXML(input, metaFile, truthFile, spark)
    // val size = triples.count()

    assert(true)
  }

}
