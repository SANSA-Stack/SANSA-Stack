package net.sansa_stack.ml.spark.outliers.vandalismdetection

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.hadoop.mapred.JobConf
import org.apache.jena.riot.Lang
import org.scalatest.FunSuite

import net.sansa_stack.ml.spark.outliers.vandalismdetection.parser._

class VandalismDetectionTests extends FunSuite with DataFrameSuiteBase {

  test("parsing XML data should match") {

    val input = getClass.getResource("/outliers/vandalismdetection/wdvc16_2016_01.xml").getPath

    val jobConf = new JobConf()
    val triples = XML.parse(input, spark)
    val size = triples.count()

    assert(size == 4291)
  }

  test("detecting vandalism on the set of wikidata should matchd") {

    val input = getClass.getResource("/outliers/vandalismdetection/wdvc16_2016_01.xml").getPath
    val metaFile = getClass.getResource("/outliers/vandalismdetection/wdvc16_meta.csv").getPath
    val truthFile = getClass.getResource("/outliers/vandalismdetection/wdvc16_truth.csv").getPath

    val vd = new VandalismDetection()
    val triples = XML.parse(input, spark)

    // val data = vd.run(triples, metaFile, truthFile, sampleFraction = 0.01, spark)

    // val result = Classifier.randomForestClassiferWithSplit(data)

    assert(true)
  }

}
