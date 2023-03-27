package net.sansa_stack.query.spark.engine

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream
import org.apache.commons.lang3.time.StopWatch
import org.apache.hadoop.fs.Path
import org.apache.jena.query._
import org.apache.jena.riot.resultset.ResultSetLang
import org.apache.jena.riot.{Lang, RDFDataMgr, ResultSetMgr}
import org.apache.jena.sparql.resultset.ResultSetCompare
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.scalatest.{FunSuite, Ignore}

import java.io.File
import java.nio.file.{Files, Paths}
import java.util.concurrent.TimeUnit
import net.sansa_stack.hadoop.format.jena.trig.RecordReaderRdfTrigDataset
import net.sansa_stack.query.spark.api.domain.ResultSetSpark
import net.sansa_stack.query.spark.rdd.op.RddOfBindingsOps
import org.aksw.jenax.arq.dataset.api.DatasetOneNg

@Ignore // Doesn't always find the bz2 file the way its used heer
class BindingEngineTests extends FunSuite with DataFrameSuiteBase {

  // JenaSystem.init

  override def conf(): SparkConf = {
    val conf = super.conf
    conf
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", String.join(", ",
        "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator"))
    conf
  }


  def createTestRdd(): RDD[DatasetOneNg] = {
    import net.sansa_stack.rdf.spark.io._

    val testFile = new File(classOf[RecordReaderRdfTrigDataset].getClassLoader.getResource("hobbit-sensor-stream-150k-events-data.trig.bz2").getPath)
    val path = new Path(testFile.getAbsolutePath)

    spark.datasets(Lang.TRIG)(path.toString)
  }


  test("group of RDD[Binding] should match expected result") {
    // val referenceFile = new File(getClass.getClassLoader.getResource("hobbit-sensor-stream-150k-events-data.trig.bz2").getPath)
    val referencePath = Paths.get("../../sansa-resource-testdata/src/main/resources/hobbit-sensor-stream-150k-events-data.trig.bz2").toAbsolutePath


    // read the target dataset
    val sw1 = StopWatch.createStarted
    val refDataset = DatasetFactory.create()
    val in = new BZip2CompressorInputStream(Files.newInputStream(referencePath))
    RDFDataMgr.read(refDataset, in, Lang.TRIG)
    // TODO Use try-with-resources ... scala 2.13's Using...
    in.close

    val qe = QueryExecutionFactory.create(
      """
        | SELECT ?qec (SUM(?qc_contrib) AS ?qc) {
        |     { SELECT (COUNT(?qe) AS ?qec) (1 as ?qc_contrib) {
        |       GRAPH ?g { ?s <http://www.w3.org/ns/sosa/#featureOfInterest> ?qe }
        |     } GROUP BY ?g }
        | }
        | GROUP BY ?qec ORDER BY ?qec
        |""".stripMargin, refDataset)

    val expectedRs = ResultSetFactory.makeRewindable(qe.execSelect())
    println("Jena took " + sw1.getTime(TimeUnit.SECONDS))

    val sw2 = StopWatch.createStarted
    val rdd = createTestRdd()

    val resultSetSpark: ResultSetSpark = RddOfBindingsOps.execSparqlSelect(rdd, QueryFactory.create(
      """
        | SELECT ?qec (SUM(?qc_contrib) AS ?qc) {
        |   SERVICE <rdd:perPartition> {
        |     { SELECT (COUNT(?qe) AS ?qec) (1 as ?qc_contrib) {
        |       GRAPH ?g { ?s <http://www.w3.org/ns/sosa/#featureOfInterest> ?qe }
        |     } GROUP BY ?g }
        |   }
        | }
        | GROUP BY ?qec ORDER BY ?qec
        |""".stripMargin), null)

    val actualRs = ResultSetFactory.makeRewindable(ResultSet.adapt(resultSetSpark.collectToTable.toRowSet))
    println("Spark took " + sw2.getTime(TimeUnit.SECONDS))

    val isEqual = ResultSetCompare.equalsByValue(expectedRs, actualRs)
    assertTrue(isEqual)

    expectedRs.reset()
    actualRs.reset()
    ResultSetMgr.write(System.out, expectedRs, ResultSetLang.RS_Text)
    ResultSetMgr.write(System.out, actualRs, ResultSetLang.RS_Text)
  }


}