package net.sansa_stack.ml.spark.kernel

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import net.sansa_stack.rdf.spark.io._
import org.apache.jena.riot.Lang
import org.scalatest.FunSuite

class RDFFastGraphKernelTests extends FunSuite with DataFrameSuiteBase {

  val path = getClass.getResource("/kernel/aifb-fixed_no_schema4.nt").getPath

  val iteration = 1

  test("running RDF Graph Kernel with Task='Affiliation' on 1 iteration or folding on validation") {
    val t0 = System.nanoTime
    val lang = Lang.NTRIPLES

    val triples = spark.rdf(lang)(path)
      .filter(_.getPredicate.getURI != "http://swrc.ontoware.org/ontology#employs")

    val rdfFastGraphKernel = RDFFastGraphKernel(spark, triples, "http://swrc.ontoware.org/ontology#affiliation")
    val data = rdfFastGraphKernel.getMLLibLabeledPoints

    val t1 = System.nanoTime
    RDFFastTreeGraphKernelUtil.printTime("Initialization", t0, t1)

    RDFFastTreeGraphKernelUtil.predictLogisticRegressionMLLIB(data, 4, iteration)

    val t2 = System.nanoTime
    RDFFastTreeGraphKernelUtil.printTime("Run Prediction", t1, t2)

    assert(true)
  }

}