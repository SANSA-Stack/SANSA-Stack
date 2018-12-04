package net.sansa_stack.inference.spark.forwardchaining.axioms

import net.sansa_stack.owl.spark.rdd.FunctionalSyntaxOWLAxiomsRDDBuilder
import net.sansa_stack.owl.spark.rdd.OWLAxiomsRDD
import org.scalatest.FunSuite
import org.semanticweb.owlapi.model.OWLAxiom
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD


class ForwardRuleReasonerRDFSTest extends FunSuite {


  val sparkSession = SparkSession.builder
        .master("local[*]")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .appName("OWL Axioms Forward Rule Reasoner")
        .getOrCreate()

  val sc: SparkContext = sparkSession.sparkContext

  val reasoner = new ForwardRuleReasonerRDFS(sc, 4)

  test("RDFS Axiom Forward Chaining Rule Reasoner") {

      val input = getClass.getResource("/ont_functional.owl").getPath

      var owlAxiomsRDD: OWLAxiomsRDD = FunctionalSyntaxOWLAxiomsRDDBuilder.build(sparkSession, input)
      val reasoner: RDD[OWLAxiom] = new ForwardRuleReasonerRDFS(sc, 4).apply(owlAxiomsRDD, input)

      assert(reasoner.count() == 16)

  }
}
