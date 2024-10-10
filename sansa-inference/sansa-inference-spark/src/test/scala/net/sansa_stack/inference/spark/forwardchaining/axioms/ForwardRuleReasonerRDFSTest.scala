package net.sansa_stack.inference.spark.forwardchaining.axioms

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import net.sansa_stack.owl.spark.rdd.{FunctionalSyntaxOWLAxiomsRDDBuilder, OWLAxiomsRDD}
import org.apache.spark.rdd.RDD
import org.scalatest.funsuite.AnyFunSuite
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.model.OWLAxiom

/**
  * Entailment pattern naming taken from
  * https://www.w3.org/TR/rdf11-mt/#patterns-of-rdfs-entailment-informative
  */
class ForwardRuleReasonerRDFSTest extends AnyFunSuite with SharedSparkContext with DataFrameSuiteBase {
  private val resourcePath = "/forward_chaining/axioms/"
  private val defaultPrefix = "http://ex.com/default#"
  private val df = OWLManager.getOWLDataFactory

  import net.sansa_stack.owl.spark.owl._

  val reasoner = new ForwardRuleReasonerRDFS(sc, 4)

  test("RDFS Axiom Forward Chaining Rule Reasoner") {

      val input = getClass.getResource("/ont_functional.owl").getPath

      val owlAxiomsRDD: OWLAxiomsRDD = FunctionalSyntaxOWLAxiomsRDDBuilder.build(spark, input)
      val reasoner: RDD[OWLAxiom] = new ForwardRuleReasonerRDFS(sc, 4)(owlAxiomsRDD)

      assert(reasoner.count() == 102)

  }

  /**
    * rdfs2
    *   Condition:
    *     aaa rdfs:domain xxx .
    *     yyy aaa zzz .
    *   Consequence:
    *     yyy rdf:type xxx .
    */
  test("Rule rdfs2 should return correct results") {
    val cls01 = df.getOWLClass(defaultPrefix + "Cls01")
    val cls02 = df.getOWLClass(defaultPrefix + "Cls02")
    val cls03 = df.getOWLClass(defaultPrefix + "Cls03")
    val indivB = df.getOWLNamedIndividual(defaultPrefix + "indivB")
    val indivD = df.getOWLNamedIndividual(defaultPrefix + "indivD")
    val indivF = df.getOWLNamedIndividual(defaultPrefix + "indivF")

    val input = getClass.getResource(resourcePath + "test_rdfs2.owl").getPath

    val axiomsRDD = spark.owl(Syntax.FUNCTIONAL)(input)
    val reasoner = new ForwardRuleReasonerRDFS(sc, sc.defaultMinPartitions)
    val inferred: Seq[OWLAxiom] = reasoner.apply(axiomsRDD).collect()

    // Three axioms should be inferred:
    // ClassAssertion(:Cls01 :indivB)
    // ClassAssertion(:Cls02 :indivD)
    // ClassAssertion(:Cls03 :indivF)
    assert(inferred.size == 27)
    assert(inferred.contains(df.getOWLClassAssertionAxiom(cls01, indivB)))
    assert(inferred.contains(df.getOWLClassAssertionAxiom(cls02, indivD)))
    assert(inferred.contains(df.getOWLClassAssertionAxiom(cls03, indivF)))
  }

  /**
    * rdfs3
    *   Condition:
    *     aaa rdfs:range xxx .
    *     yyy aaa zzz .
    *   Consequence:
    *     zzz rdf:type xxx .
    */
  test("Rule rdfs3 should return correct results") {
    val cls01 = df.getOWLClass(defaultPrefix + "Cls01")
    val indivC = df.getOWLNamedIndividual(defaultPrefix + "indivC")

    val input = getClass.getResource(resourcePath + "test_rdfs3.owl").getPath

    val axiomsRDD = spark.owl(Syntax.FUNCTIONAL)(input)
    val reasoner = new ForwardRuleReasonerRDFS(sc, sc.defaultMinPartitions)
    val inferred: Seq[OWLAxiom] = reasoner.apply(axiomsRDD).collect()

    // One axiom should be inferred:
    // ClassAssertion(:Cls01 :indivC)
    //
    // The axiom
    //   AnnotationPropertyRange(:annProp02 :Cls03)
    // in connection with
    //   AnnotationAssertion(:annProp02 :indivF :someIRI)
    // doesn't generate a new axiom (which is consistent with what e.g. HermiT
    // does).

    assert(inferred.size == 25)
    assert(inferred.contains(df.getOWLClassAssertionAxiom(cls01, indivC)))
  }

  /**
    * rdfs5
    *   Condition:
    *     xxx rdfs:subPropertyOf yyy .
    *     yyy rdfs:subPropertyOf zzz .
    *   Consequence:
    *     xxx rdfs:subPropertyOf zzz .
    */
  test("Rule rdfs5 should return correct results") {
    /*
     * Property hierarchies:
     *
     *       :objProp01              :dataProp01             :annProp01
     *        /      \                 /      \                /      \
     * :objProp02  :objProp05  :dataProp02  :dataProp05  :annProp02  :annProp05
     *       |                        |                       |
     * :objProp03              :dataProp03               :annProp03
     *       |                        |                       |
     * :objProp04              :dataProp04               :annProp04
     */
    val objProp01 = df.getOWLObjectProperty(defaultPrefix + "objProp01")
    val objProp02 = df.getOWLObjectProperty(defaultPrefix + "objProp02")
    val objProp03 = df.getOWLObjectProperty(defaultPrefix + "objProp03")
    val objProp04 = df.getOWLObjectProperty(defaultPrefix + "objProp04")

    val dataProp01 = df.getOWLDataProperty(defaultPrefix + "dataProp01")
    val dataProp02 = df.getOWLDataProperty(defaultPrefix + "dataProp02")
    val dataProp03 = df.getOWLDataProperty(defaultPrefix + "dataProp03")
    val dataProp04 = df.getOWLDataProperty(defaultPrefix + "dataProp04")

    val annProp01 = df.getOWLAnnotationProperty(defaultPrefix + "annProp01")
    val annProp02 = df.getOWLAnnotationProperty(defaultPrefix + "annProp02")
    val annProp03 = df.getOWLAnnotationProperty(defaultPrefix + "annProp03")
    val annProp04 = df.getOWLAnnotationProperty(defaultPrefix + "annProp04")

    val input = getClass.getResource(resourcePath + "test_rdfs5.owl").getPath

    val axiomsRDD = spark.owl(Syntax.FUNCTIONAL)(input)
    val reasoner = new ForwardRuleReasonerRDFS(sc, sc.defaultMinPartitions)
    val inferred: Seq[OWLAxiom] = reasoner.apply(axiomsRDD).collect()

    // Nine axioms should be inferred:
    // SubObjectPropertyOf(:objProp03 :objProp01)
    // SubObjectPropertyOf(:objProp04 :objProp01)
    // SubObjectPropertyOf(:objProp04 :objProp02)
    // SubDataPropertyOf(:dataProp03 :dataProp01)
    // SubDataPropertyOf(:dataProp04 :dataProp01)
    // SubDataPropertyOf(:dataProp04 :dataProp02)
    // SubAnnotationProperty(:annProp03 :annProp01)
    // SubAnnotationProperty(:annProp04 :annProp01)
    // SubAnnotationProperty(:annProp04 :annProp02)
    assert(inferred.size == 36)
    assert(inferred.contains(df.getOWLSubObjectPropertyOfAxiom(objProp03, objProp01)))
    assert(inferred.contains(df.getOWLSubObjectPropertyOfAxiom(objProp04, objProp01)))
    assert(inferred.contains(df.getOWLSubObjectPropertyOfAxiom(objProp04, objProp02)))
    assert(inferred.contains(df.getOWLSubDataPropertyOfAxiom(dataProp03, dataProp01)))
    assert(inferred.contains(df.getOWLSubDataPropertyOfAxiom(dataProp04, dataProp01)))
    assert(inferred.contains(df.getOWLSubDataPropertyOfAxiom(dataProp04, dataProp02)))
    assert(inferred.contains(df.getOWLSubAnnotationPropertyOfAxiom(annProp03, annProp01)))
    assert(inferred.contains(df.getOWLSubAnnotationPropertyOfAxiom(annProp04, annProp01)))
    assert(inferred.contains(df.getOWLSubAnnotationPropertyOfAxiom(annProp04, annProp02)))
  }

  /**
    * rdfs7
    *   Condition:
    *     aaa rdfs:subPropertyOf bbb .
    *     xxx aaa yyy .
    *   Consequence:
    *     xxx bbb yyy .
    */
  test("Rule rdfs7 should return correct results") {
    val objProp01 = df.getOWLObjectProperty(defaultPrefix + "objProp01")
    val dataProp01 = df.getOWLDataProperty(defaultPrefix + "dataProp01")
    val annProp01 = df.getOWLAnnotationProperty(defaultPrefix + "annProp01")

    val indivA = df.getOWLNamedIndividual(defaultPrefix + "indivA")
    val indivB = df.getOWLNamedIndividual(defaultPrefix + "indivB")

    val input = getClass.getResource(resourcePath + "test_rdfs7.owl").getPath

    val axiomsRDD = spark.owl(Syntax.FUNCTIONAL)(input)
    val reasoner = new ForwardRuleReasonerRDFS(sc, sc.defaultMinPartitions)
    val inferred: Seq[OWLAxiom] = reasoner.apply(axiomsRDD).collect()

    // Three axioms should be inferred:
    // ObjectPropertyAssertion(:objProp02 :indivA :indivB)
    // DataPropertyAssertion(:dataProp2 :indivA "ABCD")
    // AnnotationAssertion(:annProp01 :indivA "wxyz")
    assert(inferred.size == 24)
    assert(inferred.contains(
      df.getOWLObjectPropertyAssertionAxiom(objProp01, indivA, indivB)))
    assert(inferred.contains(
      df.getOWLDataPropertyAssertionAxiom(dataProp01, indivA,
        df.getOWLLiteral("ABCD"))))
    assert(inferred.contains(
      df.getOWLAnnotationAssertionAxiom(annProp01, indivA.getIRI,
        df.getOWLLiteral("wxyz"))))
  }

  /**
    * rdfs9
    *   Condition:
    *     xxx rdfs:subClassOf yyy .
    *     zzz rdf:type xxx .
    *   Consequence:
    *     zzz rdf:type yyy .
    */
  test("Rule rdfs9 should return correct results") {
    val cls01 = df.getOWLClass(defaultPrefix + "Cls01")
    val indivB = df.getOWLNamedIndividual(defaultPrefix + "indivB")

    val input = getClass.getResource(resourcePath + "test_rdfs9.owl").getPath

    val axiomsRDD = spark.owl(Syntax.FUNCTIONAL)(input)
    val reasoner = new ForwardRuleReasonerRDFS(sc, sc.defaultMinPartitions)
    val inferred: Seq[OWLAxiom] = reasoner.apply(axiomsRDD).collect()

    // One axiom should be inferred:
    // ClassAssertion(:Cls01 :indivB)
    assert(inferred.size == 11)
    assert(inferred.contains(df.getOWLClassAssertionAxiom(cls01, indivB)))
  }

  /**
    * rdfs11
    *   Condition:
    *     xxx rdfs:subClassOf yyy .
    *     yyy rdfs:subClassOf zzz .
    *   Consequence:
    *     xxx rdfs:subClassOf zzz .
    */
  test("Rule rdfs11 should return correct results") {
    /*
     * Class hierarchy:
     *
     *    :Cls01
     *    /    \
     * :Cls02  :Cls05
     *   |
     * :Cls03
     *   |
     * :Cls04
     */
    val cls01 = df.getOWLClass(defaultPrefix + "Cls01")
    val cls02 = df.getOWLClass(defaultPrefix + "Cls02")
    val cls03 = df.getOWLClass(defaultPrefix + "Cls03")
    val cls04 = df.getOWLClass(defaultPrefix + "Cls04")

    val input = getClass.getResource(resourcePath + "test_rdfs11.owl").getPath

    val axiomsRDD = spark.owl(Syntax.FUNCTIONAL)(input)
    val reasoner = new ForwardRuleReasonerRDFS(sc, sc.defaultMinPartitions)
    val inferred: Seq[OWLAxiom] = reasoner.apply(axiomsRDD).collect()

    // Three new axioms should be inferred:
    // SubClassOf(<http://ex.com/default#Cls04> <http://ex.com/default#Cls02>)
    // SubClassOf(<http://ex.com/default#Cls03> <http://ex.com/default#Cls01>)
    // SubClassOf(<http://ex.com/default#Cls04> <http://ex.com/default#Cls01>)
    assert(inferred.size == 12)
    assert(inferred.contains(df.getOWLSubClassOfAxiom(cls03, cls01)))
    assert(inferred.contains(df.getOWLSubClassOfAxiom(cls04, cls01)))
    assert(inferred.contains(df.getOWLSubClassOfAxiom(cls04, cls02)))
  }
}
