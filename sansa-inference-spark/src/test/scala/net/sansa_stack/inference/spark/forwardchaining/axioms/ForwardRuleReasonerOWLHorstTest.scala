package net.sansa_stack.inference.spark.forwardchaining.axioms

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import net.sansa_stack.owl.spark.rdd.FunctionalSyntaxOWLAxiomsRDDBuilder
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.model.OWLAxiom

import scala.collection.JavaConverters._


/**
  * Rule names refer to the name scheme used in
  * 'RORS: Enhanced Rule-based OWL Reasoning on Spark' by Liu, Feng, Zhang,
  * Wang, Rao
  */
class ForwardRuleReasonerOWLHorstTest extends FunSuite with SharedSparkContext with DataFrameSuiteBase {
  private val resourcePath = "/forward_chaining/axioms/"
  private val defaultPrefix = "http://ex.com/default#"
  private val df = OWLManager.getOWLDataFactory

  import net.sansa_stack.owl.spark.owl._

  val reasoner = new ForwardRuleReasonerOWLHorst(sc, 4)

  test("OWL Horst Axiom Forward Chaining Rule Reasoner") {

    val input = getClass.getResource("/ont_functional.owl").getPath

    var owlAxiomsRDD = FunctionalSyntaxOWLAxiomsRDDBuilder.build(spark, input)
    val reasoner = new ForwardRuleReasonerOWLHorst(sc, 4)
    val res: RDD[OWLAxiom] = reasoner(owlAxiomsRDD)

    assert(res.count() == 211)
  }

  /**
    * R1:
    *   Condition:
    *     c rdfs:subClassOf c1
    *     c1 rdfs:subClassOf c2
    *   Consequence:
    *     c rdfs:subClassOf c2
    */
  test("Rule R1 should return correct results") {
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

    val input = getClass.getResource(resourcePath + "test_r1.owl").getPath

    val axiomsRDD = spark.owl(Syntax.FUNCTIONAL)(input)
    val reasoner = new ForwardRuleReasonerOWLHorst(sc, sc.defaultMinPartitions)
    val inferred: Seq[OWLAxiom] = reasoner.apply(axiomsRDD).collect()

    // Three new axioms should be inferred:
    // SubClassOf(<http://ex.com/default#Cls04> <http://ex.com/default#Cls02>)
    // SubClassOf(<http://ex.com/default#Cls03> <http://ex.com/default#Cls01>)
    // SubClassOf(<http://ex.com/default#Cls04> <http://ex.com/default#Cls01>)
    assert(inferred.size == 3)
    assert(inferred.contains(df.getOWLSubClassOfAxiom(cls03, cls01)))
    assert(inferred.contains(df.getOWLSubClassOfAxiom(cls04, cls01)))
    assert(inferred.contains(df.getOWLSubClassOfAxiom(cls04, cls02)))
  }

  /**
    * R2:
    *   Condition:
    *     p rdfs:subPropertyOf p1
    *     p1 rdfs:subPropertyOf p2
    *   Consequence:
    *     p rdfs:subPropertyOf p2
    */
  test("Rule R2 should return correct results") {
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


    val input = getClass.getResource(resourcePath + "test_r2.owl").getPath

    val axiomsRDD = spark.owl(Syntax.FUNCTIONAL)(input)
    val reasoner = new ForwardRuleReasonerOWLHorst(sc, sc.defaultMinPartitions)
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
    assert(inferred.size == 9)
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
    * R3:
    *   Condition:
    *     s p o
    *     p rdfs:subPropertyOf p1
    *   Consequence:
    *     s p1 o
    */
  test("Rule R3 should return correct results") {
    val objProp01 = df.getOWLObjectProperty(defaultPrefix + "objProp01")
    val dataProp01 = df.getOWLDataProperty(defaultPrefix + "dataProp01")
    val annProp01 = df.getOWLAnnotationProperty(defaultPrefix + "annProp01")

    val indivA = df.getOWLNamedIndividual(defaultPrefix + "indivA")
    val indivB = df.getOWLNamedIndividual(defaultPrefix + "indivB")

    val input = getClass.getResource(resourcePath + "test_r3.owl").getPath

    val axiomsRDD = spark.owl(Syntax.FUNCTIONAL)(input)
    val reasoner = new ForwardRuleReasonerOWLHorst(sc, sc.defaultMinPartitions)
    val inferred: Seq[OWLAxiom] = reasoner.apply(axiomsRDD).collect()

    // Three axioms should be inferred:
    // ObjectPropertyAssertion(:objProp01 :indivA :indivB)
    // DataPropertyAssertion(:dataProp1 :indivA "ABCD")
    // AnnotationAssertion(:annProp01 :indivA "wxyz")
    assert(inferred.size == 3)
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
    * R4:
    *   Condition:
    *     s rdfs:domain x
    *     u s y
    *   Consequence:
    *     u rdf:type x
    */
  test("Rule R4 should return correct results") {
    val cls01 = df.getOWLClass(defaultPrefix + "Cls01")
    val cls02 = df.getOWLClass(defaultPrefix + "Cls02")
    val cls03 = df.getOWLClass(defaultPrefix + "Cls03")
    val indivB = df.getOWLNamedIndividual(defaultPrefix + "indivB")
    val indivD = df.getOWLNamedIndividual(defaultPrefix + "indivD")
    val indivF = df.getOWLNamedIndividual(defaultPrefix + "indivF")

    val input = getClass.getResource(resourcePath + "test_r4.owl").getPath

    val axiomsRDD = spark.owl(Syntax.FUNCTIONAL)(input)
    val reasoner = new ForwardRuleReasonerOWLHorst(sc, sc.defaultMinPartitions)
    val inferred: Seq[OWLAxiom] = reasoner.apply(axiomsRDD).collect()

    // Three axioms should be inferred:
    // ClassAssertion(:Cls01 :indivB)
    // ClassAssertion(:Cls02 :indivD)
    // ClassAssertion(:Cls03 :indivF)
    assert(inferred.size == 3)
    assert(inferred.contains(df.getOWLClassAssertionAxiom(cls01, indivB)))
    assert(inferred.contains(df.getOWLClassAssertionAxiom(cls02, indivD)))
    assert(inferred.contains(df.getOWLClassAssertionAxiom(cls03, indivF)))
  }

  /**
    * R5:
    *   Condition:
    *     p rdfs:range o
    *     s p v
    *   Consequence:
    *     v rdf:type o
    */
  test("Rule R5 should return correct results") {
    val cls01 = df.getOWLClass(defaultPrefix + "Cls01")
    val indivC = df.getOWLNamedIndividual(defaultPrefix + "indivC")

    val input = getClass.getResource(resourcePath + "test_r5.owl").getPath

    val axiomsRDD = spark.owl(Syntax.FUNCTIONAL)(input)
    val reasoner = new ForwardRuleReasonerOWLHorst(sc, sc.defaultMinPartitions)
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
    assert(inferred.size == 1)
    assert(inferred.contains(df.getOWLClassAssertionAxiom(cls01, indivC)))
  }

  /**
    * R6:
    *   Condition:
    *     c rdfs:subClassOf c1
    *     v rdf:type c
    *   Consequence:
    *     v rdf:type c1
    */
  test("Rule R6 should return correct results") {
    val cls01 = df.getOWLClass(defaultPrefix + "Cls01")
    val indivA = df.getOWLNamedIndividual(defaultPrefix + "indivA")
    val indivB = df.getOWLNamedIndividual(defaultPrefix + "indivB")

    val input = getClass.getResource(resourcePath + "test_r6.owl").getPath

    val axiomsRDD = spark.owl(Syntax.FUNCTIONAL)(input)
    val reasoner = new ForwardRuleReasonerOWLHorst(sc, sc.defaultMinPartitions)
    val inferred: Seq[OWLAxiom] = reasoner.apply(axiomsRDD).collect()

    // Two axioms should be inferred:
    // ClassAssertion(bar:Cls1 :indivA)
    // ClassAssertion(bar:Cls1 :indivB)
    assert(inferred.size == 2)
    assert(inferred.contains(df.getOWLClassAssertionAxiom(cls01, indivA)))
    assert(inferred.contains(df.getOWLClassAssertionAxiom(cls01, indivB)))
  }

  /**
    * O1:
    *   Condition:
    *     p rdf:type owl:FunctionalProperty
    *     u p v
    *     u p w
    *   Consequence:
    *     v owl:sameAs w
    */
  test("Rule O1 should return correct results") {
    val indivB = df.getOWLNamedIndividual(defaultPrefix + "indivB")
    val indivC = df.getOWLNamedIndividual(defaultPrefix + "indivC")
    val indivD = df.getOWLNamedIndividual(defaultPrefix + "indivD")

    val input = getClass.getResource(resourcePath + "test_o1.owl").getPath

    val axiomsRDD = spark.owl(Syntax.FUNCTIONAL)(input)
    val reasoner = new ForwardRuleReasonerOWLHorst(sc, sc.defaultMinPartitions)
    val inferred: Seq[OWLAxiom] = reasoner.apply(axiomsRDD).collect()

    // It should be either one axiom inferred:
    //   SameIndividual(:indivB :indivC :indivD)
    // or three pairwise axioms:
    //   SameIndividual(:indivB :indivC)
    //   SameIndividual(:indivB :indivD)
    //   SameIndividual(:indivC :indivD)
    // (where the individual operand pairs may be in arbitrary order)
    // Permutations are assumed to be handled by the axiom's equals method
    inferred.size match {
      case 1 => assert(
        inferred.contains(
          df.getOWLSameIndividualAxiom(Seq(indivB, indivC, indivD).asJava)))
      case 3 =>
        assert( // B == C
          inferred.contains(df.getOWLSameIndividualAxiom(indivB, indivC)))
        assert( // B == D
          inferred.contains(df.getOWLSameIndividualAxiom(indivB, indivD)))
        assert(  // C == D
          inferred.contains(df.getOWLSameIndividualAxiom(indivC, indivD))
        )
      case _ => assert(false)
    }
  }

  /**
    * O2:
    *   Condition:
    *     p rdf:type owl:InverseFunctionalProperty
    *     v p u
    *     w p u
    *   Consequence:
    *     v owl:sameAs w
    */
  test("Rule O2 should return correct results") {
    val indivA = df.getOWLNamedIndividual(defaultPrefix + "indivA")
    val indivC = df.getOWLNamedIndividual(defaultPrefix + "indivC")
    val indivD = df.getOWLNamedIndividual(defaultPrefix + "indivD")

    val input = getClass.getResource(resourcePath + "test_o2.owl").getPath

    val axiomsRDD = spark.owl(Syntax.FUNCTIONAL)(input)
    val reasoner = new ForwardRuleReasonerOWLHorst(sc, sc.defaultMinPartitions)
    val inferred: Seq[OWLAxiom] = reasoner.apply(axiomsRDD).collect()

    // It should be either one axiom inferred:
    //   SameIndividual(:indivA :indivC :indivD)
    // or three pairwise axioms:
    //   SameIndividual(:indivA :indivC)
    //   SameIndividual(:indivA :indivD)
    //   SameIndividual(:indivC :indivD)
    // (where the individual operand pairs may be in arbitrary order)
    // Permutations are assumed to be handled by the axiom's equals method.
    inferred.size match {
      case 1 => assert(
        inferred.contains(
          df.getOWLSameIndividualAxiom(Seq(indivA, indivC, indivD).asJava)))
      case 3 =>
        assert( // B == C
          inferred.contains(df.getOWLSameIndividualAxiom(indivA, indivC)))
        assert( // B == D
          inferred.contains(df.getOWLSameIndividualAxiom(indivA, indivD)))
        assert(  // C == D
          inferred.contains(df.getOWLSameIndividualAxiom(indivC, indivD))
        )
      case _ => assert(false)
    }
  }

  /**
    * O3:
    *   Condition:
    *     p rdf:type owl:SymmetricProperty
    *     v p u
    *   Consequence:
    *     u p v
    */
  test("Rule O3 should return correct results") {
    val indivA = df.getOWLNamedIndividual(defaultPrefix + "indivA")
    val indivB = df.getOWLNamedIndividual(defaultPrefix + "indivB")
    val objProp01 = df.getOWLObjectProperty(defaultPrefix + "objProp01")

    val input = getClass.getResource(resourcePath + "test_o3.owl").getPath

    val axiomsRDD = spark.owl(Syntax.FUNCTIONAL)(input)
    val reasoner = new ForwardRuleReasonerOWLHorst(sc, sc.defaultMinPartitions)
    val inferred: Seq[OWLAxiom] = reasoner.apply(axiomsRDD).collect()

    // One axiom should be inferred:
    // ObjectPropertyAssertion(:objProp01 :indivB :indivA)
    assert(inferred.size == 1)
    assert(inferred.contains(
      df.getOWLObjectPropertyAssertionAxiom(objProp01, indivB, indivA)))
  }

  /**
    * O4:
    *   Condition:
    *     p rdf:type owl:TransitiveProperty
    *     u p w
    *     w p v
    *   Consequence:
    *     u p v
    */
  test("Rule O4 should return correct results") {
    val indivA = df.getOWLNamedIndividual(defaultPrefix + "indivA")
    val indivB = df.getOWLNamedIndividual(defaultPrefix + "indivB")
    val indivC = df.getOWLNamedIndividual(defaultPrefix + "indivC")
    val indivD = df.getOWLNamedIndividual(defaultPrefix + "indivD")
    val objProp01 = df.getOWLObjectProperty(defaultPrefix + "objProp01")

    val input = getClass.getResource(resourcePath + "test_o4.owl").getPath

    val axiomsRDD = spark.owl(Syntax.FUNCTIONAL)(input)
    val reasoner = new ForwardRuleReasonerOWLHorst(sc, sc.defaultMinPartitions)
    val inferred: Seq[OWLAxiom] = reasoner.apply(axiomsRDD).collect()

    // Three axioms should be inferred:
    // ObjectPropertyAssertion(:objProp01 :indivA :indivC)
    // ObjectPropertyAssertion(:objProp01 :indivA :indivD)
    // ObjectPropertyAssertion(:objProp01 :indivB :indivD)
    assert(inferred.size == 3)
    assert(inferred.contains(df.getOWLObjectPropertyAssertionAxiom(objProp01, indivA, indivC)))
    assert(inferred.contains(df.getOWLObjectPropertyAssertionAxiom(objProp01, indivA, indivD)))
    assert(inferred.contains(df.getOWLObjectPropertyAssertionAxiom(objProp01, indivB, indivD)))
  }

  /**
    * O7a:
    *   Condition:
    *     p owl:inverseOf q
    *     v p w
    *   Consequence:
    *     w q v
    */
  test("Rule O7a should return correct results") {
    val indivA = df.getOWLNamedIndividual(defaultPrefix + "indivA")
    val indivB = df.getOWLNamedIndividual(defaultPrefix + "indivB")
    val objProp02 = df.getOWLObjectProperty(defaultPrefix + "objProp02")

    val input = getClass.getResource(resourcePath + "test_o7a.owl").getPath

    val axiomsRDD = spark.owl(Syntax.FUNCTIONAL)(input)
    val reasoner = new ForwardRuleReasonerOWLHorst(sc, sc.defaultMinPartitions)
    val inferred: Seq[OWLAxiom] = reasoner.apply(axiomsRDD).collect()

    // One axiom should be inferred:
    // ObjectPropertyAssertion(:objProp02 :indivB :indivA)
    assert(inferred.size == 1)
    assert(inferred.contains(
      df.getOWLObjectPropertyAssertionAxiom(objProp02, indivB, indivA)))
  }

  /**
    * O7b:
    *   Condition:
    *     p owl:inverseOf q
    *     v q w
    *   Consequence:
    *     w p v
    */
  test("Rule O7b should return correct results") {
    val indivA = df.getOWLNamedIndividual(defaultPrefix + "indivA")
    val indivB = df.getOWLNamedIndividual(defaultPrefix + "indivB")
    val objProp01 = df.getOWLObjectProperty(defaultPrefix + "objProp01")

    val input = getClass.getResource(resourcePath + "test_o7b.owl").getPath

    val axiomsRDD = spark.owl(Syntax.FUNCTIONAL)(input)
    val reasoner = new ForwardRuleReasonerOWLHorst(sc, sc.defaultMinPartitions)
    val inferred: Seq[OWLAxiom] = reasoner.apply(axiomsRDD).collect()

    // One axiom should be inferred:
    // ObjectPropertyAssertion(:objProp01 :indivA :indivB)
    assert(inferred.size == 1)
    assert(inferred.contains(
      df.getOWLObjectPropertyAssertionAxiom(objProp01, indivA, indivB)))
  }
}
