package net.sansa_stack.inference.spark.forwardchaining.triples

import java.io.{File, FileReader, StringReader}

import com.holdenkarau.spark.testing.SharedSparkContext
import net.sansa_stack.inference.spark.data.model.RDFGraph
import org.apache.jena.graph.{NodeFactory, Triple}
import org.apache.jena.riot.lang.CollectorStreamTriples
import org.apache.jena.riot.{Lang, RDFParser}
import org.apache.jena.vocabulary.{OWL2, RDFS}
import org.apache.spark.rdd.RDD
import org.scalatest.funsuite.AnyFunSuite

import scala.jdk.CollectionConverters._


class ForwardRuleReasonerELTest extends AnyFunSuite with SharedSparkContext {
  private val rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
  private val rdfs = "http://www.w3.org/2000/01/rdf-schema#"
  private val owl = "http://www.w3.org/2002/07/owl#"
  private val ex = "http://ex.com/"
  private val snsa = "http://sansa-stack.net/ontologies/inference/el-example.owl#"

  private val prefixDeclStr =
    """@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
      |@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
      |@prefix owl: <http://www.w3.org/2002/07/owl#> .
      |@prefix ex: <http://ex.com/> .
      |
    """.stripMargin

  private def toTriples(turtleString: String): Seq[Triple] = {
    val parser = RDFParser.create().source(new StringReader(prefixDeclStr + turtleString)).lang(Lang.TTL).build()
    val sink = new CollectorStreamTriples
    parser.parse(sink)

    sink.getCollected.asScala.toSeq
  }

  private def uri(uriString: String) = NodeFactory.createURI(uriString)

  test("Subclass-of relations with intersections as subclass should be extracted correctly") {
    val ttlStr =
      """
        | [
        |   owl:intersectionOf ( ex:C1 ex:C2 ) ;
        |   rdfs:subClassOf ex:SuperCls
        | ] .
        |
        | [ owl:intersectionOf ( ex:C3 ex:C4) ] .
        |
        | ex:C5 rdfs:subClassOf ex:SuperCls .
        | ex:C6 rdfs:subClassOf ex:C7 .
        |
        | [
        |   owl:intersectionOf (ex:C8 ex:C9 ) ;
        |   rdfs:subClassOf ex:AnotherSuperCls
        | ] .
      """.stripMargin
    val triples: RDD[Triple] = sc.parallelize(toTriples(ttlStr))

    val reasoner = new ForwardRuleReasonerEL(sc, 4)
    val res = reasoner.extractIntersectionSCORelations(triples).collect()

    val expected1 = (uri(ex + "C8"), uri(ex + "C9"), uri(ex + "AnotherSuperCls"))
    assert(res.contains(expected1))

    val expected2 = (uri(ex + "C1"), uri(ex + "C2"), uri(ex + "SuperCls"))
    assert(res.contains(expected2))

    assert(res.length == 2)
  }

  test("Subclass-of relations with existential restrictions as sub-class should be extracted correctly") {
    val ttlStr =
      """
        |ex:r1 ex:p1 ex:r2 .
        |[
        |  owl:onProperty ex:p1 ;
        |  owl:someValuesFrom ex:C1 ;
        |  rdfs:subClassOf ex:SuperClass1
        |] .
        |[
        |  owl:onProperty ex:p2 ;
        |  owl:someValuesFrom ex:C2
        |] .
        |
        |ex:r3 ex:p3 ex:r4 .
        |[
        |  owl:onProperty ex:p4 ;
        |  owl:someValuesFrom ex:C3 ;
        |  rdfs:subClassOf ex:SuperClass2
        |].
      """.stripMargin
    val triples: RDD[Triple] = sc.parallelize(toTriples(ttlStr))

    val reasoner = new ForwardRuleReasonerEL(sc, 4)
    val res = reasoner.extractExistentialSCORelations(triples).collect()

    val expected1 = (uri(ex + "p1"), uri(ex + "C1"), uri(ex + "SuperClass1"))
    assert(res.contains(expected1))

    val expected2 = (uri(ex + "p4"), uri(ex + "C3"), uri(ex + "SuperClass2"))
    assert(res.contains(expected2))

    assert(res.length == 2)
  }

  test("Subclass-of relations with existential restrictions as superclass should be extracted correctly") {
    val ttlStr =
      """
        |ex:r1 ex:p1 ex:r2 .
        |ex:SubClass1 rdfs:subClassOf [
        |  owl:onProperty ex:p2 ;
        |  owl:someValuesFrom ex:C1
        |] .
        |ex:C2 rdfs:subClassOf ex:C3 .
        |ex:r3 ex:p3 ex:r4 .
        |[
        |  owl:onProperty ex:p4 ;
        |  owl:someValuesFrom ex:C4
        |] .
        |ex:SubClass2 rdfs:subClassOf [
        |  owl:onProperty ex:p5 ;
        |  owl:someValuesFrom ex:C5
        |] .
      """.stripMargin

    val triples: RDD[Triple] = sc.parallelize(toTriples(ttlStr))

    val reasoner = new ForwardRuleReasonerEL(sc, 4)
    val res = reasoner.extractSCOExistentialRelations(triples).collect()

    val expected1 = (uri(ex + "SubClass1"), uri(ex + "p2"), uri(ex + "C1"))
    assert(res.contains(expected1))

    val expected2 = (uri(ex + "SubClass2"), uri(ex + "p5"), uri(ex + "C5"))
    assert(res.contains(expected2))

    assert(res.length == 2)
  }

  test("Subproperty-of relations should be extracted correctly") {
    val ttlStr =
      """
        |ex:r1 ex:p1 ex:r2 .
        |ex:p2 rdfs:subPropertyOf ex:p3 .
        |ex:SubClass1 rdfs:subClassOf [
        |  owl:onProperty ex:p2 ;
        |  owl:someValuesFrom ex:C1
        |] .
        |ex:p3 rdfs:subPropertyOf ex:p4 .
        |ex:r3 ex:p4 ex:r4 .
        |ex:p4 rdfs:subPropertyOf ex:p5 .
      """.stripMargin

    val triples: RDD[Triple] = sc.parallelize(toTriples(ttlStr))

    val reasoner = new ForwardRuleReasonerEL(sc, 4)
    val res = reasoner.extractSubPropertyOfRelations(triples).collect()

    val expected1 = (uri(ex + "p2"), uri(ex + "p3"))
    assert(res.contains(expected1))

    val expected2 = (uri(ex + "p3"), uri(ex + "p4"))
    assert(res.contains(expected2))

    val expected3 = (uri(ex + "p4"), uri(ex + "p5"))
    assert(res.contains(expected3))

    assert(res.length == 3)
  }

  test("Property chain axioms should be extracted correctly") {
    val ttlStr =
      """
        |ex:r1 ex:p1 ex:r2 .
        |ex:p2 rdfs:subPropertyOf ex:p3 .
        |ex:SubClass1 rdfs:subClassOf [
        |  owl:onProperty ex:p2 ;
        |  owl:someValuesFrom ex:C1
        |] .
        |ex:superProp1 owl:propertyChainAxiom ( ex:cp1 ex:cp2 ).
        |ex:r3 ex:p4 ex:r4 .
        |ex:superProp2 owl:propertyChainAxiom ( ex:cp3 ex:cp4 ).
        |ex:superProp3 owl:propertyChainAxiom ( ex:cp5 ex:cp6 ).
      """.stripMargin

    val triples: RDD[Triple] = sc.parallelize(toTriples(ttlStr))

    val reasoner = new ForwardRuleReasonerEL(sc, 4)
    val res = reasoner.extractPropertyChainRelations(triples).collect()

    val expected1 = (uri(ex + "cp1"), uri(ex + "cp2"), uri(ex + "superProp1"))
    assert(res.contains(expected1))

    val expected2 = (uri(ex + "cp3"), uri(ex + "cp4"), uri(ex + "superProp2"))
    assert(res.contains(expected2))

    val expected3 = (uri(ex + "cp5"), uri(ex + "cp6"), uri(ex + "superProp3"))
    assert(res.contains(expected3))

    assert(res.length == 3)
  }

  test("CR1 should work correctly") {
    val ttlStr =
      """
        |ex:C1 rdfs:subClassOf ex:C2 .
        |ex:C2 rdfs:subClassOf ex:C3 .
        |ex:C2 rdfs:subClassOf ex:C4 .
        |ex:r1 ex:p1 ex:r2 .
        |ex:C4 rdfs:subClassOf ex:C5 .
      """.stripMargin
    val triples: RDD[Triple] = sc.parallelize(toTriples(ttlStr))

    val reasoner = new ForwardRuleReasonerEL(sc, 4)
    val res = reasoner.cr1.getInferredTriples(triples).collect()

    val expected1 = Triple.create(uri(ex + "C1"), RDFS.subClassOf.asNode(), uri(ex + "C3"))
    val expected2 = Triple.create(uri(ex + "C1"), RDFS.subClassOf.asNode(), uri(ex + "C4"))
    val expected3 = Triple.create(uri(ex + "C2"), RDFS.subClassOf.asNode(), uri(ex + "C5"))
    assert(res.length == 3)
    assert(reasoner.cr1.execute(triples).count() == 8)
  }

  test("CR2 should work correctly") {
    val ttlStr =
      """
        |[
        |  owl:intersectionOf ( ex:C2 ex:C3 ) ;
        |  rdfs:subClassOf ex:D1
        |] .
        |
        |ex:C1 rdfs:subClassOf ex:C2 .
        |ex:C1 rdfs:subClassOf ex:C3 .
        |ex:C1 rdfs:subClassOf ex:C4 .
        |ex:r1 ex:p1 ex:r2 .
        |[
        |  owl:intersectionOf ( ex:C1 ex:C4 ) ;
        |  rdfs:subClassOf ex:C5
        |] .
        |
        |ex:C6 rdfs:subClassOf ex:C7 .
        |ex:C6 rdfs:subClassOf ex:C8 .
        |
        |[
        |  owl:intersectionOf ( ex:C7 ex:C8 ) ;
        |  rdfs:subClassOf ex:D2
        |]
      """.stripMargin

    val triples: RDD[Triple] = sc.parallelize(toTriples(ttlStr))

    val reasoner = new ForwardRuleReasonerEL(sc, 4)
    val res = reasoner.cr2.getInferredTriples(triples).collect()

    val expected1 = Triple.create(uri(ex + "C1"), RDFS.subClassOf.asNode(), uri(ex + "D1"))
    assert(res.contains(expected1))

    val expected2 = Triple.create(uri(ex + "C6"), RDFS.subClassOf.asNode(), uri(ex + "D2"))
    assert(res.contains(expected2))

    assert(res.length == 2)
    assert(reasoner.cr2.execute(triples).count() == 24 + 2)
  }

  /** TODO: Test are too weak! */
  test("CR3 should work correctly") {
    val ttlStr =
      """
        |ex:r1 ex:p1 ex:r2 .
        |ex:C1 rdfs:subClassOf ex:C2 .
        |ex:C3 rdfs:subClassOf ex:C4 .
        |ex:C5 rdfs:subClassOf ex:C6 .
        |ex:C7 rdfs:subClassOf ex:C8 .
        |ex:C8 rdfs:subClassOf [
        |  owl:onProperty ex:p1 ;
        |  owl:someValuesFrom ex:D1
        |] .
        |ex:C4 rdfs:subClassOf [
        |  owl:onProperty ex:p2 ;
        |  owl:someValuesFrom ex:D2
        |] .
        |ex:r3 ex:p3 ex:r4 .
        |ex:C2 rdfs:subClassOf [
        |  owl:onProperty ex:p3 ;
        |  owl:someValuesFrom ex:D3
        |] .
      """.stripMargin

    val triples: RDD[Triple] = sc.parallelize(toTriples(ttlStr))

    val reasoner = new ForwardRuleReasonerEL(sc, 4)
    val res = reasoner.cr3.getInferredTriples(triples).collect()

    // -------- C1 \sqsubseteq p3.D3 --------
    // ex:C1 rdfs:subClassOf _:23 .
    assert(res.exists(trpl => {
      val c7 = uri(ex + "C1")

      (trpl.subjectMatches(c7) && trpl.predicateMatches(RDFS.subClassOf.asNode())
        && trpl.getObject.isBlank)
    }))
    // _:23 owl:onProperty ex:p3 .
    assert(res.exists(trpl => {
      val p1 = uri(ex + "p3")

      (trpl.getSubject.isBlank && trpl.predicateMatches(OWL2.onProperty.asNode())
        && trpl.objectMatches(p1))
    }))
    // _:23 owl:someValuesFrom ex:D3 .
    assert(res.exists(trpl => {
      val d1 = uri(ex + "D3")

      (trpl.getSubject.isBlank && trpl.predicateMatches(OWL2.someValuesFrom.asNode())
        && trpl.objectMatches(d1))
    }))

    // -------- C3 \sqsubseteq p2.D2 --------
    // ex:C3 rdfs:subClassOf _:23 .
    assert(res.exists(trpl => {
      val c7 = uri(ex + "C3")

      (trpl.subjectMatches(c7) && trpl.predicateMatches(RDFS.subClassOf.asNode())
        && trpl.getObject.isBlank)
    }))
    // _:23 owl:onProperty ex:p2 .
    assert(res.exists(trpl => {
      val p1 = uri(ex + "p2")

      (trpl.getSubject.isBlank && trpl.predicateMatches(OWL2.onProperty.asNode())
        && trpl.objectMatches(p1))
    }))
    // _:23 owl:someValuesFrom ex:D2 .
    assert(res.exists(trpl => {
      val d1 = uri(ex + "D2")

      (trpl.getSubject.isBlank && trpl.predicateMatches(OWL2.someValuesFrom.asNode())
        && trpl.objectMatches(d1))
    }))

    // -------- C7 \sqsubseteq \exists p1.D1 --------
    // ex:C7 rdfs:subClassOf _:23 .
    assert(res.exists(trpl => {
      val c7 = uri(ex + "C7")

      (trpl.subjectMatches(c7) && trpl.predicateMatches(RDFS.subClassOf.asNode())
        && trpl.getObject.isBlank)
    }))
    // _:23 owl:onProperty ex:p1 .
    assert(res.exists(trpl => {
      val p1 = uri(ex + "p1")

      (trpl.getSubject.isBlank && trpl.predicateMatches(OWL2.onProperty.asNode())
        && trpl.objectMatches(p1))
    }))
    // _:23 owl:someValuesFrom ex:D1 .
    assert(res.exists(trpl => {
      val d1 = uri(ex + "D1")

      (trpl.getSubject.isBlank && trpl.predicateMatches(OWL2.someValuesFrom.asNode())
        && trpl.objectMatches(d1))
    }))
  }

  test("CR4 should work correctly") {
    val ttlStr =
      """
        |ex:r1 ex:p1 ex:r2 .
        |ex:C1 rdfs:subClassOf ex:C2 .
        |ex:C3 rdfs:subClassOf [
        |  owl:onProperty ex:p3 ;
        |  owl:someValuesFrom ex:D3
        |] .
        |ex:D3 rdfs:subClassOf ex:D4 .
        |[
        |  owl:onProperty ex:p4 ;
        |  owl:someValuesFrom ex:D4 ;
        |  rdfs:subClassOf ex:E
        |] .
        |ex:r3 ex:p5 ex:r1 .
        |
        |ex:C10 rdfs:subClassOf [
        |  owl:onProperty ex:p10 ;
        |  owl:someValuesFrom ex:D10
        |] .
        |ex:D10 rdfs:subClassOf ex:D11 .
        |[
        |  owl:onProperty ex:p10 ;
        |  owl:someValuesFrom ex:D11 ;
        |  rdfs:subClassOf ex:E10
        |] .
        |
        |ex:C20 rdfs:subClassOf [
        |  owl:onProperty ex:p20 ;
        |  owl:someValuesFrom ex:D20
        |] .
        |ex:D20 rdfs:subClassOf ex:D21 .
        |[
        |  owl:onProperty ex:p20 ;
        |  owl:someValuesFrom ex:D21 ;
        |  rdfs:subClassOf ex:E20
        |]
      """.stripMargin

    val triples: RDD[Triple] = sc.parallelize(toTriples(ttlStr))

    val reasoner = new ForwardRuleReasonerEL(sc, 4)
    val res = reasoner.cr4.getInferredTriples(triples).collect()

    val expected1 = Triple.create(uri(ex + "C10"), RDFS.subClassOf.asNode(), uri(ex + "E10"))
    assert(res.contains(expected1))

    val expected2 = Triple.create(uri(ex + "C20"), RDFS.subClassOf.asNode(), uri(ex + "E20"))
    assert(res.contains(expected2))

    assert(res.length == 2)
  }

  test("CR5 should work correctly") {
    val ttlStr =
      """
        |ex:r1 ex:r1 ex:r2 .
        |ex:C10 rdfs:subClassOf [
        |  owl:onProperty ex:p10 ;
        |  owl:someValuesFrom ex:D10
        |] .
        |ex:D10 rdfs:subClassOf owl:Nothing .
        |
        |ex:C1 rdfs:subClassOf [
        |  owl:onProperty ex:p1 ;
        |  owl:someValuesFrom ex:D1
        |] .
        |ex:D1 rdfs:subClassOf ex:D5 .
        |
        |ex:C20 rdfs:subClassOf [
        |  owl:onProperty ex:p20 ;
        |  owl:someValuesFrom ex:D20
        |] .
        |ex:D20 rdfs:subClassOf owl:Nothing .
      """.stripMargin

    val triples: RDD[Triple] = sc.parallelize(toTriples(ttlStr))

    val reasoner = new ForwardRuleReasonerEL(sc, 4)
    val res = reasoner.cr5.getInferredTriples(triples).collect()

    val expected1 = Triple.create(uri(ex + "C10"), RDFS.subClassOf.asNode(), OWL2.Nothing.asNode())
    val expected2 = Triple.create(uri(ex + "C20"), RDFS.subClassOf.asNode(), OWL2.Nothing.asNode())

    assert(res.length == 2)
  }

  test("CR10 should work correctly") {
    val ttlStr =
      """
        |ex:r1 ex:p1 ex:r2 .
        |ex:C10 rdfs:subClassOf [
        |  owl:onProperty ex:p10 ;
        |  owl:someValuesFrom ex:D10
        |] .
        |ex:p10 rdfs:subPropertyOf ex:p11 .
        |ex:p11 rdfs:subPropertyOf ex:p12 .
        |ex:C1 rdfs:subClassOf [
        |  owl:onProperty ex:p1 ;
        |  owl:someValuesFrom ex:D1
        |] .
        |
        |ex:C20 rdfs:subClassOf [
        |  owl:onProperty ex:p20 ;
        |  owl:someValuesFrom ex:D20
        |] .
        |ex:p20 rdfs:subPropertyOf ex:p21 .
      """.stripMargin

    val triples: RDD[Triple] = sc.parallelize(toTriples(ttlStr))

    val reasoner = new ForwardRuleReasonerEL(sc, 4)
    val res = reasoner.cr10.getInferredTriples(triples).collect()

    // -------- C10 \sqsubseteq \exists p11.D10
    // ex:C10 rdfs:subClassOf _:23 .
    assert(res.exists(trpl => {
      val c10 = uri(ex + "C10")

      (trpl.subjectMatches(c10) && trpl.predicateMatches(RDFS.subClassOf.asNode())
        && trpl.getObject.isBlank)
    }))
    // _:23 owl:onProperty ex:p11 .
    assert(res.exists(trpl => {
      val p11 = uri(ex + "p11")

      (trpl.getSubject.isBlank && trpl.predicateMatches(OWL2.onProperty.asNode())
        && trpl.objectMatches(p11))
    }))
    // _:23 owl:someValuesFrom ex:D10 .
    assert(res.exists(trpl => {
      val d10 = uri(ex + "D10")

      (trpl.getSubject.isBlank && trpl.predicateMatches(OWL2.someValuesFrom.asNode())
        && trpl.objectMatches(d10))
    }))

    // -------- C20 \sqsubseteq \exists p21.D20
    // ex:C20 rdfs:subClassOf _:23 .
    assert(res.exists(trpl => {
      val c20 = uri(ex + "C20")

      (trpl.subjectMatches(c20) && trpl.predicateMatches(RDFS.subClassOf.asNode())
        && trpl.getObject.isBlank)
    }))
    // _:23 owl:onProperty ex:p21 .
    assert(res.exists(trpl => {
      val p21 = uri(ex + "p21")

      (trpl.getSubject.isBlank && trpl.predicateMatches(OWL2.onProperty.asNode())
        && trpl.objectMatches(p21))
    }))
    // _:23 owl:someValuesFrom ex:D20 .
    assert(res.exists(trpl => {
      val d20 = uri(ex + "D20")

      (trpl.getSubject.isBlank && trpl.predicateMatches(OWL2.someValuesFrom.asNode())
        && trpl.objectMatches(d20))
    }))
  }

  test("CR11 should work correctly") {
    val ttlStr =
      """
        |ex:r1 ex:p1 ex:r2 .
        |ex:p12 owl:propertyChainAxiom ( ex:p10 ex:p11 ) .
        |ex:p22 owl:propertyChainAxiom ( ex:p20 ex:p21 ) .
        |ex:p3 owl:propertyChainAxiom ( ex:p1 ex:p2 ) .
        |ex:C1 rdfs:subClassOf [
        |  owl:onProperty ex:p1 ;
        |  owl:someValuesFrom ex:C1
        |] .
        |
        |ex:C10 rdfs:subClassOf [
        |  owl:onProperty ex:p10 ;
        |  owl:someValuesFrom ex:D10
        |] .
        |ex:D10 rdfs:subClassOf [
        |  owl:onProperty ex:p11 ;
        |  owl:someValuesFrom ex:E10
        |] .
        |
        |ex:r3 ex:p3 ex:r4.
        |
        |ex:C20 rdfs:subClassOf [
        |  owl:onProperty ex:p20 ;
        |  owl:someValuesFrom ex:D20
        |] .
        |ex:D20 rdfs:subClassOf [
        |  owl:onProperty ex:p21 ;
        |  owl:someValuesFrom ex:E20
        |] .
        |
        |ex:C1 rdfs:subClassOf [
        |  owl:onProperty ex:p1 ;
        |  owl:someValuesFrom ex:D1
        |] .
        |ex:D1 rdfs:subClassOf [
        |  owl:onProperty ex:p11 ;
        |  owl:someValuesFrom ex:E1
        |]
      """.stripMargin

    val triples: RDD[Triple] = sc.parallelize(toTriples(ttlStr))

    val reasoner = new ForwardRuleReasonerEL(sc, 4)
    val res = reasoner.cr11.getInferredTriples(triples).collect()

    // -------- C10 \sqsubseteq \exists r12.E10 --------
    // ex:C10 rdfs:subClassOf _:23 .
    assert(res.exists(trpl => {
      val c10 = uri(ex + "C10")

      (trpl.subjectMatches(c10) && trpl.predicateMatches(RDFS.subClassOf.asNode())
        && trpl.getObject.isBlank)
    }))
    // _:23 owl:onProperty ex:r12 .
    assert(res.exists(trpl => {
      val r12 = uri(ex + "p12")

      (trpl.getSubject.isBlank && trpl.predicateMatches(OWL2.onProperty.asNode())
        && trpl.objectMatches(r12))
    }))
    // _:23 owl:someValuesFrom ex:E10 .
    assert(res.exists(trpl => {
      val e10 = uri(ex + "E10")

      (trpl.getSubject.isBlank && trpl.predicateMatches(OWL2.someValuesFrom.asNode())
        && trpl.objectMatches(e10))
    }))

    // -------- C20 \sqsubseteq \exists r22.E20 --------
    // ex:C20 rdfs:subClassOf _:23 .
    assert(res.exists(trpl => {
      val c20 = uri(ex + "C20")

      (trpl.subjectMatches(c20) && trpl.predicateMatches(RDFS.subClassOf.asNode())
        && trpl.getObject.isBlank)
    }))
    // _:23 owl:onProperty ex:r22 .
    assert(res.exists(trpl => {
      val r22 = uri(ex + "p22")

      (trpl.getSubject.isBlank && trpl.predicateMatches(OWL2.onProperty.asNode())
        && trpl.objectMatches(r22))
    }))
    // _:23 owl:someValuesFrom ex:E20 .
    assert(res.exists(trpl => {
      val e20 = uri(ex + "E20")

      (trpl.getSubject.isBlank && trpl.predicateMatches(OWL2.someValuesFrom.asNode())
        && trpl.objectMatches(e20))
    }))
  }

  test("Overall reasoning should give expected results") {
    val parser = RDFParser.create().source(
      new FileReader(new File(getClass.getResource("/el_ontology.nt").getPath))).lang(Lang.NTRIPLES).build()
    val sink = new CollectorStreamTriples
    parser.parse(sink)
    val nttrpls = sc.parallelize(sink.getCollected.asScala.toSeq)

    val reasoner = new ForwardRuleReasonerEL(sc, 4)
    val inferredRDFGraph = reasoner(RDFGraph(nttrpls))
    val inferredTriples = inferredRDFGraph.triples.collect()

    // CR1: C \sqsubseteq C1, C1 \sqsubseteq D => C \sqsubseteq D
    val cr1_c = uri(snsa + "CR1_C")
    val cr1_d = uri(snsa + "CR1_D")
    assert(
      inferredTriples.exists(triple =>
        (triple.subjectMatches(cr1_c)
          && triple.predicateMatches(RDFS.subClassOf.asNode())
          && triple.objectMatches(cr1_d))))

    // CR2: C \sqsubseteq C1, C\sqsubseteq C2, C1 \sqcap C2 \sqsubseteq D
    //                                               => C \sqsubseteq D
    val cr2_c = uri(snsa + "CR2_C")
    val cr2_d = uri(snsa + "CR2_D")
    assert(
      inferredTriples.exists(triple =>
        (triple.subjectMatches(cr2_c)
          && triple.predicateMatches(RDFS.subClassOf.asNode())
          && triple.objectMatches(cr2_d))))

    // CR3: C \squbseteq C1, C1 \sqsubseteq \exists r.D
    //                                         => C \sqsubseteq \exists r.D
    val cr3_c = uri(snsa + "CR3_C")
    val cr3_r = uri(snsa + "cr3_r")
    val cr3_d = uri(snsa + "CR3_D")
    // C rdfs:subClassOf _:23 .
    assert(
      inferredTriples.exists(triple =>
        (triple.subjectMatches(cr3_c)
          && triple.predicateMatches(RDFS.subClassOf.asNode())
          && triple.getObject.isBlank)))
    // _:23 owl:onProperty r .
    assert(
      inferredTriples.exists(triple =>
        (triple.getSubject.isBlank
          && triple.predicateMatches(OWL2.onProperty.asNode())
          && triple.objectMatches(cr3_r))))
    // _:23 owl:someValuesFrom D
    assert(
      inferredTriples.exists(triple =>
        (triple.getSubject.isBlank
          && triple.predicateMatches(OWL2.someValuesFrom.asNode())
          && triple.objectMatches(cr3_d))))

    // CR4: C \sqsubseteq \exists r.D, D \sqsubseteq D2,
    //      \exists r.D2 \sqsubseteq E          => C \sqsubseteq E
    val cr4_c = uri(snsa + "CR4_C")
    val cr4_e = uri(snsa + "CR4_E")
    assert(
      inferredTriples.exists(triple =>
        (triple.subjectMatches(cr4_c)
          && triple.predicateMatches(RDFS.subClassOf.asNode())
          && triple.objectMatches(cr4_e))))

    // CR5: C \sqsubseteq \exists r.D, D \sqsubseteq \bot => C \sqsubseteq \bot
    val cr5_c = uri(snsa + "CR5_C")
    assert(
      inferredTriples.exists(triple =>
        (triple.subjectMatches(cr5_c)
          && triple.predicateMatches(RDFS.subClassOf.asNode())
          && triple.objectMatches(OWL2.Nothing.asNode()))))

    // CR10: C \sqsubseteq \exists r.D, r \sqsubseteq s => C \sqsubseteq \exists s.D
    val cr10_c = uri(snsa + "CR10_C")
    val cr10_s = uri(snsa + "cr10_s")
    val cr10_d = uri(snsa + "CR10_D")
    // C rdfs:subClassOf _:23
    assert(
      inferredTriples.exists(triple =>
        (triple.subjectMatches(cr10_c)
          && triple.predicateMatches(RDFS.subClassOf.asNode())
          && triple.getObject.isBlank)))
    // _:23 owl:onProperty s
    assert(
      inferredTriples.exists(triple =>
        (triple.getSubject.isBlank
          && triple.predicateMatches(OWL2.onProperty.asNode())
          && triple.objectMatches(cr10_s))))
    // _:23 owl:someValuesFrom D
    assert(
      inferredTriples.exists(triple =>
        (triple.getSubject.isBlank
          && triple.predicateMatches(OWL2.someValuesFrom.asNode())
          && triple.objectMatches(cr10_d))))

    // CR11: C \sqsubseteq \exists r1.D, D \sqsubseteq \exists r2.E,
    //       r1 o r2 \sqsubseteq r3 => C \sqsubseteq \exists r3.E
    val cr11_c = uri(snsa + "CR11_C")
    val cr11_r3 = uri(snsa + "cr11_r3")
    val cr11_e = uri(snsa + "CR11_E")
    // C rdfs:subClassOf _:23
    assert(
      inferredTriples.exists(triple =>
        (triple.subjectMatches(cr11_c)
          && triple.predicateMatches(RDFS.subClassOf.asNode())
          && triple.getObject.isBlank)))
    // _:23 owl:onProperty r3
    assert(
      inferredTriples.exists(triple =>
        (triple.getSubject.isBlank
          && triple.predicateMatches(OWL2.onProperty.asNode())
          && triple.objectMatches(cr11_r3))))
    // _:23 owl:someValuesFrom E
    assert(
      inferredTriples.exists(triple =>
        (triple.getSubject.isBlank
          && triple.predicateMatches(OWL2.someValuesFrom.asNode())
          && triple.objectMatches(cr11_e))))
  }
}
