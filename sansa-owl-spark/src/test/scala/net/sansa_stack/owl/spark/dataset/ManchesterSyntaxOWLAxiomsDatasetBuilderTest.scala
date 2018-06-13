package net.sansa_stack.owl.spark.dataset

import java.util.stream.Collectors

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.scalatest.FunSuite
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.model._
import org.semanticweb.owlapi.vocab.XSDVocabulary
import uk.ac.manchester.cs.owl.owlapi.{OWLDatatypeImpl, OWLEquivalentClassesAxiomImpl}

import scala.collection.JavaConverters._


class ManchesterSyntaxOWLAxiomsDatasetBuilderTest extends FunSuite with SharedSparkContext {
  lazy val spark = SparkSession.builder().appName(sc.appName).master(sc.master)
    .config(
      "spark.kryo.registrator",
      "net.sansa_stack.owl.spark.dataset.UnmodifiableCollectionKryoRegistrator")
    .getOrCreate()
  val dataFactory = OWLManager.getOWLDataFactory
  var _dataset: OWLAxiomsDataset = null
  def dataset: OWLAxiomsDataset = {
    if (_dataset == null) {
      _dataset = ManchesterSyntaxOWLAxiomsDatasetBuilder.build(
        spark, this.getClass.getClassLoader.getResource("ont_manchester.owl").getPath)
//        spark, "hdfs://localhost:54310/ont_manchester.owl")
      _dataset.cache()
    }
    _dataset
  }

  /* *|Annotations:
   *  |    bar:hasTitle "Title",
   *  |    description "A longer
   *  |description running over
   *  |several lines",
   *  |    foo:hasName "Name"
   * --> omitted
   *
   * *|AnnotationProperty: bar:annProp1
   *  |
   *  |    SubPropertyOf:
   *  |        bar:annProp2
   *  |
   *  |    Domain:
   *  |        <http://ex.com/bar#Cls1>
   *  |
   *  |    Range:
   *  |        <http://ex.com/bar#Cls2>
   * *->   1) Declaration(AnnotationProperty(<http://ex.com/bar#annProp1>))
   * *->   2) SubAnnotationPropertyOf(<http://ex.com/bar#annProp1> <http://ex.com/bar#annProp2>)
   * *->   3) AnnotationPropertyDomain(<http://ex.com/bar#annProp1> <http://ex.com/bar#Cls1>)
   * *->   4) AnnotationPropertyRange(<http://ex.com/bar#annProp1> <http://ex.com/bar#Cls2>)
   *
   * *|AnnotationProperty: bar:annProp2
   * *->   5) Declaration(AnnotationProperty(<http://ex.com/bar#annProp2>))
   *
   * *|AnnotationProperty: bar:hasTitle
   * *->   6) Declaration(AnnotationProperty(<http://ex.com/bar#hasTitle>))
   *
   * *|AnnotationProperty: bar:label
   * *->   7) Declaration(AnnotationProperty(<http://ex.com/bar#label>))
   *
   * *|AnnotationProperty: description
   * *->   8) Declaration(AnnotationProperty(<http://ex.com/default#description>))
   *
   * *|AnnotationProperty: foo:ann
   * *->   9) Declaration(AnnotationProperty(<http://ex.com/foo#ann>))
   *
   * *|AnnotationProperty: foo:hasName
   * *->  10) Declaration(AnnotationProperty(<http://ex.com/foo#hasName>))
   *
   * *|AnnotationProperty: comment
   * *->  11) Declaration(AnnotationProperty(<http://ex.com/default#comment>))
   *
   * *|Datatype: bar:dtype1
   * *->  12) Declaration(Datatype(<http://ex.com/bar#dtype1>))
   *
   * *|Datatype: bar:dtype2
   * *->  13) Declaration(Datatype(<http://ex.com/bar#dtype2>))
   *
   * *|Datatype: rdf:PlainLiteral
   * *->  14) Declaration(Datatype(rdf:PlainLiteral))
   *
   * *|Datatype: rdfs:Literal
   * *->  15) Declaration(Datatype(rdfs:Literal))
   *
   * *|Datatype: xsd:int
   * *->  16) Declaration(Datatype(xsd:int))
   *
   * *|Datatype: xsd:integer
   * *->  17) Declaration(Datatype(xsd:integer))
   *
   * *|Datatype: xsd:string
   * *->  18) Declaration(Datatype(xsd:string))
   *
   * *|ObjectProperty: bar:Prop2
   * *->  19) Declaration(ObjectProperty(<http://ex.com/bar#Prop2>))
   *
   * *|ObjectProperty: bar:asymmObjProp
   *  |
   *  |    Characteristics:
   *  |        Asymmetric
   * *->  20) Declaration(ObjectProperty(<http://ex.com/bar#asymmObjProp>))
   * *->  21) AsymmetricObjectProperty(<http://ex.com/bar#asymmObjProp>)
   *
   * *|ObjectProperty: bar:invObjProp1
   *  |
   *  |    EquivalentTo:
   *  |         inverse bar:objProp1
   *  |
   *  |    Characteristics:
   *  |        InverseFunctional
   *  |
   *  |    InverseOf:
   *  |        bar:objProp1
   * *->  22) Declaration(ObjectProperty(<http://ex.com/bar#invObjProp1>))
   * *->  23) EquivalentObjectProperties(<http://ex.com/bar#invObjProp1> ObjectInverseOf(<http://ex.com/bar#objProp1>) )
   * *->  24) InverseFunctionalObjectProperty(<http://ex.com/bar#invObjProp1>)
   * *->  25) InverseObjectProperties(<http://ex.com/bar#invObjProp1> <http://ex.com/bar#objProp1>)
   *
   * *|ObjectProperty: bar:objProp1
   *  |
   *  |    EquivalentTo:
   *  |        bar:sameAsObjProp1
   *  |
   *  |    DisjointWith:
   *  |        bar:objProp2
   *  |
   *  |    Characteristics:
   *  |        Transitive,
   *  |        Reflexive
   *  |
   *  |    Domain:
   *  |        bar:Cls1
   *  |
   *  |    Range:
   *  |        bar:AllIndividualsCls
   *  |
   *  |    InverseOf:
   *  |        bar:invObjProp1
   * *->  26) Declaration(ObjectProperty(<http://ex.com/bar#objProp1>))
   * *->  27) EquivalentObjectProperties(<http://ex.com/bar#objProp1> <http://ex.com/bar#sameAsObjProp1> )
   * *->  28) DisjointObjectProperties(<http://ex.com/bar#objProp1> <http://ex.com/bar#objProp2> )
   * *->  29) TransitiveObjectProperty(<http://ex.com/bar#objProp1>)
   * *->  30) ReflexiveObjectProperty(<http://ex.com/bar#objProp1>)
   * *->  31) ObjectPropertyDomain(<http://ex.com/bar#objProp1> <http://ex.com/bar#Cls1>)
   * *->  32) ObjectPropertyRange(<http://ex.com/bar#objProp1> <http://ex.com/bar#AllIndividualsCls>)
   * *->  33) InverseObjectProperties(<http://ex.com/bar#objProp1> <http://ex.com/bar#invObjProp1>)
   *
   * *|ObjectProperty: bar:objProp2
   *  |
   *  |    DisjointWith:
   *  |        bar:objProp1
   *  |
   *  |    Characteristics:
   *  |        Functional,
   *  |        Symmetric,
   *  |        Irreflexive
   * *->  34) Declaration(ObjectProperty(<http://ex.com/bar#objProp2>))
   * *->  35) DisjointObjectProperties(<http://ex.com/bar#objProp1> <http://ex.com/bar#objProp2> )
   * *->  36) FunctionalObjectProperty(<http://ex.com/bar#objProp2>)
   * *->  37) SymmetricObjectProperty(<http://ex.com/bar#objProp2>)
   * *->  38) IrreflexiveObjectProperty(<http://ex.com/bar#objProp2>)
   *
   * *|ObjectProperty: bar:sameAsObjProp1
   *  |
   *  |    EquivalentTo:
   *  |        bar:objProp1
   * *->  39) Declaration(ObjectProperty(<http://ex.com/bar#sameAsObjProp1>))
   * *->  40) EquivalentObjectProperties(<http://ex.com/bar#objProp1> <http://ex.com/bar#sameAsObjProp1> )
   *
   * *|ObjectProperty: bar:subObjProp1
   *  |
   *  |    SubPropertyOf:
   *  |        bar:objProp1
   * *->  41) Declaration(ObjectProperty(<http://ex.com/bar#subObjProp1>))
   * *->  42) SubObjectPropertyOf(<http://ex.com/bar#subObjProp1> <http://ex.com/bar#objProp1>)
   *
   * *|DataProperty: bar:dataProp1
   *  |
   *  |    Characteristics:
   *  |        Annotations:
   *  |            comment "This annotation is required"
   *  |        Functional
   *  |
   *  |    Domain:
   *  |        bar:Cls1
   *  |
   *  |    Range:
   *  |        xsd:string
   *  |
   *  |    EquivalentTo:
   *  |        bar:sameAsDataProp1
   *  |
   *  |    DisjointWith:
   *  |        bar:dataProp2
   * *->  43) Declaration(DataProperty(<http://ex.com/bar#dataProp1>))
   * *->  44) FunctionalDataProperty(Annotation(<http://ex.com/default#comment> "This annotation is required"^^xsd:string) <http://ex.com/bar#dataProp1>)
   * *->  45) DataPropertyDomain(<http://ex.com/bar#dataProp1> <http://ex.com/bar#Cls1>)
   * *->  46) DataPropertyRange(<http://ex.com/bar#dataProp1> xsd:string)
   * *->  47) EquivalentDataProperties(<http://ex.com/bar#dataProp1> <http://ex.com/bar#sameAsDataProp1> )
   * *->  48) DisjointDataProperties(<http://ex.com/bar#dataProp1> <http://ex.com/bar#dataProp2> )
   *
   * *|DataProperty: bar:dataProp2
   *  |
   *  |    Domain:
   *  |        bar:Cls1
   *  |
   *  |    Range:
   *  |        xsd:int
   *  |
   *  |    DisjointWith:
   *  |        bar:dataProp1
   * *->  49) Declaration(DataProperty(<http://ex.com/bar#dataProp2>))
   * *->  50) DataPropertyDomain(<http://ex.com/bar#dataProp2> <http://ex.com/bar#Cls1>)
   * *->  51) DataPropertyRange(<http://ex.com/bar#dataProp2> xsd:int)
   * *->  52) DisjointDataProperties(<http://ex.com/bar#dataProp1> <http://ex.com/bar#dataProp2> )
   *
   * *|DataProperty: bar:sameAsDataProp1
   *  |
   *  |    EquivalentTo:
   *  |        bar:dataProp1
   * *->  53) Declaration(DataProperty(<http://ex.com/bar#sameAsDataProp1>))
   * *->  54) EquivalentDataProperties(<http://ex.com/bar#dataProp1> <http://ex.com/bar#sameAsDataProp1> )
   *
   * *|DataProperty: bar:subDataProp1
   *  |
   *  |    SubPropertyOf:
   *  |        bar:dataProp1
   * *->  55) Declaration(DataProperty(<http://ex.com/bar#subDataProp1>))
   * *->  56) SubDataPropertyOf(<http://ex.com/bar#subDataProp1> <http://ex.com/bar#dataProp1>)
   *
   * *|Class: bar:AllIndividualsCls
   *  |
   *  |    EquivalentTo:
   *  |        {foo:indivA , foo:indivB}
   * *->  57) Declaration(Class(<http://ex.com/bar#AllIndividualsCls>))
   * *->  58) EquivalentClasses(<http://ex.com/bar#AllIndividualsCls> ObjectOneOf(<http://ex.com/foo#indivA> <http://ex.com/foo#indivB>))
   *
   * *|Class: bar:AllProp1Cls1
   *  |
   *  |    EquivalentTo:
   *  |        bar:objProp1 only bar:Cls1
   * *->  59) Declaration(Class(<http://ex.com/bar#AllProp1Cls1>))
   * *->  60) EquivalentClasses(<http://ex.com/bar#AllProp1Cls1> ObjectAllValuesFrom(<http://ex.com/bar#objProp1> <http://ex.com/bar#Cls1>))
   *
   * *|Class: bar:Cl1OrNegate
   *  |
   *  |    DisjointUnionOf:
   *  |        Annotations:
   *  |            comment "This annotation is required"
   *  |        bar:Cls1, bar:ComplementCls1
   * *->  61) Declaration(Class(<http://ex.com/bar#Cl1OrNegate>))
   * *->  62) DisjointUnion(Annotation(<http://ex.com/default#comment> "This annotation is required"^^xsd:string) <http://ex.com/bar#Cl1OrNegate> <http://ex.com/bar#Cls1> <http://ex.com/bar#ComplementCls1>)
   *
   * *|Class: bar:Cls1
   *  |
   *  |    HasKey:
   *  |        Annotations:
   *  |            comment "This annotation is required"
   *  |        bar:dataProp1
   * *->  63) Declaration(Class(<http://ex.com/bar#Cls1>))
   * *->  64) FIXME: HasKey(Annotation(<http://ex.com/default#comment> "This annotation is required"^^xsd:string) <http://ex.com/bar#Cls1> (<http://ex.com/bar#dataProp1> ) ())
   *          <http://ex.com/bar#dataProp1> is probably parsed as object property
   *
   * *|Class: bar:Cls2
   * *->  65) Declaration(Class(<http://ex.com/bar#Cls2>))
   *
   * *|Class: bar:ComplementCls
   *  |
   *  |    EquivalentTo:
   *  |        not (bar:Cls1)
   * *->  66) Declaration(Class(<http://ex.com/bar#ComplementCls>))
   * *->  67) EquivalentClasses(<http://ex.com/bar#ComplementCls> ObjectComplementOf(<http://ex.com/bar#Cls1>) )
   *
   * *|Class: bar:ComplementCls1
   * *->  68) Declaration(Class(<http://ex.com/bar#ComplementCls1>))
   *
   * *|Class: bar:DataAllIntGT10
   *  |
   *  |    EquivalentTo:
   *  |        bar:dataProp2 only xsd:integer [>= 10]
   * *->  69) Declaration(Class(<http://ex.com/bar#DataAllIntGT10>))
   * *->  70) EquivalentClasses(<http://ex.com/bar#DataAllIntGT10> DataAllValuesFrom(<http://ex.com/bar#dataProp2> DataRangeRestriction(xsd:integer facetRestriction(minInclusive "10"^^xsd:integer))))
   *
   * *|Class: bar:DataExact5Prop1
   *  |
   *  |    EquivalentTo:
   *  |        bar:dataProp1 exactly 5 rdfs:Literal
   * *->  71) Declaration(Class(<http://ex.com/bar#DataExact5Prop1>))
   * *->  72) FIXME: EquivalentClasses(<http://ex.com/bar#DataExact5Prop1> ObjectExactCardinality(5 <http://ex.com/bar#dataProp1> rdfs:Literal) )
   *          should be EquivalentClasses(<http://ex.com/bar#DataExact5Prop1> DataExactCardinality(5 <http://ex.com/bar#dataProp1> rdfs:Literal) )
   *
   *
   * *|Class: bar:DataHasVal5
   *  |
   *  |    EquivalentTo:
   *  |        bar:dataProp2 value 5
   * *->  73) Declaration(Class(<http://ex.com/bar#DataHasVal5>))
   * *->  74) EquivalentClasses(<http://ex.com/bar#DataHasVal5> DataHasValue(<http://ex.com/bar#dataProp2> "5"^^xsd:integer))
   *
   * *|Class: bar:DataMax2Prop1
   *  |
   *  |    EquivalentTo:
   *  |        bar:dataProp1 max 2 rdfs:Literal
   *  |
   *  |    DisjointWith:
   *  |        bar:DataMin3Prop1
   * *->  75) Declaration(Class(<http://ex.com/bar#DataMax2Prop1>))
   * *->  76) FIXME: EquivalentClasses(<http://ex.com/bar#DataMax2Prop1> ObjectMaxCardinality(2 <http://ex.com/bar#dataProp1> rdfs:Literal))
   *          should be EquivalentClasses(<http://ex.com/bar#DataMax2Prop1> DataMaxCardinality(2 <http://ex.com/bar#dataProp1> rdfs:Literal))
   * *->  77) DisjointClasses(<http://ex.com/bar#DataMax2Prop1> <http://ex.com/bar#DataMin3Prop1>))
   *
   * *|Class: bar:DataMin3Prop1
   *  |
   *  |    EquivalentTo:
   *  |        bar:dataProp1 min 3 rdfs:Literal
   *  |
   *  |    DisjointWith:
   *  |        bar:DataMax2Prop1
   * *->  78) Declaration(Class(<http://ex.com/bar#DataMin3Prop1>))
   * *->  79) FIXME: EquivalentClasses(<http://ex.com/bar#DataMin3Prop1> ObjectMinCardinality(3 <http://ex.com/bar#dataProp1> rdfs:Literal) )
   *          should be EquivalentClasses(<http://ex.com/bar#DataMin3Prop1> DataMinCardinality(3 <http://ex.com/bar#dataProp1> rdfs:Literal) )
   * *->  80) DisjointClasses(<http://ex.com/bar#DataMax2Prop1> <http://ex.com/bar#DataMin3Prop1>))
   *
   * *|Class: bar:DataSomeIntLT20
   *  |
   *  |    EquivalentTo:
   *  |        bar:dataProp2 some xsd:integer[< 20]
   * *->  81) Declaration(Class(<http://ex.com/bar#DataSomeIntLT20>))
   * *->  82) EquivalentClasses(<http://ex.com/bar#DataSomeIntLT20> DataSomeValuesFrom(<http://ex.com/bar#dataProp2> DataRangeRestriction(xsd:integer facetRestriction(maxExclusive "20"^^xsd:integer))))
   *
   * *|Class: bar:Exact5Prop1Cls1
   *  |
   *  |    EquivalentTo:
   *  |        bar:objProp1 exactly 5 bar:Cls1
   * *->  83) Declaration(Class(<http://ex.com/bar#Exact5Prop1Cls1>))
   * *->  84) EquivalentClasses(<http://ex.com/bar#Exact5Prop1Cls1> ObjectExactCardinality(5 <http://ex.com/bar#objProp1> <http://ex.com/bar#Cls1>))
   *
   * *|Class: bar:HasSelfProp1
   *  |
   *  |    EquivalentTo:
   *  |        bar:objProp1 Self
   * *->  85) Declaration(Class(<http://ex.com/bar#HasSelfProp1>))
   * *->  86) EquivalentClasses(<http://ex.com/bar#HasSelfProp1> ObjectHasSelf(<http://ex.com/bar#objProp1>) )
   *
   * *|Class: bar:HasValProp1IndivB
   *  |
   *  |    EquivalentTo:
   *  |        bar:objProp1 value foo:indivB
   * *->  87) Declaration(Class(<http://ex.com/bar#HasValProp1IndivB>))
   * *->  88) EquivalentClasses(<http://ex.com/bar#HasValProp1IndivB> ObjectHasValue(<http://ex.com/bar#objProp1> <http://ex.com/foo#indivB>))
   *
   * *|ClassA: bar:IntersectionCls
   *  |
   *  |    EquivalentTo:
   *  |        bar:Cls1
   *  |         and bar:Cls2
   * *->  89) Declaration(Class(<http://ex.com/bar#IntersectionCls>))
   * *->  90) EquivalentClasses(<http://ex.com/bar#IntersectionCls> ObjectIntersectionOf(<http://ex.com/bar#Cls1> <http://ex.com/bar#Cls2>) )
   *
   * *|Class: bar:Max3Prop1Cls1
   *  |
   *  |    EquivalentTo:
   *  |        bar:objProp1 max 3 bar:Cls1
   * *->  91) Declaration(Class(<http://ex.com/bar#Max3Prop1Cls1>))
   * *->  92) EquivalentClasses(<http://ex.com/bar#Max3Prop1Cls1> ObjectMaxCardinality(3 <http://ex.com/bar#objProp1> <http://ex.com/bar#Cls1>))
   *
   * *|Class: bar:Min2Prop1Cls1
   *  |
   *  |    EquivalentTo:
   *  |        bar:objProp1 min 2 bar:Cls1
   * *->  93) Declaration(Class(<http://ex.com/bar#Min2Prop1Cls1>))
   * *->  94) EquivalentClasses(<http://ex.com/bar#Min2Prop1Cls1> ObjectMinCardinality(2 <http://ex.com/bar#objProp1> <http://ex.com/bar#Cls1>))
   *
   * *|Class: bar:SomeProp1Cls1
   *  |
   *  |    EquivalentTo:
   *  |        bar:objProp1 some bar:Cls1
   * *->  95) Declaration(Class(<http://ex.com/bar#SomeProp1Cls1>))
   * *->  96) EquivalentClasses(<http://ex.com/bar#SomeProp1Cls1> ObjectSomeValuesFrom(<http://ex.com/bar#objProp1> <http://ex.com/bar#Cls1>))
   *
   * *|Class: bar:UnionCls
   *  |
   *  |    EquivalentTo:
   *  |        bar:Cls1 or bar:Cls2
   * *->  97) Declaration(Class(<http://ex.com/bar#UnionCls>))
   * *->  98) EquivalentClasses(<http://ex.com/bar#UnionCls> ObjectUnionOf(<http://ex.com/bar#Cls1> <http://ex.com/bar#Cls2>) )
   *
   * *|Individual: foo:indivA
   *  |
   *  |    Types:
   *  |        bar:Cls1
   *  |
   *  |    Facts:
   *  |     bar:objProp1  foo:indivB,
   *  |     bar:dataProp1  "ABCD",
   *  |      not  bar:dataProp2  23
   *  |
   *  |    SameAs:
   *  |        foo:sameAsIndivA
   *  |
   *  |    DifferentFrom:
   *  |        foo:indivB
   * *->  99) Declaration(NamedIndividual(<http://ex.com/foo#indivA>))
   * *-> 100) ClassAssertion(<http://ex.com/bar#Cls1> <http://ex.com/foo#indivA>)
   * *-> 101) ObjectPropertyAssertion(<http://ex.com/bar#objProp1> <http://ex.com/foo#indivA> <http://ex.com/foo#indivB>)
   * *-> 102) DataPropertyAssertion(<http://ex.com/bar#dataProp1> <http://ex.com/foo#indivA> "ABCD"^^xsd:string)
   * *-> 103) NegativeDataPropertyAssertion(<http://ex.com/bar#dataProp2> <http://ex.com/foo#indivA> "23"^^xsd:integer)
   * *-> 104) SameIndividual(<http://ex.com/foo#indivA> <http://ex.com/foo#sameAsIndivA> )
   * *-> 105) DifferentIndividuals(<http://ex.com/foo#indivA> <http://ex.com/foo#indivB>)
   *
   * *|Individual: foo:indivB
   *  |
   *  |    Facts:
   *  |      not  bar:Prop2  foo:indivA,
   *  |     bar:dataProp1  "BCDE"
   *  |
   *  |    DifferentFrom:
   *  |        foo:indivA
   * *-> 106) Declaration(NamedIndividual(<http://ex.com/foo#indivB>))
   * *-> 107) NegativeObjectPropertyAssertion(<http://ex.com/bar#Prop2> <http://ex.com/foo#indivB> <http://ex.com/foo#indivA>)
   * *-> 108) DataPropertyAssertion(<http://ex.com/bar#dataProp1> <http://ex.com/foo#indivB> "BCDE"^^xsd:string
   * *-> 109) DifferentIndividuals(<http://ex.com/foo#indivA> <http://ex.com/foo#indivB>)
   *
   * *|Individual: foo:sameAsIndivA
   *  |
   *  |    SameAs:
   *  |        foo:indivA
   * *-> 110) Declaration(NamedIndividual(<http://ex.com/foo#sameAsIndivA>))
   * *-> 111) SameIndividual(<http://ex.com/foo#indivA> <http://ex.com/foo#sameAsIndivA> )
   *
   */
  test("The number of axioms should match") {
    val expectedNumberOfAxioms = 111
    assert(dataset.count() == expectedNumberOfAxioms)
  }

  test("Annotation property domain axioms should be created correctly") {
    // AnnotationPropertyDomain(<http://ex.com/bar#annProp1> <http://ex.com/bar#Cls1>)
    val expectedNumberOfAxioms = 1
    val expectedAnnProperty = dataFactory.getOWLAnnotationProperty("http://ex.com/bar#annProp1")
    val expextedAnnPropertyDomain = IRI.create("http://ex.com/bar#Cls1")

    implicit val encoder = Encoders.kryo[OWLAnnotationPropertyDomainAxiom]
    val filteredDataset: Dataset[OWLAnnotationPropertyDomainAxiom] =
      dataset.filter(_.isInstanceOf[OWLAnnotationPropertyDomainAxiom]).map(
        _.getAxiomWithoutAnnotations[OWLAnnotationPropertyDomainAxiom])

    assert(filteredDataset.count() == expectedNumberOfAxioms)

    val annDomainAxiom: OWLAnnotationPropertyDomainAxiom = filteredDataset.first()
    assert(annDomainAxiom.getProperty == expectedAnnProperty)
    assert(annDomainAxiom.getDomain == expextedAnnPropertyDomain)
  }

  test("Annotation property range axioms should be created correctly") {
    // AnnotationPropertyRange(<http://ex.com/bar#annProp1> <http://ex.com/bar#Cls2>)
    val expectedNumberOfAxioms = 1
    val expectedAnnProperty = dataFactory.getOWLAnnotationProperty("http://ex.com/bar#annProp1")
    val expectedAnnPropertyRange = IRI.create("http://ex.com/bar#Cls2")

    implicit val encoder = Encoders.kryo[OWLAnnotationPropertyRangeAxiom]
    val filteredDataset: Dataset[OWLAnnotationPropertyRangeAxiom] =
      dataset.filter(_.isInstanceOf[OWLAnnotationPropertyRangeAxiom]).map(
        _.getAxiomWithoutAnnotations[OWLAnnotationPropertyRangeAxiom])

    assert(filteredDataset.count() == expectedNumberOfAxioms)

    val annPropertyRangeAxiom = filteredDataset.first()
    assert(annPropertyRangeAxiom.getProperty == expectedAnnProperty)
    assert(annPropertyRangeAxiom.getRange == expectedAnnPropertyRange)
  }

  test("Sub-annotation-property-Of axioms should be created correctly") {
    // SubAnnotationPropertyOf(<http://ex.com/bar#annProp1> <http://ex.com/bar#annProp2>)
    val expectedNumberOfAxioms = 1
    val expectedSuperProperty = dataFactory.getOWLAnnotationProperty("http://ex.com/bar#annProp2")
    val expectedSubProperty = dataFactory.getOWLAnnotationProperty("http://ex.com/bar#annProp1")

    implicit val encoder = Encoders.kryo[OWLSubAnnotationPropertyOfAxiom]
    val filteredDataset: Dataset[OWLSubAnnotationPropertyOfAxiom] =
      dataset.filter(_.isInstanceOf[OWLSubAnnotationPropertyOfAxiom]).map(
        _.getAxiomWithoutAnnotations[OWLSubAnnotationPropertyOfAxiom])

    assert(filteredDataset.count() == expectedNumberOfAxioms)

    val subAnnPropOfAxiom = filteredDataset.first()
    assert(subAnnPropOfAxiom.getSuperProperty == expectedSuperProperty)
    assert(subAnnPropOfAxiom.getSubProperty == expectedSubProperty)
  }

  test("Declaration axioms should be created correctly") {
    // Declaration(AnnotationProperty(<http://ex.com/bar#annProp1>))
    // Declaration(AnnotationProperty(<http://ex.com/bar#annProp2>))
    // Declaration(AnnotationProperty(<http://ex.com/bar#hasTitle>))
    // Declaration(AnnotationProperty(<http://ex.com/bar#label>))
    // Declaration(AnnotationProperty(<http://ex.com/default#description>))
    // Declaration(AnnotationProperty(<http://ex.com/foo#ann>))
    // Declaration(AnnotationProperty(<http://ex.com/foo#hasName>))
    // Declaration(Datatype(<http://ex.com/bar#dtype1>))
    // Declaration(Datatype(<http://ex.com/bar#dtype2>))
    // Declaration(Datatype(rdf:PlainLiteral))
    // Declaration(Datatype(rdfs:Literal))
    // Declaration(Datatype(xsd:int))
    // Declaration(Datatype(xsd:integer))
    // Declaration(Datatype(xsd:string))
    // Declaration(ObjectProperty(<http://ex.com/bar#Prop2>))
    // Declaration(ObjectProperty(<http://ex.com/bar#asymmObjProp>))
    // Declaration(ObjectProperty(<http://ex.com/bar#invObjProp1>))
    // Declaration(ObjectProperty(<http://ex.com/bar#objProp1>))
    // Declaration(ObjectProperty(<http://ex.com/bar#objProp2>))
    // Declaration(ObjectProperty(<http://ex.com/bar#sameAsObjProp1>))
    // Declaration(ObjectProperty(<http://ex.com/bar#subObjProp1>))
    // Declaration(DataProperty(<http://ex.com/bar#dataProp1>))
    // Declaration(DataProperty(<http://ex.com/bar#dataProp2>))
    // Declaration(DataProperty(<http://ex.com/bar#sameAsDataProp1>))
    // Declaration(DataProperty(<http://ex.com/bar#subDataProp1>))
    // Declaration(Class(<http://ex.com/bar#AllIndividualsCls>))
    // Declaration(Class(<http://ex.com/bar#AllProp1Cls1>))
    // Declaration(Class(<http://ex.com/bar#Cl1OrNegate>))
    // Declaration(Class(<http://ex.com/bar#Cls1>))
    // Declaration(Class(<http://ex.com/bar#Cls2>))
    // Declaration(Class(<http://ex.com/bar#ComplementCls>))
    // Declaration(Class(<http://ex.com/bar#ComplementCls1>))
    // Declaration(Class(<http://ex.com/bar#DataAllIntGT10>))
    // Declaration(Class(<http://ex.com/bar#DataExact5Prop1>))
    // Declaration(Class(<http://ex.com/bar#DataHasVal5>))
    // Declaration(Class(<http://ex.com/bar#DataMax2Prop1>))
    // Declaration(Class(<http://ex.com/bar#DataMin3Prop1>))
    // Declaration(Class(<http://ex.com/bar#DataSomeIntLT20>))
    // Declaration(Class(<http://ex.com/bar#Exact5Prop1Cls1>))
    // Declaration(Class(<http://ex.com/bar#HasSelfProp1>))
    // Declaration(Class(<http://ex.com/bar#HasValProp1IndivB>))
    // Declaration(Class(<http://ex.com/bar#IntersectionCls>))
    // Declaration(Class(<http://ex.com/bar#Max3Prop1Cls1>))
    // Declaration(Class(<http://ex.com/bar#Min2Prop1Cls1>))
    // Declaration(Class(<http://ex.com/bar#SomeProp1Cls1>))
    // Declaration(Class(<http://ex.com/bar#UnionCls>))
    // Declaration(NamedIndividual(<http://ex.com/foo#indivA>))
    // Declaration(NamedIndividual(<http://ex.com/foo#indivB>))
    // Declaration(NamedIndividual(<http://ex.com/foo#sameAsIndivA>))
    val expectedNumberOfAxioms = 49
    val filteredDataset = dataset.filter(axiom => axiom.isInstanceOf[OWLDeclarationAxiom])

    assert(filteredDataset.count() == expectedNumberOfAxioms)
  }

  test("Disjoint union axioms should be created correctly") {
    // DisjointUnion(<http://ex.com/bar#Cl1OrNegate> <http://ex.com/bar#Cls1> <http://ex.com/bar#ComplementCls1> )
    val expectedNumberOfAxioms = 1
    val expectedClasses = Set(
      dataFactory.getOWLClass("http://ex.com/bar#Cl1OrNegate"),
      dataFactory.getOWLClass("http://ex.com/bar#Cls1"),
      dataFactory.getOWLClass("http://ex.com/bar#ComplementCls1")
    )

    implicit val encoder = Encoders.kryo[OWLDisjointUnionAxiom]
    val filteredDataset: Dataset[OWLDisjointUnionAxiom] =
      dataset.filter(_.isInstanceOf[OWLDisjointUnionAxiom]).map(
        _.getAxiomWithoutAnnotations[OWLDisjointUnionAxiom])

    assert(filteredDataset.count() == expectedNumberOfAxioms)

    val disjUnionAxiom = filteredDataset.first()
    val ceIter = disjUnionAxiom.classExpressions().iterator()

    while (ceIter.hasNext) {
      val classExpression = ceIter.next().asOWLClass()
      assert(expectedClasses.contains(classExpression))
    }
  }

  test("Disjoint classes axioms should be created correctly") {
    // DisjointClasses(<http://ex.com/bar#DataMax2Prop1> <http://ex.com/bar#DataMin3Prop1>))
    // DisjointClasses(<http://ex.com/bar#DataMax2Prop1> <http://ex.com/bar#DataMin3Prop1>))
    val expectedNumberOfAxioms = 2
    val expectedClasses = Set(
      dataFactory.getOWLClass("http://ex.com/bar#DataMin3Prop1"),
      dataFactory.getOWLClass("http://ex.com/bar#DataMax2Prop1")
    )

    implicit val encoder = Encoders.kryo[OWLDisjointClassesAxiom]
    val filteredDataset: Dataset[OWLDisjointClassesAxiom] =
      dataset.filter(_.isInstanceOf[OWLDisjointClassesAxiom]).map(
        _.getAxiomWithoutAnnotations[OWLDisjointClassesAxiom])

    assert(filteredDataset.count() == expectedNumberOfAxioms)

    assert(
      filteredDataset.filter(
        _.classExpressions().collect(Collectors.toSet()) == expectedClasses.asJava).count() ==
        expectedNumberOfAxioms)
  }

  def equivClasses(ce1: OWLClassExpression, ce2: OWLClassExpression): OWLEquivalentClassesAxiom =
    new OWLEquivalentClassesAxiomImpl(
      List(ce1, ce2).asJavaCollection,
      List.empty[OWLAnnotation].asJavaCollection
    )

  /** FIXME */
  test("Equivalent classes axioms should be created correctly") {
    // 57) EquivalentClasses(<http://ex.com/bar#AllIndividualsCls> ObjectOneOf(<http://ex.com/foo#indivA> <http://ex.com/foo#indivB>))
    // 59) EquivalentClasses(<http://ex.com/bar#AllProp1Cls1> ObjectAllValuesFrom(<http://ex.com/bar#objProp1> <http://ex.com/bar#Cls1>))
    // 66) EquivalentClasses(<http://ex.com/bar#ComplementCls> ObjectComplementOf(<http://ex.com/bar#Cls1>) )
    // 69) EquivalentClasses(<http://ex.com/bar#DataAllIntGT10> DataAllValuesFrom(<http://ex.com/bar#dataProp2> DataRangeRestriction(xsd:integer facetRestriction(minInclusive "10"^^xsd:integer))))
    // 71) FIXME: EquivalentClasses(<http://ex.com/bar#DataExact5Prop1> ObjectExactCardinality(5 <http://ex.com/bar#dataProp1> rdfs:Literal) )
    //     should be EquivalentClasses(<http://ex.com/bar#DataExact5Prop1> DataExactCardinality(5 <http://ex.com/bar#dataProp1> rdfs:Literal) )
    // 73) EquivalentClasses(<http://ex.com/bar#DataHasVal5> DataHasValue(<http://ex.com/bar#dataProp2> "5"^^xsd:integer))
    // 75) FIXME: EquivalentClasses(<http://ex.com/bar#DataMax2Prop1> ObjectMaxCardinality(2 <http://ex.com/bar#dataProp1> rdfs:Literal))
    //     should be EquivalentClasses(<http://ex.com/bar#DataMax2Prop1> DataMaxCardinality(2 <http://ex.com/bar#dataProp1> rdfs:Literal))
    // 78) FIXME: EquivalentClasses(<http://ex.com/bar#DataMin3Prop1> ObjectMinCardinality(3 <http://ex.com/bar#dataProp1> rdfs:Literal) )
    //     should be EquivalentClasses(<http://ex.com/bar#DataMin3Prop1> DataMinCardinality(3 <http://ex.com/bar#dataProp1> rdfs:Literal) )
    // 81) EquivalentClasses(<http://ex.com/bar#DataSomeIntLT20> DataSomeValuesFrom(<http://ex.com/bar#dataProp2> DataRangeRestriction(xsd:integer facetRestriction(maxExclusive "20"^^xsd:integer))))
    // 83) EquivalentClasses(<http://ex.com/bar#Exact5Prop1Cls1> ObjectExactCardinality(5 <http://ex.com/bar#objProp1> <http://ex.com/bar#Cls1>))
    // 85) EquivalentClasses(<http://ex.com/bar#HasSelfProp1> ObjectHasSelf(<http://ex.com/bar#objProp1>) )
    // 87) EquivalentClasses(<http://ex.com/bar#HasValProp1IndivB> ObjectHasValue(<http://ex.com/bar#objProp1> <http://ex.com/foo#indivB>))
    // 89) EquivalentClasses(<http://ex.com/bar#IntersectionCls> ObjectIntersectionOf(<http://ex.com/bar#Cls1> <http://ex.com/bar#Cls2>) )
    // 91) EquivalentClasses(<http://ex.com/bar#Max3Prop1Cls1> ObjectMaxCardinality(3 <http://ex.com/bar#objProp1> <http://ex.com/bar#Cls1>))
    // 93) EquivalentClasses(<http://ex.com/bar#Min2Prop1Cls1> ObjectMinCardinality(2 <http://ex.com/bar#objProp1> <http://ex.com/bar#Cls1>))
    // 95) EquivalentClasses(<http://ex.com/bar#SomeProp1Cls1> ObjectSomeValuesFrom(<http://ex.com/bar#objProp1> <http://ex.com/bar#Cls1>))
    // 97) EquivalentClasses(<http://ex.com/bar#UnionCls> ObjectUnionOf(<http://ex.com/bar#Cls1> <http://ex.com/bar#Cls2>) )
    val df = dataFactory
    val expectedNumberOfAxioms = 17
    val expectedEquivClassesAxioms: List[OWLEquivalentClassesAxiom] = List(
      //  1) EquivalentClasses(<http://ex.com/bar#AllIndividualsCls> ObjectOneOf(<http://ex.com/foo#indivA> <http://ex.com/foo#indivB>))
      equivClasses(
        df.getOWLClass("http://ex.com/bar#AllIndividualsCls"),
        df.getOWLObjectOneOf(
          List(
            df.getOWLNamedIndividual("http://ex.com/foo#indivA"),
            df.getOWLNamedIndividual("http://ex.com/foo#indivB")).asJava)),
      //  2) EquivalentClasses(<http://ex.com/bar#AllProp1Cls1> ObjectAllValuesFrom(<http://ex.com/bar#objProp1> <http://ex.com/bar#Cls1>))
      equivClasses(
        df.getOWLClass("http://ex.com/bar#AllProp1Cls1"),
        df.getOWLObjectAllValuesFrom(
          df.getOWLObjectProperty("http://ex.com/bar#objProp1"),
          df.getOWLClass("http://ex.com/bar#Cls1"))),
      //  3) EquivalentClasses(<http://ex.com/bar#ComplementCls> ObjectComplementOf(<http://ex.com/bar#Cls1>) )
      equivClasses(
        df.getOWLClass("http://ex.com/bar#ComplementCls"),
        df.getOWLObjectComplementOf(df.getOWLClass("http://ex.com/bar#Cls1"))),
      //  4) EquivalentClasses(<http://ex.com/bar#DataAllIntGT10> DataAllValuesFrom(<http://ex.com/bar#dataProp2> DataRangeRestriction(xsd:integer facetRestriction(minInclusive "10"^^xsd:integer))))
      equivClasses(
        df.getOWLClass("http://ex.com/bar#DataAllIntGT10"),
        df.getOWLDataAllValuesFrom(
          df.getOWLDataProperty("http://ex.com/bar#dataProp2"),
          df.getOWLDatatypeMinInclusiveRestriction(10))),
      //  5) FIXME: EquivalentClasses(<http://ex.com/bar#DataExact5Prop1> ObjectExactCardinality(5 <http://ex.com/bar#dataProp1> rdfs:Literal) )
      //     should be EquivalentClasses(<http://ex.com/bar#DataExact5Prop1> DataExactCardinality(5 <http://ex.com/bar#dataProp1> rdfs:Literal) )
      equivClasses(
        df.getOWLClass("http://ex.com/bar#DataExact5Prop1"),
        df.getOWLObjectExactCardinality(
          5,
          df.getOWLObjectProperty("http://ex.com/bar#dataProp1"),
          df.getOWLClass("http://www.w3.org/2000/01/rdf-schema#Literal"))),
      //  6) EquivalentClasses(<http://ex.com/bar#DataHasVal5> DataHasValue(<http://ex.com/bar#dataProp2> "5"^^xsd:integer))
      equivClasses(
        df.getOWLClass("http://ex.com/bar#DataHasVal5"),
        df.getOWLDataHasValue(
          df.getOWLDataProperty("http://ex.com/bar#dataProp2"),
          df.getOWLLiteral(5))),
      //  7) FIXME: EquivalentClasses(<http://ex.com/bar#DataMax2Prop1> ObjectMaxCardinality(2 <http://ex.com/bar#dataProp1> rdfs:Literal))
      //     should be EquivalentClasses(<http://ex.com/bar#DataMax2Prop1> DataMaxCardinality(2 <http://ex.com/bar#dataProp1> rdfs:Literal))
      equivClasses(
        df.getOWLClass("http://ex.com/bar#DataMax2Prop1"),
        df.getOWLObjectMaxCardinality(
          2,
          df.getOWLObjectProperty("http://ex.com/bar#dataProp1"),
          df.getOWLClass("http://www.w3.org/2000/01/rdf-schema#Literal"))),
      //  8) FIXME: EquivalentClasses(<http://ex.com/bar#DataMin3Prop1> ObjectMinCardinality(3 <http://ex.com/bar#dataProp1> rdfs:Literal) )
      //     should be EquivalentClasses(<http://ex.com/bar#DataMin3Prop1> DataMinCardinality(3 <http://ex.com/bar#dataProp1> rdfs:Literal) )
      equivClasses(
        df.getOWLClass("http://ex.com/bar#DataMin3Prop1"),
        df.getOWLObjectMinCardinality(
          3,
          df.getOWLObjectProperty("http://ex.com/bar#dataProp1"),
          df.getOWLClass("http://www.w3.org/2000/01/rdf-schema#Literal"))),
      //  9) EquivalentClasses(<http://ex.com/bar#DataSomeIntLT20> DataSomeValuesFrom(<http://ex.com/bar#dataProp2> DataRangeRestriction(xsd:integer facetRestriction(maxExclusive "20"^^xsd:integer))))
      equivClasses(
        df.getOWLClass("http://ex.com/bar#DataSomeIntLT20"),
        df.getOWLDataSomeValuesFrom(
          df.getOWLDataProperty("http://ex.com/bar#dataProp2"),
          df.getOWLDatatypeMaxExclusiveRestriction(20))),
      // 10) EquivalentClasses(<http://ex.com/bar#Exact5Prop1Cls1> ObjectExactCardinality(5 <http://ex.com/bar#objProp1> <http://ex.com/bar#Cls1>))
      equivClasses(
        df.getOWLClass("http://ex.com/bar#Exact5Prop1Cls1"),
        df.getOWLObjectExactCardinality(5, df.getOWLObjectProperty("http://ex.com/bar#objProp1"),
          df.getOWLClass("http://ex.com/bar#Cls1"))),
      // 11) EquivalentClasses(<http://ex.com/bar#HasSelfProp1> ObjectHasSelf(<http://ex.com/bar#objProp1>) )
      equivClasses(
        df.getOWLClass("http://ex.com/bar#HasSelfProp1"),
        df.getOWLObjectHasSelf(df.getOWLObjectProperty("http://ex.com/bar#objProp1"))),
      // 12) EquivalentClasses(<http://ex.com/bar#HasValProp1IndivB> ObjectHasValue(<http://ex.com/bar#objProp1> <http://ex.com/foo#indivB>))
      equivClasses(
        df.getOWLClass("http://ex.com/bar#HasValProp1IndivB"),
        df.getOWLObjectHasValue(
          df.getOWLObjectProperty("http://ex.com/bar#objProp1"),
          df.getOWLNamedIndividual("http://ex.com/foo#indivB"))),
      // 13) EquivalentClasses(<http://ex.com/bar#IntersectionCls> ObjectIntersectionOf(<http://ex.com/bar#Cls1> <http://ex.com/bar#Cls2>) )
      equivClasses(
        df.getOWLClass("http://ex.com/bar#IntersectionCls"),
        df.getOWLObjectIntersectionOf(
          df.getOWLClass("http://ex.com/bar#Cls1"),
          df.getOWLClass("http://ex.com/bar#Cls2"))),
      // 14) EquivalentClasses(<http://ex.com/bar#Max3Prop1Cls1> ObjectMaxCardinality(3 <http://ex.com/bar#objProp1> <http://ex.com/bar#Cls1>))
      equivClasses(
        df.getOWLClass("http://ex.com/bar#Max3Prop1Cls1"),
        df.getOWLObjectMaxCardinality(
          3,
          df.getOWLObjectProperty("http://ex.com/bar#objProp1"),
          df.getOWLClass("http://ex.com/bar#Cls1"))),
      // 15) EquivalentClasses(<http://ex.com/bar#Min2Prop1Cls1> ObjectMinCardinality(2 <http://ex.com/bar#objProp1> <http://ex.com/bar#Cls1>))
      equivClasses(
        df.getOWLClass("http://ex.com/bar#Min2Prop1Cls1"),
        df.getOWLObjectMinCardinality(
          2,
          df.getOWLObjectProperty("http://ex.com/bar#objProp1"),
          df.getOWLClass("http://ex.com/bar#Cls1"))),
      // 16) EquivalentClasses(<http://ex.com/bar#SomeProp1Cls1> ObjectSomeValuesFrom(<http://ex.com/bar#objProp1> <http://ex.com/bar#Cls1>))
      equivClasses(
        df.getOWLClass("http://ex.com/bar#SomeProp1Cls1"),
        df.getOWLObjectSomeValuesFrom(
          df.getOWLObjectProperty("http://ex.com/bar#objProp1"),
          df.getOWLClass("http://ex.com/bar#Cls1"))),
      // 17) EquivalentClasses(<http://ex.com/bar#UnionCls> ObjectUnionOf(<http://ex.com/bar#Cls1> <http://ex.com/bar#Cls2>) )
      equivClasses(
        df.getOWLClass("http://ex.com/bar#UnionCls"),
        df.getOWLObjectUnionOf(
          df.getOWLClass("http://ex.com/bar#Cls1"),
          df.getOWLClass("http://ex.com/bar#Cls2")))
    )

    implicit val encoder = Encoders.kryo[OWLEquivalentClassesAxiom]
    val filteredDataset: Dataset[OWLEquivalentClassesAxiom] =
      dataset.filter(_.isInstanceOf[OWLEquivalentClassesAxiom]).map(
        _.getAxiomWithoutAnnotations[OWLEquivalentClassesAxiom])

    assert(
      filteredDataset.filter(expectedEquivClassesAxioms.contains(_)).count() == expectedNumberOfAxioms)
  }

  test("Sub-class-of axioms should be created correctly") {
    val expectedNumberOfAxioms = 0
    val filteredDataset = dataset.filter(_.isInstanceOf[OWLSubClassOfAxiom])

    assert(filteredDataset.count() == expectedNumberOfAxioms)
  }

  test("Functional data property axioms should be created correctly") {
    // FunctionalDataProperty(bar:dataProp1)
    val expectedNumberOfAxioms = 1
    val filteredDataset = dataset.filter(_.isInstanceOf[OWLFunctionalDataPropertyAxiom])

    assert(filteredDataset.count() == expectedNumberOfAxioms)
  }

  test("Data property domain axioms should be created correctly") {
    // DataPropertyDomain(<http://ex.com/bar#dataProp1> <http://ex.com/bar#Cls1>)
    // DataPropertyDomain(<http://ex.com/bar#dataProp2> <http://ex.com/bar#Cls1>)
    val expectedNumberOfAxioms = 2
    val filteredDataset = dataset.filter(_.isInstanceOf[OWLDataPropertyDomainAxiom])

    assert(filteredDataset.count() == expectedNumberOfAxioms)
  }

  test("Data property range axioms should be created correctly") {
    // DataPropertyRange(<http://ex.com/bar#dataProp1> xsd:string)
    // DataPropertyRange(<http://ex.com/bar#dataProp2> xsd:int)
    val expectedNumberOfAxioms = 2
    val expectedRanges = Set(
      (dataFactory.getOWLDataProperty("http://ex.com/bar#dataProp1"), XSDVocabulary.STRING.getIRI),
      (dataFactory.getOWLDataProperty("http://ex.com/bar#dataProp2"), XSDVocabulary.INT.getIRI)
    )

    implicit val encoder = Encoders.kryo[OWLDataPropertyRangeAxiom]
    val filteredDataset: Dataset[OWLDataPropertyRangeAxiom] =
      dataset.filter(_.isInstanceOf[OWLDataPropertyRangeAxiom]).map(
        _.getAxiomWithoutAnnotations[OWLDataPropertyRangeAxiom])

    assert(filteredDataset.count() == expectedNumberOfAxioms)

    import spark.implicits._
    assert(
      filteredDataset.map(axiom => {
        expectedRanges.count(e => {
          e._1 == axiom.getProperty.asOWLDataProperty() &&
            e._2 == axiom.getRange.asInstanceOf[OWLDatatypeImpl].getIRI
        })
      }).filter(_ == 1).count() == expectedNumberOfAxioms)
  }

  test("Disjoint-data-properties axioms should be created correctly") {
    // DisjointDataProperties(<http://ex.com/bar#dataProp1> <http://ex.com/bar#dataProp2> )
    // DisjointDataProperties(<http://ex.com/bar#dataProp1> <http://ex.com/bar#dataProp2> ) (duplicate)
    val expectedNumberOfAxioms = 2
    val filteredDataset = dataset.filter(_.isInstanceOf[OWLDisjointDataPropertiesAxiom])

    assert(filteredDataset.count() == expectedNumberOfAxioms)
  }

  test("Equivalent-data-properties axioms should be created correctly") {
    // EquivalentDataProperties(<http://ex.com/bar#dataProp1> <http://ex.com/bar#sameAsDataProp1> )
    // EquivalentDataProperties(<http://ex.com/bar#dataProp1> <http://ex.com/bar#sameAsDataProp1> ) (duplicate)
    val expectedNumberOfAxioms = 2
    val filteredDataset = dataset.filter(_.isInstanceOf[OWLEquivalentDataPropertiesAxiom])

    assert(filteredDataset.count() == expectedNumberOfAxioms)
  }

  test("Sub-data-property-of axioms should be created correctly") {
    // SubDataPropertyOf(<http://ex.com/bar#subDataProp1> <http://ex.com/bar#dataProp1>)
    val expectedNumberOfAxioms = 1
    val filteredDataset = dataset.filter(_.isInstanceOf[OWLSubDataPropertyOfAxiom])

    assert(filteredDataset.count() == expectedNumberOfAxioms)
  }

  test("Datatype definition axioms should be created correctly") {
    val expectedNumberOfAxioms = 0
    val filteredDataset = dataset.filter(_.isInstanceOf[OWLDatatypeDefinitionAxiom])

    assert(filteredDataset.count() == expectedNumberOfAxioms)
  }

  test("Has-key axioms should be created correctly") {
    // HasKey(Annotation(<http://ex.com/default#comment> "This annotation is required"^^xsd:string) <http://ex.com/bar#Cls1> (<http://ex.com/bar#dataProp1> ) ())
    val expectedNumberOfAxioms = 1

    val filteredDataset = dataset.filter(_.isInstanceOf[OWLHasKeyAxiom])

    assert(filteredDataset.count() == expectedNumberOfAxioms)
  }

  test("Class assertion axioms should be created correctly") {
    // ClassAssertion(bar:Cls1 foo:indivA)
    val expectedNumberOfAxioms = 1
    val filteredDataset = dataset.filter(_.isInstanceOf[OWLClassAssertionAxiom])

    assert(filteredDataset.count() == expectedNumberOfAxioms)
  }

  test("Different-individuals axioms should be created correctly") {
    // DifferentIndividuals(foo:indivA foo:indivB)
    // DifferentIndividuals(foo:indivA foo:indivB) (duplicate)
    val expectedNumberOfAxioms = 2
    val filteredDataset = dataset.filter(_.isInstanceOf[OWLDifferentIndividualsAxiom])

    assert(filteredDataset.count() == expectedNumberOfAxioms)
  }

  test("Same-individual axioms should be created correctly") {
    // SameIndividual(<http://ex.com/foo#indivA> <http://ex.com/foo#sameAsIndivA> )
    // SameIndividual(<http://ex.com/foo#indivA> <http://ex.com/foo#sameAsIndivA> )
    val expectedNumberOfAxioms = 2
    val filteredDataset = dataset.filter(_.isInstanceOf[OWLSameIndividualAxiom])

    assert(filteredDataset.count() == expectedNumberOfAxioms)
  }

  test("Negative data property assertion axioms should be created correctly") {
    // NegativeDataPropertyAssertion(<http://ex.com/bar#dataProp2> <http://ex.com/foo#indivA> "23"^^xsd:integer)
    val expectedNumberOfAxioms = 1
    val expectedSubject = dataFactory.getOWLNamedIndividual("http://ex.com/foo#indivA")
    val expectedProperty = dataFactory.getOWLDataProperty("http://ex.com/bar#dataProp2")
    val expectedValue = dataFactory.getOWLLiteral(23)

    implicit val encoder = Encoders.kryo[OWLNegativeDataPropertyAssertionAxiom]
    val filteredDataset: Dataset[OWLNegativeDataPropertyAssertionAxiom] =
      dataset.filter(_.isInstanceOf[OWLNegativeDataPropertyAssertionAxiom]).map(
        _.getAxiomWithoutAnnotations[OWLNegativeDataPropertyAssertionAxiom])

    assert(filteredDataset.count() == expectedNumberOfAxioms)

    val axiom = filteredDataset.first()
    assert(axiom.getSubject == expectedSubject)
    assert(axiom.getProperty == expectedProperty)
    assert(axiom.getObject == expectedValue)
  }

  test("Negative object property assertion axioms should be created correctly") {
    // NegativeObjectPropertyAssertion(<http://ex.com/bar#Prop2> <http://ex.com/foo#indivB> <http://ex.com/foo#indivA>)
    val expectedNumberOfAxioms = 1
    val expectedSubject = dataFactory.getOWLNamedIndividual("http://ex.com/foo#indivB")
    val expectedProperty = dataFactory.getOWLObjectProperty("http://ex.com/bar#Prop2")
    val expectedObject = dataFactory.getOWLNamedIndividual("http://ex.com/foo#indivA")

    implicit val encoder = Encoders.kryo[OWLNegativeObjectPropertyAssertionAxiom]
    val filteredDataset: Dataset[OWLNegativeObjectPropertyAssertionAxiom] =
      dataset.filter(_.isInstanceOf[OWLNegativeObjectPropertyAssertionAxiom]).map(
        _.getAxiomWithoutAnnotations[OWLNegativeObjectPropertyAssertionAxiom])

    assert(filteredDataset.count() == expectedNumberOfAxioms)

    val axiom = filteredDataset.first()
    assert(axiom.getSubject == expectedSubject)
    assert(axiom.getProperty == expectedProperty)
    assert(axiom.getObject == expectedObject)
  }

  test("Object property assertion axioms should be created correctly") {
    // ObjectPropertyAssertion(<http://ex.com/bar#objProp1> <http://ex.com/foo#indivA> <http://ex.com/foo#indivB>)
    val expectedNumberOfAxioms = 1
    val expectedSubject = dataFactory.getOWLNamedIndividual("http://ex.com/foo#indivA")
    val expectedProperty = dataFactory.getOWLObjectProperty("http://ex.com/bar#objProp1")
    val expectedObject = dataFactory.getOWLNamedIndividual("http://ex.com/foo#indivB")

    implicit val encoder = Encoders.kryo[OWLObjectPropertyAssertionAxiom]
    val filteredDataset: Dataset[OWLObjectPropertyAssertionAxiom] =
      dataset.filter(_.isInstanceOf[OWLObjectPropertyAssertionAxiom]).map(
        _.getAxiomWithoutAnnotations[OWLObjectPropertyAssertionAxiom])

    assert(filteredDataset.count() == expectedNumberOfAxioms)

    val axiom = filteredDataset.first()
    assert(axiom.getSubject == expectedSubject)
    assert(axiom.getProperty == expectedProperty)
    assert(axiom.getObject == expectedObject)
  }

  test("Disjoint object properties axioms should be created correctly") {
    // DisjointObjectProperties(<http://ex.com/bar#objProp1> <http://ex.com/bar#objProp2> )
    // DisjointObjectProperties(<http://ex.com/bar#objProp1> <http://ex.com/bar#objProp2> ) (duplicate)
    val expectedNumberOfAxioms = 2
    val filteredDataset = dataset.filter(_.isInstanceOf[OWLDisjointObjectPropertiesAxiom])

    assert(filteredDataset.count() == expectedNumberOfAxioms)
  }

  test("Equivalent-object-properties axioms should be created correctly") {
    // EquivalentObjectProperties(<http://ex.com/bar#invObjProp1> InverseOf(<http://ex.com/bar#objProp1>) )
    // EquivalentObjectProperties(<http://ex.com/bar#objProp1> <http://ex.com/bar#sameAsObjProp1> )
    // EquivalentObjectProperties(<http://ex.com/bar#objProp1> <http://ex.com/bar#sameAsObjProp1> ) (duplicate)
    val expectedNumberOfAxioms = 3
    val filteredDataset = dataset.filter(_.isInstanceOf[OWLEquivalentObjectPropertiesAxiom])

    assert(filteredDataset.count() == expectedNumberOfAxioms)
  }

  test("Inverse-object-properties axioms should be created correctly") {
    // InverseObjectProperties(<http://ex.com/bar#invObjProp1> <http://ex.com/bar#objProp1>)
    // InverseObjectProperties(<http://ex.com/bar#objProp1> <http://ex.com/bar#invObjProp1>)
    val expectedNumberOfAxioms = 2
    val filteredDataset = dataset.filter(_.isInstanceOf[OWLInverseObjectPropertiesAxiom])

    assert(filteredDataset.count() == expectedNumberOfAxioms)
  }

  test("Asymmetric object property axioms should be created correctly") {
    // AsymmetricObjectProperty(<http://ex.com/bar#asymmObjProp>)
    val expectedNumberOfAxioms = 1
    val filteredDataset = dataset.filter(_.isInstanceOf[OWLAsymmetricObjectPropertyAxiom])

    assert(filteredDataset.count() == expectedNumberOfAxioms)
  }

  test("Functional object property axioms should be created correctly") {
    // FunctionalObjectProperty(<http://ex.com/bar#objProp2>)
    val expectedNumberOfAxioms = 1
    val filteredDataset = dataset.filter(_.isInstanceOf[OWLFunctionalObjectPropertyAxiom])

    assert(filteredDataset.count() == expectedNumberOfAxioms)
  }

  test("Inverse functional object property axioms should be created correctly") {
    // InverseFunctionalObjectProperty(<http://ex.com/bar#invObjProp1>)
    val expectedNumberOfAxioms = 1
    val filteredDataset = dataset.filter(_.isInstanceOf[OWLInverseFunctionalObjectPropertyAxiom])

    assert(filteredDataset.count() == expectedNumberOfAxioms)
  }

  test("Irreflexive object property axioms should be created correctly") {
    // IrreflexiveObjectProperty(<http://ex.com/bar#objProp2>)
    val expectedNumberOfAxioms = 1
    val filteredDataset = dataset.filter(_.isInstanceOf[OWLIrreflexiveObjectPropertyAxiom])

    assert(filteredDataset.count() == expectedNumberOfAxioms)
  }

  test("Reflexive object property axioms should be created correctly") {
    // ReflexiveObjectProperty(<http://ex.com/bar#objProp1>)
    val expectedNumberOfAxioms = 1
    val filteredDataset = dataset.filter(_.isInstanceOf[OWLReflexiveObjectPropertyAxiom])

    assert(filteredDataset.count() == expectedNumberOfAxioms)
  }

  test("Symmetric object property axioms should be created correctly") {
    // SymmetricObjectProperty(<http://ex.com/bar#objProp2>)
    val expectedNumberOfAxioms = 1
    val filteredDataset = dataset.filter(_.isInstanceOf[OWLSymmetricObjectPropertyAxiom])

    assert(filteredDataset.count() == expectedNumberOfAxioms)
  }

  test("Transitive object property axioms should be created correctly") {
    // TransitiveObjectProperty(<http://ex.com/bar#objProp1>)
    val expectedNumberOfAxioms = 1
    val filteredDataset = dataset.filter(_.isInstanceOf[OWLTransitiveObjectPropertyAxiom])

    assert(filteredDataset.count() == expectedNumberOfAxioms)
  }

  test("Object property domain axioms should be created correctly") {
    // ObjectPropertyDomain(<http://ex.com/bar#objProp1> <http://ex.com/bar#Cls1>)
    val expectedNumberOfAxioms = 1
    val filteredDataset = dataset.filter(_.isInstanceOf[OWLObjectPropertyDomainAxiom])

    assert(filteredDataset.count() == expectedNumberOfAxioms)
  }

  test("Object property range axioms should be created correctly") {
    // ObjectPropertyRange(<http://ex.com/bar#objProp1> <http://ex.com/bar#AllIndividualsCls>)
    val expectedNumberOfAxioms = 1
    val filteredDataset = dataset.filter(_.isInstanceOf[OWLObjectPropertyRangeAxiom])

    assert(filteredDataset.count() == expectedNumberOfAxioms)
  }

  test("Sub-object-property-of axioms should be created correctly") {
    // SubObjectPropertyOf(<http://ex.com/bar#subObjProp1> <http://ex.com/bar#objProp1>)
    val expectedNumberOfAxioms = 1
    val filteredDataset = dataset.filter(_.isInstanceOf[OWLSubObjectPropertyOfAxiom])

    assert(filteredDataset.count() == expectedNumberOfAxioms)
  }

  test("Sub-property-chain-of axioms should be created correctly") {
    val expectedNumberOfAxioms = 0
    val filteredDataset = dataset.filter(_.isInstanceOf[OWLSubPropertyChainOfAxiom])

    assert(filteredDataset.count() == expectedNumberOfAxioms)
  }

  test("SWRL rules should be created correctly") {
    val expectedNumberOfAxioms = 0
    val filteredDataset = dataset.filter(_.isInstanceOf[SWRLRule])

    assert(filteredDataset.count() == expectedNumberOfAxioms)
  }

  test("Data property assertion axioms should be created correctly") {
    // DataPropertyAssertion(<http://ex.com/bar#dataProp1> <http://ex.com/foo#indivA> "ABCD")
    // DataPropertyAssertion(<http://ex.com/bar#dataProp1> <http://ex.com/foo#indivB> "BCDE")
    val expectedNumberOfAxioms = 2
    val filteredDataset = dataset.filter(_.isInstanceOf[OWLDataPropertyAssertionAxiom])

    assert(filteredDataset.count() == expectedNumberOfAxioms)
  }

  test("There should not be any null values") {
    assert(dataset.filter(a => a == null).count() == 0)
  }
}
