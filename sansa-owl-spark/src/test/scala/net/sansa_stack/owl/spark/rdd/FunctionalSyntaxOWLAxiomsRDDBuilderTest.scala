package net.sansa_stack.owl.spark.rdd

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite
import org.semanticweb.owlapi.model._

import net.sansa_stack.owl.spark.owl._


class FunctionalSyntaxOWLAxiomsRDDBuilderTest extends FunSuite with SharedSparkContext {
  lazy val spark = SparkSession.builder().appName(sc.appName).master(sc.master)
    .config(
      "spark.kryo.registrator",
      "net.sansa_stack.owl.spark.dataset.UnmodifiableCollectionKryoRegistrator")
    .getOrCreate()

  var _rdd: OWLAxiomsRDD = null
  val syntax = Syntax.FUNCTIONAL

  val filePath = this.getClass.getClassLoader.getResource("ont_functional.owl").getPath
  def rdd: OWLAxiomsRDD = {
    if (_rdd == null) {
      _rdd = spark.owl(syntax)(filePath)
      _rdd.cache()
    }
    _rdd
  }

  test("The number of axioms should match") {
    val expectedNumberOfAxioms = 67 // = 71 - commented out Import(...) - 3 x null
    assert(rdd.count() == expectedNumberOfAxioms)
  }

  test("The number of generated OWLAnnotationAssertionAxiom objects should be correct") {
    // --> AnnotationAssertion(bar:label bar:Cls1 "Class 1")
    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLAnnotationAssertionAxiom])
    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("The number of generated OWLAnnotationPropertyDomainAxiom objects should be correct") {
    // --> AnnotationPropertyDomain(bar:annProp1 bar:Cls1)
    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLAnnotationPropertyDomainAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("The number of generated OWLAnnotationPropertyRangeAxiom objects should be correct") {
    // --> AnnotationPropertyRange(bar:annProp1 bar:Cls2)
    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLAnnotationPropertyRangeAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("The number of generated OWLSubAnnotationPropertyOfAxiom objects should be correct") {
    // --> SubAnnotationPropertyOf(bar:annProp1 bar:annProp2)
    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLSubAnnotationPropertyOfAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("The number of generated OWLDeclarationAxiom objects should be correct") {
    // --> Declaration(Annotation(foo:ann "some annotation") Class(bar:Cls1))
    // --> Declaration(Class(bar:Cls2))
    // --> Declaration(Datatype(bar:dtype1))
    // --> Declaration(Datatype(bar:dtype2))
    // --> Declaration(ObjectProperty(bar:objProp1))
    // --> Declaration(ObjectProperty(bar:objProp2))
    // --> Declaration(DataProperty(bar:dataProp1))
    // --> Declaration(DataProperty(bar:dataProp2))
    // --> Declaration(AnnotationProperty(bar:annProp1))
    // --> Declaration(AnnotationProperty(bar:annProp2))
    // --> Declaration(NamedIndividual(foo:indivA))
    // --> Declaration(NamedIndividual(foo:indivB))
    val expectedNumberOfAxioms = 12
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLDeclarationAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("The number of generated OWLDisjointUnionAxiom objects should be correct") {
    // --> DisjointUnion(bar:Cl1OrNegate bar:Cls bar:ComplementCls)
    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLDisjointUnionAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("The number of generated OWLDisjointClassesAxiom objects should be correct") {
    // --> DisjointClasses(bar:DataMin3Prop1 bar:DataMax2Prop1)
    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLDisjointClassesAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("The number of generated OWLEquivalentClassesAxiom objects should be correct") {
    // --> EquivalentClasses(bar:IntersectionCls ObjectIntersectionOf(bar:Cls1 bar:Cls2))
    // --> EquivalentClasses(bar:UnionCls ObjectUnionOf(bar:Cls1 bar:Cls2))
    // --> EquivalentClasses(bar:ComplementCls ObjectComplementOf(bar:Cls1))
    // --> EquivalentClasses(bar:AllIndividualsCls ObjectOneOf(foo:indivA foo:indivB))
    // --> EquivalentClasses(bar:SomeProp1Cls1 ObjectSomeValuesFrom(bar:objProp1 bar:Cls1))
    // --> EquivalentClasses(bar:AllProp1Cls1 ObjectAllValuesFrom(bar:objProp1 bar:Cls1))
    // --> EquivalentClasses(bar:HasValProp1IndivB ObjectHasValue(bar:objProp1 foo:indivB))
    // --> EquivalentClasses(bar:HasSelfProp1 ObjectHasSelf(bar:objProp1))
    // --> EquivalentClasses(bar:Min2Prop1Cls1 ObjectMinCardinality(2 bar:objProp1 bar:Cls1))
    // --> EquivalentClasses(bar:Max3Prop1Cls1 ObjectMaxCardinality(3 bar:objProp1 bar:Cls1))
    // --> EquivalentClasses(bar:Exact5Prop1Cls1 ObjectExactCardinality(5 bar:objProp1 bar:Cls1))
    // --> EquivalentClasses(bar:DataSomeIntLT20 DataSomeValuesFrom(bar:dataProp2 DatatypeRestriction(xsd:integer xsd:maxExclusive "20"^^xsd:integer)))
    // --> EquivalentClasses(bar:DataAllIntGT10 DataAllValuesFrom(bar:dataProp2 DatatypeRestriction(xsd:integer xsd:minInclusive "10"^^xsd:integer)))
    // --> EquivalentClasses(bar:DataHasVal5 DataHasValue(bar:dataProp2 "5"^^xsd:integer))
    // --> EquivalentClasses(bar:DataMin3Prop1 DataMinCardinality(3 bar:dataProp1))
    // --> EquivalentClasses(bar:DataMax2Prop1 DataMaxCardinality(2 bar:dataProp1))
    // --> EquivalentClasses(bar:DataExact5Prop1 DataExactCardinality(5 bar:dataProp1))
    val expectedNumberOfAxioms = 17
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLEquivalentClassesAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("The number of generated OWLSubClassOfAxiom objects should be correct") {
    // --> SubClassOf(bar:Cls1 bar:UnionCls)
    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLSubClassOfAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("The number of generated OWLFunctionalDataPropertyAxiom objects should be correct") {
    // --> FunctionalDataProperty(bar:dataProp1)
    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLFunctionalDataPropertyAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("The number of generated OWLDataPropertyDomainAxiom objects should be correct") {
    // --> DataPropertyDomain(bar:dataProp1 bar:Cls1)
    // --> DataPropertyDomain(bar:dataProp2 bar:Cls1)
    val expectedNumberOfAxioms = 2
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLDataPropertyDomainAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("The number of generated OWLDataPropertyRangeAxiom objects should be correct") {
    // --> DataPropertyRange(bar:dataProp1 xsd:string)
    // --> DataPropertyRange(bar:dataProp2 xsd:int)
    val expectedNumberOfAxioms = 2
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLDataPropertyRangeAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("The number of generated OWLDisjointDataPropertiesAxiom objects should be correct") {
    // --> DisjointDataProperties(bar:dataProp1 bar:dataProp2)
    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLDisjointDataPropertiesAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("The number of generated OWLEquivalentDataPropertiesAxiom objects should be correct") {
    // --> EquivalentDataProperties(bar:sameAsDataProp1 bar:dataProp1)
    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLEquivalentDataPropertiesAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("The number of generated OWLSubDataPropertyOfAxiom objects should be correct") {
    // --> SubDataPropertyOf(bar:subDataProp1 bar:dataProp1)
    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLSubDataPropertyOfAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("The number of generated OWLDatatypeDefinitionAxiom objects should be correct") {
    val expectedNumberOfAxioms = 0
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLDatatypeDefinitionAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("The number of generated OWLHasKeyAxiom objects should be correct") {
    // --> HasKey(bar:Cls1 () (bar:dataProp1))
    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLHasKeyAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("The number of generated OWLClassAssertionAxiom objects should be correct") {
    // --> ClassAssertion(bar:Cls1 foo:indivA)
    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLClassAssertionAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("The number of generated OWLDifferentIndividualsAxiom objects should be correct") {
    // --> DifferentIndividuals(foo:indivA foo:indivB)
    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLDifferentIndividualsAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("The number of generated OWLSameIndividualAxiom objects should be correct") {
    // --> SameIndividual(foo:sameAsIndivA foo:indivA)
    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLSameIndividualAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("The number of generated OWLNegativeDataPropertyAssertionAxiom objects should be correct") {
    // --> NegativeDataPropertyAssertion(bar:dataProp2 foo:indivA "23"^^xsd:integer)
    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLNegativeDataPropertyAssertionAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("The number of generated OWLNegativeObjectPropertyAssertionAxiom objects should be correct") {
    // --> NegativeObjectPropertyAssertion(bar:Prop2 foo:indivB foo:indivA)
    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLNegativeObjectPropertyAssertionAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("The number of generated OWLObjectPropertyAssertionAxiom objects should be correct") {
    // --> ObjectPropertyAssertion(bar:objProp1 foo:indivA foo:indivB)
    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLObjectPropertyAssertionAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("The number of generated OWLDisjointObjectPropertiesAxiom objects should be correct") {
    // --> DisjointObjectProperties(bar:objProp1 bar:objProp2)
    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLDisjointObjectPropertiesAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("The number of generated OWLEquivalentObjectPropertiesAxiom objects should be correct") {
    // --> EquivalentObjectProperties(bar:invObjProp1 ObjectInverseOf(bar:objProp1))
    // --> EquivalentObjectProperties(bar:sameAsObjProp1 bar:objProp1)
    val expectedNumberOfAxioms = 2
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLEquivalentObjectPropertiesAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("The number of generated OWLInverseObjectPropertiesAxiom objects should be correct") {
    // --> InverseObjectProperties(bar:invObjProp1 bar:objProp1)
    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLInverseObjectPropertiesAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("The number of generated OWLAsymmetricObjectPropertyAxiom objects should be correct") {
    // --> AsymmetricObjectProperty(bar:asymmObjProp)
    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLAsymmetricObjectPropertyAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("The number of generated OWLFunctionalObjectPropertyAxiom objects should be correct") {
    // --> FunctionalObjectProperty(bar:objProp2)
    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLFunctionalObjectPropertyAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("The number of generated OWLInverseFunctionalObjectPropertyAxiom objects should be correct") {
    // --> InverseFunctionalObjectProperty(bar:invObjProp1)
    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLInverseFunctionalObjectPropertyAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("The number of generated OWLIrreflexiveObjectPropertyAxiom objects should be correct") {
    // --> IrreflexiveObjectProperty(bar:objProp2)
    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLIrreflexiveObjectPropertyAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("The number of generated OWLReflexiveObjectPropertyAxiom objects should be correct") {
    // --> ReflexiveObjectProperty(bar:objProp1)
    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLReflexiveObjectPropertyAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("The number of generated OWLSymmetricObjectPropertyAxiom objects should be correct") {
    // --> SymmetricObjectProperty(bar:objProp2)
    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLSymmetricObjectPropertyAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("The number of generated OWLTransitiveObjectPropertyAxiom objects should be correct") {
    // --> TransitiveObjectProperty(bar:objProp1)
    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLTransitiveObjectPropertyAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("The number of generated OWLObjectPropertyDomainAxiom objects should be correct") {
    // --> ObjectPropertyDomain(bar:objProp1 bar:Cls1)
    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLObjectPropertyDomainAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("The number of generated OWLObjectPropertyRangeAxiom objects should be correct") {
    // --> ObjectPropertyRange(bar:objProp1 bar:AllIndividualsCls)
    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLObjectPropertyRangeAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("The number of generated OWLSubObjectPropertyOfAxiom objects should be correct") {
    // --> SubObjectPropertyOf(bar:subObjProp1 bar:objProp1)
    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLSubObjectPropertyOfAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("The number of generated OWLSubPropertyChainOfAxiom objects should be correct") {
    val expectedNumberOfAxioms = 0
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLSubPropertyChainOfAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("The number of generated SWRLRule objects should be correct") {
    val expectedNumberOfAxioms = 0
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[SWRLRule])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("The number of generated OWLDataPropertyAssertionAxiom objects should be correct") {
    // --> DataPropertyAssertion(bar:dataProp1 foo:indivA "ABCD")
    // --> DataPropertyAssertion(bar:dataProp1 foo:indivB "BCDE")
    val expectedNumberOfAxioms = 2
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLDataPropertyAssertionAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  /* The following test fails since the ontology annotations are not considered
   * as part of the axioms but are rather properties of the ontology.
   */
  //  test("The number of generated OWLAnnotationAxiom objects should be correct") {
  //    // --> Annotation(foo:hasName "Name")
  //    // --> Annotation(bar:hasTitle "Title")
  //    // --> Annotation(:description "A longer
  //    // description running over
  //    // several lines")
  //    val expectedNumberOfAxioms = 3
  //    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLAnnotation])
  //
  //    filteredRDD.collect()
  //    filteredRDD.foreach(ax => println(ax))
  //    assert(filteredRDD.count() == expectedNumberOfAxioms)
  //  }

  test("There should not be any null values") {
    assert(rdd.filter(a => a == null).count() == 0)
  }
}
