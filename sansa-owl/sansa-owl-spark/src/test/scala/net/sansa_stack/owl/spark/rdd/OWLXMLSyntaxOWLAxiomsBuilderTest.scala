 package net.sansa_stack.owl.spark.rdd

 import com.holdenkarau.spark.testing.SharedSparkContext
 import org.apache.spark.sql.SparkSession
 import org.scalatest.FunSuite
 import org.semanticweb.owlapi.model._

 import net.sansa_stack.owl.spark.owl._

 class OWLXMLSyntaxOWLAxiomsBuilderTest extends FunSuite with SharedSparkContext {

  lazy val spark = SparkSession.builder().appName(sc.appName).master(sc.master)
    .config(
      "spark.kryo.registrator",
      "net.sansa_stack.owl.spark.dataset.UnmodifiableCollectionKryoRegistrator")
    .getOrCreate()

  var _rdd: OWLAxiomsRDD = null
  val syntax = Syntax.OWLXML

  val filePath = this.getClass.getClassLoader.getResource("ont_OWLXML.owl").getPath

  def rdd: OWLAxiomsRDD = {
    if (_rdd == null) {
      _rdd = spark.owl(syntax)(filePath)
      _rdd.cache()
    }
    _rdd
  }

  test("The number of axioms should match") {
    val expectedNumberOfAxioms = 94
    assert(rdd.count() == expectedNumberOfAxioms)
  }

  // //////////// Annotation Properties /////////////////////


  test("The number of generated OWLAnnotationAssertionAxiom objects should be correct") {
   /*
      <owl:Class rdf:about="http://ex.com/bar#Cls1">
          <bar:label>Class 1</bar:label>
          <owl:annotatedSource rdf:resource="http://ex.com/bar#Cls1"/>
          <owl:annotatedProperty rdf:resource="http://www.w3.org/1999/02/22-rdf-syntax-ns#type"/>
          <owl:annotatedTarget rdf:resource="http://www.w3.org/2002/07/owl#Class"/>
          <foo:ann>some annotation</foo:ann>
      </owl:Class>
   */

    val expectedNumberOfAxioms = 2
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLAnnotationAssertionAxiom])
    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  /* <owl:AnnotationProperty rdf:about="http://ex.com/bar#annProp1">
        <rdfs:subPropertyOf rdf:resource="http://ex.com/bar#annProp2"/>
        <rdfs:range rdf:resource="http://ex.com/bar#Cls2"/>
        <rdfs:domain rdf:resource="http://ex.com/bar#Cls1"/>
     </owl:AnnotationProperty>
  */

  test("The number of generated OWLAnnotationPropertyDomainAxiom objects should be correct") {
    // The output include --> AnnotationPropertyDomain(<http://ex.com/bar#annProp1> <http://ex.com/bar#Cls1>)

    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLAnnotationPropertyDomainAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }



  test("The number of generated OWLAnnotationPropertyRangeAxiom objects should be correct") {
    // The output include --> AnnotationPropertyRange(<http://ex.com/bar#annProp1> <http://ex.com/bar#Cls2>)

    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLAnnotationPropertyRangeAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }


  test("The number of generated OWLSubAnnotationPropertyOfAxiom objects should be correct") {
    // The output include --> SubAnnotationPropertyOf(<http://ex.com/bar#annProp1> <http://ex.com/bar#annProp2>)

    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLSubAnnotationPropertyOfAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  // //////////// Declarations /////////////////////


  test("The number of generated OWLDeclarationAxiom objects should be correct") {

    val expectedNumberOfAxioms = 40
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLDeclarationAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("The number of generated OWLDisjointUnionAxiom objects should be correct") {

    /*
        <owl:Class rdf:about="http://ex.com/bar#Cl1OrNegate">
          <owl:disjointUnionOf rdf:parseType="Collection">
              <rdf:Description rdf:about="http://ex.com/bar#Cls1"/>
              <rdf:Description rdf:about="http://ex.com/bar#ComplementCls1"/>
          </owl:disjointUnionOf>
       </owl:Class>
     */

    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLDisjointUnionAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  // //////////// Classes /////////////////////


  test("The number of generated OWLDisjointClassesAxiom objects should be correct") {
    /*
      <owl:Class rdf:about="http://ex.com/bar#DataMax2Prop1">
        <owl:disjointWith rdf:resource="http://ex.com/bar#DataMin3Prop1"/>
      </owl:Class>
    */

    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLDisjointClassesAxiom])
    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("The number of generated OWLClassAssertionAxiom objects should be correct") {

    /* <owl:NamedIndividual rdf:about="http://ex.com/foo#indivA">
            <rdf:type rdf:resource="http://ex.com/bar#Cls1"/>
       </owl:NamedIndividual>
    */

    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLClassAssertionAxiom])
    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }


  test("The number of generated OWLEquivalentClassesAxiom objects should be correct") {
    /* OWLXML Sample of one EquivalentClasse:
     <owl:Class rdf:about="http://ex.com/bar#DataMax2Prop1">
        <owl:equivalentClass>
            <owl:Restriction>
                <owl:onProperty>
                    <owl:DatatypeProperty rdf:about="http://ex.com/bar#dataProp1"/>
                </owl:onProperty>
                <owl:maxCardinality rdf:datatype="http://www.w3.org/2001/XMLSchema#nonNegativeInteger">2</owl:maxCardinality>
            </owl:Restriction>
        </owl:equivalentClass>
      </owl:Class>
    */

    val expectedNumberOfAxioms = 17
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLEquivalentClassesAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("The number of generated OWLSubClassOfAxiom objects should be correct") {

    /*
      <owl:Class rdf:about="http://ex.com/bar#Cls1">
          <rdfs:subClassOf rdf:resource="http://ex.com/bar#UnionCls"/>
      </owl:Class>
     */

    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLSubClassOfAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  // //////////// Data Properties /////////////////////

  test("The number of generated OWLFunctionalDataPropertyAxiom objects should be correct") {
    /*
      <owl:DatatypeProperty rdf:about="http://ex.com/bar#dataProp1">
          <rdf:type rdf:resource="http://www.w3.org/2002/07/owl#FunctionalProperty"/>
      </owl:DatatypeProperty>
    */

    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLFunctionalDataPropertyAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("The number of generated OWLDataPropertyDomainAxiom objects should be correct") {
    /* Sample owlXml snippet:
      <owl:DatatypeProperty rdf:about="http://ex.com/bar#dataProp1">
          <rdfs:domain rdf:resource="http://ex.com/bar#Cls1"/>
      </owl:DatatypeProperty>
    */

    val expectedNumberOfAxioms = 2
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLDataPropertyDomainAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("The number of generated OWLDataPropertyRangeAxiom objects should be correct") {

    /* Sample owlXml snippet:
      <owl:DatatypeProperty rdf:about="http://ex.com/bar#dataProp1">
          <rdfs:range rdf:resource="http://www.w3.org/2001/XMLSchema#string"/>
      </owl:DatatypeProperty>
    */

    val expectedNumberOfAxioms = 2
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLDataPropertyRangeAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("The number of generated OWLDisjointDataPropertiesAxiom objects should be correct") {

    /* Sample owlXml snippet:
      <owl:DatatypeProperty rdf:about="http://ex.com/bar#dataProp1">
          <owl:propertyDisjointWith>
              <owl:DatatypeProperty rdf:about="http://ex.com/bar#dataProp2"/>
          </owl:propertyDisjointWith>
      </owl:DatatypeProperty>
    */

    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLDisjointDataPropertiesAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("The number of generated OWLEquivalentDataPropertiesAxiom objects should be correct") {
    /* Sample owlXml snippet:
      <owl:DatatypeProperty rdf:about="http://ex.com/bar#dataProp1">
          <owl:equivalentProperty>
              <owl:DatatypeProperty rdf:about="http://ex.com/bar#sameAsDataProp1"/>
          </owl:equivalentProperty>
      </owl:DatatypeProperty>
    */

    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLEquivalentDataPropertiesAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("The number of generated OWLSubDataPropertyOfAxiom objects should be correct") {

    /* <owl:DatatypeProperty rdf:about="http://ex.com/bar#subDataProp1">
          <rdfs:subPropertyOf rdf:resource="http://ex.com/bar#dataProp1"/>
       </owl:DatatypeProperty>
    */

    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLSubDataPropertyOfAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }


  test("The number of generated OWLNegativeDataPropertyAssertionAxiom objects should be correct") {

    /* <owl:NamedIndividual rdf:about="http://ex.com/foo#indivA">
          <rdf:type rdf:resource="http://www.w3.org/2002/07/owl#NegativePropertyAssertion"/>
          <owl:sourceIndividual rdf:resource="http://ex.com/foo#indivA"/>
          <owl:assertionProperty rdf:resource="http://ex.com/bar#dataProp2"/>
          <owl:targetValue rdf:datatype="http://www.w3.org/2001/XMLSchema#integer">23</owl:targetValue>
      </owl:NamedIndividual>
    */

    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLNegativeDataPropertyAssertionAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("The number of generated OWLHasKeyAxiom objects should be correct") {
    /* <owl:Class rdf:about="http://ex.com/bar#Cls1">
            <owl:hasKey rdf:parseType="Collection">
               <rdf:Description rdf:about="http://ex.com/bar#dataProp1"/>
            </owl:hasKey>
       </owl:Class>
    */

    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLHasKeyAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }


  test("The number of generated OWLDifferentIndividualsAxiom objects should be correct") {
    /* <owl:NamedIndividual rdf:about="http://ex.com/foo#indivA">
            <rdf:type rdf:resource="http://www.w3.org/2002/07/owl#AllDifferent"/>
            <owl:distinctMembers rdf:parseType="Collection">
                <rdf:Description rdf:about="http://ex.com/foo#indivA"/>
                <rdf:Description rdf:about="http://ex.com/foo#indivB"/>
            </owl:distinctMembers>
      </owl:NamedIndividual>
    */

    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLDifferentIndividualsAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("The number of generated OWLSameIndividualAxiom objects should be correct") {
    /* <owl:NamedIndividual rdf:about="http://ex.com/foo#indivA">
            <owl:sameAs rdf:resource="http://ex.com/foo#sameAsIndivA"/>
       </owl:NamedIndividual>
    */

    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLSameIndividualAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  // //////////// Object Properties /////////////////////

  test("The number of generated OWLNegativeObjectPropertyAssertionAxiom objects should be correct") {

    /* <owl:NamedIndividual rdf:about="http://ex.com/foo#indivB">
            <rdf:type rdf:resource="http://www.w3.org/2002/07/owl#NegativePropertyAssertion"/>
            <owl:sourceIndividual rdf:resource="http://ex.com/foo#indivB"/>
            <owl:assertionProperty rdf:resource="http://ex.com/bar#Prop2"/>
            <owl:targetIndividual rdf:resource="http://ex.com/foo#indivA"/>
       </owl:NamedIndividual>
    */

    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLNegativeObjectPropertyAssertionAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("The number of generated OWLDisjointObjectPropertiesAxiom objects should be correct") {
    /* <owl:ObjectProperty rdf:about="http://ex.com/bar#objProp1">
            <owl:propertyDisjointWith>
                <owl:ObjectProperty rdf:about="http://ex.com/bar#objProp2"/>
            </owl:propertyDisjointWith>
       </owl:ObjectProperty>
    */

    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLDisjointObjectPropertiesAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("The number of generated OWLEquivalentObjectPropertiesAxiom objects should be correct") {

    /* <owl:ObjectProperty rdf:about="http://ex.com/bar#invObjProp1">
          <owl:equivalentProperty>
              <rdf:Description>
                  <owl:inverseOf rdf:resource="http://ex.com/bar#objProp1"/>
              </rdf:Description>
          </owl:equivalentProperty>
      </owl:ObjectProperty>


      <owl:ObjectProperty rdf:about="http://ex.com/bar#objProp1">
          <owl:equivalentProperty>
              <owl:ObjectProperty rdf:about="http://ex.com/bar#sameAsObjProp1"/>
          </owl:equivalentProperty>
       </owl:ObjectProperty>
    */

    val expectedNumberOfAxioms = 2
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLEquivalentObjectPropertiesAxiom])
    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("The number of generated OWLInverseObjectPropertiesAxiom objects should be correct") {
    /* <owl:ObjectProperty rdf:about="http://ex.com/bar#invObjProp1">
          <owl:inverseOf rdf:resource="http://ex.com/bar#objProp1"/>
           <owl:equivalentProperty>
            <rdf:Description>
                <owl:inverseOf rdf:resource="http://ex.com/bar#objProp1"/>
            </rdf:Description>
        </owl:equivalentProperty>
       </owl:ObjectProperty>
    */

    val expectedNumberOfAxioms = 2
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLInverseObjectPropertiesAxiom])
    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("The number of generated OWLAsymmetricObjectPropertyAxiom objects should be correct") {
    /* <owl:ObjectProperty rdf:about="http://ex.com/bar#asymmObjProp">
           <rdf:type rdf:resource="http://www.w3.org/2002/07/owl#AsymmetricProperty"/>
       </owl:ObjectProperty>
    */

    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLAsymmetricObjectPropertyAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("The number of generated OWLFunctionalObjectPropertyAxiom objects should be correct") {
    /* <owl:ObjectProperty rdf:about="http://ex.com/bar#objProp2">
          <rdf:type rdf:resource="http://www.w3.org/2002/07/owl#FunctionalProperty"/>
       </owl:ObjectProperty>
    */

    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLFunctionalObjectPropertyAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("The number of generated OWLInverseFunctionalObjectPropertyAxiom objects should be correct") {
    /* <owl:ObjectProperty rdf:about="http://ex.com/bar#invObjProp1">
            <rdf:type rdf:resource="http://www.w3.org/2002/07/owl#InverseFunctionalProperty"/>
       </owl:ObjectProperty>
    */

    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLInverseFunctionalObjectPropertyAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("The number of generated OWLIrreflexiveObjectPropertyAxiom objects should be correct") {
    /* <owl:ObjectProperty rdf:about="http://ex.com/bar#objProp2">
          <rdf:type rdf:resource="http://www.w3.org/2002/07/owl#IrreflexiveProperty"/>
      </owl:ObjectProperty>
    */

    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLIrreflexiveObjectPropertyAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("The number of generated OWLReflexiveObjectPropertyAxiom objects should be correct") {
      /* <owl:ObjectProperty rdf:about="http://ex.com/bar#objProp1">
            <rdf:type rdf:resource="http://www.w3.org/2002/07/owl#ReflexiveProperty"/>
         </owl:ObjectProperty>
      */

    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLReflexiveObjectPropertyAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("The number of generated OWLSymmetricObjectPropertyAxiom objects should be correct") {
    /*  <owl:ObjectProperty rdf:about="http://ex.com/bar#objProp2">
            <rdf:type rdf:resource="http://www.w3.org/2002/07/owl#SymmetricProperty"/>
        </owl:ObjectProperty>
    */

    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLSymmetricObjectPropertyAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("The number of generated OWLTransitiveObjectPropertyAxiom objects should be correct") {
    /*  <owl:ObjectProperty rdf:about="http://ex.com/bar#objProp1">
            <rdf:type rdf:resource="http://www.w3.org/2002/07/owl#TransitiveProperty"/>
        </owl:ObjectProperty>
    */

    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLTransitiveObjectPropertyAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("The number of generated OWLObjectPropertyDomainAxiom objects should be correct") {
   /* <owl:ObjectProperty rdf:about="http://ex.com/bar#objProp1">
        <rdfs:domain rdf:resource="http://ex.com/bar#Cls1"/>
      </owl:ObjectProperty>
    */

    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLObjectPropertyDomainAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("The number of generated OWLObjectPropertyRangeAxiom objects should be correct") {
    /* <owl:ObjectProperty rdf:about="http://ex.com/bar#objProp1">
         <rdfs:range rdf:resource="http://ex.com/bar#AllIndividualsCls"/>
      </owl:ObjectProperty>
    */

    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLObjectPropertyRangeAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("The number of generated OWLSubObjectPropertyOfAxiom objects should be correct") {
    /*  <owl:ObjectProperty rdf:about="http://ex.com/bar#subObjProp1">
            <rdfs:subPropertyOf rdf:resource="http://ex.com/bar#objProp1"/>
        </owl:ObjectProperty>
    */

    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLSubObjectPropertyOfAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("The number of generated OWLSubPropertyChainOfAxiom objects should be correct") {
    val expectedNumberOfAxioms = 0
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLSubPropertyChainOfAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

 }
