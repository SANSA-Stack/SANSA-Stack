package net.sansa_stack.owl.spark.rdd

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.model._
import org.semanticweb.owlapi.vocab.XSDVocabulary
import uk.ac.manchester.cs.owl.owlapi.OWLDatatypeImpl

import scala.collection.JavaConverters._


class ManchesterSyntaxOWLAxiomsRDDBuilderTest extends FunSuite with SharedSparkContext {
  var _rdd: OWLAxiomsRDD = null
  val dataFactory = OWLManager.getOWLDataFactory

  def rdd = {
    if (_rdd == null) {
      _rdd = ManchesterSyntaxOWLAxiomsRDDBuilder.build(
        sc, "src/test/resources/ont_manchester.owl")
//        sc, "hdfs://localhost:9000/ont_manchester.owl")
      _rdd.cache()
    }

    _rdd
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
   * -->   1) Declaration(AnnotationProperty(<http://ex.com/bar#annProp1>))
   * -->   2) SubAnnotationPropertyOf(<http://ex.com/bar#annProp1> <http://ex.com/bar#annProp2>)
   * -->   3) AnnotationPropertyDomain(<http://ex.com/bar#annProp1> <http://ex.com/bar#Cls1>)
   * -->   4) AnnotationPropertyRange(<http://ex.com/bar#annProp1> <http://ex.com/bar#Cls2>)
   *
   * *|AnnotationProperty: bar:annProp2
   * -->   5) Declaration(AnnotationProperty(<http://ex.com/bar#annProp2>))
   *
   * *|AnnotationProperty: bar:hasTitle
   * -->   6) Declaration(AnnotationProperty(<http://ex.com/bar#hasTitle>))
   *
   * *|AnnotationProperty: bar:label
   * -->   7) Declaration(AnnotationProperty(<http://ex.com/bar#label>))
   *
   * *|AnnotationProperty: description
   * -->   8) Declaration(AnnotationProperty(<http://ex.com/default#description>))
   *
   * *|AnnotationProperty: foo:ann
   * -->   9) Declaration(AnnotationProperty(<http://ex.com/foo#ann>))
   *
   * *|AnnotationProperty: foo:hasName
   * -->  10) Declaration(AnnotationProperty(<http://ex.com/foo#hasName>))
   *
   * *|Datatype: bar:dtype1
   * -->  11) Declaration(Datatype(<http://ex.com/bar#dtype1>))
   *
   * *|Datatype: bar:dtype2
   * -->  12) Declaration(Datatype(<http://ex.com/bar#dtype2>))
   *
   * *|Datatype: rdf:PlainLiteral
   * -->  13) Declaration(Datatype(rdf:PlainLiteral))
   *
   * *|Datatype: rdfs:Literal
   * -->  14) Declaration(Datatype(rdfs:Literal))
   *
   * *|Datatype: xsd:int
   * -->  15) Declaration(Datatype(xsd:int))
   *
   * *|Datatype: xsd:integer
   * -->  16) Declaration(Datatype(xsd:integer))
   *
   * *|Datatype: xsd:string
   * -->  17) Declaration(Datatype(xsd:string))
   *
   * *|ObjectProperty: bar:Prop2
   * -->  18) Declaration(ObjectProperty(<http://ex.com/bar#Prop2>))
   *
   * *|ObjectProperty: bar:asymmObjProp
   *  |
   *  |    Characteristics:
   *  |        Asymmetric
   * -->  19) Declaration(ObjectProperty(<http://ex.com/bar#asymmObjProp>))
   * -->  20) AsymmetricObjectProperty(<http://ex.com/bar#asymmObjProp>)
   *
   * *|ObjectProperty: bar:invObjProp1
   *  |
   *  |    EquivalentTo:
   *  |         inverse (bar:objProp1)
   *  |
   *  |    Characteristics:
   *  |        InverseFunctional
   *  |
   *  |    InverseOf:
   *  |        bar:objProp1
   * -->  21) Declaration(ObjectProperty(<http://ex.com/bar#invObjProp1>))
   * -->  22) EquivalentObjectProperties(<http://ex.com/bar#invObjProp1> InverseOf(<http://ex.com/bar#objProp1>) )
   * -->  23) InverseFunctionalObjectProperty(<http://ex.com/bar#invObjProp1>)
   * -->  24) InverseObjectProperties(<http://ex.com/bar#invObjProp1> <http://ex.com/bar#objProp1>)
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
   * -->  25) Declaration(ObjectProperty(<http://ex.com/bar#objProp1>))
   * -->  26) EquivalentObjectProperties(<http://ex.com/bar#objProp1> <http://ex.com/bar#sameAsObjProp1> )
   * -->  27) DisjointObjectProperties(<http://ex.com/bar#objProp1> <http://ex.com/bar#objProp2> )
   * -->  28) TransitiveObjectProperty(<http://ex.com/bar#objProp1>)
   * -->  29) ReflexiveObjectProperty(<http://ex.com/bar#objProp1>)
   * -->  30) ObjectPropertyDomain(<http://ex.com/bar#objProp1> <http://ex.com/bar#Cls1>)
   * -->  31) ObjectPropertyRange(<http://ex.com/bar#objProp1> <http://ex.com/bar#AllIndividualsCls>)
   * -->  32) InverseObjectProperties(<http://ex.com/bar#objProp1> <http://ex.com/bar#invObjProp1>)
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
   * -->  33) Declaration(ObjectProperty(<http://ex.com/bar#objProp2>))
   * -->  34) DisjointObjectProperties(<http://ex.com/bar#objProp1> <http://ex.com/bar#objProp2> )
   * -->  35) FunctionalObjectProperty(<http://ex.com/bar#objProp2>)
   * -->  36) SymmetricObjectProperty(<http://ex.com/bar#objProp2>)
   * -->  37) IrreflexiveObjectProperty(<http://ex.com/bar#objProp2>)
   *
   * *|ObjectProperty: bar:sameAsObjProp1
   *  |
   *  |    EquivalentTo:
   *  |        bar:objProp1
   * -->  38) Declaration(ObjectProperty(<http://ex.com/bar#sameAsObjProp1>))
   * -->  39) EquivalentObjectProperties(<http://ex.com/bar#objProp1> <http://ex.com/bar#sameAsObjProp1> )
   *
   * *|ObjectProperty: bar:subObjProp1
   *  |
   *  |    SubPropertyOf:
   *  |        bar:objProp1
   * -->  40) Declaration(ObjectProperty(<http://ex.com/bar#subObjProp1>))
   * -->  41) SubObjectPropertyOf(<http://ex.com/bar#subObjProp1> <http://ex.com/bar#objProp1>)
   *
   * *|DataProperty: bar:dataProp1
   *  |
   *  |    Characteristics:
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
   * -->  42) Declaration(DataProperty(<http://ex.com/bar#dataProp1>))
   * -->  43) FunctionalDataProperty(<http://ex.com/bar#dataProp1>)
   * -->  44) DataPropertyDomain(<http://ex.com/bar#dataProp1> <http://ex.com/bar#Cls1>)
   * -->  45) DataPropertyRange(<http://ex.com/bar#dataProp1> xsd:string)
   * -->  46) EquivalentDataProperties(<http://ex.com/bar#dataProp1> <http://ex.com/bar#sameAsDataProp1> )
   * -->  47) DisjointDataProperties(<http://ex.com/bar#dataProp1> <http://ex.com/bar#dataProp2> )
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
   * -->  48) Declaration(DataProperty(<http://ex.com/bar#dataProp2>))
   * -->  49) DataPropertyDomain(<http://ex.com/bar#dataProp2> <http://ex.com/bar#Cls1>)
   * -->  50) DataPropertyRange(<http://ex.com/bar#dataProp2> xsd:int)
   * -->  51) DisjointDataProperties(<http://ex.com/bar#dataProp1> <http://ex.com/bar#dataProp2> )
   *
   * *|DataProperty: bar:sameAsDataProp1
   *  |
   *  |    EquivalentTo:
   *  |        bar:dataProp1
   * -->  52) Declaration(DataProperty(<http://ex.com/bar#sameAsDataProp1>))
   * -->  XX) EquivalentDataProperties(<http://ex.com/bar#dataProp1> <http://ex.com/bar#sameAsDataProp1> )
   *
   * *|DataProperty: bar:subDataProp1
   *  |
   *  |    SubPropertyOf:
   *  |        bar:dataProp1
   * -->  53) Declaration(DataProperty(<http://ex.com/bar#subDataProp1>))
   * -->  54) SubDataPropertyOf(<http://ex.com/bar#subDataProp1> <http://ex.com/bar#dataProp1>)
   *
   * *|Class: bar:AllIndividualsCls
   *  |
   *  |    EquivalentTo:
   *  |        {foo:indivA , foo:indivB}
   * -->  55) Declaration(Class(<http://ex.com/bar#AllIndividualsCls>))
   * -->  56) FIXME: EquivalentClasses(<http://ex.com/bar#AllIndividualsCls> <http://ex.com/default#{> )
   *          should be EquivalentClasses(<http://ex.com/bar#AllIndividualsCls> ObjectOneOf(<http://ex.com/foo#indivA> <http://ex.com/foo#indivB>))
   *
   * *|Class: bar:AllProp1Cls1
   *  |
   *  |    EquivalentTo:
   *  |        bar:objProp1 only bar:Cls1
   * -->  57) Declaration(Class(<http://ex.com/bar#AllProp1Cls1>))
   * -->  58) FIXME: EquivalentClasses(<http://ex.com/bar#AllProp1Cls1> <http://ex.com/bar#objProp1> )
   *          should be EquivalentClasses(<http://ex.com/bar#AllProp1Cls1> ObjectAllValuesFrom(<http://ex.com/bar#objProp1> <http://ex.com/bar#Cls1>))
   *
   * *|Class: bar:Cl1OrNegate
   *  |
   *  |    DisjointUnionOf:
   *  |        bar:Cls1, bar:ComplementCls1
   * -->  59) Declaration(Class(<http://ex.com/bar#Cl1OrNegate>))
   * -->  60) DisjointUnion(<http://ex.com/bar#Cl1OrNegate> <http://ex.com/bar#Cls1> <http://ex.com/bar#ComplementCls1> )
   *
   * *|Class: bar:Cls1
   *  |
   *  |    Annotations:
   *  |        bar:label "Class 1"
   *  |
   *  |    SubClassOf:
   *  |        bar:UnionCls
   *  |
   *  |    HasKey:
   *  |        bar:dataProp1
   * -->  XX) Declaration(Class(<http://ex.com/bar#Cls1>))
   * -->  XX) AnnotationAssertion(<http://ex.com/bar#label> <http://ex.com/bar#Cls1> "Class 1")
   * -->  XX) SubClassOf(<http://ex.com/bar#Cls1> <http://ex.com/bar#UnionCls>)
   * -->  XX) HasKey(<http://ex.com/bar#Cls1> () (<http://ex.com/bar#dataProp1>))
   * > Parser error for frame
   * > Class: <http://ex.com/bar#Cls1>
   * >
   * >     Annotations:
   * >         <http://ex.com/bar#label> "Class 1"
   * >
   * >     SubClassOf:
   * >         <http://ex.com/bar#UnionCls>
   * >
   * >     HasKey:
   * >         <http://ex.com/bar#dataProp1>
   * >
   * > Prefix not registered for prefix name: :
   * > org.semanticweb.owlapi.model.OWLRuntimeException: Prefix not registered for prefix name: :
   * > 	at org.semanticweb.owlapi.util.DefaultPrefixManager.getIRI(DefaultPrefixManager.java:194)
   * > 	at org.semanticweb.owlapi.manchestersyntax.parser.ManchesterOWLSyntaxParserImpl.getIRI(ManchesterOWLSyntaxParserImpl.java:2331)
   *
   * *|Class: bar:Cls2
   * -->  61) Declaration(Class(<http://ex.com/bar#Cls2>))
   *
   * *|Class: bar:ComplementCls
   *  |
   *  |    EquivalentTo:
   *  |        not (bar:Cls1)
   * -->  62) Declaration(Class(<http://ex.com/bar#ComplementCls>))
   * -->  63) EquivalentClasses(<http://ex.com/bar#ComplementCls> ObjectComplementOf(<http://ex.com/bar#Cls1>) )
   *
   * *|Class: bar:ComplementCls1
   * -->  64) Declaration(Class(<http://ex.com/bar#ComplementCls1>))
   *
   * *|Class: bar:DataAllIntGT10
   *  |
   *  |    EquivalentTo:
   *  |        bar:dataProp2 only xsd:integer[>= 10]
   * -->  65) Declaration(Class(<http://ex.com/bar#DataAllIntGT10>))
   * -->  66) FIXME: EquivalentClasses(<http://ex.com/bar#DataAllIntGT10> <http://ex.com/bar#dataProp2> )
   *          should be EquivalentClasses(<http://ex.com/bar#DataAllIntGT10> DataAllValuesFrom(<http://ex.com/bar#dataProp2> DatatypeRestriction(xsd:integer xsd:minInclusive "10"^^xsd:integer)))
   *
   * *|Class: bar:DataExact5Prop1
   *  |
   *  |    EquivalentTo:
   *  |        bar:dataProp1 exactly 5 rdfs:Literal
   * -->  67) Declaration(Class(<http://ex.com/bar#DataExact5Prop1>))
   * -->  68) FIXME: EquivalentClasses(<http://ex.com/bar#DataExact5Prop1> <http://ex.com/bar#dataProp1> )
   *          should be EquivalentClasses(<http://ex.com/bar#Exact5Prop1Cls1> ObjectExactCardinality(5 <http://ex.com/bar#objProp1> <http://ex.com/bar#Cls1>))
   *
   * *|Class: bar:DataHasVal5
   *  |
   *  |    EquivalentTo:
   *  |        bar:dataProp2 value 5
   * -->  69) Declaration(Class(<http://ex.com/bar#DataHasVal5>))
   * -->  70) FIXME: EquivalentClasses(<http://ex.com/bar#DataHasVal5> <http://ex.com/bar#dataProp2> )
   *          should be EquivalentClasses(<http://ex.com/bar#DataHasVal5> DataHasValue(<http://ex.com/bar#dataProp2> "5"^^xsd:integer))
   *
   * *|Class: bar:DataMax2Prop1
   *  |
   *  |    EquivalentTo:
   *  |        bar:dataProp1 max 2 rdfs:Literal
   *  |
   *  |    DisjointWith:
   *  |        bar:DataMin3Prop1
   * -->  71) Declaration(Class(<http://ex.com/bar#DataMax2Prop1>))
   * -->  72) FIXME: EquivalentClasses(<http://ex.com/bar#DataMax2Prop1> <http://ex.com/bar#dataProp1> )
   *          should be EquivalentClasses(<http://ex.com/bar#Min2Prop1Cls1> ObjectMinCardinality(2 <http://ex.com/bar#objProp1> <http://ex.com/bar#Cls1>))
   * -->  XX) DisjointClasses(<http://ex.com/bar#DataMin3Prop1> <http://ex.com/bar#DataMax2Prop1>)
   *
   * *|Class: bar:DataMin3Prop1
   *  |
   *  |    EquivalentTo:
   *  |        bar:dataProp1 min 3 rdfs:Literal
   *  |
   *  |    DisjointWith:
   *  |        bar:DataMax2Prop1
   * -->  73) Declaration(Class(<http://ex.com/bar#DataMin3Prop1>))
   * -->  74) FIXME: EquivalentClasses(<http://ex.com/bar#DataMin3Prop1> <http://ex.com/bar#dataProp1> )
   *          should be EquivalentClasses(<http://ex.com/bar#DataMin3Prop1> DataMinCardinality(3 <http://ex.com/bar#dataProp1>))
   * -->  XX) DisjointClasses(<http://ex.com/bar#DataMin3Prop1> <http://ex.com/bar#DataMax2Prop1>)
   *
   * *|Class: bar:DataSomeIntLT20
   *  |
   *  |    EquivalentTo:
   *  |        bar:dataProp2 some xsd:integer[< 20]
   * -->  75) Declaration(Class(<http://ex.com/bar#DataSomeIntLT20>))
   * -->  76) FIXME: EquivalentClasses(<http://ex.com/bar#DataSomeIntLT20> <http://ex.com/bar#dataProp2> )
   *          should be EquivalentClasses(<http://ex.com/bar#DataSomeIntLT20> DataSomeValuesFrom(<http://ex.com/bar#dataProp2> DatatypeRestriction(xsd:integer xsd:maxExclusive "20"^^xsd:integer)))
   *
   * *|Class: bar:Exact5Prop1Cls1
   *  |
   *  |    EquivalentTo:
   *  |        bar:objProp1 exactly 5 bar:Cls1
   * -->  77) Declaration(Class(<http://ex.com/bar#Exact5Prop1Cls1>))
   * -->  78) FIXME: EquivalentClasses(<http://ex.com/bar#Exact5Prop1Cls1> <http://ex.com/bar#objProp1> )
   *          should be EquivalentClasses(<http://ex.com/bar#Exact5Prop1Cls1> ObjectExactCardinality(5 <http://ex.com/bar#objProp1> <http://ex.com/bar#Cls1>))
   *
   * *|Class: bar:HasSelfProp1
   *  |
   *  |    EquivalentTo:
   *  |        bar:objProp1 some  Self
   * -->  79) Declaration(Class(<http://ex.com/bar#HasSelfProp1>))
   * -->  80) FIXME: EquivalentClasses(<http://ex.com/bar#HasSelfProp1> <http://ex.com/bar#objProp1> )
   *          should be EquivalentClasses(<http://ex.com/bar#HasSelfProp1> ObjectHasSelf(<http://ex.com/bar#objProp1>))
   *
   * *|Class: bar:HasValProp1IndivB
   *  |
   *  |    EquivalentTo:
   *  |        bar:objProp1 value foo:indivB
   * -->  81) Declaration(Class(<http://ex.com/bar#HasValProp1IndivB>))
   * -->  82) FIXME: EquivalentClasses(<http://ex.com/bar#HasValProp1IndivB> <http://ex.com/bar#objProp1> )
   *          should be EquivalentClasses(<http://ex.com/bar#HasValProp1IndivB> ObjectHasValue(<http://ex.com/bar#objProp1> <http://ex.com/foo#indivB>))
   *
   * *|Class: bar:IntersectionCls
   *  |
   *  |    EquivalentTo:
   *  |        bar:Cls1
   *  |         and bar:Cls2
   * -->  83) Declaration(Class(<http://ex.com/bar#IntersectionCls>))
   * -->  84) EquivalentClasses(<http://ex.com/bar#IntersectionCls> ObjectIntersectionOf(<http://ex.com/bar#Cls1> <http://ex.com/bar#Cls2>) )
   *
   * *|Class: bar:Max3Prop1Cls1
   *  |
   *  |    EquivalentTo:
   *  |        bar:objProp1 max 3 bar:Cls1
   * -->  85) Declaration(Class(<http://ex.com/bar#Max3Prop1Cls1>))
   * -->  86) FIXME: EquivalentClasses(<http://ex.com/bar#Max3Prop1Cls1> <http://ex.com/bar#objProp1> )
   *          should be EquivalentClasses(<http://ex.com/bar#Max3Prop1Cls1> ObjectMaxCardinality(3 <http://ex.com/bar#objProp1> <http://ex.com/bar#Cls1>))
   *
   * *|Class: bar:Min2Prop1Cls1
   *  |
   *  |    EquivalentTo:
   *  |        bar:objProp1 min 2 bar:Cls1
   * -->  87) Declaration(Class(<http://ex.com/bar#Min2Prop1Cls1>))
   * -->  88) FIXME: EquivalentClasses(<http://ex.com/bar#Min2Prop1Cls1> <http://ex.com/bar#objProp1> )
   *          should be EquivalentClasses(<http://ex.com/bar#Min2Prop1Cls1> ObjectMinCardinality(2 <http://ex.com/bar#objProp1> <http://ex.com/bar#Cls1>))
   *
   * *|Class: bar:SomeProp1Cls1
   *  |
   *  |    EquivalentTo:
   *  |        bar:objProp1 some bar:Cls1
   * -->  89) Declaration(Class(<http://ex.com/bar#SomeProp1Cls1>))
   * -->  90) FIXME: EquivalentClasses(<http://ex.com/bar#SomeProp1Cls1> <http://ex.com/bar#objProp1> )
   *          should be EquivalentClasses(<http://ex.com/bar#SomeProp1Cls1> ObjectSomeValuesFrom(<http://ex.com/bar#objProp1> <http://ex.com/bar#Cls1>))
   *
   * *|Class: bar:UnionCls
   *  |
   *  |    EquivalentTo:
   *  |        bar:Cls1 or bar:Cls2
   * -->  91) Declaration(Class(<http://ex.com/bar#UnionCls>))
   * -->  92) EquivalentClasses(<http://ex.com/bar#UnionCls> ObjectUnionOf(<http://ex.com/bar#Cls1> <http://ex.com/bar#Cls2>) )
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
   * -->  93) Declaration(NamedIndividual(<http://ex.com/foo#sameAsIndivA>))
   * -->  XX) ClassAssertion(<http://ex.com/bar#Cls1> <http://ex.com/foo#indivA>)
   * -->  XX) ObjectPropertyAssertion(<http://ex.com/bar#objProp1> <http://ex.com/foo#indivA> <http://ex.com/foo#indivB>)
   * -->  XX) DataPropertyAssertion(<http://ex.com/bar#dataProp1> <http://ex.com/foo#indivA> "ABCD")
   * -->  XX) NegativeDataPropertyAssertion(<http://ex.com/bar#dataProp2> <http://ex.com/foo#indivA> "23"^^xsd:integer)
   * -->  94) SameIndividual(<http://ex.com/foo#indivA> <http://ex.com/foo#sameAsIndivA> )
   * -->  XX) DifferentIndividuals(<http://ex.com/foo#indivA> <http://ex.com/foo#indivB>)
   *
   * > Parser error for frame
   * > Individual: <http://ex.com/foo#indivA>
   * >
   * >     Types:
   * >         <http://ex.com/bar#Cls1>
   * >
   * >     Facts:
   * >      <http://ex.com/bar#objProp1>  <http://ex.com/foo#indivB>,
   * >      <http://ex.com/bar#dataProp1>  "ABCD",
   * >       not  <http://ex.com/bar#dataProp2>  23
   * >
   * >     SameAs:
   * >         <http://ex.com/foo#sameAsIndivA>
   * >
   * >     DifferentFrom:
   * >         <http://ex.com/foo#indivB>
   * >
   * > Encountered <http://ex.com/foo#indivB> at line 7 column 35. Expected one of:
   * > 	$integer$
   * > 	"$Literal$"@<lang>
   * > 	or
   * > 	and
   * > 	"$Literal$"^^<datatype>
   * > 	true
   * > 	false
   * > 	$float$
   * > 	"$Literal$"
   * > 	$double$
   *
   * *|Individual: foo:indivB
   *  |
   *  |    Facts:
   *  |      not  bar:Prop2  foo:indivA,
   *  |     bar:dataProp1  "BCDE"
   *  |
   *  |    DifferentFrom:
   *  |        foo:indivA
   * -->  XX) Declaration(NamedIndividual(<http://ex.com/foo#indivB>))
   * -->  XX) NegativeObjectPropertyAssertion(<http://ex.com/bar#Prop2> <http://ex.com/foo#indivB> <http://ex.com/foo#indivA>)
   * -->  XX) DataPropertyAssertion(<http://ex.com/bar#dataProp1> <http://ex.com/foo#indivB> "BCDE")
   * -->  XX) DifferentIndividuals(<http://ex.com/foo#indivA> <http://ex.com/foo#indivB>)
   *
   * > Parser error for frame
   * > Individual: <http://ex.com/foo#indivB>
   * >
   * >     Facts:
   * >       not  <http://ex.com/bar#Prop2>  <http://ex.com/foo#indivA>,
   * >      <http://ex.com/bar#dataProp1>  "BCDE"
   * >
   * >     DifferentFrom:
   * >         <http://ex.com/foo#indivA>
   * >
   * > Encountered <http://ex.com/foo#indivA> at line 4 column 38. Expected one of:
   * > 	$integer$
   * > 	"$Literal$"@<lang>
   * > 	"$Literal$"^^<datatype>
   * > 	true
   * > 	false
   * > 	$float$
   * > 	"$Literal$"
   * > 	$double$
   */
  ignore("The number of axioms should match") {
    val expectedNumberOfAxioms = 100
    assert(rdd.count() == expectedNumberOfAxioms)
  }

  ignore("Annotation assertion axioms should be created correctly") {
    // -->  XX) AnnotationAssertion(<http://ex.com/bar#label> <http://ex.com/bar#Cls1> "Class 1")
    val expectedNumberOfAxioms = 1
    val expectedAnnProperty = dataFactory.getOWLAnnotationProperty("http://ex.com/bar#label")
    val expectedAnnTarget = dataFactory.getOWLClass("http://ex.com/bar#Cls1")
    val expectedAnnValue = dataFactory.getOWLLiteral("Class 1")

    val filteredRDD: RDD[OWLAnnotationAssertionAxiom] =
      rdd.filter(_.isInstanceOf[OWLAnnotationAssertionAxiom]).map(
        _.getAxiomWithoutAnnotations[OWLAnnotationAssertionAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)

    val annAxiom = filteredRDD.first()
    assert(annAxiom.getSubject == expectedAnnTarget)
    assert(annAxiom.getProperty == expectedAnnProperty)
    assert(annAxiom.getValue == expectedAnnValue)
  }

  test("Annotation property domain axioms should be created correctly") {
    // --> AnnotationPropertyDomain(<http://ex.com/bar#annProp1> <http://ex.com/bar#Cls1>)
    val expectedNumberOfAxioms = 1
    val expectedAnnProperty = dataFactory.getOWLAnnotationProperty("http://ex.com/bar#annProp1")
    val expextedAnnPropertyDomain = IRI.create("http://ex.com/bar#Cls1")

    val filteredRDD: RDD[OWLAnnotationPropertyDomainAxiom] =
      rdd.filter(_.isInstanceOf[OWLAnnotationPropertyDomainAxiom]).map(
        _.getAxiomWithoutAnnotations[OWLAnnotationPropertyDomainAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)

    val annDomainAxiom: OWLAnnotationPropertyDomainAxiom = filteredRDD.first()
    assert(annDomainAxiom.getProperty == expectedAnnProperty)
    assert(annDomainAxiom.getDomain == expextedAnnPropertyDomain)
  }

  test("Annotation property range axioms should be created correctly") {
    // --> AnnotationPropertyRange(<http://ex.com/bar#annProp1> <http://ex.com/bar#Cls2>)
    val expectedNumberOfAxioms = 1
    val expectedAnnProperty = dataFactory.getOWLAnnotationProperty("http://ex.com/bar#annProp1")
    val expectedAnnPropertyRange = IRI.create("http://ex.com/bar#Cls2")

    val filteredRDD: RDD[OWLAnnotationPropertyRangeAxiom] =
      rdd.filter(_.isInstanceOf[OWLAnnotationPropertyRangeAxiom]).map(
        _.getAxiomWithoutAnnotations[OWLAnnotationPropertyRangeAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)

    val annPropertyRangeAxiom = filteredRDD.first()
    assert(annPropertyRangeAxiom.getProperty == expectedAnnProperty)
    assert(annPropertyRangeAxiom.getRange == expectedAnnPropertyRange)
  }

  test("Sub-annotation-property-Of axioms should be created correctly") {
    // --> SubAnnotationPropertyOf(<http://ex.com/bar#annProp1> <http://ex.com/bar#annProp2>)
    val expectedNumberOfAxioms = 1
    val expectedSuperProperty = dataFactory.getOWLAnnotationProperty("http://ex.com/bar#annProp2")
    val expectedSubProperty = dataFactory.getOWLAnnotationProperty("http://ex.com/bar#annProp1")

    val filteredRDD: RDD[OWLSubAnnotationPropertyOfAxiom] =
      rdd.filter(_.isInstanceOf[OWLSubAnnotationPropertyOfAxiom]).map(
        _.getAxiomWithoutAnnotations[OWLSubAnnotationPropertyOfAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)

    val subAnnPropOfAxiom = filteredRDD.first()
    assert(subAnnPropOfAxiom.getSuperProperty == expectedSuperProperty)
    assert(subAnnPropOfAxiom.getSubProperty == expectedSubProperty)
  }

  ignore("Declaration axioms should be created correctly") {
    // --> Declaration(AnnotationProperty(<http://ex.com/bar#annProp1>))
    // --> Declaration(AnnotationProperty(<http://ex.com/bar#annProp2>))
    // --> Declaration(AnnotationProperty(<http://ex.com/bar#hasTitle>))
    // --> Declaration(AnnotationProperty(<http://ex.com/bar#label>))
    // --> Declaration(AnnotationProperty(<http://ex.com/default#description>))
    // --> Declaration(AnnotationProperty(<http://ex.com/foo#ann>))
    // --> Declaration(AnnotationProperty(<http://ex.com/foo#hasName>))
    // --> Declaration(Datatype(<http://ex.com/bar#dtype1>))
    // --> Declaration(Datatype(<http://ex.com/bar#dtype2>))
    // --> Declaration(Datatype(rdf:PlainLiteral))
    // --> Declaration(Datatype(rdfs:Literal))
    // --> Declaration(Datatype(xsd:int))
    // --> Declaration(Datatype(xsd:integer))
    // --> Declaration(Datatype(xsd:string))
    // --> Declaration(ObjectProperty(<http://ex.com/bar#Prop2>))
    // --> Declaration(ObjectProperty(<http://ex.com/bar#asymmObjProp>))
    // --> Declaration(ObjectProperty(<http://ex.com/bar#invObjProp1>))
    // --> Declaration(ObjectProperty(<http://ex.com/bar#objProp1>))
    // --> Declaration(ObjectProperty(<http://ex.com/bar#objProp2>))
    // --> Declaration(ObjectProperty(<http://ex.com/bar#sameAsObjProp1>))
    // --> Declaration(ObjectProperty(<http://ex.com/bar#subObjProp1>))
    // --> Declaration(DataProperty(<http://ex.com/bar#dataProp1>))
    // --> Declaration(DataProperty(<http://ex.com/bar#dataProp2>))
    // --> Declaration(DataProperty(<http://ex.com/bar#sameAsDataProp1>))
    // --> Declaration(DataProperty(<http://ex.com/bar#subDataProp1>))
    // --> Declaration(Class(<http://ex.com/bar#AllIndividualsCls>))
    // --> Declaration(Class(<http://ex.com/bar#AllProp1Cls1>))
    // --> Declaration(Class(<http://ex.com/bar#Cl1OrNegate>))
    // --> Declaration(Class(<http://ex.com/bar#Cls1>))
    // --> Declaration(Class(<http://ex.com/bar#Cls2>))
    // --> Declaration(Class(<http://ex.com/bar#ComplementCls>))
    // --> Declaration(Class(<http://ex.com/bar#ComplementCls1>))
    // --> Declaration(Class(<http://ex.com/bar#DataAllIntGT10>))
    // --> Declaration(Class(<http://ex.com/bar#DataExact5Prop1>))
    // --> Declaration(Class(<http://ex.com/bar#DataHasVal5>))
    // --> Declaration(Class(<http://ex.com/bar#DataMax2Prop1>))
    // --> Declaration(Class(<http://ex.com/bar#DataMin3Prop1>))
    // --> Declaration(Class(<http://ex.com/bar#DataSomeIntLT20>))
    // --> Declaration(Class(<http://ex.com/bar#Exact5Prop1Cls1>))
    // --> Declaration(Class(<http://ex.com/bar#HasSelfProp1>))
    // --> Declaration(Class(<http://ex.com/bar#HasValProp1IndivB>))
    // --> Declaration(Class(<http://ex.com/bar#IntersectionCls>))
    // --> Declaration(Class(<http://ex.com/bar#Max3Prop1Cls1>))
    // --> Declaration(Class(<http://ex.com/bar#Min2Prop1Cls1>))
    // --> Declaration(Class(<http://ex.com/bar#SomeProp1Cls1>))
    // --> Declaration(Class(<http://ex.com/bar#UnionCls>))
    // --> Declaration(NamedIndividual(<http://ex.com/foo#sameAsIndivA>))
    // --> Declaration(NamedIndividual(<http://ex.com/foo#indivB>))
    val expectedNumberOfAxioms = 48
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLDeclarationAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("Disjoint union axioms should be created correctly") {
    // --> DisjointUnion(<http://ex.com/bar#Cl1OrNegate> <http://ex.com/bar#Cls1> <http://ex.com/bar#ComplementCls1> )
    val expectedNumberOfAxioms = 1
    val expectedClasses = Set(
      dataFactory.getOWLClass("http://ex.com/bar#Cl1OrNegate"),
      dataFactory.getOWLClass("http://ex.com/bar#Cls1"),
      dataFactory.getOWLClass("http://ex.com/bar#ComplementCls1")
    )

    val filteredRDD: RDD[OWLDisjointUnionAxiom] =
      rdd.filter(_.isInstanceOf[OWLDisjointUnionAxiom]).map(
        _.getAxiomWithoutAnnotations[OWLDisjointUnionAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)

    val disjUnionAxiom = filteredRDD.first()
    val ceIter = disjUnionAxiom.classExpressions().iterator()

    while (ceIter.hasNext) {
      val classExpression = ceIter.next().asOWLClass()
      assert(expectedClasses.contains(classExpression))
    }
  }

  ignore("Disjoint classes axioms should be created correctly") {
    // -->  XX) DisjointClasses(<http://ex.com/bar#DataMin3Prop1> <http://ex.com/bar#DataMax2Prop1>)
    // -->  XX) DisjointClasses(<http://ex.com/bar#DataMin3Prop1> <http://ex.com/bar#DataMax2Prop1>) (duplicate)
    val expectedNumberOfAxioms = 2
    val expectedClasses = Set(
      dataFactory.getOWLClass("http://ex.com/bar#DataMin3Prop1"),
      dataFactory.getOWLClass("http://ex.com/bar#DataMax2Prop1")
    )
    val filteredRDD: RDD[OWLDisjointClassesAxiom] =
      rdd.filter(_.isInstanceOf[OWLDisjointClassesAxiom]).map(
        _.getAxiomWithoutAnnotations[OWLDisjointClassesAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)

    filteredRDD.foreach(
      ax => {
        val ceIter = ax.classExpressions().iterator()

        while (ceIter.hasNext) {
          val classExpression = ceIter.next().asOWLClass()
          assert(expectedClasses.contains(classExpression))
        }
      }
    )
  }

  ignore("Equivalent classes axioms should be created correctly") {
    // -->  EquivalentClasses(<http://ex.com/bar#AllIndividualsCls> ObjectOneOf(<http://ex.com/foo#indivA> <http://ex.com/foo#indivB>))
    // -->  EquivalentClasses(<http://ex.com/bar#AllProp1Cls1> ObjectAllValuesFrom(<http://ex.com/bar#objProp1> <http://ex.com/bar#Cls1>))
    // -->  EquivalentClasses(<http://ex.com/bar#ComplementCls> ObjectComplementOf(<http://ex.com/bar#Cls1>) )
    // -->  EquivalentClasses(<http://ex.com/bar#DataAllIntGT10> DataAllValuesFrom(<http://ex.com/bar#dataProp2> DatatypeRestriction(xsd:integer xsd:minInclusive "10"^^xsd:integer)))
    // -->  EquivalentClasses(<http://ex.com/bar#Exact5Prop1Cls1> ObjectExactCardinality(5 <http://ex.com/bar#objProp1> <http://ex.com/bar#Cls1>))
    // -->  EquivalentClasses(<http://ex.com/bar#DataHasVal5> DataHasValue(<http://ex.com/bar#dataProp2> "5"^^xsd:integer))
    // -->  EquivalentClasses(<http://ex.com/bar#Min2Prop1Cls1> ObjectMinCardinality(2 <http://ex.com/bar#objProp1> <http://ex.com/bar#Cls1>))
    // -->  EquivalentClasses(<http://ex.com/bar#DataMin3Prop1> DataMinCardinality(3 <http://ex.com/bar#dataProp1>))
    // -->  EquivalentClasses(<http://ex.com/bar#DataSomeIntLT20> DataSomeValuesFrom(<http://ex.com/bar#dataProp2> DatatypeRestriction(xsd:integer xsd:maxExclusive "20"^^xsd:integer)))
    // -->  EquivalentClasses(<http://ex.com/bar#Exact5Prop1Cls1> ObjectExactCardinality(5 <http://ex.com/bar#objProp1> <http://ex.com/bar#Cls1>))
    // -->  EquivalentClasses(<http://ex.com/bar#HasSelfProp1> ObjectHasSelf(<http://ex.com/bar#objProp1>))
    // -->  EquivalentClasses(<http://ex.com/bar#HasValProp1IndivB> ObjectHasValue(<http://ex.com/bar#objProp1> <http://ex.com/foo#indivB>))
    // -->  EquivalentClasses(<http://ex.com/bar#IntersectionCls> ObjectIntersectionOf(<http://ex.com/bar#Cls1> <http://ex.com/bar#Cls2>) )
    // -->  EquivalentClasses(<http://ex.com/bar#Max3Prop1Cls1> ObjectMaxCardinality(3 <http://ex.com/bar#objProp1> <http://ex.com/bar#Cls1>))
    // -->  EquivalentClasses(<http://ex.com/bar#Min2Prop1Cls1> ObjectMinCardinality(2 <http://ex.com/bar#objProp1> <http://ex.com/bar#Cls1>))
    // -->  EquivalentClasses(<http://ex.com/bar#SomeProp1Cls1> ObjectSomeValuesFrom(<http://ex.com/bar#objProp1> <http://ex.com/bar#Cls1>))
    // -->  EquivalentClasses(<http://ex.com/bar#UnionCls> ObjectUnionOf(<http://ex.com/bar#Cls1> <http://ex.com/bar#Cls2>) )
    val df = dataFactory
    val expectedNumberOfAxioms = 17
    val expectedEquivClasses: List[(OWLClassExpression, OWLClassExpression)] = List(
      // EquivalentClasses(bar:AllIndividualsCls ObjectOneOf(foo:indivA foo:indivB))
      (df.getOWLClass("http://ex.com/bar#AllIndividualsCls").asInstanceOf[OWLClassExpression],
        df.getOWLObjectOneOf(List(df.getOWLNamedIndividual("http://ex.com/foo#indivA"),
          df.getOWLNamedIndividual("http://ex.com/foo#indivB")).asJava)),
      // EquivalentClasses(bar:AllProp1Cls1 ObjectAllValuesFrom(bar:objProp1 bar:Cls1))
      (df.getOWLClass("http://ex.com/bar#AllProp1Cls1"),
        df.getOWLObjectAllValuesFrom(df.getOWLObjectProperty("http://ex.com/bar#objProp1"),
          df.getOWLClass("http://ex.com/bar#Cls1"))),
      // EquivalentClasses(bar:ComplementCls ObjectComplementOf(bar:Cls1) )
      (df.getOWLClass("http://ex.com/bar#ComplementCls"),
        df.getOWLObjectComplementOf(df.getOWLClass("http://ex.com/bar#Cls1"))),
      // EquivalentClasses(bar:DataAllIntGT10 DataAllValuesFrom(bar:dataProp2 DatatypeRestriction(xsd:integer xsd:minInclusive "10"^^xsd:integer)))
      (df.getOWLClass("http://ex.com/bar#DataAllIntGT10"),
        df.getOWLDataAllValuesFrom(df.getOWLDataProperty("http://ex.com/bar#dataProp2"),
          df.getOWLDatatypeMinInclusiveRestriction(10))),
      // EquivalentClasses(bar:Exact5Prop1Cls1 ObjectExactCardinality(5 bar:objProp1 bar:Cls1))
      (df.getOWLClass("http://ex.com/bar#Exact5Prop1Cls1"),
        df.getOWLObjectExactCardinality(5, df.getOWLObjectProperty("http://ex.com/bar#objProp1"),
          df.getOWLClass("http://ex.com/bar#Cls1"))),
      // EquivalentClasses(bar:DataHasVal5 DataHasValue(bar:dataProp2 "5"^^xsd:integer))
      (df.getOWLClass("http://ex.com/bar#DataHasVal5"),
        df.getOWLDataHasValue(df.getOWLDataProperty("http://ex.com/bar#dataProp2"), df.getOWLLiteral(5))),
      // EquivalentClasses(bar:Min2Prop1Cls1 ObjectMinCardinality(2 bar:objProp1 bar:Cls1))
      (df.getOWLClass("http://ex.com/bar#Min2Prop1Cls1"),
        df.getOWLObjectMinCardinality(2, df.getOWLObjectProperty("http://ex.com/bar#objProp1"),
          df.getOWLClass("http://ex.com/bar#Cls1"))),
      // EquivalentClasses(bar:DataMin3Prop1 DataMinCardinality(3 bar:dataProp1))
      (df.getOWLClass("http://ex.com/bar#DataMin3Prop1"),
        df.getOWLDataMinCardinality(3, df.getOWLDataProperty("http://ex.com/bar#dataProp1"))),
      // EquivalentClasses(bar:DataSomeIntLT20 DataSomeValuesFrom(bar:dataProp2 DatatypeRestriction(xsd:integer xsd:maxExclusive "20"^^xsd:integer)))
      (df.getOWLClass("http://ex.com/bar#DataSomeIntLT20"),
        df.getOWLDataSomeValuesFrom(df.getOWLDataProperty("http://ex.com/bar#dataProp2"),
          df.getOWLDatatypeMaxExclusiveRestriction(20))),
      // EquivalentClasses(bar:Exact5Prop1Cls1 ObjectExactCardinality(5 bar:objProp1 bar:Cls1))
      (df.getOWLClass("http://ex.com/bar#Exact5Prop1Cls1"),
        df.getOWLObjectExactCardinality(5, df.getOWLObjectProperty("http://ex.com/bar#objProp1"),
          df.getOWLClass("http://ex.com/bar#Cls1"))),
      // EquivalentClasses(bar:HasSelfProp1 ObjectHasSelf(bar:objProp1))
      (df.getOWLClass("http://ex.com/bar#HasSelfProp1"),
        df.getOWLObjectHasSelf(df.getOWLObjectProperty("http://ex.com/bar#objProp1"))),
      // EquivalentClasses(bar:HasValProp1IndivB ObjectHasValue(bar:objProp1 foo:indivB))
      (df.getOWLClass("http://ex.com/bar#HasValProp1IndivB"),
        df.getOWLObjectHasValue(df.getOWLObjectProperty("http://ex.com/bar#objProp1"),
          df.getOWLNamedIndividual("http://ex.com/foo#indivB"))),
      // EquivalentClasses(bar:IntersectionCls ObjectIntersectionOf(bar:Cls1 bar:Cls2) )
      (df.getOWLClass("http://ex.com/bar#IntersectionCls"),
        df.getOWLObjectIntersectionOf(df.getOWLClass("http://ex.com/bar#Cls1"),
          df.getOWLClass("http://ex.com/bar#Cls2"))),
      // EquivalentClasses(bar:Max3Prop1Cls1 ObjectMaxCardinality(3 bar:objProp1 bar:Cls1))
      (df.getOWLClass("http://ex.com/bar#Max3Prop1Cls1"),
        df.getOWLObjectMaxCardinality(3, df.getOWLObjectProperty("http://ex.com/bar#objProp1"),
          df.getOWLClass("http://ex.com/bar#Cls1"))),
      // EquivalentClasses(bar:Min2Prop1Cls1 ObjectMinCardinality(2 bar:objProp1 bar:Cls1))
      (df.getOWLClass("http://ex.com/bar#Min2Prop1Cls1"),
        df.getOWLObjectMinCardinality(2, df.getOWLObjectProperty("http://ex.com/bar#objProp1"),
          df.getOWLClass("http://ex.com/bar#Cls1"))),
      // EquivalentClasses(bar:SomeProp1Cls1 ObjectSomeValuesFrom(bar:objProp1 bar:Cls1))
      (df.getOWLClass("http://ex.com/bar#SomeProp1Cls1"),
        df.getOWLObjectSomeValuesFrom(df.getOWLObjectProperty("http://ex.com/bar#objProp1"),
          df.getOWLClass("http://ex.com/bar#Cls1"))),
      // EquivalentClasses(bar:UnionCls ObjectUnionOf(bar:Cls1 bar:Cls2) )
      (df.getOWLClass("http://ex.com/bar#UnionCls"),
        df.getOWLObjectUnionOf(df.getOWLClass("http://ex.com/bar#Cls1"),
          df.getOWLClass("http://ex.com/bar#Cls2")))
    )

    val filteredRDD: RDD[OWLEquivalentClassesAxiom] =
      rdd.filter(_.isInstanceOf[OWLEquivalentClassesAxiom]).map(
        _.getAxiomWithoutAnnotations[OWLEquivalentClassesAxiom])

    assert(
      // get count of how often the equivalent classes of an axiom match a pair
      // in expectedEquivClasses (should be exactly 1 for each axiom)
      filteredRDD.map(
        axiom => {
          expectedEquivClasses.count(e => {
            axiom.contains(e._1) && axiom.contains(e._2)
          })
        }
      // check for how many axioms there was exactly 1 match in
      // expectedEquivClasses (should be for each of them, so #expectedNumberOfAxioms)
      ).filter(_ == 1).count() == expectedNumberOfAxioms)
  }

  ignore("Sub-class-of axioms should be created correctly") {
    // --> SubClassOf(<http://ex.com/bar#Cls1> <http://ex.com/bar#UnionCls>)
    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(_.isInstanceOf[OWLSubClassOfAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("Functional data property axioms should be created correctly") {
    // --> FunctionalDataProperty(bar:dataProp1)
    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(_.isInstanceOf[OWLFunctionalDataPropertyAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("Data property domain axioms should be created correctly") {
    // --> DataPropertyDomain(<http://ex.com/bar#dataProp1> <http://ex.com/bar#Cls1>)
    // --> DataPropertyDomain(<http://ex.com/bar#dataProp2> <http://ex.com/bar#Cls1>)
    val expectedNumberOfAxioms = 2
    val filteredRDD = rdd.filter(_.isInstanceOf[OWLDataPropertyDomainAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("Data property range axioms should be created correctly") {
    // --> DataPropertyRange(<http://ex.com/bar#dataProp1> xsd:string)
    // --> DataPropertyRange(<http://ex.com/bar#dataProp2> xsd:int)
    val expectedNumberOfAxioms = 2
    val expectedRanges = Set(
      (dataFactory.getOWLDataProperty("http://ex.com/bar#dataProp1"), XSDVocabulary.STRING.getIRI),
      (dataFactory.getOWLDataProperty("http://ex.com/bar#dataProp2"), XSDVocabulary.INT.getIRI)
    )

    val filteredRDD: RDD[OWLDataPropertyRangeAxiom] =
      rdd.filter(_.isInstanceOf[OWLDataPropertyRangeAxiom]).map(
        _.getAxiomWithoutAnnotations[OWLDataPropertyRangeAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)

    assert(
      filteredRDD.map(axiom => {
        expectedRanges.count(e => {
          e._1 == axiom.getProperty.asOWLDataProperty() &&
            e._2 == axiom.getRange.asInstanceOf[OWLDatatypeImpl].getIRI
        })
      }).filter(_ == 1).count() == expectedNumberOfAxioms)
  }

  test("Disjoint-data-properties axioms should be created correctly") {
    // --> DisjointDataProperties(<http://ex.com/bar#dataProp1> <http://ex.com/bar#dataProp2> )
    // --> DisjointDataProperties(<http://ex.com/bar#dataProp1> <http://ex.com/bar#dataProp2> ) (duplicate)
    val expectedNumberOfAxioms = 2
    val filteredRDD = rdd.filter(_.isInstanceOf[OWLDisjointDataPropertiesAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  ignore("Equivalent-data-properties axioms should be created correctly") {
    // -->     EquivalentDataProperties(<http://ex.com/bar#dataProp1> <http://ex.com/bar#sameAsDataProp1> )
    // --> XX) EquivalentDataProperties(<http://ex.com/bar#dataProp1> <http://ex.com/bar#sameAsDataProp1> ) (duplicate)
    val expectedNumberOfAxioms = 2
    val filteredRDD = rdd.filter(_.isInstanceOf[OWLEquivalentDataPropertiesAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("Sub-data-property-of axioms should be created correctly") {
    // --> SubDataPropertyOf(<http://ex.com/bar#subDataProp1> <http://ex.com/bar#dataProp1>)
    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(_.isInstanceOf[OWLSubDataPropertyOfAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("Datatype definition axioms should be created correctly") {
    val expectedNumberOfAxioms = 0
    val filteredRDD = rdd.filter(_.isInstanceOf[OWLDatatypeDefinitionAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  ignore("Has-key axioms should be created correctly") {
    // -->  XX) HasKey(<http://ex.com/bar#Cls1> () (<http://ex.com/bar#dataProp1>))
    val expectedNumberOfAxioms = 1
    val expectedKeyProp = dataFactory.getOWLDataProperty("http://ex.com/bar#dataProp1")

    val filteredRDD = rdd.filter(_.isInstanceOf[OWLHasKeyAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)

    val dataPropExprs =
      filteredRDD.first().getAxiomWithoutAnnotations[OWLHasKeyAxiom].dataPropertyExpressions()

    assert(dataPropExprs.count() == 1)
    assert(dataPropExprs.findFirst().get().asOWLDataProperty() == expectedKeyProp)
  }

  ignore("Class assertion axioms should be created correctly") {
    // -->  XX) ClassAssertion(bar:Cls1 foo:indivA)
    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(_.isInstanceOf[OWLClassAssertionAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  ignore("Different-individuals axioms should be created correctly") {
    // -->  XX) DifferentIndividuals(foo:indivA foo:indivB)
    // -->  XX) DifferentIndividuals(foo:indivA foo:indivB) (duplicate)
    val expectedNumberOfAxioms = 2
    val filteredRDD = rdd.filter(_.isInstanceOf[OWLDifferentIndividualsAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("Same-individual axioms should be created correctly") {
    // --> SameIndividual(<http://ex.com/foo#indivA> <http://ex.com/foo#sameAsIndivA> )
    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(_.isInstanceOf[OWLSameIndividualAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  ignore("Negative data property assertion axioms should be created correctly") {
    // -->  XX) NegativeDataPropertyAssertion(<http://ex.com/bar#dataProp2> <http://ex.com/foo#indivA> "23"^^xsd:integer)
    val expectedNumberOfAxioms = 1
    val expectedSubject = dataFactory.getOWLNamedIndividual("http://ex.com/foo#indivA")
    val expectedProperty = dataFactory.getOWLDataProperty("http://ex.com/bar#dataProp2")
    val expectedValue = dataFactory.getOWLLiteral(23)

    val filteredRDD =
      rdd.filter(_.isInstanceOf[OWLNegativeDataPropertyAssertionAxiom]).map(
        _.getAxiomWithoutAnnotations[OWLNegativeDataPropertyAssertionAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)

    val axiom = filteredRDD.first()
    assert(axiom.getSubject == expectedSubject)
    assert(axiom.getProperty == expectedProperty)
    assert(axiom.getObject == expectedValue)
  }

  ignore("Negative object property assertion axioms should be created correctly") {
    // -->  XX) NegativeObjectPropertyAssertion(<http://ex.com/bar#Prop2> <http://ex.com/foo#indivB> <http://ex.com/foo#indivA>)
    val expectedNumberOfAxioms = 1
    val expectedSubject = dataFactory.getOWLNamedIndividual("http://ex.com/foo#indivB")
    val expectedProperty = dataFactory.getOWLObjectProperty("http://ex.com/bar#Prop2")
    val expectedObject = dataFactory.getOWLNamedIndividual("http://ex.com/foo#indivA")

    val filteredRDD =
      rdd.filter(_.isInstanceOf[OWLNegativeObjectPropertyAssertionAxiom]).map(
        _.getAxiomWithoutAnnotations[OWLNegativeObjectPropertyAssertionAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)

    val axiom = filteredRDD.first()
    assert(axiom.getSubject == expectedSubject)
    assert(axiom.getProperty == expectedProperty)
    assert(axiom.getObject == expectedObject)
  }

  ignore("Object property assertion axioms should be created correctly") {
    // -->  XX) ObjectPropertyAssertion(<http://ex.com/bar#objProp1> <http://ex.com/foo#indivA> <http://ex.com/foo#indivB>)
    val expectedNumberOfAxioms = 1
    val expectedSubject = dataFactory.getOWLNamedIndividual("http://ex.com/foo#indivA")
    val expectedProperty = dataFactory.getOWLObjectProperty("http://ex.com/bar#objProp1")
    val expectedObject = dataFactory.getOWLNamedIndividual("http://ex.com/foo#indivB")

    val filteredRDD =
      rdd.filter(_.isInstanceOf[OWLObjectPropertyAssertionAxiom]).map(
        _.getAxiomWithoutAnnotations[OWLObjectPropertyAssertionAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)

    val axiom = filteredRDD.first()
    assert(axiom.getSubject == expectedSubject)
    assert(axiom.getProperty == expectedProperty)
    assert(axiom.getObject == expectedObject)
  }

  test("Disjoint object properties axioms should be created correctly") {
    // --> DisjointObjectProperties(<http://ex.com/bar#objProp1> <http://ex.com/bar#objProp2> )
    // --> DisjointObjectProperties(<http://ex.com/bar#objProp1> <http://ex.com/bar#objProp2> ) (duplicate)
    val expectedNumberOfAxioms = 2
    val filteredRDD = rdd.filter(_.isInstanceOf[OWLDisjointObjectPropertiesAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("Equivalent-object-properties axioms should be created correctly") {
    // --> EquivalentObjectProperties(<http://ex.com/bar#invObjProp1> InverseOf(<http://ex.com/bar#objProp1>) )
    // --> EquivalentObjectProperties(<http://ex.com/bar#objProp1> <http://ex.com/bar#sameAsObjProp1> )
    // --> EquivalentObjectProperties(<http://ex.com/bar#objProp1> <http://ex.com/bar#sameAsObjProp1> ) (duplicate)
    val expectedNumberOfAxioms = 3
    val filteredRDD = rdd.filter(_.isInstanceOf[OWLEquivalentObjectPropertiesAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("Inverse-object-properties axioms should be created correctly") {
    // --> InverseObjectProperties(<http://ex.com/bar#invObjProp1> <http://ex.com/bar#objProp1>)
    // --> InverseObjectProperties(<http://ex.com/bar#objProp1> <http://ex.com/bar#invObjProp1>)
    val expectedNumberOfAxioms = 2
    val filteredRDD = rdd.filter(_.isInstanceOf[OWLInverseObjectPropertiesAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("Asymmetric object property axioms should be created correctly") {
    // --> AsymmetricObjectProperty(<http://ex.com/bar#asymmObjProp>)
    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(_.isInstanceOf[OWLAsymmetricObjectPropertyAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("Functional object property axioms should be created correctly") {
    // --> FunctionalObjectProperty(<http://ex.com/bar#objProp2>)
    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(_.isInstanceOf[OWLFunctionalObjectPropertyAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("Inverse functional object property axioms should be created correctly") {
    // --> InverseFunctionalObjectProperty(<http://ex.com/bar#invObjProp1>)
    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(_.isInstanceOf[OWLInverseFunctionalObjectPropertyAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("Irreflexive object property axioms should be created correctly") {
    // --> IrreflexiveObjectProperty(<http://ex.com/bar#objProp2>)
    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(_.isInstanceOf[OWLIrreflexiveObjectPropertyAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("Reflexive object property axioms should be created correctly") {
    // --> ReflexiveObjectProperty(<http://ex.com/bar#objProp1>)
    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(_.isInstanceOf[OWLReflexiveObjectPropertyAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("Symmetric object property axioms should be created correctly") {
    // --> SymmetricObjectProperty(<http://ex.com/bar#objProp2>)
    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(_.isInstanceOf[OWLSymmetricObjectPropertyAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("Transitive object property axioms should be created correctly") {
    // --> TransitiveObjectProperty(<http://ex.com/bar#objProp1>)
    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(_.isInstanceOf[OWLTransitiveObjectPropertyAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("Object property domain axioms should be created correctly") {
    // --> ObjectPropertyDomain(<http://ex.com/bar#objProp1> <http://ex.com/bar#Cls1>)
    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(_.isInstanceOf[OWLObjectPropertyDomainAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("Object property range axioms should be created correctly") {
    // --> ObjectPropertyRange(<http://ex.com/bar#objProp1> <http://ex.com/bar#AllIndividualsCls>)
    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(_.isInstanceOf[OWLObjectPropertyRangeAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("Sub-object-property-of axioms should be created correctly") {
    // --> SubObjectPropertyOf(<http://ex.com/bar#subObjProp1> <http://ex.com/bar#objProp1>)
    val expectedNumberOfAxioms = 1
    val filteredRDD = rdd.filter(_.isInstanceOf[OWLSubObjectPropertyOfAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("Sub-property-chain-of axioms should be created correctly") {
    val expectedNumberOfAxioms = 0
    val filteredRDD = rdd.filter(_.isInstanceOf[OWLSubPropertyChainOfAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("SWRL rules should be created correctly") {
    val expectedNumberOfAxioms = 0
    val filteredRDD = rdd.filter(_.isInstanceOf[SWRLRule])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  ignore("Data property assertion axioms should be created correctly") {
    // -->  XX) DataPropertyAssertion(<http://ex.com/bar#dataProp1> <http://ex.com/foo#indivA> "ABCD")
    // -->  XX) DataPropertyAssertion(<http://ex.com/bar#dataProp1> <http://ex.com/foo#indivB> "BCDE")
    val expectedNumberOfAxioms = 2
    val filteredRDD = rdd.filter(_.isInstanceOf[OWLDataPropertyAssertionAxiom])

    assert(filteredRDD.count() == expectedNumberOfAxioms)
  }

  test("There should not be any null values") {
    assert(rdd.filter(a => a == null).count() == 0)
  }
}
