package net.sansa_stack.owl.spark.stats

import org.apache.jena.vocabulary.XSD
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.semanticweb.owlapi.model._

import net.sansa_stack.owl.spark.extractOWLAxioms
import net.sansa_stack.owl.spark.rdd.FunctionalSyntaxOWLAxiomsRDDBuilder

/**
  * A Distributed implementation of OWL Statistics.
  *
  * @author Heba Mohamed
  */
class OWLStats(spark: SparkSession) extends Serializable {

  // val spark: SparkSession = SparkSession.builder().getOrCreate()

  def run(axioms: RDD[OWLAxiom]): RDD[String] = {

    val stats = UsedClassesCount(axioms, spark).Voidify()
      .union(UsedDataProperties(axioms, spark).Voidify())
      .union(UsedObjectProperties(axioms, spark).Voidify())
      .union(UsedAnnotationProperties(axioms, spark).Voidify())

    println("\n =========== OWL Statistics ===========\n")
    stats.collect().foreach(println(_))

    stats

  }

  def extractAxiom(axiom: RDD[OWLAxiom], T: AxiomType[_]): RDD[OWLAxiom] =
        axiom.filter(a => a.getAxiomType.equals(T))

  // New Criterion
  def ClassAssertionCount(axioms: RDD[OWLAxiom]): Long = extractOWLAxioms.ClassAssertion(axioms).count()

  def subClassCount (axioms: RDD[OWLAxiom]): Long = extractOWLAxioms.SubClasses(axioms).count()

  def subDataPropCount (axioms: RDD[OWLAxiom]): Long = extractOWLAxioms.SubDataProperty(axioms).count()

  def subObjectPropCount (axioms: RDD[OWLAxiom]): Long = extractOWLAxioms.SubObjectProperty(axioms).count()

  def subAnnPropCount (axioms: RDD[OWLAxiom]): Long = extractOWLAxioms.SubAnnotationProperty(axioms).count()

  def diffIndividualsCount (axioms: RDD[OWLAxiom]): Long = extractOWLAxioms.DifferentIndividuals(axioms).count()

  // Criterion 14.
  def Axioms (axioms: RDD[OWLAxiom]): Long = axioms.count()


  /**
    * Criterion 17. Literals
    *
    * @param axioms RDD of OWLAxioms
    * @return number of triples that are referencing literals to subjects.
    */
  def Literals(axioms: RDD[OWLAxiom]): Long = {

    val dataPropAssertion = extractOWLAxioms.DataPropertyAssertion(axioms)

    val negativeDataPropAssertion = extractOWLAxioms.NegativeDataPropertyAssertion(axioms)

    val l1 = dataPropAssertion.filter(_.getObject.isLiteral).distinct().count()
    val l2 = negativeDataPropAssertion.filter(_.getObject.isLiteral).distinct().count()

    l1 + l2
  }

  /**
    * Criterion 20. Datatypes
    *
    * @param axioms RDD of axioms
    * @return histogram of types used for literals.
    */
  def Datatypes(axioms: RDD[OWLAxiom]): RDD[(IRI, Int)] = {

    val dataPropAssertion = extractOWLAxioms.DataPropertyAssertion(axioms)

    val negativeDataPropAssertion = extractOWLAxioms.NegativeDataPropertyAssertion(axioms)

    val l1 = dataPropAssertion.filter(a => a.getObject.isLiteral && a.getObject.getDatatype.getIRI.length() != 0)
                              .map(a => (a.getObject.getDatatype.getIRI, 1))
                              .reduceByKey(_ + _)

    val l2 = negativeDataPropAssertion.filter(a => a.getObject.isLiteral && a.getObject.getDatatype.getIRI.length() != 0)
                                      .map(a => (a.getObject.getDatatype.getIRI, 1))
                                      .reduceByKey(_ + _)

    l1.union(l2)

  }

  /**
    * Criterion 21. Languages
    *
    * @param axioms RDD of OWLAxioms
    * @return histogram of languages used for literals.
    */
  def Languages(axioms: RDD[OWLAxiom]): RDD[(String, Int)] = {

    val dataPropAssertion = extractOWLAxioms.DataPropertyAssertion(axioms)

    val negativeDataPropAssertion = extractOWLAxioms.NegativeDataPropertyAssertion(axioms)

    val l1 = dataPropAssertion.filter(a => a.getObject.isLiteral && !a.getObject.getLang.isEmpty)
                              .map(a => (a.getObject.getLang, 1))
                              .reduceByKey(_ + _)

    val l2 = negativeDataPropAssertion.filter(a => a.getObject.isLiteral && !a.getObject.getLang.isEmpty)
                                      .map(a => (a.getObject.getLang, 1))
                                      .reduceByKey(_ + _)
    l1.union(l2)
  }

  /**
    * Criterion 22. Average typed string length criterion.
    *
    * @param axioms RDD of OWLAxioms
    * @return the average typed string length used throughout OWL ontology.
    */
  def AvgTypedStringLength(axioms: RDD[OWLAxiom]): Double = {

    val dataPropAssertion = extractOWLAxioms.DataPropertyAssertion(axioms)

    val negativeDataPropAssertion = extractOWLAxioms.NegativeDataPropertyAssertion(axioms)

    val t1 = dataPropAssertion
                .filter(a => a.getObject.isLiteral && a.getObject.getDatatype.getIRI.toString.equals(XSD.xstring.getURI))
                .map(_.getObject.getLiteral.length)

    val t2 = negativeDataPropAssertion
                .filter(a => a.getObject.isLiteral && a.getObject.getDatatype.getIRI.toString.equals(XSD.xstring.getURI))
                .map(_.getObject.getLiteral.length)

    t1.union(t2).mean()
  }

  /**
    * Criterion 23. Average untyped string length criterion.
    *
    * @param axioms RDD of OWLAxioms
    * @return the average untyped string length used throughout OWL ontology.
    */
  def AvgUntypedStringLength(axioms: RDD[OWLAxiom]): Double = {

    val dataPropAssertion = extractOWLAxioms.DataPropertyAssertion(axioms)

    val negativeDataPropAssertion = extractOWLAxioms.NegativeDataPropertyAssertion(axioms)

    val t1 = dataPropAssertion
                  .filter(a => a.getObject.isLiteral && !a.getObject.getLang.isEmpty)
                  .map(_.getObject.getLiteral.length)

    val t2 = negativeDataPropAssertion
                  .filter(a => a.getObject.isLiteral && !a.getObject.getLang.isEmpty)
                  .map(_.getObject.getLiteral.length)

    t1.union(t2).mean()

  }

  // Criterion 24. typed subject
  def TypedSubject(axioms: RDD[OWLAxiom]): RDD[String] = {

    val declarations = extractOWLAxioms.Declarations(axioms).map(a => a.getEntity.toString)
    val classAssertion = extractOWLAxioms.ClassAssertion(axioms).map(a => a.getIndividual.toString)
    val funcDataProp = extractOWLAxioms.FunctionalDataProperty(axioms).map(a => a.getProperty.toString)
    val funcObjProp = extractOWLAxioms.FunctionalObjectProperty(axioms).map(a => a.getProperty.toString)
    val inverseFuncObjProp = extractOWLAxioms.InverseFunctionalObjectProperty(axioms).map(a => a.getProperty.toString)
    val reflexiveFuncObjProp = extractOWLAxioms.ReflexiveFunctionalObjectProperty(axioms).map(a => a.getProperty.toString)
    val irreflexiveFuncObjProp = extractOWLAxioms.IrreflexiveObjectProperty(axioms).map(a => a.getProperty.toString)
    val symmetricObjProp = extractOWLAxioms.SymmetricObjectProperty(axioms).map(a => a.getProperty.toString)
    val aSymmetricObjProp = extractOWLAxioms.ASymmetricObjectProperty(axioms).map(a => a.getProperty.toString)
    val transitiveObjProp = extractOWLAxioms.TransitiveObjectProperty(axioms).map(a => a.getProperty.toString)

    val typedSubject = spark.sparkContext
                         .union(declarations, classAssertion, funcDataProp, funcObjProp,
                                inverseFuncObjProp, reflexiveFuncObjProp, irreflexiveFuncObjProp,
                                symmetricObjProp, aSymmetricObjProp, transitiveObjProp)
                    //     .distinct()

    typedSubject
  }


  /**
    * Criterion 25. Labeled subjects criterion.
    *
    * @param axioms RDD of triples
    * @return list of labeled subjects.
    */
  def LabeledSubjects(axioms: RDD[OWLAxiom]): RDD[OWLAnnotationSubject] = {

    val annPropAssertion = extractOWLAxioms.AnnotationPropertyAssertion(axioms)

    annPropAssertion
      .filter(a => a.getProperty.isLabel)
      .map(_.getSubject)

  }

  // Criterion 26. SameAs axioms
  def SameAs(axioms: RDD[OWLAxiom]): Long = extractOWLAxioms.SameIndividuals(axioms).count()

}

// Criterion 1.
class UsedClasses (axioms: RDD[OWLAxiom], spark: SparkSession) {

  // ?p=rdf:type && isIRI(?o)
  // ClassAssertions (C, indv)
  def Filter(): RDD[OWLClassAssertionAxiom] = extractOWLAxioms.ClassAssertion(axioms)

  // M[?o]++
  def Action(): RDD[OWLClassExpression] = Filter().map(_.getClassExpression).distinct()

  // top(M,100)
  def PostProc(): Array[OWLClassExpression] = Action().take(100)

  def Voidify(): RDD[String] = {
    val cd = new Array[String](1)
    cd(0) = "\nvoid:classes  " + PostProc() + ";"
    spark.sparkContext.parallelize(cd)
  }


}
object UsedClasses {

  def apply(axioms: RDD[OWLAxiom], spark: SparkSession): UsedClasses = new UsedClasses(axioms, spark)

}

// Criterion 2.
class UsedClassesCount (axioms: RDD[OWLAxiom], spark: SparkSession) {

  // ?p=rdf:type && isIRI(?o)
  // ClassAssertions (C, indv)
  def Filter(): RDD[OWLClassExpression] = {

    val usedClasses = extractOWLAxioms.ClassAssertion(axioms)
                        .map(_.getClassExpression)
                     //   .filter(a => a.getClassExpression.isIRI)

    usedClasses
  }

  // M[?o]++
  def Action(): RDD[(OWLClassExpression, Int)] = Filter().map(e => (e, 1)).reduceByKey(_ + _)

  // top(M,100)
  def PostProc(): Array[(OWLClassExpression, Int)] = Action().sortBy(_._2, false).take(100)

  def Voidify(): RDD[String] = {

    var axiomsString = new Array[String](1)
    axiomsString(0) = "\nvoid:classPartition "

    val classes = spark.sparkContext.parallelize(PostProc())
    val vc = classes.map(t => "[ void:class " + "<" + t._1 + ">;   void:axioms " + t._2 + "; ], ")

    var c_action = new Array[String](1)
    c_action(0) = "\nvoid:classes " + Action().map(f => f._1).distinct().count + ";"
    val c_p = spark.sparkContext.parallelize(axiomsString)
    val c = spark.sparkContext.parallelize(c_action)
    if (classes.count() > 0) {
      c.union(c_p).union(vc)
    } else c.union(vc)
  }
}

object UsedClassesCount {

  def apply(axioms: RDD[OWLAxiom], spark: SparkSession): UsedClassesCount = new UsedClassesCount(axioms, spark)

}

// Criterion 3.
class DefinedClasses (axioms: RDD[OWLAxiom], spark: SparkSession) {

  // ?p=rdf:type && isIRI(?s) &&(?o=rdfs:Class||?o=owl:Class)
  // isIRI(C) && Declaration(Class(C))
  def Filter(): RDD[OWLDeclarationAxiom] = {

    val declaration = extractOWLAxioms.Declarations(axioms)

    val classesDeclarations = declaration.filter(a => a.getEntity.isOWLClass)

    classesDeclarations
  }

  def Action(): RDD[IRI] = Filter().map(_.getEntity.getIRI)

  def PostProc(): Long = Action().count()

  def Voidify(): RDD[String] = {
    var cd = new Array[String](1)
    cd(0) = "\nvoid:classes  " + PostProc() + ";"
    spark.sparkContext.parallelize(cd)
  }
}

object DefinedClasses {

  def apply(axioms: RDD[OWLAxiom], spark: SparkSession): DefinedClasses = new DefinedClasses(axioms, spark)
}

// Criterion 5(1).
class UsedDataProperties (axioms: RDD[OWLAxiom], spark: SparkSession) {

  def Filter(): RDD[OWLDataPropertyExpression] = {

    val dataPropertyAssertion = extractOWLAxioms.DataPropertyAssertion(axioms)

    val usedDataProperties = dataPropertyAssertion.map(_.getProperty).distinct()

    usedDataProperties
  }

  // M[?p]++
  def Action(): RDD[(OWLDataPropertyExpression, Int)] = Filter().map(e => (e, 1)).reduceByKey(_ + _)

  // top(M,100)
  def PostProc(): Array[(OWLDataPropertyExpression, Int)] = Action().sortBy(_._2, false).take(100)

  def Voidify(): RDD[String] = {

    var axiomsString = new Array[String](1)
    axiomsString(0) = "\nvoid:propertyPartition "

    val dataProperties = spark.sparkContext.parallelize(PostProc())
    val vdp = dataProperties.map(t => "[ void:dataProperty " + "<" + t._1 + ">;   void:axioms " + t._2 + "; ], ")

    var dp_action = new Array[String](1)
    dp_action(0) = "\nvoid:dataProperties " + Action().map(f => f._1).distinct().count + ";"
    val c_p = spark.sparkContext.parallelize(axiomsString)
    val c = spark.sparkContext.parallelize(dp_action)
    c.union(c_p).union(vdp)
  }
}
object UsedDataProperties {

  def apply(axioms: RDD[OWLAxiom], spark: SparkSession): UsedDataProperties = new UsedDataProperties(axioms, spark)

}

// Criterion 5(b).
class UsedObjectProperties (axioms: RDD[OWLAxiom], spark: SparkSession) {

  def Filter(): RDD[OWLObjectPropertyExpression] = {

    val objPropertyAssertion = extractOWLAxioms.ObjectPropertyAssertion(axioms)

    val usedObjectProperties = objPropertyAssertion.map(_.getProperty)

    usedObjectProperties
  }

  // M[?p]++
  def Action(): RDD[(OWLObjectPropertyExpression, Int)] = Filter().map(e => (e, 1)).reduceByKey(_ + _)

  // top(M,100)
  def PostProc(): Array[(OWLObjectPropertyExpression, Int)] = Action().sortBy(_._2, false).take(100)

  def Voidify(): RDD[String] = {

    val axiomsString = new Array[String](1)
    axiomsString(0) = "\nvoid:propertyPartition "

    val objProperties = spark.sparkContext.parallelize(PostProc())
    val vop = objProperties.map(t => "[ void:objectProperty " + t._1 + ";   void:axioms " + t._2 + "; ], ")

    val op_action = new Array[String](1)
    op_action(0) = "\nvoid:objectProperties " + Action().map(f => f._1).distinct().count + ";"
    val c_p = spark.sparkContext.parallelize(axiomsString)
    val c = spark.sparkContext.parallelize(op_action)
    c.union(c_p).union(vop)
  }
}
object UsedObjectProperties {

  def apply(axioms: RDD[OWLAxiom], spark: SparkSession): UsedObjectProperties = new UsedObjectProperties(axioms, spark)

}

// Criterion 5(c)
class UsedAnnotationProperties (axioms: RDD[OWLAxiom], spark: SparkSession) {

  def Filter(): RDD[OWLAnnotationProperty] = {

    val annAssertion = extractOWLAxioms.AnnotationPropertyAssertion(axioms)

    val usedAnnProperties = annAssertion.map(_.getProperty)

    usedAnnProperties
  }

  // M[?p]++
  def Action(): RDD[(OWLAnnotationProperty, Int)] = Filter().map(e => (e, 1)).reduceByKey(_ + _)

  // top(M,100)
  def PostProc(): Array[(OWLAnnotationProperty, Int)] = Action().sortBy(_._2, false).take(100)

  def Voidify(): RDD[String] = {

    val axiomsString = new Array[String](1)
    axiomsString(0) = "\nvoid:propertyPartition "

    val annProperties = spark.sparkContext.parallelize(PostProc())
    val vap = annProperties.map(t => "[ void:annotationProperty " +  t._1 + ";   void:axioms " + t._2 + "; ], ")

    val ap_action = new Array[String](1)
    ap_action(0) = "\nvoid:annotationProperty " + Action().map(f => f._1).distinct().count + ";"
    val c_p = spark.sparkContext.parallelize(axiomsString)
    val c = spark.sparkContext.parallelize(ap_action)
    c.union(c_p).union(vap)
  }
}
object UsedAnnotationProperties {

  def apply(axioms: RDD[OWLAxiom], spark: SparkSession): UsedAnnotationProperties =
    new UsedAnnotationProperties(axioms, spark)

}

object OWLStats {

  def main(args: Array[String]): Unit = {

    println("================================")
    println("|  Distributed OWL Statistics  |")
    println("================================")

/**
      * Create a SparkSession, do so by first creating a SparkConf object to configure the application .
      * 'Local' is a special value that runs Spark on one thread on the local machine, without connecting to a cluster.
      * An application name used to identify the application on the cluster manager’s UI.
      */

    @transient val spark = SparkSession.builder
      .master("local[4]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("Distributed OWL Statistics")
      .getOrCreate()

    val input: String = getClass.getResource("/ont_functional.owl").getPath

   // Call the functional syntax OWLAxiom builder
    val axioms = FunctionalSyntaxOWLAxiomsRDDBuilder.build(spark, input).distinct()
    val stats = new OWLStats(spark).run(axioms)

 //    val sparkConf = new SparkConf().setMaster("spark://172.18.160.17:3077")


//    val literals = new OWLStats().Literals(axioms)
//    println("\nLiterals = " + literals)

    println("\n\n")
    val typedSubject = new OWLStats(spark).TypedSubject(axioms)
    typedSubject.collect().foreach(println(_))

    //    val x = new OWLStats().ClassAssertionCount(axioms)
//    println ("x = " + x)

//        val datatype = new OWLStats().Datatypes(axioms)
//        datatype.collect().foreach(println(_))

    //    val language = new OWLStats().Languages(axioms)
    //    language.collect().foreach(println(_))

    //    val labeledSubject = new OWLStats().LabeledSubjects(axioms)
    //    labeledSubject.collect().foreach(println(_))

//        val typedString = new OWLStats().AvgTypedStringLength(axioms)
//        println("\ntyped = " + typedString)
    //
    //    val untypedString = new OWLStats().AvgUntypedStringLength(axioms)
    //    println("untyped = " + untypedString)

    //    val uClasses = new UsedClasses(owlAxiomsRDD, sparkSession).Voidify()

    //    println("\n ----- Used Classes ---- \n")
    //    uClasses.collect().foreach(println)

    //    val dClasses = new DefinedClasses(owlAxiomsRDD, sparkSession).Voidify()
    //
    //    println("\n ----- Defined Classes ---- \n")
    //    dClasses.collect().foreach(println)
    //
    //    val uDataProperties = new UsedDataProperties(owlAxiomsRDD, sparkSession).Voidify()
    //
    //    println("\n ----- Used Data Properties ---- \n")
    //    uDataProperties.collect().foreach(println)
    //
    //    val uObjProperties = new UsedObjectProperties(owlAxiomsRDD, sparkSession).Voidify()
    //
    //    println("\n ----- Used Object Properties ---- \n")
    //    uObjProperties.collect().foreach(println)
    //
    //    val uAnnProperties = new UsedAnnotationProperties(owlAxiomsRDD, sparkSession).Voidify()
    //
    //    println("\n ----- Used Annotation Properties ---- \n")
    //    uAnnProperties.collect().foreach(println)

    spark.stop
  }
}
