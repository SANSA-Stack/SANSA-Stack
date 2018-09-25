package net.sansa_stack.inference.spark.forwardchaining.axioms

import org.apache.spark.rdd.RDD
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.model._

import scala.reflect.ClassTag

/*
 *  A class for computing Transitive Closure for a set of given OWLAxioms with different formats.
 */

class TransitiveReasoner extends Serializable{

  val m = OWLManager.createOWLOntologyManager()
  val f : OWLDataFactory = m.getOWLDataFactory


  def computeTransitiveClosure(axioms: RDD[OWLAxiom], T: AxiomType[_]) : RDD[OWLAxiom] = {
    if (axioms.count() <= 1) return axioms

  val tcAxiom : RDD[OWLAxiom] = T match {
    case AxiomType.SUBCLASS_OF =>
        // we only need (s, o)
        val subjectObjectPairs = axioms.asInstanceOf[RDD[OWLSubClassOfAxiom]]
          .map{a => (a.getSubClass, a.getSuperClass)}
        val tc: RDD[(OWLClassExpression, OWLClassExpression)] = computeTransitiveClosure(subjectObjectPairs)
        tc.map(x => f.getOWLSubClassOfAxiom(x._1, x._2)).asInstanceOf[RDD[OWLAxiom]]

    case AxiomType.SUB_DATA_PROPERTY =>
        val subjectObjectPairs = axioms.asInstanceOf[RDD[OWLSubDataPropertyOfAxiom]]
          .map{a => (a.getSubProperty, a.getSuperProperty)}
        val tc = computeTransitiveClosure(subjectObjectPairs)
        tc.map(x => f.getOWLSubDataPropertyOfAxiom(x._1, x._2)).asInstanceOf[RDD[OWLAxiom]]

    case AxiomType.SUB_OBJECT_PROPERTY =>
        val subjectObjectPairs = axioms.asInstanceOf[RDD[OWLSubObjectPropertyOfAxiom]]
          .map { a => (a.getSubProperty, a.getSuperProperty) }
        val tc = computeTransitiveClosure(subjectObjectPairs)
        tc.map(x => f.getOWLSubObjectPropertyOfAxiom(x._1, x._2)).asInstanceOf[RDD[OWLAxiom]]

    case AxiomType.SUB_ANNOTATION_PROPERTY_OF =>
        val subjectObjectPairs = axioms.asInstanceOf[RDD[OWLSubAnnotationPropertyOfAxiom]]
          .map { a => (a.getSubProperty, a.getSuperProperty) }
        val tc = computeTransitiveClosure(subjectObjectPairs)
        tc.map(x => f.getOWLSubAnnotationPropertyOfAxiom(x._1, x._2)).asInstanceOf[RDD[OWLAxiom]]

    case _ =>
        println("No Transitive Closure")
        axioms
  }

   return tcAxiom
  }


//  def computeSubClassTransitiveClosure(axioms: RDD[OWLAxiom]): RDD[OWLAxiom] = {
//
//   if (axioms.count() <= 1) return axioms
//
//   // we only need (s, o)
//   val subjectObjectPairs = axioms.asInstanceOf[RDD[OWLSubClassOfAxiom]]
//      .map{a => (a.getSubClass, a.getSuperClass)}
//
//   val tc: RDD[(OWLClassExpression, OWLClassExpression)] = computeTransitiveClosure(subjectObjectPairs)
//
//   val tcAxiom : RDD[OWLAxiom] = tc.map(x => f.getOWLSubClassOfAxiom(x._1, x._2)).asInstanceOf[RDD[OWLAxiom]]
//   // val newAxioms = tcAxiom.asInstanceOf[RDD[OWLAxiom]].subtract(axioms)
//
// //   var newAxioms = axioms.union(unique)
//
//   return tcAxiom
//  }
//
//
//  def computeSubDataPropertyTransitiveClosure (axioms: RDD[OWLSubDataPropertyOfAxiom]): RDD[OWLSubDataPropertyOfAxiom] = {
//
//    if (axioms.count() <= 1) return axioms
//
//    // we only need (s, o)
//    val subjectObjectPairs = axioms.asInstanceOf[RDD[OWLSubDataPropertyOfAxiom]]
//      .map{a => (a.getSubProperty, a.getSuperProperty)}
//
//    val tc = computeTransitiveClosure(subjectObjectPairs)
//
//    val tcAxiom = tc.map(x => f.getOWLSubDataPropertyOfAxiom(x._1, x._2))
//
//    // val newAxioms = tcAxiom.subtract(axioms)
//
//    //   var newAxioms = axioms.union(unique)
//
//    return tcAxiom
//  }
//
//  def computeSubObjectPropertyTransitiveClosure (axioms: RDD[OWLAxiom]): RDD[OWLAxiom] = {
//
//    if (axioms.count() <= 1) return axioms
//
//    // we only need (s, o)
//    val subjectObjectPairs = axioms.asInstanceOf[RDD[OWLSubObjectPropertyOfAxiom]]
//      .map{a => (a.getSubProperty, a.getSuperProperty)}
//
//    val tc = computeTransitiveClosure(subjectObjectPairs)
//
//    val tcAxiom = tc.map(x => f.getOWLSubObjectPropertyOfAxiom(x._1, x._2)).asInstanceOf[RDD[OWLAxiom]]
//
//   // val newAxioms = tcAxiom.asInstanceOf[RDD[OWLAxiom]].subtract(axioms)
//
//    //   var newAxioms = axioms.union(unique)
//
//    return tcAxiom
//  }

  def computeTransitiveClosure[A: ClassTag](pairs: RDD[(A, A)]): RDD[(A, A)] = {
    var tc = pairs
    tc.cache()

    // get the pairs in reversed order because join() performs on keys
    val reversedPairs = tc.map(t => (t._2, t._1)).cache()

    // the join is iterated until a fixed point is reached
    var oldCount = 0L
    var nextCount = tc.count()
    do {
      oldCount = nextCount

      // perform the join on (a, b).(b, a) to get RDD of
      // (a=b, (b, a)) then map the output to get the new (a, b) paths
      tc = tc.union(tc.join(reversedPairs).map(a => (a._2._2, a._2._1)))
        .distinct().cache()

      nextCount = tc.count()
     } while (nextCount != oldCount)

    tc
  }

}
