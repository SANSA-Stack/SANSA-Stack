package net.sansa_stack.inference.spark.forwardchaining.axioms

// import org.apache.spark.SparkContext
import scala.collection.JavaConverters._
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

  def computeSubClassTransitiveClosure(axioms: RDD[OWLAxiom]): RDD[OWLAxiom] = {

   if (axioms.count() == 0) return axioms

   // we only need (s, o)
   val subjectObjectPairs = axioms.asInstanceOf[RDD[OWLSubClassOfAxiom]]
      .map{a => (a.getSubClass, a.getSuperClass)}

   val tc: RDD[(OWLClassExpression, OWLClassExpression)] = computeTransitiveClosure(subjectObjectPairs)

   val tcAxiom : RDD[OWLAxiom] = tc.map(x => f.getOWLSubClassOfAxiom(x._1, x._2)).asInstanceOf[RDD[OWLAxiom]]
   val newAxioms = tcAxiom.asInstanceOf[RDD[OWLAxiom]].subtract(axioms)

//   var newAxioms = axioms.union(unique)

   return newAxioms
  }


  def computeSubDataPropertyTransitiveClosure (axioms: RDD[OWLAxiom]): RDD[OWLAxiom] = {

    if (axioms.count() <= 1) return axioms

    // we only need (s, o)
    val subjectObjectPairs = axioms.asInstanceOf[RDD[OWLSubDataPropertyOfAxiom]]
      .map{a => (a.getSubProperty, a.getSuperProperty)}

    val tc = computeTransitiveClosure(subjectObjectPairs)

    val tcAxiom : RDD[OWLAxiom] = tc.map(x => f.getOWLSubDataPropertyOfAxiom(x._1, x._2)).asInstanceOf[RDD[OWLAxiom]]

    val newAxioms = tcAxiom.asInstanceOf[RDD[OWLAxiom]].subtract(axioms)

    //   var newAxioms = axioms.union(unique)

    return newAxioms
  }

  def computeSubObjectPropertyTransitiveClosure (axioms: RDD[OWLAxiom]): RDD[OWLAxiom] = {

    if (axioms.count() <= 1) return axioms

    // we only need (s, o)
    val subjectObjectPairs = axioms.asInstanceOf[RDD[OWLSubObjectPropertyOfAxiom]]
      .map{a => (a.getSubProperty, a.getSuperProperty)}

    val tc = computeTransitiveClosure(subjectObjectPairs)

    val tcAxiom : RDD[OWLAxiom] = tc.map(x => f.getOWLSubObjectPropertyOfAxiom(x._1, x._2)).asInstanceOf[RDD[OWLAxiom]]

    val newAxioms = tcAxiom.asInstanceOf[RDD[OWLAxiom]].subtract(axioms)

    //   var newAxioms = axioms.union(unique)

    return newAxioms
  }

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

//    println("\ntc \n -------- \n")
//    tc.take(10).foreach(println(_))

    tc
  }

}
