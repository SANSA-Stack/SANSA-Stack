package net.sansa_stack.inference.spark.forwardchaining.axioms

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.HashPartitioner
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.model._

import scala.reflect.ClassTag

/*
 *  A class for computing Transitive Closure for a set of given OWLAxioms with different formats.
 */

class TransitiveReasoner extends Serializable {

  val m = OWLManager.createOWLOntologyManager()
  val f: OWLDataFactory = m.getOWLDataFactory
  val parallism = 30
  val partitioner = new HashPartitioner(parallism)

  def computeTransitiveClosure(axioms: RDD[OWLAxiom], T: AxiomType[_]): RDD[OWLAxiom] = {

    axioms.cache()

    if (axioms.partitions.isEmpty) return axioms

    val tcAxiom: RDD[OWLAxiom] = T match {
      case AxiomType.SUBCLASS_OF =>
        // we only need (s, o)
        val subjectObjectPairs = axioms.asInstanceOf[RDD[OWLSubClassOfAxiom]]
          .map(a => (a.getSubClass, a.getSuperClass))
        val tc = computeTransitiveClosure(subjectObjectPairs)
        tc.map(x => f.getOWLSubClassOfAxiom(x._1, x._2)).asInstanceOf[RDD[OWLAxiom]]

      case AxiomType.SUB_DATA_PROPERTY =>
        val subjectObjectPairs = axioms.asInstanceOf[RDD[OWLSubDataPropertyOfAxiom]]
          .map { a => (a.getSubProperty, a.getSuperProperty) }
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


  def computeTransitiveClosure[A: ClassTag](pairs: RDD[(A, A)]): RDD[(A, A)] = {
    var nextCount = 1L
    var tc = pairs
    tc.cache()

    var q = tc
    var l = q.map(t => t.swap).partitionBy(partitioner) // (t._2, t._1)

    do{
      val r = q.partitionBy(partitioner) // .map(t => (t._1, t._2))

      // perform the join on (a, b).(b, a) to get RDD of
      // (a=b, (b, a)) then map the output to get the new (a, b) paths
      val q1 = l.join(r).map(t => (t._2._1, t._2._2))
      q = q1.subtract(tc, parallism).cache() // q contains only the new inferred axioms
      nextCount = q.count()
      if(nextCount != 0) {
        l = q.map(t => (t._2, t._1)).partitionBy(partitioner)
        val s = tc.map(t => (t._1, t._2)).partitionBy(partitioner)
        val tc1 = s.join(l).map(t => (t._2._2, t._2._1)).cache()
        tc = tc1.union(tc).union(q)
      }
    } while (nextCount != 0)
    tc
  }

//  def computeTransitiveClosure[A: ClassTag](pairs: RDD[(A, A)]): RDD[(A, A)] = {
//    var tc = pairs
//    tc.cache()
//
//    // get the pairs in reversed order because join() performs on keys
//    val reversedPairs = tc.map(t => t.swap).partitionBy(partitioner)
//   reversedPairs.cache()
//
//    // the join is iterated until a fixed point is reached
//    var oldCount = 0L
//    var nextCount = tc.count()
//    do {
//      oldCount = nextCount
//
//      // perform the join on (a, b).(b, a) to get RDD of
//      // (a=b, (b, a)) then map the output to get the new (a, b) paths
// //      var startTime = System.currentTimeMillis()
// //      tc = tc.join(reversedPairs).map(a => (a._2._2, a._2._1))
// //      var endTime = System.currentTimeMillis() - startTime
// //      println("\n...second " + (endTime) + " millisec.")
//      // tc = tc.union(tc).distinct().cache()
//      val tc1 = tc.join(reversedPairs).map(a => (a._2._2, a._2._1))
//      tc = tc.union(tc1).cache()
//      nextCount = tc.count()
//    } while (nextCount != oldCount)
//
//    tc.distinct()
//  }

  def computeTransitiveClosure(asserstion: RDD[OWLObjectPropertyAssertionAxiom]): RDD[OWLObjectPropertyAssertionAxiom] = {

    if (asserstion.isEmpty()) return asserstion

//    val soPairs = asserstion.map{ a => (a.getSubject, a.getObject)}
//    val osPairs = asserstion.map{ a => (a.getObject, a.getSubject)}
//
//    val O4_ExtVP_OS = osPairs.join(soPairs).map(a => (a._1, a._2._1))
//    val O4_ExtVP_SO = soPairs.join(osPairs).map(a => (a._1, a._2._1))
//    val O4_ExtVP = O4_ExtVP_OS.join(O4_ExtVP_SO).map(a => (a._2._1, a._2._2)).distinct()
//
//    var startTime = System.currentTimeMillis()
//    var tc = computeTransitiveClosure(O4_ExtVP_OS)
//    tc = tc.union(computeTransitiveClosure(O4_ExtVP_SO))
//    var endTime = System.currentTimeMillis() - startTime
//    println("\n1 - ...O4 object assertion  " + (endTime) + " millisec.")

    val subjectObjectPairs = asserstion.map{ a => (a.getSubject, a.getObject)}
   //  startTime = System.currentTimeMillis()
    val tc = computeTransitiveClosure(subjectObjectPairs)
  //   endTime = System.currentTimeMillis() - startTime
   //  println("\n2 - ... O4 object assertion  " + (endTime) + " millisec.")
    val prop: OWLObjectPropertyExpression = asserstion.first().getProperty

    tc.map(x => f.getOWLObjectPropertyAssertionAxiom(prop, x._1, x._2))
      .filter(a => a.getSubject != a.getObject)   // to exclude assertions with (A P A)
    }
}
