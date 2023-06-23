package net.sansa_stack.ml.spark.classification.decisionTrees

import com.google.common.collect.Lists

import java.util
import java.util.{ArrayList, HashSet, List, Set, TreeSet}
import net.sansa_stack.ml.spark.classification.decisionTrees.OWLReasoner.{EntitySearcher, StructuralReasoner}
import net.sansa_stack.ml.spark.classification.decisionTrees.OWLReasoner.StructuralReasoner.StructuralReasoner
import org.apache.spark.rdd.RDD
// import org.dllearner.core.owl.{OWLObjectIntersectionOfImplExt, OWLObjectUnionOfImplExt}
import org.semanticweb.owlapi.model._
// import org.semanticweb.owlapi.search.EntitySearcher

import scala.collection.JavaConverters._
import scala.collection.immutable._
import scala.util.Random

// object RefinementOperator {
//  val d: Double = 0.5
// }

/**
 * Experimental class
 */

class RefinementOperator(var kb: KB) extends Serializable {

  val Concepts: RDD[OWLClass] = kb.getClasses
  val ObjectProperties: RDD[OWLObjectProperty] = kb.getObjectProperties
  val dataProperties: RDD[OWLDataProperty] = kb.getDataProperties
  val df: OWLDataFactory = kb.getDataFactory
  val searcher = new EntitySearcher(kb)
  val reasoner: StructuralReasoner = kb.getReasoner
  
  
  def topSet (): util.Set[OWLClassExpression] = {
    
    val top: util.Set[OWLClassExpression] = new util.HashSet[OWLClassExpression]
//    val operands = Lists.newArrayList[OWLClassExpression](df.getOWLThing, df.getOWLThing)
//    val md = df.getOWLObjectUnionOf(operands)
//    top.add(md)
 
    // most general concepts
    val generalConcepts = searcher.getSubClasses(df.getOWLThing).collect().toSet.asJava
    generalConcepts.forEach((c: OWLClassExpression) => top.add(c))
  
    // negated most special concepts
    val specialConcepts = searcher.getSuperClasses(df.getOWLNothing).collect().toSet.asJava
    specialConcepts.forEach((c: OWLClassExpression) => top.add(df.getOWLObjectComplementOf(c)))

    
//    val concepts = Concepts.collect().toSet.asJava
//    concepts.forEach((c: OWLClassExpression) => top.add(c))
  
    // EXISTS r.TOP and ALL r.TOP for all r
    val objProperties = ObjectProperties.collect().toSet.asJava
    objProperties.forEach((r: OWLObjectProperty) => {
      top.add(df.getOWLObjectAllValuesFrom(r, df.getOWLThing))
      top.add(df.getOWLObjectSomeValuesFrom(r, df.getOWLThing))
    })

    top
  }
  
  
  
  /**
   * Generate a set of concepts subsuming the given one
   * @param currentConcept The current concept
   * @return The subsumed concept of the given one
   */
  def getSubsumedConcept(currentConcept: OWLClassExpression): util.Set[OWLClassExpression] = {
  
    val refinements: util.Set[OWLClassExpression] = new util.HashSet[OWLClassExpression]
    var tmp: util.Set[OWLClassExpression] = null
    //    val it = tmp.iterator()
    //    val searcher = new EntitySearcher(kb)
   
      if (currentConcept.isOWLThing) {
        return topSet()
      } else if (currentConcept.isOWLNothing) {
        return new util.HashSet[OWLClassExpression]()
      } else if (!currentConcept.isAnonymous) {
        refinements.addAll(searcher.getSubClasses(currentConcept).collect().toSet.asJava)
  
      } else currentConcept match {
        case of: OWLObjectComplementOf => // negated atomic concept
  
          val operand = of.getOperand
  
          if (!operand.isAnonymous) {
            tmp = searcher.getSuperClasses(operand).collect().toSet.asJava
    
            tmp.forEach((concept: OWLClassExpression) =>
              if (!concept.isOWLThing) {
                refinements.add(df.getOWLObjectComplementOf(concept))
              })
    
            //          while (it.hasNext) {
            //            if (it.next().isOWLThing) {
            //              refinements.add(df.getOWLObjectComplementOf(it.next()))
            //            }
            //          }
          }

        case of: OWLObjectIntersectionOf =>
          val operands = of.getOperandsAsList
  
          operands.forEach((child: OWLClassExpression) => {
            println("\ninside intersection")
            // Perform refinement for the child
            tmp = getSubsumedConcept(child)
    
            // construct a new MultiConjunction
            tmp.forEach((concept: OWLClassExpression) => {
              val newChildren = new util.LinkedList[OWLClassExpression](operands)
              // Exactly the previous order must be preserved
              // (at least until normal form is defined)
              val index = newChildren.indexOf(child)
              newChildren.add(index, concept)
              newChildren.remove(child)
  
              val in = df.getOWLObjectIntersectionOf(newChildren)
              refinements.add(in)
  
              //            val in = new util.ArrayList[OWLClassExpression](newChildren)
              //            in.forEach((c: OWLClassExpression) => refinements.add(c))
            })
          })

        //        for(child <- operands.asScala)
        //        {
        //          // Perform refinement for the child
        //          tmp = getSubsumedConcept(child)
        //
        //          // construct a new MultiConjunction
        //          for (concept <- tmp.asScala)
        //          {
        //            val newChildren = new util.LinkedList[OWLClassExpression](operands)
        //            // Exactly the previous order must be preserved
        //            // (at least until normal form is defined)
        //            val index = newChildren.indexOf(child)
        //            newChildren.add(index, concept)
        //            newChildren.remove(child)
        //
        //            val in = new util.ArrayList[OWLClassExpression](newChildren)
        //            for (c <- in.asScala) {
        //              refinements.add(c)
        //            }
        //
        //          }
        //        }


        //        val op = operands.iterator()

        //        while(op.hasNext)
        //        {
        //          val child = op.next()
        //          // Perform refinement for the child
        //          tmp = getSubsumedConcept(child)
        //          val it = tmp.iterator()
        //
        //          // construct a new MultiConjunction
        //          while(it.hasNext)
        //          {
        //            val itc = it.next()
        //            val newChildren = new util.LinkedList[OWLClassExpression](operands)
        //            // Exactly the previous order must be preserved
        //            // (at least until normal form is defined)
        //            val index = newChildren.indexOf(child)
        //            newChildren.add(index, itc)
        //            newChildren.remove(child)
        //            val in = new OWLObjectIntersectionOfImplExt(newChildren)
        //            refinements.add(in)
        //            }
        //        }


        case of: OWLObjectUnionOf =>
          val operands = of.getOperandsAsList

          operands.forEach((child: OWLClassExpression) => {
    
            println("inside union")
            // Perform refinement for the child
            tmp = getSubsumedConcept(child)
            tmp.forEach((concept: OWLClassExpression) => {
              val newChildren = new util.LinkedList[OWLClassExpression](operands)
              // Exactly the previous order must be preserved
              // (at least until normal form is defined)
              val index = newChildren.indexOf(child)
              newChildren.add(index, concept)
              newChildren.remove(child)
              val un = df.getOWLObjectUnionOf(newChildren)
              refinements.add(un)
              //            val un = new util.ArrayList[OWLClassExpression](newChildren)
              //            un.forEach((c: OWLClassExpression) => refinements.add(c))
            })
          })
          //        for(child <- operands.asScala)
          //        {
          //          // Perform refinement for the child
          //          tmp = getSubsumedConcept(child)
          //
          //          // construct a new MultiConjunction
          //          for (concept <- tmp.asScala)
          //          {
          //            val newChildren = new util.LinkedList[OWLClassExpression](operands)
          //            // Exactly the previous order must be preserved
          //            // (at least until normal form is defined)
          //            val index = newChildren.indexOf(child)
          //            newChildren.add(index, concept)
          //            newChildren.remove(child)
          //            val un = new util.ArrayList[OWLClassExpression](newChildren)
          //            for (c <- un.asScala) {
          //              refinements.add(c)
          //            }
          //          }
          //        }
  
          // an element of the disjunction can be omitted
          operands.forEach((child: OWLClassExpression) => {
            println("inside union2")
            val newChildren = new util.LinkedList[OWLClassExpression](operands)
            newChildren.remove(child)
            // if there is only one child, then disjunction is omitted immediately
            if (newChildren.size == 0) refinements
            else if (newChildren.size == 1) refinements.add(newChildren.get(0))
            else {
              val un = df.getOWLObjectUnionOf(newChildren)
              refinements.add(un)
              //            val un = new util.ArrayList[OWLClassExpression](newChildren)
              //            un.forEach((c: OWLClassExpression) => refinements.add(c))
            }
          })




        //         for(child <- operands.asScala) {
        //          val newChildren = new util.LinkedList[OWLClassExpression](operands)
        //          newChildren.remove(child)
        //          // if there is only one child, then disjunction is omitted immediately
        //          if (newChildren.size == 1) refinements.add(newChildren.get(0))
        //          else {
        //            val un = new util.ArrayList[OWLClassExpression](newChildren)
        //            for (c <- un.asScala) {
        //              refinements.add(c)
        //            }
        //          }
        //        }


        //        val op = operands.iterator()
        //
        //        while(op.hasNext)
        //        {
        //          val child = op.next()
        //          // Perform refinement for the child
        //          tmp = getSubsumedConcept(child)
        //          val it = tmp.iterator()
        //
        //          // construct a new MultiConjunction
        //          while(it.hasNext)
        //          {
        //            val itc = it.next()
        //            val newChildren = new util.LinkedList[OWLClassExpression](operands)
        //            // Exactly the previous order must be preserved
        //            // (at least until normal form is defined)
        //            val index = newChildren.indexOf(child)
        //            newChildren.add(index, itc)
        //            newChildren.remove(child)
        //            val un = new OWLObjectUnionOfImplExt(newChildren)
        //            refinements.add(un)
        //          }
        //        }

        //        // an element of the disjunction can be omitted
        //        val newIt = operands.iterator()
        //
        //        while(newIt.hasNext) {
        //          val child = newIt.next()
        //          val newChildren = new util.LinkedList[OWLClassExpression](operands)
        //          newChildren.remove(child)
        //          // if there is only one child, then disjunction is omitted immediately
        //          if (newChildren.size == 1) refinements.add(newChildren.get(0))
        //          else {
        //            val un = new OWLObjectUnionOfImplExt(newChildren)
        //            refinements.add(un)
        //          }
        //        }

        case of: OWLObjectSomeValuesFrom =>
          val role = of.getProperty
          val filler = of.getFiller
          println("\nsome values")
          tmp = getSubsumedConcept(filler)
  
          tmp.forEach((c: OWLClassExpression) => refinements.add(df.getOWLObjectSomeValuesFrom(role, c)))
          //        for (c <- tmp.asScala) {
          //          refinements.add(df.getOWLObjectSomeValuesFrom(role, c))
          //        }
  
          // if Kind is Bottom, then exists can be omitted
          if (filler.isOWLNothing) {
            refinements.add(df.getOWLNothing)
          }

        case of: OWLObjectAllValuesFrom =>
          val role = of.getProperty
          val filler = of.getFiller
          println("\nAll values")
          tmp = getSubsumedConcept(filler)
          tmp.forEach((c: OWLClassExpression) => refinements.add(df.getOWLObjectAllValuesFrom(role, c)))
  
          //        for (c <- tmp.asScala) {
          //          refinements.add(df.getOWLObjectAllValuesFrom(role, c))
          //        }
  
          // if Kind is Bottom, then exists can be omitted
          if (filler.isOWLNothing) {
            refinements.add(df.getOWLNothing)
          }

        case _ => throw new RuntimeException(currentConcept.toString)
      }
  
  
      // if concept is not equal to Bottom or Top, then a refinement of Top be appended
//      if (currentConcept.isInstanceOf[OWLObjectUnionOf] || !currentConcept.isAnonymous ||
//        currentConcept.isInstanceOf[OWLObjectComplementOf] ||
//        currentConcept.isInstanceOf[OWLObjectSomeValuesFrom] ||
//        currentConcept.isInstanceOf[OWLObjectAllValuesFrom]) { // AND TOP is appended
//
//        println("\nlast if statement")
//        val operands = Lists.newArrayList(currentConcept, df.getOWLThing)
//        val md = df.getOWLObjectIntersectionOf(operands)
//        refinements.add(md)
//        //      val md = new util.ArrayList[OWLClassExpression](operands)
//        //      md.forEach((c: OWLClassExpression) => refinements.add(c))
//        //      for (c <- md.asScala) {
//        //        if (!refinements.contains(c)) {
//        //          refinements.add(c)
//        //        }
//        //      }
//        //      df.getOWLObjectIntersectionOf(refinements)
//      }
  
      //    println("\n refinements are ")
      //    refinements.forEach(println(_))
    
      refinements
  }
 
 
 
  /**
   * Function to generate subsumed random concepts
   */
  
  def getSubsumedRandomConcept(currentConcept: OWLClassExpression): OWLClassExpression = {

    
//    val axiomsRDD = kb.getAxioms
    val generator: Random = new Random()
    var newConcept: OWLClassExpression = null
    do {
      if (generator.nextDouble() < 0.5) {
        newConcept = Concepts.takeSample(false, 1)(0)
      }
      else {
        // new concept restriction
        var newConceptBase: OWLClassExpression = null
        newConceptBase =
          if (generator.nextDouble() < 0.5) { getRandomConcept(kb) }
          else { Concepts.takeSample(false, 1)(0) }

        if (generator.nextDouble() < 0.5) {
          val role : OWLObjectProperty = ObjectProperties.takeSample(false, 1)(0)

          newConcept =
            if (generator.nextDouble() < 0.5) {
              kb.getDataFactory.getOWLObjectAllValuesFrom(role, newConceptBase)
            }
            else {
              kb.getDataFactory.getOWLObjectSomeValuesFrom(role, newConceptBase)
            }
        }

        else if (generator.nextDouble() < 0.75) {

          if (dataProperties.count() != 0)
          {
            val dataProperty: OWLDataProperty = dataProperties.takeSample(false, 1)(0)

//            val individuals: Set[OWLNamedIndividual] = dataProperty.individualsInSignature().collect(Collectors.toSet())
              val individuals: util.Set[OWLNamedIndividual] = dataProperty.getIndividualsInSignature
  
            val inds = new util.ArrayList[OWLNamedIndividual](individuals)
            val dPV: util.Set[OWLLiteral] = new util.HashSet[OWLLiteral]()
            for(i <- 0 until inds.size())
            {
              val element = inds.get(i)
              dPV.addAll(searcher.getDataPropertyValues(element, dataProperty).asInstanceOf[util.Collection[_ <: OWLLiteral]])
//              dPV.addAll(EntitySearcher.getDataPropertyValues(element, dataProperty, kb.getOntology).asInstanceOf[Collection[_ <: OWLLiteral]])
            }

            val values: util.ArrayList[OWLLiteral] = new util.ArrayList[OWLLiteral](dPV)
            newConcept =
              if (!values.isEmpty) {
                kb.getDataFactory.getOWLDataHasValue(dataProperty, values.get(generator.nextInt(values.size)))
              }
              else {
                kb.getDataFactory.getOWLObjectComplementOf(newConceptBase)
              }
          }
          else {
            newConcept = kb.getDataFactory.getOWLObjectComplementOf(newConceptBase)
          }
        }

        else if (generator.nextDouble() < 0.9) {

          val dataProperty: OWLObjectProperty = ObjectProperties.takeSample(false, 1)(0)
          val individuals: util.Set[OWLNamedIndividual] = dataProperty.getIndividualsInSignature
          val inds: util.ArrayList[OWLIndividual] = new util.ArrayList[OWLIndividual](individuals)
          val objValues: util.Set[OWLIndividual] = new util.HashSet[OWLIndividual]()

          for(i <- 0 until inds.size())
          {
            val element = inds.get(i)
            objValues.addAll(searcher.getObjectPropertyValues(element, dataProperty).asInstanceOf[util.Collection[_ <: OWLIndividual]])
  
//            objValues.addAll(EntitySearcher.getObjectPropertyValues(element, dataProperty, kb.getOntology).asInstanceOf[Collection[_ <: OWLIndividual]])
          }

          val values: util.ArrayList[OWLIndividual] = new util.ArrayList[OWLIndividual](objValues)
          newConcept =
            if (!values.isEmpty) {
              kb.getDataFactory.getOWLObjectHasValue(dataProperty, values.get(generator.nextInt(values.size)))
            }
            else {
              kb.getDataFactory.getOWLObjectComplementOf(newConceptBase)
            }
          }
        else {
          newConcept = kb.getDataFactory.getOWLObjectComplementOf(newConceptBase)
        }
      }
    } while (!reasoner.isSatisfiable(newConcept))
    
    newConcept.getNNF
  }


  /** Generate Random Concept
    * @param k Knowledgebase
    * @return Random Concept
    */
  def getRandomConcept(k: KB): OWLClassExpression = {
    var newConcept: OWLClassExpression = null
    val generator: Random = new Random()

      do {
          newConcept = Concepts.takeSample(false, 1)(0)
          if (generator.nextDouble() < 0.2) {
            return newConcept
          }
          else {
            var newConceptBase: OWLClassExpression = null
            newConceptBase =
              if (generator.nextDouble() < 0.2) {
                getRandomConcept(k)
              }
              else {
                newConcept
              }

            if (generator.nextDouble() < 0.75) {
              // new role restriction
              val role: OWLObjectProperty = ObjectProperties.takeSample(false, 1)(0)

              newConcept =
                if (generator.nextDouble() < 0.5) {
                  kb.getDataFactory.getOWLObjectAllValuesFrom(role, newConceptBase)
                }
                else {
                  kb.getDataFactory.getOWLObjectSomeValuesFrom(role, newConceptBase)
                }
            }
          }
      } while (newConcept == kb.getDataFactory.getOWLThing || reasoner.getInstances(newConcept, b = false).size() == 0
      )

        //      } while (newConcept == kb.getDataFactory.getOWLThing || kb.getReasoner.getInstances(newConcept, false).entities().count() == 0)

    newConcept
  }
}
