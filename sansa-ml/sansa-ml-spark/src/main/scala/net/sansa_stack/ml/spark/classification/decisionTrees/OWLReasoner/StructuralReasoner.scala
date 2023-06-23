package net.sansa_stack.ml.spark.classification.decisionTrees.OWLReasoner

import java.io.Serializable
import java.util

import net.sansa_stack.ml.spark.classification.decisionTrees.KB
import net.sansa_stack.owl.spark.owlAxioms
import net.sansa_stack.owl.spark.rdd.OWLAxiomsRDD
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.semanticweb.owlapi.model._
import scala.collection.JavaConverters._

 object StructuralReasoner {
  
   class StructuralReasoner(kb: KB) extends Serializable {
    
     val rdd: OWLAxiomsRDD = kb.getAxioms
     val df: OWLDataFactory = kb.getDataFactory
     val searcher: EntitySearcher = new EntitySearcher(kb)
     
     val classAssertions: RDD[(OWLClassExpression, OWLIndividual)] = kb.getClassAssertions
//       .asInstanceOf[RDD[OWLClassAssertionAxiom]]
       .map(a => (a.getClassExpression, a.getIndividual))
//     val classAssertionsMap: RDD[(OWLClassExpression, OWLIndividual)] = classAssertions.asInstanceOf[RDD[OWLClassAssertionAxiom]].map(a => (a.getClassExpression, a.getIndividual))
  
     val classAssertionBC: Broadcast[Map[OWLClassExpression, OWLIndividual]] = kb.getSparkSession.sparkContext.broadcast(classAssertions.collect().toMap)
     
     val objPropertyAssertions: RDD[OWLObjectPropertyAssertionAxiom] = owlAxioms.extractAxioms(rdd, AxiomType.OBJECT_PROPERTY_ASSERTION)
       .asInstanceOf[RDD[OWLObjectPropertyAssertionAxiom]]
  
//     val objPropertyAssertionsBC: Broadcast[Array[(OWLIndividual, OWLObjectPropertyExpression, OWLIndividual)]] = sc.sparkContext.broadcast(
//       objPropertyAssertions.map(a => (a.getSubject, a.getProperty, a.getObject)).collect())
  
     val subClasses: RDD[(OWLClassExpression, OWLClassExpression)] = owlAxioms.extractAxioms(rdd, AxiomType.SUBCLASS_OF)
                               .asInstanceOf[RDD[OWLSubClassOfAxiom]]
                               .map(a => (a.getSuperClass, a.getSubClass))
    
     val subClassesBC: Broadcast[Map[OWLClassExpression, OWLClassExpression]] = kb.getSparkSession.sparkContext.broadcast(subClasses.collect().toMap)
  
//     val containAxiomBC: Broadcast[OWLAxiom => Boolean] = kb.getSparkSession.sparkContext.broadcast(containsAxiom)
  
     def containsAxiom(axiom: OWLAxiom): Boolean = {
       !kb.getAxioms.filter(a => a.equals(axiom)).isEmpty()
     }
     /**
      * Method to determine if the specified axiom is entailed by the input axioms.
      *
      * @param axiom The axiom
      * @return true if axiom is entailed by the original axioms or
      *         false if axiom is not entailed by the original axioms.
      */
    
     def isEntailed(axiom: OWLAxiom): Boolean = {
       
//       val x: util.Set[OWLAxiom] = new util.HashSet[OWLAxiom]()
//       x.add(axiom)
       
//       val ax = sc.sparkContext.parallelize(Seq(axiom.asInstanceOf[OWLClassAssertionAxiom]))
// //         .asInstanceOf[RDD[OWLClassAssertionAxiom]]
//         .map(a => (a.getClassExpression, a.getIndividual))
//
//       val entailed = classAssertions.join(ax).filter(a => a._2._1 == a._2._2).collect().length > 0
       
       
//       val entailed = searcher.containsAxiom(axiom)
//      val ax = axiom.asInstanceOf[OWLClassAssertionAxiom]
  
//      val e = classAssertions.lookup(ax.getClassExpression)
//       val e = classAssertions.filter(a => classAssertionBC.value.contains(ax.getClassExpression) && )
//      val entailed = e.contains(ax.getIndividual)
      
//       val entailed = containAxiomBC.value(axiom)
//       val entailed = (classAssertionBC.value.contains(ax.getClassExpression)
//                      && classAssertionBC.value.get(ax.getClassExpression).contains(ax.getIndividual))
//         classAssertions.filter(a => a.equals(ax)).first()
//       val entailed = !classAssertions.filter(_.equals(axiom)).isEmpty()
//       if (entailed) return true
  
       axiom match {
        
         case ax: OWLClassAssertionAxiom =>
          
           val ce = ax.getClassExpression
           val ind = ax.getIndividual
          
           if (ce.isOWLThing) true
           else if (ce.isOWLNothing) false
           else if (!ce.isAnonymous) {
             subClassesBC.value.get(ce).toStream.asJavaCollection.stream()
               .anyMatch(cls => isEntailed(df.getOWLClassAssertionAxiom(cls, ind)))

//             entailed = classAssertionBC.value.contains(axiom)
//             entailed = subClasses.filter(a => ce.equals(subClassesBC.value(a._2))).map(_._1)
//  .collect().toStream.asJavaCollection.stream().anyMatch(cls => isEntailed(df.getOWLClassAssertionAxiom(cls, ind)))
//             entailed
//
//             val sub = searcher.getSubClasses(ce.asOWLClass())
//             val sub = subClasses.collect().toSet.filter(a => a._2.equals(ce)).map(_._1)
//
//              val e = sub.map(a => isEntailed(df.getOWLClassAssertionAxiom(a, ind)))
//                         .filter(a => a.equals(true)).collect()
//
//             e.length > 0
//             if (sub.isEmpty()) false
//             else {
//               val x = sub.map(s => searcher.containsAxiom(df.getOWLClassAssertionAxiom(s, ind)))
//                          .filter(a => a.equals(true))
//               x.nonEmpty
//             }
           }
           
           else ce match {
             case ex: OWLObjectIntersectionOf =>
               ex.getOperands.stream()
                 .allMatch(a => isEntailed(df.getOWLClassAssertionAxiom(a, ind)))
            
             case ex: OWLObjectUnionOf =>
               ex.getOperands.stream()
                 .anyMatch(a => isEntailed(df.getOWLClassAssertionAxiom(a, ind)))
            
             case ex: OWLObjectSomeValuesFrom =>
               
               val role = ex.getProperty
               val filler = ex.getFiller
               objPropertyAssertions.map(a => (a.getProperty, a.getSubject, a.getSubject))
                                    .filter(a => a._2.equals(ind))
                                    .map{ a =>
                                      if (role.equals(a._1)) {
                                        return isEntailed(df.getOWLClassAssertionAxiom(filler, a._3))
                                      }
                                      else {
                                        return false
                                      }
                                    }
               
               
               //              val axioms = searcher.getObjectPropertyAssertionAxioms(ind)
               //              val axioms = objPropertyAssertions.filter(a => ind == a.getSubject)
//               val objectAssertions = objPropertyAssertionsBC.value.filter(a => a._1.equals(ind))
//               objectAssertions.filter { a =>
//                 if (role.equals(a._2)) {
//                   return isEntailed(df.getOWLClassAssertionAxiom(filler, a._3))
//                 }
//                 else {
//                   return false
//                 }
//               }
               false
             case _ => false    // ce match case
             //              val x: RDD[OWLIndividualAxiom] = axioms.filter { a =>
             //                val prop = a.asInstanceOf[OWLObjectPropertyAssertionAxiom].getProperty
             //                a match {
             //                  case axiom: OWLObjectPropertyAssertionAxiom if role.equals(prop) =>
             //                    val obj = axiom.getObject
             //                    return isEntailed(df.getOWLClassAssertionAxiom(filler, obj))
             //                  case _ => false
             //                }
             //              }
            
            
           }
        
         case _ => false    // ax match case
       }
     }
    
     def isEntailed(axioms: util.Set[OWLAxiom]): Boolean = {

       axioms.forEach((ax: OWLAxiom) => {
         assert(ax != null)
         if (!isEntailed(ax)) {
           return false
         }
       })
       true
     }
    
     /**
      * Method to determine if the specified class expression is satisfiable by the input axioms.
      *
      * @param cls The class expression
      * @return true if the class expression is satisfiable by the original axioms or
      *         false if class expression is not satisfiable by the original axioms.
      */
//     def isSatisfiable(cls: OWLClassExpression): Boolean = {
//
//       !cls.isAnonymous &&
//         !searcher.getEquivalentClasses(cls.asOWLClass()).collect().toList.contains(df.getOWLNothing)
//
//     }
  
     def isSatisfiable(cls: OWLClassExpression): Boolean = {

       !cls.isAnonymous &&
         searcher.getEquivalentClasses(cls).cache()
                  .filter(c => c.isOWLNothing).isEmpty()

     }
     
     def getInstances(cls: OWLClassExpression, b: Boolean): util.Set[OWLNamedIndividual] = {

       val result: util.Set[OWLNamedIndividual] = new util.HashSet[OWLNamedIndividual]()
       
       if (!cls.isAnonymous) {
         val ce = cls.asOWLClass()
         val classes = new util.HashSet[OWLClass]
         classes.add(ce)
  
         if (!b) {
           classes.addAll(searcher.getSubClasses(cls)
             //           .filter(_.isNamed)
             .map(_.asOWLClass())
             .collect().toSet.asJava)
         }
  
         classes.forEach((c: OWLClassExpression) => {
        
          val x: RDD[OWLNamedIndividual] = searcher.getClassAssertionAxioms(c)
//             .mapPartitions(partition => partition
               .map(a => a.getIndividual)
               .filter(!_.isAnonymous)
               .map{ax => ax.asOWLNamedIndividual()
//                 result.addNode(new OWLNamedIndividualNode(ax.asOWLNamedIndividual()))
               }
          result.addAll(x.collect().toSet.asJava)
//             ).coalesce(1)
         })
       
       } else cls match {
         
         case of: OWLObjectIntersectionOf =>
//           println("\n intersection\n" + cls)
  
           val operands = of.getOperandsAsList
           val tmp = new util.HashSet[OWLNamedIndividual]
           tmp.addAll(getInstances(operands.get(0), b))
  
           for (i <- 1 until operands.size) {
             tmp.retainAll(getInstances(operands.get(i), b))
           }
//           result.addAllNodes(tmp)
           result.addAll(tmp.asScala.asJavaCollection)
         
         case of: OWLObjectUnionOf =>
//           println("\n union\n" + cls)
  
           val operands = of.getOperands
  
           operands.forEach((op: OWLClassExpression) => {
               result.addAll(getInstances(op, b))
           })

         case of: OWLObjectComplementOf =>
//           println("\n complement\n" + cls)
  
           val operand: OWLClassExpression = of.getOperand
           
           if (!operand.isAnonymous) {
             val tmp = operand.asOWLClass()
             val classes = new util.HashSet[OWLClass]
             classes.add(tmp)
  
             if (!b) {
               classes.addAll(searcher.getSubClasses(cls)
                 //           .filter(_.isNamed)
                 .map(_.asOWLClass())
                 .collect().toSet.asJava)
             }
  
             classes.forEach((c: OWLClassExpression) => {
               val x = searcher.getClassAssertionAxioms(c)
                 .map(a => a.getIndividual)
                 .filter(!_.isAnonymous)
                 .map(ax => ax.asOWLNamedIndividual)
  
               result.addAll(x.collect().toSet.asJava)
             })
           }

         case of: OWLObjectSomeValuesFrom =>
//           println("\n somevalues\n" + cls)
  
           val role: OWLObjectPropertyExpression = of.getProperty
           val filler: OWLClassExpression = of.getFiller
           
           val objectProperties = new util.HashSet[OWLObjectPropertyExpression]
           objectProperties.add(role)
           
           if (!b) objectProperties.addAll(searcher.getSubObjectProperties(role).collect().toSet.asJavaCollection)
  
           objectProperties.forEach((p: OWLObjectPropertyExpression) => {
            searcher.getObjectPropertyAssertionAxioms(p)
             .map{o =>
                 val obj = o.getObject
                 if (!obj.isAnonymous) {
                   if (filler.isOWLThing || getInstances(filler, b = false).contains(obj.asOWLNamedIndividual())) {
                      val subj = o.getSubject
                      if (!subj.isAnonymous) {
                        result.add(subj.asOWLNamedIndividual)
                      }
                   }
                 }
             }
           })

//         case of: OWLObjectAllValuesFrom =>
//
//           val role: OWLObjectPropertyExpression = of.getProperty
//           val filler: OWLClassExpression = of.getFiller
//
//           val objectProperties = new util.HashSet[OWLObjectPropertyExpression]
//           objectProperties.add(role)
//
//           if (!b) objectProperties.addAll(searcher.getSubObjectProperties(role).collect().toSet.asJavaCollection)
//
//           objectProperties.forEach((p: OWLObjectPropertyExpression) => {
//             searcher.getObjectPropertyAssertionAxioms(p)
//               .map{o =>
//                 val obj = o.getObject
//                 if (!obj.isAnonymous) {
//                   if (filler.isOWLThing || getInstances(filler, b = false).contains(obj.asOWLNamedIndividual())) {
//                     val subj = o.getSubject
//                     if (!subj.isAnonymous) {
//                       result.add(subj.asOWLNamedIndividual)
//                     }
//                   }
//                 }
//               }
//           })

         case _ => throw new RuntimeException(cls.toString)
         
       }

//       val it = result.size
//       println("result size = " + it)
       result
       }
     
       
       
       
       
       
       
//       println("\n  classes ===================\n")
//       println(classes)
      
//       for (c <- classes.asScala) {
//         searcher.getClassAssertionAxioms(c)
//           //  .mapPartitions(partition => partition
//           .map(ax => result.addNode(new OWLNamedIndividualNode(ax.getIndividual.asOWLNamedIndividual())))
//         //    .filter(!_.isAnonymous)
//         //    .map(_.asOWLNamedIndividual())
//         //       .map(i => result.add(i))
//         // ).coalesce(1)
//       }
  
       
      
       //      classes.forEach((c: OWLClass) => {
       //        searcher.getClassAssertionAxioms(c)
       //          .mapPartitions(partition => partition
       //            .map(ax => ax.getIndividual)
       //            .filter(!_.isAnonymous)
       //            .map(_.asOWLNamedIndividual())
       //            .map(i => result.add(i))
       //          ).coalesce(1)
       //
       //          .mapPartitions(partition => partition
       //          .map{i =>
       //            if (!searcher.getSameAsIndividuals(i).isEmpty) {
       //              result.addAll(searcher.getSameAsIndividuals(i))
       //            } else {
       //              result.add(i)
       //            }
       //          })
       //          .coalesce(1)
       //      })
       
     }
   }
 
