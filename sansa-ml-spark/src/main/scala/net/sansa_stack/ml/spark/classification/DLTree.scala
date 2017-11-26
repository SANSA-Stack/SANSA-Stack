package net.sansa_stack.ml.spark.classification

import java.util.ArrayList
import java.util.List
import collection.JavaConverters._
import org.semanticweb.owlapi.model.OWLClassExpression
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import net.sansa_stack.ml.spark.classification._

/*
 * Class for basic functions of DL trees
 */

class DLTree {
  
  private class DLNode(var concept: OWLClassExpression) {
    
// positive decision subtree
    var pos: DLTree = null 

// negative decision subtree
    var neg: DLTree = null

    override def toString(): String = this.concept.toString

  }

  private var root: DLNode = null      // Tree root

  def this(c: OWLClassExpression) = {
    this()
    this.root = new DLNode(c)
  }

  /**
    * @param root the root to set
    */
  def setRoot(concept: OWLClassExpression): Unit = {
    this.root = new DLNode(concept)
  }

  def setPosTree(subTree: DLTree): Unit = {
    this.root.pos = subTree
  }

  def setNegTree(subTree: DLTree): Unit = {
    this.root.neg = subTree
  }

  override def toString(): String = {
    if (root == null) null
    if (root.pos == null && root.neg == null) root.toString
    else
      root.concept.toString + " [" + root.pos.toString + " " + root.neg.toString + "]" 
   }

   /**
    * @return the root
    */
  
  def getRoot(): OWLClassExpression = root.concept
  
  def getPosSubTree(): DLTree = root.pos

  def getNegSubTree(): DLTree = root.neg

  
  /*
   * function to get the number of nodes 
   */
  
 /*def getNodi(sc: SparkSession): Double = {
    
    // visit in to make the count
    val lista: ArrayList[DLNode] = new ArrayList[DLNode]()
    var Li = sc.sparkContext.parallelize(lista.asScala)
    
    var num: Double = 0 
    if (root != null)
    {
      var ele : List[DLNode] = new ArrayList[DLNode]
      ele.add(root)
      var eleRDD = sc.sparkContext.parallelize(ele.asScala) 
      Li = eleRDD.union(Li)
      
      while (!Li.isEmpty)
      {
        val node : DLNode = Li.first()
        Li.mapPartitionsWithIndex((index, it) => if (index == 0) it.drop(1) else it,
                    preservesPartitioning = true)
        num += 1
        var sx: DLNode = null
        var SL : List[DLNode] = new ArrayList[DLNode]
        
        if (node.pos != null) {
          
          sx = node.pos.root.asInstanceOf[DLNode]
          SL.add(sx)
          var SLRDD = sc.sparkContext.parallelize(SL.asScala)
          
          
          if (sx != null)  Li.union(SLRDD)
        }
        
        if (node.neg != null) {
         
          sx = node.neg.root.asInstanceOf[DLNode]
          SL.add(sx)
          var SLRDD = sc.sparkContext.parallelize(SL.asScala)
          
          if (sx != null) Li.union(SLRDD)
        }
      }
    }
    num
  }
      
     

   def getComplexityMeasure(sc: SparkSession) : Double = getNodi(sc)*/



}
