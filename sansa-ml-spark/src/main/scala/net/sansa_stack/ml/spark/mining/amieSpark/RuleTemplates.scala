package net.sansa_stack.ml.spark.mining.amieSpark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql._
import scala.collection.mutable.ArrayBuffer

import scala.collection.mutable.Map

object RuleTemplates {
  class RuleT (){
    
   
    var ruleTTable:DataFrame = null
    var nameRuleTTable:String = "00"
    
    var so: ArrayBuffer[Tuple2[String, String]] = ArrayBuffer(Tuple2("a","b"))
    
    var highestVariable:Char = 'b'
    var variableList = ArrayBuffer("a","b")
    
    
    def getSubOb():ArrayBuffer[Tuple2[String,String]]={
      return this.so
    }
    
    def setSubOb(newSO: ArrayBuffer[Tuple2[String,String]]){
      this.so = newSO
    }
    
    
    def getnameRuleTTable():String={
      return this.nameRuleTTable
    }
    
    def setnameRuleTTable(name:String){
      this.nameRuleTTable = name
    }
    
    def notClosed(): Option[ArrayBuffer[String]] = {
      var maxVar:Char = 'a'
      var varArBuff = new ArrayBuffer[String]
         
      var tparr = so.clone()
      var maptp: Map[String, Int] = Map()
      if (tparr.length == 1 ){
          
         return Some(ArrayBuffer(tparr(0)._1,tparr(0)._2))
      }
        
         
    
      for (x <- tparr){
          
        if (!(maptp.contains(x._1))){
          varArBuff += x._1
          if (x._1(0)>maxVar){
             maxVar = x._1(0)
          }
        maptp += (x._1 -> 1)
        }
        else{
          maptp.put(x._1, (maptp.get(x._1).get + 1)).get
        }
                  
                   /**checking map for placeholder for the object*/
        if (!(maptp.contains(x._2))){
          varArBuff += x._2
                       
          if (x._2(0)>maxVar){
            maxVar = x._2(0)
          }                       
        maptp += (x._2 -> 1)
        }
        else {
          maptp.put(x._2, (maptp.get(x._2).get + 1)).get
        }
       
       }
       var out:ArrayBuffer[String] = new ArrayBuffer
       this.variableList = varArBuff
       maptp.foreach{value => if (value._2 == 1) out += value._1}
       
       this.highestVariable = maxVar
       if (out.isEmpty){
         return None
       }
       else{
         return Some(out)
       }
      
       
       
       
         
       }
       
       def getHighestVariable(): Char = {
         return this.highestVariable
       }
       
       def getVariableList(): ArrayBuffer[String] = {
         return this.variableList
       }
    
  }
}