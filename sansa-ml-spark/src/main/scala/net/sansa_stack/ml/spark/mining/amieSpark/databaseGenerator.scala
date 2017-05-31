package net.sansa_stack.ml.spark.mining.amieSpark

import org.apache.spark.SparkContext
import net.sansa_stack.ml.spark.mining.amieSpark.RuleTemplates.RuleT

import java.io.File





import java.io.File
import java.net.URI


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, SQLContext, SparkSession, _ }

import scala.collection.mutable.{ ArrayBuffer, Map }
import scala.util.Try

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

object DatabaseGenerator{
  class Generator (hdfsP:String, dfT: DataFrame, maxL: Int) {
    var hdfsPath:String = hdfsP
    var dfTable: DataFrame = dfT  
    var maxLength = maxL
    var allParquetNames:ArrayBuffer[String] = ArrayBuffer("00")
    
    def generate (sc:SparkContext, sqlContext: SQLContext){
      var firstRuleTmp = new RuleT 
      
      dfTable.write.parquet(hdfsPath + "precomputed0" + "/00" )
      
      var temp:ArrayBuffer[RuleT] = ArrayBuffer(firstRuleTmp)
      
      
      for (i <- 1 to this.maxLength ){
        var listRuleTmp:ArrayBuffer[RuleT] = temp
        temp = ArrayBuffer()
        for (rTmp <- listRuleTmp){
          //generation for cardinality of body and plusNegative examples
          if (i>2){
            var body = rTmp.getSubOb().clone()
            body.remove(0)
            var bodyTableName = calcName(null, body)
            if (!(this.allParquetNames.contains(bodyTableName))){
              //TODO: generation part
              var oldBody = body.clone()
              oldBody.remove(body.length-1)
              var oldBodyTableName = calcName(null, oldBody)
              newDataFrameTable(i-2, bodyTableName, body, oldBodyTableName, sqlContext)
              this.allParquetNames += bodyTableName
            }
          }
          //generation new indexing Table
          if (i < this.maxLength ){
            var newAtoms:ArrayBuffer[Tuple2[String,String]] = addDanglingAtom(rTmp, sc, sqlContext) ++ addClosingAtom(rTmp, sc, sqlContext)
          
            for (nA <- newAtoms){
             
            
              var newName = calcName(nA, rTmp.getSubOb())
            
            
            
	            if (!(this.allParquetNames.contains(newName))){
	             var newRuleTmp = new RuleT
	            var newSubOb =   rTmp.getSubOb().clone()
	              newSubOb +=nA
              newRuleTmp.setSubOb(newSubOb)
            
            
              newRuleTmp.setnameRuleTTable(newName)
            
              newDataFrameTable(i, newRuleTmp.getnameRuleTTable(), newRuleTmp.getSubOb(), rTmp.getnameRuleTTable(), sqlContext)
              temp +=newRuleTmp
              this.allParquetNames += newName
              }
            }
          } 
          
        }
        
        
        
      }
      
      
    }
   
    
    def calcName(nA: Tuple2[String,String], oldSubOb: ArrayBuffer[Tuple2[String,String]]):String={
     var whole = oldSubOb.clone() 
      if (nA != null){
        whole += nA
      }
      
      var countMap: Map[String,Int] = Map()
      var numberMap: Map[String, Int] = Map()
      var counter:Int = 1
      for (w <- whole){
        if (countMap.contains(w._1)){
          var temp = countMap.remove(w._1).get +1
          countMap += (w._1 -> temp)
        }
        else {
          countMap += (w._1 -> 1)
        }
         if (countMap.contains(w._2)){
          var temp = countMap.remove(w._2).get +1
          countMap += (w._2 -> temp)
        }
        else {
          countMap += (w._2 -> 1)
        }
         if (!(numberMap.contains(w._1))){
           numberMap += (w._1 -> counter)
           counter+= 1
         }
         if (!(numberMap.contains(w._2))){
           numberMap += (w._2 -> counter)
           counter+= 1
         }
        
      }
      
      var out = ""
      for (wh <- whole){
        var a = ""
        var b = ""
        if (countMap(wh._1)>1){
          a = numberMap(wh._1).toString
        }
        else {
          a="0"
        }
        
        if (countMap(wh._2)>1){
          b = numberMap(wh._2).toString
        }
        else {
          b="0"
        }
        
        out += a + b + "_"
      }
      out = out.stripSuffix("_")
      return out
    }
    
        def addDanglingAtom(rule: RuleT, sc: SparkContext, sqlContext:SQLContext):ArrayBuffer[Tuple2[String,String]]= 
    {
      val tpAr = rule.getSubOb()
      var RXY:ArrayBuffer[Tuple2[String,String]] = new ArrayBuffer
     
      val notC = rule.notClosed()
      
      val variables = rule.getVariableList()
      val freshVar = (rule.getHighestVariable() + 1).toChar.toString
      
      if (notC.isEmpty){
        for(v <- variables){
          RXY ++= ArrayBuffer(Tuple2(v,freshVar), Tuple2(freshVar, v))
          
        }
      }
      else{
        for (nc <- notC.get){
          RXY ++= ArrayBuffer(Tuple2(nc,freshVar), Tuple2(freshVar, nc))
        }
      }
      
      //var x = this.countProjectionQueriesDF(c, id, "OD", minHC, tpAr, RXY, sc, sqlContext)
      
      return RXY
    }
    
    
    
    
    def addClosingAtom(rule: RuleT, sc: SparkContext, sqlContext:SQLContext): ArrayBuffer[Tuple2[String,String]] =
    {
      val tpAr = rule.getSubOb()
      var RXY:ArrayBuffer[Tuple2[String,String]] = new ArrayBuffer
    
      val notC = rule.notClosed()
      
      val variables = rule.getVariableList()
      
      
      if (notC.isEmpty){
      
        for(v <- variables){
         for (w <-variables){
           if (!(v == w)){
             RXY += Tuple2(v,w)
           }
         }
          
       }
      }
      else{
        var notCVars = notC.get
        
        if (notCVars.length == 1){
          for (v <- variables){
            RXY ++= ArrayBuffer(Tuple2(notCVars(0),v), Tuple2(v, notCVars(0)))
          }
        }
        else{
          for (a <- notCVars){
            for (b <- notCVars){
              if (!(a == b)){
                RXY += Tuple2(a,b)
              }
            }
          }
        }
      
           
    }
    //var x = this.countProjectionQueriesDF(c, id, "OC", minHC, tpAr, RXY, sc, sqlContext)
    
    return RXY
    }
    
    
    def newDataFrameTable (id : Int,newRuleTmpTableName: String, newRuleTmpSubOb: ArrayBuffer[Tuple2[String,String]], oldRuleTTableName: String, sqlContext: SQLContext){
      
      var df:DataFrame = sqlContext.read.parquet(hdfsPath+"precomputed"+(id-1)+"/"+oldRuleTTableName)
      df.registerTempTable("table")
     
      dfTable.registerTempTable("dftable")
  
       
      
      
      
      
       var tpMap:Map[String, ArrayBuffer[String]] = Map()
       var wholeAr = newRuleTmpSubOb
   
    
   
   
        var w = sqlContext.sql("SELECT sub0 AS sub"+(id)+", pr0 AS pr"+(id)+", ob0 AS ob"+(id)+" FROM dftable")
        w.registerTempTable("newColumn")
   
   
   var checkMap: Map[Int, Tuple2[String,String]] = Map()
  var lastA = wholeAr.last._1
  var lastB = wholeAr.last._2
   var varAr = ArrayBuffer(lastA, lastB)
   
   var joinAB = Tuple2("", "")
   //count
   var pred:String = ""
   
   
   for(i <- 0 to wholeAr.length-1){
     var a = wholeAr(i)._1
     var b = wholeAr(i)._2
   
       
       pred += "pr" + i+" AS pred"+i+ ","
      
     
     
     checkMap += (i -> Tuple2(a,b))
     if ((a == lastA)||(a==lastB)){ 
       if(!(tpMap.contains(a))){
         tpMap += ((a) -> ArrayBuffer("sub"+ i))
       
       }
       else {
         var temp = tpMap.get(a).get 
         temp += "sub"+i
         tpMap.put(a, temp)
       }
     }
     
     if ((b == lastA)||(b == lastB)){
       if(!(tpMap.contains(b))){
         tpMap += ((b) -> ArrayBuffer("ob"+i))
         
       }
    
       else{
         var temp = tpMap.get(b).get 
         temp += "ob"+i
         tpMap.put(b, temp)
       
       }
     }
   }
   
   
   var cloneTpAr = wholeAr.clone()
   
   var removedMap:Map[String, ArrayBuffer[Tuple2[Int,String]]] = Map()
   
    
   
   var checkSQLWHERE = ""
   checkMap.foreach{ab =>
     var a = ab._2._1
     var b = ab._2._2
     
     if (varAr.contains(a)){
      
       varAr -= a
       var x = tpMap.get(a).get
       
       
       for (k <- x){
         if(k.takeRight(1).toInt != ab._1){
           if ((joinAB == Tuple2("", ""))&&((k.takeRight(1).toInt == wholeAr.length-1)||(ab._1 == wholeAr.length-1))){
             if ((k.takeRight(1).toInt == wholeAr.length-1)){
               joinAB = ("sub"+ab._1,k)
             }
             if (ab._1 == wholeAr.length-1){
               joinAB = (k, "sub"+ab._1)
             }
           }
           else{
             checkSQLWHERE += "sub"+ab._1+" = "+k+" AND "
           }
         }
         
       }
       
       
     }
     
    
     
     if (varAr.contains(b)){
       
       varAr -= b
       
       var y = tpMap.get(b).get
       var counter = 0
       for (k <- y){
         if(k.takeRight(1).toInt != ab._1){
           if ((joinAB == Tuple2("", ""))&&((k.takeRight(1).toInt == wholeAr.length-1)||(ab._1 == wholeAr.length-1))){
             if ((k.takeRight(1).toInt == wholeAr.length-1)){
               joinAB = ("ob"+ab._1,k)
             }
             if (ab._1 == wholeAr.length-1){
               joinAB = (k, "ob"+ab._1)
             }
           }
           
           else{
             checkSQLWHERE += "ob"+ab._1+" = "+k+" AND "
           }
         }
       }
                
     }
     
   }
    
    
    checkSQLWHERE = checkSQLWHERE.stripSuffix(" AND ")
    
    var v:DataFrame = null
   
    if (joinAB == Tuple2("","")){
      v = w.join(df)
    }
    else {
      v = w.join(df, df(joinAB._1) === w(joinAB._2))
    }
    
    
   v.registerTempTable("t")
   var last = sqlContext.sql("SELECT * FROM t WHERE "+checkSQLWHERE)
    last.write.parquet(hdfsPath + "precomputed"+id + "/"+newRuleTmpTableName )
    
   pred = pred.stripSuffix(",")
   last.registerTempTable("lastTable")
    var count = sqlContext.sql("SELECT "+pred+" FROM lastTable")
    
    var counting = count.rdd.map{
     x => 
       var name:String = ""
       x.toSeq.map{
        y => 
          name += y.toString + "_"
       }
      name = name.stripSuffix("_")
      (name, 1)
    }.reduceByKey(_ + _)
    
    import sqlContext.implicits._
    var countingDf = counting.toDF("predicates","count")
    countingDf.write.parquet(hdfsPath + "count/"+newRuleTmpTableName )
    
    }
    
   
  
  }
  
  def main(args: Array[String]) = {
    

    val sparkSession = SparkSession.builder

      .master("local[*]")
      .appName("generateInMemoryDatabase")

      .getOrCreate()

    if (args.length < 2) {
      System.err.println(
        "Usage: Triple reader <input> <output>")
      System.exit(1)
    }

    val input = args(0)
    val outputPath: String = args(1)
    val hdfsPath: String = outputPath + "/"

    val sc = sparkSession.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    

    val algo = new Generator(hdfsPath, DfLoader.loadFromFileDF(input, sc, sqlContext, 2), 3)
    algo.generate(sc, sqlContext)
    
    

   

  

    sc.stop

  }
  
}