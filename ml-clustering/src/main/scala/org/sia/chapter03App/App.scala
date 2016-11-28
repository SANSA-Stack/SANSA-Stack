
/**
 * Test class
 */
package org.sia.chapter03App

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.functions
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.clustering.PowerIterationClustering
import scala.reflect.runtime.universe._
import scopt.OptionParser
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.util.MLUtils
import java.io.{FileReader, FileNotFoundException, IOException}
import org.apache.spark.mllib.linalg.Vectors
import java.lang.{ Long => JLong }
import java.lang.{Long=>JLong}
import breeze.linalg.{squaredDistance, DenseVector, Vector}


 

    object App {
   
 
 case class Params(
     
      input: String = null,
      k: Int = 100,
      
      maxIterations: Int = 400
     
    ) extends AbstractParams[Params]{
     
 }
 abstract class AbstractParams[T: TypeTag] {

  private def tag: TypeTag[T] = typeTag[T]

  
  override def toString: String = {
    val tpe = tag.tpe
    val allAccessors = tpe.declarations.collect {
      case m: MethodSymbol if m.isCaseAccessor => m
    }
    val mirror = runtimeMirror(getClass.getClassLoader)
    val instanceMirror = mirror.reflect(this)
    allAccessors.map { f =>
      val paramName = f.name.toString
      val fieldMirror = instanceMirror.reflectField(f)
      val paramValue = fieldMirror.get
      s"  $paramName:\t$paramValue"
    }.mkString("{\n", ",\n", "\n}")
  }
}
  def main(args : Array[String]) {
   
    
  
    
    val defaultParams = Params()

    val parser = new OptionParser[Params]("PowerIterationClusteringExample") {
      head("PowerIterationClusteringExample: an example PIC app using concentric circles.")
     
      opt[Int]('k', "k")
        .text(s"number of circles (/clusters), default: ${defaultParams.k}")
        .action((x, c) => c.copy(k = x))
     
      opt[Int]("maxIterations")
        .text(s"number of iterations, default: ${defaultParams.maxIterations}")
        .action((x, c) => c.copy(maxIterations = x))
      
    }

    parser.parse(args, defaultParams).map { params =>
      run(params)
    }.getOrElse {
      sys.exit(1)
    }
  }

  def run(params: Params) {
    
    
    val conf = new SparkConf()
    
      .setMaster("local")
      .setAppName(s"PowerIterationClustering with $params")
    val sc = new SparkContext(conf)

     Logger.getRootLogger.setLevel(Level.WARN) 
     
    val data = sc.textFile("/home/tina/sample.txt")
    
    data.collect().foreach(println)
  
    
 
    val splitRdd = data.map( line => line.split("\t") )


   val yourRdd  = splitRdd.map( line => {
    val node1 = line( 0 ).toLong
    val node2= line( 1 ).toLong
    val similarity  = line(2).toDouble
    (node1, node2, similarity)  
     })
     

   
   
    val model = new PowerIterationClustering()
      .setK(params.k)
      .setMaxIterations(params.maxIterations)
      .run(yourRdd)

    val clusters = model.assignments.collect().groupBy(_.cluster).mapValues(_.map(_.id))
    val assignments = clusters.toList.sortBy { case (k, v) => v.length}
    val assignmentsStr = assignments
      .map { case (k, v) =>
      s"$k -> ${v.sorted.mkString("[", ",", "]")}"
    }.mkString(",")
    val sizesStr = assignments.map {
      _._2.size
    }.sorted.mkString("(", ",", ")")
    println(s"Cluster assignments: $assignmentsStr\ncluster sizes: $sizesStr")

    sc.stop()
  }

  

 
   

  
 }


