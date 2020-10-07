package net.sansa_stack.ml.spark.kge.linkprediction.run

import net.sansa_stack.rdf.spark.kge.convertor.ByIndex
import net.sansa_stack.rdf.spark.kge.triples._
import org.apache.log4j.{ Level, Logger }
import org.apache.spark.sql._

object TriplesRun extends App {

  def printType[T](x: T): Unit = { println(x.getClass.toString) }

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val spark = SparkSession.builder
    .master("local")
    .appName("TransE")
    .getOrCreate()
  import spark.implicits._

  //  spark.sparkContext.setLogLevel("ERROR")
  spark.sparkContext.setLogLevel("OFF")

  println("<<< STARTING >>>")

  var startTime = System.currentTimeMillis()

  startTime = System.currentTimeMillis()
  val trp = new Triples("/home/hamed/workspace/TransE/DataSets/FB15k/freebase_mtr100_mte100-train.txt", "\t", false, false, spark)
  println("Reading triples done in " + (System.currentTimeMillis() - startTime) + " seconds")

  startTime = System.currentTimeMillis()
  var num: Long = trp.triples.count()

  println("\n\n No triples = " + num.toString + " - Done in " + (System.currentTimeMillis() - startTime) + " seconds.")

  startTime = System.currentTimeMillis()
  num = trp.getEntities().length

  println("\n\n No Entities = " + num.toString + " - Done in " + (System.currentTimeMillis() - startTime) + " seconds.")

  startTime = System.currentTimeMillis()
  num = trp.getRelations().length

  println("\n\n No Predicates = " + num.toString + " - Done in " + (System.currentTimeMillis() - startTime) + " seconds.")
  //  trp.getAllDistinctEntities().take(10).foreach(println)
  //  println("\n \n No entities = ",trp.getAllDistinctEntities().count() )
  //  println("\n \n No predicates = ",trp.getAllDistinctPredicates().count() )

  //  val e1 = trp.getAllDistinctEntities().take(10).toSeq.toDS()
  //  println("\n \n ----------")
  //  e1.foreach(x=>println(x))
  val n = 10
  val conv = new ByIndex(trp.triples, spark)

  //  val id1 = conv.entities.select("ID").sample(false,0.2).take(n)
  //  val ind1 = id1.map( row => row(0).asInstanceOf[Long]).toSeq.toDS()
  //
  //  val r1 = conv.getEntitiesByIndex(ind1).persist()
  //  println(" count = ", r1.count)
  //  r1.show()
  //
  //  val id2 = conv.predicates.select("ID").sample(false, 0.2).take(n)
  //  val ind2 = id2.map( row => row(0).asInstanceOf[Long]).toSeq.toDS()
  //
  //  val r2 = conv.getPredicatesByIndex(ind2).persist()
  //  println(" count = ", r2.count)
  //  r2.show()
  //
  //
  println("\n\n------ TESTING -----")

  lazy val smp1 = trp.triples.take(n)
  lazy val sample1 = smp1.toSeq.toDF().asInstanceOf[Dataset[StringTriples]]

  sample1.show()

  // val r3 = conv.getTriplesByIndex(sample1)
  // r3.printSchema()
  // r3.show

  // val r4 = conv.getTriplesByString(r3)
  // println("<<< DONE >>>")
}
