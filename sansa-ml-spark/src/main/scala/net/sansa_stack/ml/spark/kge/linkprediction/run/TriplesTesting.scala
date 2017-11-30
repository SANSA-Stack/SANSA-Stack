package tensor.ml.kge.run

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import scala.util.Random
import org.apache.spark.sql.functions._

import net.sansa_stack.ml.spark.kge.linkprediction.dataframe.Triples

object TriplesTesting extends App {

  def printType[T](x: T): Unit = { println(x.getClass.toString()) }

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val spark = SparkSession.builder
    .master("local")
    .appName("TransE")
    .getOrCreate()
  import spark.implicits._

  spark.sparkContext.setLogLevel("ERROR")

  println("<<< STARTING >>>")

  var train: Triples = new Triples("train", "/home/hamed/PROGRAMS/git/Spark-Tensors/src/main/scala/tensor/ml/kge/dataset/DataSets/FB15k/freebase_mtr100_mte100-train.txt", spark)
  //	var test : Triples = new Triples("test",spark,"/home/hamed/PROGRAMS/git/Spark-Tensors/src/main/scala/tensor/ml/kge/dataset/DataSets/FB15k/freebase_mtr100_mte100-test.txt")
  //	var valid : Triples = new Triples("train",spark,"/home/hamed/PROGRAMS/git/Spark-Tensors/src/main/scala/tensor/ml/kge/dataset/DataSets/FB15k/freebase_mtr100_mte100-valid.txt")

  var selected = train.triples.head(20)

  val tmp = spark.sqlContext.createDataFrame(spark.sparkContext.parallelize(selected), train.schema)

  train.triples.show()

  val corrupted = train.fun2(tmp)

  corrupted.persist().count()

  println("\nSelected to be corrupted:")
  tmp.sort("Predicate").show()

  println("Corrupted:")
  corrupted.sort("Predicate").show()

  println("ended !!!!!!!")

  //	val z = train.corruptSubjectOrObject2(tmp)
  //	z.show()
  //	println("train count= ",train.triples.count() )
  //	println("z count= ",z.count() )

  //	train.triples.show()
  //	train.corruptSubjectOrObject(train.triples.sample(false, .05),.3).show()

  //	println("Total rows="+train.triples.count())
  //	println("Num Distinct Entities = "+train.getAllDistinctEntities().count().toString())
  //	println("Num Distinct Predicates = "+train.getAllDistinctPredicates().count().toString())

  //	val ent = train.getAllDistinctEntities()
  //	val pred = train.getAllDistinctPredicates()
  //	
  //	println(ent.zipWithIndex().top(10).foreach(println))
  //	println(pred.zipWithIndex().top(10).foreach(println))
  //
  //	
  //	println("Num Distinct entities in test = "+test.getAllDistinctEntities().count()) 
  //	println("num dist ent joint = "+(train.getAllDistinctEntities() ++ test.getAllDistinctEntities() ).distinct().count())
  //	println("num dist pred joint = "+(train.getAllDistinctPredicates() ++ test.getAllDistinctPredicates() ).distinct().count())

  //	System.exit(0)

  //	
  //	println("++++++++++++++++++++++++++++++++++")
  //	val tt = new RDDTriples("train",spark,"./DataSets/FB15k/freebase_mtr100_mte100-train.txt")
  //	
  //	tt.triples.collect().take(10).foreach(println)
  //	println( tt.getAllDistinctEntities().count() )
  //	println( tt.getAllDistinctPredicates().count() )
  //	val r = tt.triples.sample(false, 0.01)
  //	println("count = ",r.count().toString() )
  //	tt.corruptSubjectOrObject(r).count()

  println("<<< DONE >>>")
}