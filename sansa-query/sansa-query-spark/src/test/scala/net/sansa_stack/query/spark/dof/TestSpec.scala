package net.sansa_stack.query.spark.dof

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import net.sansa_stack.query.spark.dof.sparql.{QueryExecutionFactory, Sparql}
import net.sansa_stack.query.spark.dof.tensor.{TensorRush, TensorStore}
import net.sansa_stack.query.spark.graph.jena.model.SparkExecutionModel
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.immutable.TreeMap
import scala.io.Source
import scala.jdk.CollectionConverters._
import scala.collection.parallel.CollectionConverters._

class TestSpec extends AnyFunSuite with DataFrameSuiteBase {
  val FROM_ID = 0
  val TO_ID = 26
  var FROM_TENSOR = 0
  var TO_TENSOR = 4
  var mode = "test"
  var externalTests = ""
  var externalQueryId = -1
  var runs = 0

  test("Query test") {
    SparkExecutionModel.createSparkSession(spark)
    load(tr)

    for (index <- FROM_ID to TO_ID)
      index match {
        case i if i < 18 || i == 23 => runInternalTest(i)
        case i if i < 21 => runInternalTest(i, 1)
        case i if i < 23 => runInternalTest(i, 3)
        case i if i < 27 => runInternalTest(i, 4)
        case _ => println(s"no test $index found")
    }
  }

  def runExternalTests: Unit = {
    val pattern = "\\w*?([\\d]+)\\.dat".r

    new java.io.File(externalTests).listFiles.filter(_.getName.endsWith(".dat")).sorted.foreach(
      q => {
        val name = q.getName
        val pattern(tid) = name
        val tensorId = tid.toInt

        if (tensorId >= FROM_TENSOR && tensorId <= TO_TENSOR) {
          assert(tr.getModels.contains(tensorId.toInt), s"\nTensor with ID=$tensorId does not exist. Check $name")

          val queries = new String(java.nio.file.Files.readAllBytes(q.toPath), java.nio.charset.StandardCharsets.UTF_8);
          queries.split(",").zipWithIndex.foreach {
            case (str, id) =>
              if (externalQueryId < 0 || id == externalQueryId) {
                for (fileNumber <- (0 to runs).par) runTest(id, str, tensorId.toInt)
              }
          }
        }
      })
  }

  lazy val tr: TensorRush = TensorStore.instantiateUniqueStore(spark)

  def load(tr: TensorRush, basePath: String = ""): Unit = {
    println("Loading datasets ... ")
    for (fileNumber <- (FROM_TENSOR to TO_TENSOR).par) {
      val resource = s"university0_$fileNumber.nt"

      val path = if (basePath == null || basePath.isEmpty) {
                   getClass.getResource(resource).getPath
                 } else {
                   basePath + resource
                 }

      tr.addTensor(path, fileNumber)
      println(s"File loaded $resource --> $path")
    }
    println("Finished loading datasets.")
  }

  type Bindings = TreeMap[String, String]
  type QuerySolution = List[Bindings]

  val queryBaseName = s"answers_query"

  def runInternalTest(queryId: Int, tensorId: Int = 0): Unit = {
    if (queryId >= FROM_ID && queryId <= TO_ID) {
      println(s"Query $queryId")
      runTest(queryId, Queries.sparqlQueries(queryId), tensorId)
    }
  }

  def runTest(queryId: Int, queryStr: String, tensorId: Int = 0): Unit =
    executeOnQueryEngine(queryId, queryStr, tensorId)

  def executeOnQueryEngine(queryId: Int, queryStr: String, tensorId: Int): Unit = {
    val model = tr.getModels(tensorId)
    val query = QueryExecutionFactory.makeQuery(queryStr)
    val result = Sparql(query, model)

    if (mode == "test") {
      val resultVars = query.getProjectVars.asScala.toList
      val resource = getClass.getResource(queryBaseName + tensorId + "_" + queryId + ".txt")

      val source = Source.fromURL(resource)
      val referenceResult = source.getLines().drop(1).toList.sortWith((x, y) => x < y)
      val rr = referenceResult.mkString("\n")

      val refRdd = spark.sparkContext.parallelize(referenceResult)
      val compare = model.compareResult(result, refRdd, resultVars)
      var output = ""
      if (!compare) {
        output = model.output(result, resultVars).mkString("\n")
      }
      assert(compare, s"TR result for query $queryId did not match reference result:\nTR=\n$output \nreference result=\n$rr.")
    }
  }

  def getQueryBindings(lines: Iterator[String]): QuerySolution = {
    var currentLine = lines.next()
    if (currentLine == "NO ANSWERS.") {
      // No bindings.
      List()
    } else {
      val variables = currentLine.split("\t").toIndexedSeq
      var solution = List[Bindings]()
      while (lines.hasNext) {
        var binding = TreeMap[String, String]()
        currentLine = lines.next()
        val values = currentLine.split("\t").toIndexedSeq
        for (i <- variables.indices) {
          binding += variables(i) -> values(i)
        }
        solution = binding :: solution
      }
      solution.sortBy(map => map.values)
    }
  }
}

