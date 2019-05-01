package net.sansa_stack.query.spark.dof

import collection.JavaConverters._
import net.sansa_stack.query.spark.dof.node.Helper
import net.sansa_stack.query.spark.dof.sparql.{ QueryExecutionFactory, Sparql }
import net.sansa_stack.query.spark.dof.tensor.{ TensorRush, TensorStore }
import org.scalatest.ConfigMap
import scala.collection.immutable.TreeMap
import scala.io.Source

// @RunWith(value = classOf[Parameterized])
class TestSpec extends TestBase {
  var logger: Logger = null
  var FROM_ID = 0
  var TO_ID = 0
  var FROM_TENSOR = 0
  var TO_TENSOR = 0
  var mode = ""
  var externalTests = ""
  var externalQueryId = -1
  var runs = 0

  override def beforeAll(configMap: ConfigMap): Unit = {
    def getParam(name: String, default: String = "") =
      if (configMap.contains(name)) configMap.getOrElse(name, "").toString
      else default

    println("Args passed: " + configMap.mkString(" "))
    FROM_ID = getParam("from_id", "0").toInt // 26 use 23 to run all tests when building in IDE or in maven
    TO_ID = getParam("to_id", "26").toInt // 0 use 23 to run all tests when building in IDE or in maven
    mode = getParam("mode", "test") // "" use "test" to run all tests when building in IDE or in maven
    FROM_TENSOR = getParam("from_tensor", "0").toInt
    TO_TENSOR = getParam("to_tensor", "4").toInt
    init(getParam("master"), getParam("output_dir"))
    load(tr, getParam("path"))

    externalTests = getParam("externalQueries", "") // "/home/hduser/dof/queries" external tests dir
    externalQueryId = getParam("externalQueryId", "-1").toInt
    runs = getParam("runs", "0").toInt // for warm/no-warm test running

    val useHelper = getParam("useHelper", "false").toBoolean
    Helper.DEBUG = useHelper
    Helper.SHOW_TIME_MEASUREMENT = useHelper

    logger = new Logger(getParam("useLogger", "false").toBoolean)
    logger.log("\n") // try to keep first log line
  }

  run

  def runExternalTests: Unit = {
    val pattern = "\\w*?([\\d]+)\\.dat".r

    new java.io.File(externalTests).listFiles.sorted.filter(_.getName.endsWith(".dat")).foreach(
      q => {
        val name = q.getName
        val pattern(tid) = name
        val tensorId = tid.toInt

        if (tensorId >= FROM_TENSOR && tensorId <= TO_TENSOR) {
          assert(tr.getModels.contains(tensorId.toInt), s"\nTensor with ID=$tensorId does not exist. Check $name")

          val queries = new String(java.nio.file.Files.readAllBytes(q.toPath()), java.nio.charset.StandardCharsets.UTF_8);
          queries.split(",").zipWithIndex.foreach {
            case (str, id) =>
              if (externalQueryId < 0 || id == externalQueryId) {
                for (fileNumber <- (0 to runs).par) runTest(id, str, tensorId.toInt)
              }
          }
        }
      })
  }

  def run: Unit = {

    "Query 0" should "match the reference results" in {
      runInternalTest(0, 0)
    }

    "Query 1" should "match the reference results" in {
      runInternalTest(1)
    }

    "Query 2" should "match the reference results" in {
      runInternalTest(2)
    }

    "Query 3" should "match the reference results" in {
      runInternalTest(3)
    }

    "Query 4" should "match the reference results" in {
      runInternalTest(4)
    }

    "Query 5" should "match the reference results" in {
      runInternalTest(5)
    }

    "Query 6" should "match the reference results" in {
      runInternalTest(6)
    }

    "Query 7" should "match the reference results" in {
      runInternalTest(7)
    }

    "Query 8" should "match the reference results" in {
      runInternalTest(8)
    }

    "Query 9" should "match the reference results" in {
      runInternalTest(9)
    }

    "Query 10" should "match the reference results" in {
      runInternalTest(10)
    }

    "Query 11" should "match the reference results" in {
      runInternalTest(11)
    }

    "Query 12" should "match the reference results" in {
      runInternalTest(12)
    }

    "Query 13" should "match the reference results" in {
      runInternalTest(13)
    }

    "Query 14" should "match the reference results" in {
      runInternalTest(14)
    }

    "Query 15" should "match the reference results" in {
      runInternalTest(15)
    }

    "Query 16" should "match the reference results" in {
      runInternalTest(16)
    }

    "Query 17" should "match the reference results" in {
      runInternalTest(17)
    }

    "Query 18" should "match the reference results" in {
      runInternalTest(18, 1)
    }

    "Query 19" should "match the reference results" in {
      runInternalTest(19, 1)
    }

    "Query 20" should "match the reference results" in {
      runInternalTest(20, 1)
    }

    "Query 21" should "match the reference results" in {
      runInternalTest(21, 3)
    }

    "Query 22" should "match the reference results" in {
      runInternalTest(22, 3)
    }

    "Optional 23" should "match the reference results" in {
      runInternalTest(23)
    }

    "Watdiv 24" should "match the reference results" in {
      runInternalTest(24, 4)
    }

    "Watdiv 25" should "match the reference results" in {
      runInternalTest(25, 4)
    }

    "Watdiv 26" should "match the reference results" in {
      runInternalTest(26, 4)
    }

    "Dbpedia 27" should "match the reference results" in {
      runInternalTest(27, 5)
    }

    "Dbpedia 28" should "match the reference results" in {
      runInternalTest(28, 5)
    }

    "External test" should "init beforeAll" in {
      if (externalTests != "") runExternalTests
    }
  }

  lazy val tr: TensorRush = TensorStore.instantiateUniqueStore(spark)

  def load(tr: TensorRush, basePath: String = ""): Unit = {
    println("Loading LUBM1 ... ")
    // val start = Helper.start
    for (fileNumber <- (FROM_TENSOR to TO_TENSOR).par) {
      val resource = s"university0_$fileNumber.nt"

      val path = if (basePath == null || basePath.isEmpty) getClass.getResource(resource).getPath()
      else basePath + resource
      // val path = "hdfs://localhost:9000/user/hduser/" + resource
      println(s"Loading file $resource --> $path")

      val start = Helper.start
      tr.addTensor(path, fileNumber)
      Helper.measureTime(start, s"Tensor creation time for $resource")
      println(s"Done loading $resource.")
    }
    // Helper.measureTime(start, "Total tensor creation time")
    println("Finished loading LUBM1.")
  }

  type Bindings = TreeMap[String, String]
  type QuerySolution = List[Bindings]

  val queryBaseName = s"answers_query"

  def runInternalTest(queryId: Int, tensorId: Int = 0): Unit = {
    if (queryId >= FROM_ID && queryId <= TO_ID) {
      runTest(queryId, Tests.sparqlQueries(queryId), tensorId)
    }
  }

  def runTest(queryId: Int, queryStr: String, tensorId: Int = 0): Unit = {
    Helper.log("\nQuery " + queryId)
    val start = Helper.start
    val result = executeOnQueryEngine(queryId, queryStr, tensorId)
    Helper.measureTime(start, s"\nTime measurement Query $queryId")
  }

  def executeOnQueryEngine(queryId: Int, queryStr: String, tensorId: Int): Unit = {
    val model = tr.getModels(tensorId)
    val query = QueryExecutionFactory.makeQuery(queryStr)
    val start = Helper.start
    val result = Sparql(query, model)
    val long = Helper.measure(start, s"Total querying time")
    Helper.log("\n" + long)
    logger.log(s"Tensor $tensorId. Query $queryId")
    logger.log(result)
    logger.log(s"$long\n")

    if (mode == "test") {
      val resultVars = query.getProjectVars.asScala.toList
      val resource = getClass.getResource(queryBaseName + tensorId + "_" + queryId + ".txt")

      val source = Source.fromURL(resource)
      val referenceResult = source.getLines.drop(1).toList.sortWith((x, y) => x < y)
      val rr = referenceResult.mkString("\n")

      val refRdd = spark.sparkContext.parallelize(referenceResult)
      val compare = model.compareResult(result, refRdd, resultVars)
      var output = ""
      if (!compare) {
        output = model.output(result, resultVars).mkString("\n")
        Helper.log(output)
      }
      assert(compare, s"TR result for query $queryId did not match reference result:\nTR=\n$output \nreference result=\n$rr.")
    }
  }

  def getQueryBindings(lines: Iterator[String]): QuerySolution = {
    var currentLine = lines.next
    if (currentLine == "NO ANSWERS.") {
      // No bindings.
      List()
    } else {
      val variables = currentLine.split("\t").toIndexedSeq
      var solution = List[Bindings]()
      while (lines.hasNext) {
        var binding = TreeMap[String, String]()
        currentLine = lines.next
        val values = currentLine.split("\t").toIndexedSeq
        for (i <- 0 until variables.size) {
          binding += variables(i) -> values(i)
        }
        solution = binding :: solution
      }
      solution.sortBy(map => map.values)
    }
  }
}

