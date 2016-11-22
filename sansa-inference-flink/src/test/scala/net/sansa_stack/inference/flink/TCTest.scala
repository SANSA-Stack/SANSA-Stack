package net.sansa_stack.inference.flink

import org.apache.flink.api.common.functions.RichJoinFunction
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.test.util.{MultipleProgramsTestBase, TestBaseUtils}
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.apache.flink.util.Collector
import org.apache.jena.vocabulary.RDFS
import org.junit.{After, Before, Rule, Test}
import org.junit.rules.TemporaryFolder

import scala.collection.mutable
import org.apache.flink.api.scala._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import net.sansa_stack.inference.data.RDFTriple

/**
  * A test case for the computation of the transitive closure (TC).
  * @author Lorenz Buehmann
  */
@RunWith(classOf[Parameterized])
class TCTest(mode: TestExecutionMode) extends MultipleProgramsTestBase(mode) {

  val ns = "http://example.org/"
  val p1 = RDFS.subClassOf.getURI

  val _tempFolder = new TemporaryFolder()

  private var resultPath: String = null
  private var expectedResult: String = ""

  @Rule
  def tempFolder = _tempFolder

  @Before
  def before(): Unit = {
    resultPath = tempFolder.newFile("flink-tcTest").toURI.toString
  }

  @After
  def after(): Unit = {
    TestBaseUtils.compareResultsByLinesInMemory(expectedResult, resultPath)
  }

  @Test
  def testNaiveDataSimple(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val scale = 1
    val length = 10

    // generate dataset
    val ds = getDataSimple(env, scale)

    // compute
    val res = performNaive(ds)
      .partitionByRange(0)
      .sortPartition(0, order = Order.ASCENDING)
      .sortPartition(1, order = Order.ASCENDING)

    // write to disk
    res.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)

    env.execute()

    // return expected result
    expectedResult = getExpectedResultSimple(scale)
  }

  @Test
  def testNaiveDataSinglePath(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val length = 10

    // generate dataset
    val ds = getDataSinglePath(env, length)

    // compute
    val res = performNaive(ds)
      .partitionByRange(0)
      .sortPartition(0, order = Order.ASCENDING)
      .sortPartition(1, order = Order.ASCENDING)

    // write to disk
    res.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)

    env.execute()

    // return expected result
    expectedResult = getExpectedResultSinglePath(length)
  }

  def performNaive(ds: DataSet[RDFTriple]): DataSet[(String, String)] = {
    log.info("naive computation...")

    // convert to tuples
    val tuples = ds.map(t => (t.subject, t.`object`))

    // perform fix point iteration
    var i = 1
    val res = tuples.iterateWithTermination(10) {
      prevPaths: DataSet[(String, String)] =>
        log.info(s"iteration $i")
        println(i)

        val nextPaths = prevPaths
          .join(tuples).where(1).equalTo(0)(
          new RichJoinFunction[(String, String), (String, String), (String, String)] {
            override def join(left: (String, String), right: (String, String)): (String, String) = {
//              val context = getIterationRuntimeContext
//              println("Iteration #" + context.getSuperstepNumber)
//              println(context.getIndexOfThisSubtask + "/" + context.getNumberOfParallelSubtasks)
              (left._1, right._2)
            }
          }
        )
//        {
//          (left, right) => (left._1, right._2)
//        }
          .union(prevPaths)
          .groupBy(0, 1)
          .reduce((l ,r) => l)

        val terminate = prevPaths
          .coGroup(nextPaths)
          .where(0).equalTo(0) {
          (prev, next, out: Collector[(String, String)]) => {
            val prevPaths = prev.toSet
            for (n <- next)
              if (!prevPaths.contains(n)) out.collect(n)
          }
        }.withForwardedFieldsSecond("*")

        i += 1

        (nextPaths, terminate)
    }

    res
  }

  def performSemiNaive(ds: DataSet[RDFTriple]): DataSet[(String, String)] = {
    log.info("semi-naive computation...")

    // convert to tuples
    val tuples = ds.map(t => (t.subject, t.`object`))

    val maxIterations = 100
    val keyPosition = 0

    val initialSolutionSet = tuples
    val initialWorkset = tuples

//    val res = initialSolutionSet.iterateDelta(initialWorkset, maxIterations, Array(keyPosition)) {
//      (solution, workset) =>
//        val deltas = workset.join(solution).where(1).equalTo(0){
//          (prev, next, out: Collector[(String, String)]) => {
//            val prevPaths = prev.toSet
//            for (n <- next)
//              if (!prevPaths.contains(n)) out.collect(n)
//          }
//        }
//
//        val nextWorkset = deltas.filter(new FilterByThreshold())
//
//        (deltas, nextWorkset)
//    }
//    res

    tuples
  }

  def getDataSimple(env: ExecutionEnvironment, scale: Int = 1) : DataSet[RDFTriple] = {
    val triples = new mutable.HashSet[RDFTriple]()

    val begin = 1
    val end = 10 * scale

    for(i <- begin to end) {
      triples += RDFTriple(ns + "x" + i, p1, ns + "y" + i)
      triples += RDFTriple(ns + "y" + i, p1, ns + "z" + i)
    }

    env.fromCollection(triples)
  }

  def getExpectedResultSimple(scale: Int = 1) : String = {
    var res = ""

    for(i <- 1 to scale * 10) {
      res += s"${ns}x$i,${ns}y$i\n"
      res += s"${ns}y$i,${ns}z$i\n"
      res += s"${ns}x$i,${ns}z$i\n"
    }
    res = res.split("\\n").sorted.mkString("\n")
    res
  }

  def getDataSinglePath(env: ExecutionEnvironment, length: Int = 10) : DataSet[RDFTriple] = {
    val triples = new mutable.HashSet[RDFTriple]()

    // graph is a path of length n
    // (x1, p, x2), (x2, p, x3), ..., (x(n-1), p, xn)
    val n = 10
    for(i <- 1 until length) {
      triples += RDFTriple(ns + "x" + i, p1, ns + "x" + (i+1))
    }

    env.fromCollection(triples)
  }

  def getExpectedResultSinglePath(length: Int = 10) : String = {
    var res = ""

    for(i <- 1 to length) {
      for(j <- i+1 to length) {
        res += s"${ns}x$i,${ns}x${j}\n"
      }
    }
    res = res.split("\\n").sorted.mkString("\n")
    res
  }
}
