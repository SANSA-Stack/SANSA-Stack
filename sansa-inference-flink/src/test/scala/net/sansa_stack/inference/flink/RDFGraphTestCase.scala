package net.sansa_stack.inference.flink

import java.util.Comparator

import com.google.common.collect.ComparisonChain
import net.sansa_stack.inference.flink.data.RDFGraph
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.apache.flink.test.util.{MultipleProgramsTestBase, TestBaseUtils}
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import net.sansa_stack.inference.data.RDFTriple

import scala.collection.JavaConverters._

/**
  * A test case for the computation of the transitive closure (TC).
  * @author Lorenz Buehmann
  */
@RunWith(classOf[Parameterized])
class RDFGraphTestCase(mode: TestExecutionMode) extends MultipleProgramsTestBase(mode) {

  @Test
  def testSubtract(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment


    // generate dataset
    val g1 = RDFGraph(env.fromCollection(
      Seq(
        RDFTriple("s1", "p1", "o1"),
        RDFTriple("s1", "p1", "o2"),
        RDFTriple("s1", "p1", "o3")
      )
    ))
    val g2 = RDFGraph(env.fromCollection(
      Seq(
        RDFTriple("s1", "p1", "o1"),
        RDFTriple("s1", "p1", "o2")
      )
    ))

    // compute
    val g_diff = g1.subtract(g2)

    val result = g_diff.triples.collect()
    val expected = Seq(
      RDFTriple("s1", "p1", "o3")
    )

    TestBaseUtils.compareResultCollections(result.asJava, expected.asJava, new Comparator[RDFTriple] {
      override def compare(t1: RDFTriple, t2: RDFTriple): Int =
        ComparisonChain.start()
          .compare(t1.s, t2.s)
          .compare(t1.p, t2.p)
          .compare(t1.o, t2.o)
        .result()
    })
  }

}
