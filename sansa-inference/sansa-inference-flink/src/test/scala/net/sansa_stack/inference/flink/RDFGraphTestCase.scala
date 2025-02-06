package net.sansa_stack.inference.flink

import java.util
import java.util.Comparator

import scala.jdk.CollectionConverters._

import com.google.common.collect.ComparisonChain
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.apache.flink.test.util.{MultipleProgramsTestBase, TestBaseUtils}
import org.apache.jena.graph.{NodeFactory, Triple}
import org.apache.jena.sparql.util.TripleComparator
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import net.sansa_stack.inference.data.RDFTriple
import net.sansa_stack.inference.flink.data.RDFGraph

/**
  * A test case for the computation of the transitive closure (TC).
  * @author Lorenz Buehmann
  */
@RunWith(classOf[Parameterized])
class RDFGraphTestCase(mode: TestExecutionMode) extends MultipleProgramsTestBase(mode) {

  @Test
  def testSubtract(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val s1 = NodeFactory.createURI("s1")
    val p1 = NodeFactory.createURI("p1")
    val o1 = NodeFactory.createURI("o1")
    val o2 = NodeFactory.createURI("o2")
    val o3 = NodeFactory.createURI("o3")

    // generate dataset
    val g1 = RDFGraph(env.fromCollection(
      Seq(
        Triple.create(s1, p1, o1),
        Triple.create(s1, p1, o2),
        Triple.create(s1, p1, o3)
      )
    ))
    val g2 = RDFGraph(env.fromCollection(
      Seq(
        Triple.create(s1, p1, o1),
        Triple.create(s1, p1, o2)
      )
    ))

    // compute
    val g_diff = g1.subtract(g2)

    val result = g_diff.triples.collect()
    val expected = Seq(
      Triple.create(s1, p1, o3)
    )

    TestBaseUtils.compareResultCollections(
      new util.ArrayList(result.asJava),
      new util.ArrayList(expected.asJava),
      new TripleComparator())
  }
}
