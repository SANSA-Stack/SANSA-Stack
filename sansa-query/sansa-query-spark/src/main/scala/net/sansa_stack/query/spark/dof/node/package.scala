package net.sansa_stack.query.spark.dof

import org.apache.jena.graph.{ Node, Triple }
import org.apache.jena.sparql.core.Var
import org.apache.spark.rdd.RDD

package object node {
  type Triple3S = (String, String, String)
  type Triple3L = (Long, Long, Long)
  type SIndexed = Map[String, Long]
  val SIndexed = Map[String, Long]()
  type DofTriple = (Int, Triple)

  type Vars = Set[Var]
  val Vars = Set[Var]()

  type DofTripleList = List[DofTriple]
  val DofTripleList = List[DofTriple]()
  type Triples = Map[Int, DofTripleList]
  val Triples = Map[Int, DofTripleList]()

  type RddIndexed = RDD[(Node, Long)]
  type VarRDD[N] = Map[Var, RDD[N]]
}
