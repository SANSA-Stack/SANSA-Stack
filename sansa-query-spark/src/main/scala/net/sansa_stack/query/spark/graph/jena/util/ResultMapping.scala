package net.sansa_stack.query.spark.graph.jena.util

import org.apache.jena.graph.Node
import org.apache.spark.graphx.Graph
import org.apache.spark.sql.SparkSession

/**
  * Generate mappings from variables in sparql query to vertices in target rdf graph
  *
  * @author Zhe Wang
  */
object ResultMapping {

  /**
    * run algorithm to generate solution mapping.
    *
    * @return basic graph pattern mapping.
    */
  def run(graph: Graph[Node, Node],
          bgp: BasicGraphPattern,
          session: SparkSession): Array[Map[Node, Node]] = {

    val ms = new MatchSetNode(graph, bgp, session)
    var finalMatchSet = ms.matchCandidateSet
    var tempMatchSet = ms.matchCandidateSet
    var changed = true
    while(changed) {
      tempMatchSet = ms.validateRemoteMatchSet(ms.validateLocalMatchSet(tempMatchSet))
      if(tempMatchSet.count().equals(finalMatchSet.count())){
        changed = false
      }
      finalMatchSet = tempMatchSet
    }
    //finalMatchSet.collect().foreach(println(_))

    var bgpMapping = Array[Map[Node,Node]]()
    bgp.triplePatterns.foreach{ tp =>
      val tpMapping = finalMatchSet
        .filter(_.pattern.equals(tp))
        .map(_.mapping).collect()
        .map(_.filterKeys(_.toString.startsWith("?")))
        .distinct
      if(bgpMapping.isEmpty){
        bgpMapping = tpMapping
      }
      else{
        bgpMapping = arrayOfMapJoin(bgpMapping, tpMapping).distinct
      }
    }
    bgpMapping
  }

  private def arrayOfMapJoin[Node](a: Array[Map[Node,Node]], b: Array[Map[Node,Node]]): Array[Map[Node,Node]] = {
    var c = Array[Map[Node,Node]]()
    if(a.head.keySet.intersect(b.head.keySet).isEmpty){   //two arrays have no common keys
      a.foreach(x => b.foreach(y => c = c :+ x.++(y)))
      c
    } else if(a.head.keySet.intersect(b.head.keySet).size == 1){  //two arrays has one common keys
      val intVar = a.head.keySet.intersect(b.head.keySet).head
      a.foreach(x =>
        b.foreach(y =>
          if(x.get(intVar).equals(y.get(intVar))){
            c = c :+ x.++(y)
          }))
      c
    } else {  //two arrays has two common keys
      val intVar = a.head.keySet.intersect(b.head.keySet)
      a.foreach(x =>
        b.foreach(y =>
          if(x.get(intVar.head).equals(y.get(intVar.head)) && x.get(intVar.tail.head).equals(y.get(intVar.tail.head))){
            c = c :+ x.++(y)
          }))
      c
    }
  }
}
