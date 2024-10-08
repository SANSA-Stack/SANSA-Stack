package net.sansa_stack.query.spark.graph.jena.util

import net.sansa_stack.query.spark.graph.jena.model.SparkExecutionModel
import org.apache.jena.graph.Node
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.util.control.Breaks._

/**
 * A match set of vertex v is the set of all candidates of the target vertex matching triple patterns
 * from basic graph pattern.
 *
 * @author Zhe Wang
 */
object MatchSet {

  type candidates = Iterable[MatchCandidate]

  /**
   * Create an RDD of vertices that set the vertex attributes as a set of match candidates.
   *
   * @param graph Input RDF graph
   * @param patterns Basic triple patterns
   * @return RDD for vertices. Each vertex has an attribute that is a set of match candidate for this vertex.
   */
  def createCandidateVertices(
    graph: Graph[Node, Node],
    patterns: Broadcast[List[TriplePattern]]): RDD[(VertexId, candidates)] = {

    val vertices = graph.triplets.flatMap { triplet =>

      val result = scala.collection.mutable.ArrayBuffer.empty[(VertexId, MatchCandidate)]

      patterns.value.foreach { pattern =>
        if (pattern.isFulfilledByTriplet(triplet)) {
          if (pattern.getSubjectIsVariable) {
            result += ((triplet.srcId, new MatchCandidate(triplet, pattern, NodeType.s)))
          }
          if (pattern.getPredicateIsVariable) {
            result += ((triplet.srcId, new MatchCandidate(triplet, pattern, NodeType.p)))
            result += ((triplet.dstId, new MatchCandidate(triplet, pattern, NodeType.p)))
          }
          if (pattern.getObjectIsVariable) {
            result += ((triplet.dstId, new MatchCandidate(triplet, pattern, NodeType.o)))
          }
        }
      }

      result
    }.groupByKey()

    vertices
  }

  /**
   *
   * @param graph Input RDF graph.
   * @param patterns Basic triple patterns
   * @return A candidate graph whose vertex attribute is a set of match candidates for each vertex.
   */
  def createCandidateGraph(
    graph: Graph[Node, Node],
    patterns: Broadcast[List[TriplePattern]]): Graph[candidates, Node] = {
    val vertices = createCandidateVertices(graph, patterns)
    val edges = graph.edges.cache()
    val defaultAttr = Iterable.empty[MatchCandidate]
    val candidateGraph = Graph.apply(vertices, edges, defaultAttr)

    vertices.unpersist()
    edges.unpersist()

    candidateGraph
  }

  /**
   * Filter match candidates. If a match candidate (?var, mapping, pattern) for variable ?var in vertex v is local
   * matched, it must have a match candidate regarding ?var for all triple patterns that contain ?var. Furthermore,
   * the mappings of all these match candidates must be compatible. We will remove candidate that don't have local
   * match from the attributes.
   *
   * @param candidateGraph A graph whose vertex attribute is a set of match candidates for a vertex v.
   * @param patterns Basic triple patterns
   * @return A graph whose vertex attributes a set of match candidates after removing those have no local match.
   */
  def localMatch(
    candidateGraph: Graph[candidates, Node],
    patterns: Broadcast[List[TriplePattern]]): Graph[candidates, Node] = {
    candidateGraph.mapVertices {
      case (_, iter) =>
        if (iter.isEmpty) {
          iter
        } else {
          // foreach candidate belongs to v.candidates do
          iter.filter { candidate =>
            var exists = true
            breakable {
              // foreach triple pattern belongs to pattens != candidate's triple pattern do
              patterns.value.filterNot(_.equals(candidate.pattern)).foreach { pattern =>
                // if candidate's variable in pattern's variable set then
                if (pattern.getVariable.contains(candidate.variable)) {
                  // if not exists other candidate meet the condition, set the exists false.
                  val numOfExists = iter.count { other =>
                    other.pattern.equals(pattern) && other.variable.equals(candidate.variable) && compatible(other.mapping, candidate.mapping)
                  }
                  if (numOfExists == 0) {
                    exists = false
                    break()
                  }
                }
              }
            }
            exists
          }
        }
    }
  }

  /**
   * Based on the candidate graph, for each vertex in graph, merging the candidates of its neighbours to a set and then
   * add it to its vertex attribute.
   * @param candidateGraph Graph has messages of each vertex's match candidates.
   * @return New graph that, for a vertex, contains both the candidates of its local match and neighbours'(call them
   *         remote candidates). The vertex attribute is a tuple2 that the first one is local candidates and the second
   *         one is remote candidates.
   */
  def joinNeighbourCandidate(candidateGraph: Graph[candidates, Node]): Graph[(candidates, candidates), Node] = {

    // Aggregate properties of neighboring vertices on a graph and merge them as one set for each vertex.
    val neighbourCandidate = candidateGraph.aggregateMessages[candidates](tripletFields =>
      {
        tripletFields.sendToDst(tripletFields.srcAttr)
        tripletFields.sendToSrc(tripletFields.dstAttr)
      }, (a, b) => a.++(b).toSet).cache()

    val vertices = candidateGraph.vertices.join(neighbourCandidate).cache()
    neighbourCandidate.unpersist()
    val edges = candidateGraph.edges.cache()
    val defaultAttr = (Iterable.empty[MatchCandidate], Iterable.empty[MatchCandidate])
    val mergedGraph = Graph.apply(vertices, edges, defaultAttr)
    vertices.unpersist()
    edges.unpersist()

    mergedGraph
  }

  /**
   * Filter match candidates. If a match candidate (?var, mapping, pattern) for variable ?var in vertex v is remote
   * matched, it must have a match candidate regarding another ?var2(!= ?var, ?var2 in pattern) in the remote candidates.
   * Furthermore, the mappings of all these match candidates must be compatible. We will remove candidate that don't have
   * remote match from the attributes.
   *
   * @param mergedGraph A graph whose vertex attribute is a tuple of its local and remote candidates for a vertex v.
   * @return A graph whose vertex attributes a set of match candidates after removing those have no local match.
   */
  def remoteMatch(mergedGraph: Graph[(candidates, candidates), Node]): Graph[candidates, Node] = {
    mergedGraph.mapVertices {
      case (_, (local, remote)) =>
        if (local.isEmpty) {
          local
        } else {
          // foreach local candidate belongs to v.candidates do
          local.filter { candidate =>
            var exists = true
            // ?var2 = pattern.var - ?var
            val var2 = candidate.pattern.getVariable.filterNot(_.equals(candidate.variable))
            // if ?var2 != Null then
            if (var2.length != 0) {
              // if not exists other candidate meet the condition, set the exists false.
              val numOfExists = remote.count { other =>
                other.variable.equals(var2.head) && other.pattern.equals(candidate.pattern) && compatible(other.mapping, candidate.mapping)
              }
              if (numOfExists == 0) {
                exists = false
              }
            }
            exists
          }
        }
    }
  }

  /**
   * Produce this final results that match the triple patterns in RDF graph.
   */
  def generateResultRDD(
    candidateGraph: Graph[candidates, Node],
    patterns: Broadcast[List[TriplePattern]],
    spark: SparkSession): RDD[Result[Node]] = {
    val matchSet = candidateGraph.vertices.filter { case (_, candidate) => candidate.nonEmpty }
      .flatMap {
        case (_, candidates) =>
          candidates
      }.cache()
    candidateGraph.unpersist()

    var intermediate: RDD[Result[Node]] = null
    patterns.value.foreach { pattern =>
      val mapping = matchSet
        .filter(_.pattern.equals(pattern))
        .map(_.mapping).collect()
        .map(_.filterKeys(_.toString().startsWith("?")))
        .map(_.toMap)
        .distinct
      if (mapping.isEmpty) {
        throw new UnknownError("No results were returned by the query")
      }
      if (intermediate == null) {
        intermediate = ResultFactory.create(mapping, spark)
      } else {
        intermediate = SparkExecutionModel.leftJoin(intermediate, ResultFactory.create(mapping, spark))
      }
    }
    matchSet.unpersist()

    intermediate
  }

  /**
   * Check whether two maps have conflicts(if they have shared keys, the values must be equal).
   */
  private def compatible(map1: Map[Node, Node], map2: Map[Node, Node]): Boolean = {
    val interKeySet = map1.keySet.intersect(map2.keySet)
    var compatible = true
    if (interKeySet.isEmpty) {
      compatible
    } else {
      interKeySet.foreach { v =>
        if (!map1(v).equals(map2(v))) {
          compatible = false
        }
      }
      compatible
    }
  }
}
