package net.sansa_stack.rdf.flink.qualityassessment.metrics.completeness

import net.sansa_stack.rdf.flink.data.RDFGraph
import net.sansa_stack.rdf.flink.qualityassessment.dataset.DatasetUtils
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.jena.graph.{ Node, Triple }

/**
 * This metric measures the interlinking completeness. Since any resource of a
 * dataset can be interlinked with another resource of a foreign dataset this
 * metric makes a statement about the ratio of interlinked resources to
 * resources that could potentially be interlinked.
 *
 * An interlink here is assumed to be a statement like
 *
 *   <local resource> <some predicate> <external resource>
 *
 * or
 *
 *   <external resource> <some predicate> <local resource>
 *
 * Local resources are those that share the same URI prefix of the considered
 * dataset, external resources are those that don't.
 *
 * Zaveri et. al [http://www.semantic-web-journal.net/system/files/swj414.pdf]
 */
object InterlinkingCompleteness extends Serializable {
  @transient var env: ExecutionEnvironment = _
  val prefixes = DatasetUtils.getPrefixes()

  def apply(rdfgraph: RDFGraph): Long = {

    /**
     * isIRI(?s) && internal(?s) && isIRI(?o) && external(?o)
     * union
     * isIRI(?s) && external(?s) && isIRI(?o) && internal(?o)
     */
    val dataset = rdfgraph.triples

    val Interlinked =
      dataset.filter(f =>
        f.getSubject.isURI() && isInternal(f.getSubject) && f.getObject.isURI() && isExternal(f.getObject))
        .union(
          dataset.filter(f =>
            f.getSubject.isURI() && isExternal(f.getSubject) && f.getObject.isURI() && isInternal(f.getObject)))

    val numSubj = Interlinked.map(_.getSubject).distinct().count()
    val numObj = Interlinked.map(_.getSubject).distinct().count()

    val numResources = numSubj + numObj
    val numInterlinkedResources = Interlinked.count()

    val value = if (numResources > 0) {
      numInterlinkedResources / numResources
    } else 0

    value
  }

  /**
   *  Checks if a resource ?node is local
   */
  def isInternal(node: Node): Boolean = prefixes.contains(if (node.isLiteral) node.getLiteralLexicalForm else node.toString())

  /**
   *  Checks if a resource ?node is local
   */
  def isExternal(node: Node): Boolean = !prefixes.contains(if (node.isLiteral) node.getLiteralLexicalForm else node.toString())
}
