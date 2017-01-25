package net.sansa_stack.inference.spark.forwardchaining

import org.apache.jena.rdf.model.Resource
import org.apache.jena.vocabulary.{RDF, RDFS}
import org.apache.spark.sql.functions.{broadcast, lit}
import org.apache.spark.sql.{Column, Dataset, SparkSession}

import net.sansa_stack.inference.data.RDFTriple
import net.sansa_stack.inference.spark.data.RDFGraphDataset

/**
  * @author Lorenz Buehmann
  */
class ForwardRuleReasonerRDFSDataset(session: SparkSession, parallelism: Int = 2)
  extends AbstractForwardRuleReasonerRDFS[Dataset[RDFTriple], RDFGraphDataset](session, parallelism) {

  import session.implicits._

  var subprops: Dataset[RDFTuple]
  var subclasses: Dataset[RDFTuple]
  var domains: Dataset[RDFTuple]
  var ranges: Dataset[RDFTuple]

  case class RDFTuple(s: String, o: String)

  implicit def resourceConverter(resource: Resource): String = resource.getURI

  def extractTuplesForProperty(graph: RDFGraphDataset, p: String): Set[(String, String)] = {
    graph.triples
      .filter($"p" === p)
      .select("s", "o")
      .collect
      .map(row => (row.getAs[String](0), row.getAs[String](1))).toSet
  }

  override def preprocess(graph: RDFGraphDataset): RDFGraphDataset = {

    subprops = broadcast(
      computeTransitiveClosure(extractTuplesForProperty(graph, RDFS.subPropertyOf)).toSeq.toDF("s", "o").as[RDFTuple]
    ).as("subproperties")

    subclasses = broadcast(
      computeTransitiveClosure(extractTuplesForProperty(graph, RDFS.subClassOf)).toSeq.toDF("s", "o").as[RDFTuple]
    ).as("subclasses")

    domains = broadcast(graph.triples.filter($"p" === RDFS.domain).select("s", "o").as[RDFTuple]).as("domains")

    ranges = broadcast(graph.triples.filter($"p" === RDFS.range).select("s", "o").as[RDFTuple]).as("ranges")

    graph
  }

  override def postprocess(graph: RDFGraphDataset): RDFGraphDataset = {
    graph
  }

  override def rule5(graph: RDFGraphDataset): RDFGraphDataset = {
    val g = graph.find(None, Some(RDFS.subPropertyOf), None)
    new RDFGraphDataset(computeTransitiveClosure(g.triples))
  }

  override def rule11(graph: RDFGraphDataset): RDFGraphDataset = {
    val g = graph.find(None, Some(RDFS.subClassOf), None)
    new RDFGraphDataset(computeTransitiveClosure(g.triples))
  }

  override def rule2(graph: RDFGraphDataset): RDFGraphDataset = {
    new RDFGraphDataset(
      graph.triples
        .join(broadcast(domains), $"r7.p" === $"domains.s", "inner")
        .select($"r7.s", $"r7.p", $"domains.o")
        .as[RDFTriple]
    )
  }

  override def rule3(graph: RDFGraphDataset): RDFGraphDataset = {
    new RDFGraphDataset(
      graph.triples
        .join(broadcast(ranges), $"r7.p" === $"ranges.s", "inner")
        .select($"r7.o".alias("s"), $"r7.p", $"ranges.o")
        .as[RDFTriple]
    )
  }

  override def rule7(graph: RDFGraphDataset): RDFGraphDataset = {
    new RDFGraphDataset(
      graph.triples
        .join(broadcast(subprops), $"triples.p" === $"subproperties.s", "inner")
        .select($"triples.s", $"subproperties.o".alias("p"), $"triples.o")
        .as[RDFTriple]
    )
  }

  override def rule9(graph: RDFGraphDataset): RDFGraphDataset = {
    new RDFGraphDataset(
      graph.triples
        .join(broadcast(subclasses), graph.triples("o") === $"subclasses.s", "inner")
        .select(graph.triples("s"), lit(RDF.`type`), $"subclasses.o")
        .as[RDFTriple]
    )
  }


}
