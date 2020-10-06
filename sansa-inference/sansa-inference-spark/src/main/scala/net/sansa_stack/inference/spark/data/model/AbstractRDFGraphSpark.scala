package net.sansa_stack.inference.spark.data.model

import net.sansa_stack.inference.data._

/**
  * A data structure that comprises a collection of triples. Note, due to the implementation of the Spark
  * datastructures, this doesn't necessarily mean to be free of duplicates which is why a `distinct` operation
  * is provided.
  *
  * @author Lorenz Buehmann
  *
  */
abstract class AbstractRDFGraphSpark[Rdf <: RDF, D, G <: AbstractRDFGraphSpark[Rdf, D, G]](
  override val triples: D
) extends AbstractRDFGraph[Rdf, D, G](triples)
    with SparkGraphExtensions[Rdf, D, G] { self: G =>
}
