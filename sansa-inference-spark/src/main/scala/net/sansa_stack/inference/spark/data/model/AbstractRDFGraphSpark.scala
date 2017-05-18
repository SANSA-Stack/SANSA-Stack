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
abstract class AbstractRDFGraphSpark[D[T], N <: RDF#Node, T <: RDF#Triple, G <: AbstractRDFGraphSpark[D, N, T, G]](
  override val triples: D[T]
) extends AbstractRDFGraph[D, N, T, G](triples)
    with SparkGraphExtensions[N, T, G] { self: G =>
}
