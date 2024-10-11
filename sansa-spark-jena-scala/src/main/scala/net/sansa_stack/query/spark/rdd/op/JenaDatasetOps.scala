package net.sansa_stack.query.spark.rdd.op

import org.apache.jena.query.Dataset
import org.apache.jena.rdf.model.Resource

object JenaDatasetOps {

  import scala.jdk.CollectionConverters._

  /**
   * Natural resources of a dataset are all those IRIs that match the name of the enclosing
   * named graph.
   *
   * For example, in the example below a {@link Resource} of dbr:Leipzig
   * is the 'natural resource' in that named graph.
   * <pre>
   * dbr:Leipzig {
   * dbr:Leipzig a dbo:City .
   * eg:foo a eg:Bar .
   * }
   * </pre>
   *
   * TODO Change Resource result to ResourceInDataset in order to retain the Dataset reference
   */
  def naturalResources(dataset: Dataset): Seq[Resource] = {
    dataset.listNames().asScala.map(name => {
      val model = dataset.getNamedModel(name)
      model.createResource(name)
    }).toSeq
  }

}
