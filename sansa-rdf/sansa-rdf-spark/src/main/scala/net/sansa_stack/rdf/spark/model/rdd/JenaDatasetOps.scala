package net.sansa_stack.rdf.spark.model.rdd

import org.apache.jena.query.Dataset
import org.apache.jena.rdf.model.Resource

object JenaDatasetOps {

  import scala.collection.JavaConverters._

  /**
   * Natural resources of a dataset are all those IRIs that match the name of the enclosing
   * named graph
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
