package net.sansa_stack.rdf.flink.qualityassessment.metrics.licensing

import net.sansa_stack.rdf.common.qualityassessment.utils.NodeUtils._
import org.apache.flink.api.scala.DataSet
import org.apache.jena.graph.{ Node, Triple }

/**
 * @author Gezim Sejdiu
 */
object MachineReadableLicense {

  /**
   * Machine -readable indication of a license
   * This metric checks whether a machine-readable text, stating the of licensing model
   * attributed to the resource, has been provided as part of the dataset.
   * It looks for objects containing literal values and analyzes the text searching for key, licensing related terms.
   */
  def assessMachineReadableLicense(dataset: DataSet[Triple]): Double = {
    val hasAssociatedLicense = dataset.filter(f => hasLicenceAssociated(f.getPredicate))
    if (hasAssociatedLicense.count() > 0) 1.0 else 0.0
  }
}
