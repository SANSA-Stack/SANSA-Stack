package net.sansa_stack.rdf.spark.qualityassessment.metrics.licensing

import net.sansa_stack.rdf.spark.qualityassessment.utils.NodeUtils._
import org.apache.jena.graph.{ Node, Triple }
import org.apache.spark.rdd.RDD

/**
 * @author Gezim Sejdiu
 */
object HumanReadableLicense {

  /**
   * Human -readable indication of a license
   * This metric checks whether a human-readable text, stating the of licensing model
   * attributed to the resource, has been provided as part of the dataset.
   * It looks for objects containing literal values and analyzes the text searching for key, licensing related terms.
   */
  def assessHumanReadableLicense(dataset: RDD[Triple]): Double = {

    val hasValidLicense = dataset.filter { f =>
      f.getSubject.isURI() && hasLicenceIndications(f.getPredicate) &&
        f.getObject.isLiteral() && isLicenseStatement(f.getObject)
    }

    if (hasValidLicense.count() > 0) 1.0 else 0.0
  }

}
