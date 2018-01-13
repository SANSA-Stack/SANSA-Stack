package net.sansa_stack.rdf.spark.qualityassessment.metrics.licensing

import org.apache.spark.rdd.RDD
import org.apache.jena.graph.{ Triple, Node }
import net.sansa_stack.rdf.spark.qualityassessment.utils.NodeUtils._
import net.sansa_stack.rdf.spark.qualityassessment.vocabularies.DQV

object MachineReadableLicense {
  implicit class MachineReadableLicenseFunctions(dataset: RDD[Triple]) extends Serializable {

    /**
     * Machine -readable indication of a license
     * This metric checks whether a machine-readable text, stating the of licensing model
     * attributed to the resource, has been provided as part of the dataset.
     * It looks for objects containing literal values and analyzes the text searching for key, licensing related terms.
     */
    def assessMachineReadableLicense() = {
      val hasAssociatedLicense = dataset.filter(f => hasLicenceAssociated(f.getPredicate))
      if (hasAssociatedLicense.count() > 0) 1.0 else 0.0
    }

    def hasLicenceAssociated(node: Node) = {
      val licenceAssociated = Seq(DQV.cclicence, DQV.dbolicense, DQV.xhtmllicense, DQV.dclicence,
        DQV.dcrights, DQV.dctlicence, DQV.dbplicence, DQV.doaplicense,
        DQV.dctrights, DQV.schemalicense, "wrcc:license", "sz:license_text")
      licenceAssociated.contains(node.getURI)
    }
  }
}