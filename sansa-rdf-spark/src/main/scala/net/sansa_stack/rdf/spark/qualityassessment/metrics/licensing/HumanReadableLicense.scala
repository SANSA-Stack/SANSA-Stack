package net.sansa_stack.rdf.spark.qualityassessment.metrics.licensing

import org.apache.spark.rdd.RDD
import org.apache.jena.graph.{ Triple, Node }
import net.sansa_stack.rdf.spark.qualityassessment.utils.NodeUtils._
import scala.util.matching.Regex
import net.sansa_stack.rdf.spark.qualityassessment.vocabularies.DQV

object HumanReadableLicense {
  implicit class HumanReadableLicenseFunctions(dataset: RDD[Triple]) extends Serializable {

    /**
     * Human -readable indication of a license
     * This metric checks whether a human-readable text, stating the of licensing model
     * attributed to the resource, has been provided as part of the dataset.
     * It looks for objects containing literal values and analyzes the text searching for key, licensing related terms.
     */
    def assessHumanReadableLicense() = {

      val hasValidLicense = dataset.filter { f =>
        f.getSubject.isURI() && hasLicenceIndications(f.getPredicate) && f.getObject.isLiteral() && isLicenseStatement(f.getObject)
      }
        if (hasValidLicense.count() > 0) 1.0 else 0.0
      }
    
      def isLicenseStatement(node: Node) = {
        val check = new Regex(".*(licensed?|copyrighte?d?).*(under|grante?d?|rights?).*")
        check.findFirstIn(node.getLiteralLexicalForm).size != 0
      }

      def hasLicenceIndications(node: Node) = {
        val licenceIndications = Seq(DQV.dqv + "description", DQV.RDFS + "comment", DQV.RDFS + "label")
        licenceIndications.contains(node.getURI)
      }
  }
}