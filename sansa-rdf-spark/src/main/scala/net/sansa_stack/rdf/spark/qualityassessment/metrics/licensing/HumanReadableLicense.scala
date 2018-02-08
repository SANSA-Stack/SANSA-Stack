package net.sansa_stack.rdf.spark.qualityassessment.metrics.licensing

import org.apache.spark.rdd.RDD
import org.apache.jena.graph.{ Triple, Node }
import net.sansa_stack.rdf.spark.qualityassessment.utils.NodeUtils._
import scala.util.matching.Regex
import net.sansa_stack.rdf.spark.qualityassessment.vocabularies.DQV
import org.apache.jena.vocabulary.RDFS

/**
 * @author Gezim Sejdiu
 */
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
        f.getSubject.isURI() && f.getPredicate.hasLicenceIndications() && f.getObject.isLiteral() && f.getObject.isLicenseStatement()
      }
      if (hasValidLicense.count() > 0) 1.0 else 0.0
    }

  }

  implicit class LicenceIndicationFunctions(node: Node) extends Serializable {

    val isLicenseDefination = new Regex(".*(licensed?|copyrighte?d?).*(under|grante?d?|rights?).*")
    val licenceIndications = Seq(DQV.dqv_description, RDFS.comment, RDFS.label)

    /**
     * Checks if a given [[resource]] contains license statements.  
     * License statements : .*(licensed?|copyrighte?d?).*(under|grante?d?|rights?).* 
     * @param node the resource to be checked.
     * @return `true` if contains these definition, otherwise `false`.
     */
    def isLicenseStatement() = isLicenseDefination.findFirstIn(node.getLiteralLexicalForm).size != 0

    /**
     * Checks if a given [[resource]] contains license indications.
     * License indications : [[http://www.w3.org/ns/dqv#description dqv:description]], [[https://www.w3.org/2000/01/rdf-schema#comment RDFS.comment]], [[https://www.w3.org/2000/01/rdf-schema#label RDFS.label]]
     * @param node the resource to be checked.
     * @return `true` if contains these indications, otherwise `false`.
     */
    def hasLicenceIndications() = licenceIndications.contains(node.getURI)

  }
}