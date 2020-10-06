package net.sansa_stack.owl.common.parsing

import scala.util.matching.Regex.Match

import com.typesafe.scalalogging.Logger
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.functional.parser.OWLFunctionalSyntaxOWLParserFactory
import org.semanticweb.owlapi.io.{OWLParserException, StringDocumentSource}
import org.semanticweb.owlapi.model.OWLAxiom

/**
  * Object containing several constants used by the FunctionalSyntaxParsing
  * trait and the FunctionalSyntaxExpressionBuilder
  */
object FunctionalSyntaxParsing {
  /** marker used to store the prefix for the default namespace */
  val _empty = "_EMPTY_"
  // TODO: refine
  val prefixPattern = "Prefix\\(([a-zA-Z]*)\\:=<(.*)>\\)".r
  val ontologyPattern = "Ontology\\(<(.*)>".r
}


/**
  * Trait to support the parsing of input OWL files in functional syntax. This
  * trait mainly defines how to make axioms from input functional syntax
  * expressions.
  */
trait FunctionalSyntaxParsing {
  private val logger = Logger(classOf[FunctionalSyntaxParsing])
  private def parser = new OWLFunctionalSyntaxOWLParserFactory().createParser()
  private def man = OWLManager.createOWLOntologyManager()
  private def ontConf = man.getOntologyLoaderConfiguration

  /**
    * Builds a snippet conforming to the OWL functional syntax which then can
    * be parsed by the OWLAPI functional syntax parser. A single expression,
    * e.g.
    *
    * Declaration(Class(bar:Cls2))
    *
    * has thus to be wrapped into an ontology declaration as follows
    *
    * Ontology( <http://the.ontology.uri#>
    * Declaration(Class(bar:Cls2))
    * )
    *
    * @param expression A String containing an expression in OWL functional
    *                   syntax, e.g. Declaration(Class(bar:Cls2))
    * @return The parsed axiom or null in case something went wrong during parsing
    */
  @throws(classOf[OWLParserException])
  def makeAxiom(expression: String): OWLAxiom = {
    val ontStr = "Ontology(<http://example.com/dummy>\n"
    val axStr = ontStr + expression + "\n)"
    val ont = man.createOntology()

    parser.parse(new StringDocumentSource(axStr), ont, ontConf)

    val it = ont.axioms().iterator()

    if (it.hasNext) {
      it.next()
    } else {
      logger.warn("No axiom was created for expression " + expression)
      null
    }
  }
}


/**
  * Trait to support the parsing of prefixes from expressions given in
  * functional syntax.
  */
trait FunctionalSyntaxPrefixParsing {
  /**
    * Parses the prefix declaration of a namespace URI and returns the
    * pair (prefix, namespace URI)
    *
    * @param prefixLine Sth like
    *                   Prefix(:=<http://purl.obolibrary.org/obo/pato.owl#>) or
    *                   Prefix(dc:=<http://purl.org/dc/elements/1.1/>)
    */
  def parsePrefix(prefixLine: String): (String, String) = {
    var prefix, uri: String = null

    prefixLine.trim match {
      case FunctionalSyntaxParsing.prefixPattern(p, u) =>
        prefix = p
        uri = u
    }

    if (prefix.isEmpty) prefix = FunctionalSyntaxParsing._empty

    (prefix, uri)
  }

  def isPrefixDeclaration(expression: String): Boolean = {
    FunctionalSyntaxParsing.prefixPattern.pattern.matcher(expression).matches()
  }
}


/**
  * The main purpose of this class is to provide an object which is initialized
  * with
  *
  * - all prefix declarations
  *
  * and, fed with an expression in functional syntax, returns expressions in
  * functional syntax, with
  *
  * - all prefix URIs being extended
  * - all comments removed
  * - all non-axiom expressions (e.g. Ontology(...), Prefix(...)) removed
  *
  * 'Removed' here means that a null value is returned discarding the input
  * string.
  *
  * @param prefixes Map containing all read prefixes; required to expand
  *                 namespace shortcuts in the cleaned (functional syntax)
  *                 axiom string
  */
class FunctionalSyntaxExpressionBuilder(val prefixes: Map[String, String]) extends Serializable {
  def clean(expression: String): String = {
    var trimmedExpr = expression.trim

    /* Throw away expressions that are of no use for further processing:
     * 1) empty lines
     * 2) comments
     * 3) the last, outermost closing parenthesis
     * 4) prefix declaration
     * 5) ontology declaration
     * 6) URL being part of the ontology declaration, e.g.
     *    <http://purl.obolibrary.org/obo/pato/releases/2016-05-22/pato.owl>
     *    being part of Ontology(...)
     */
    val discardExpression: Boolean =
      trimmedExpr.isEmpty  || // 1)
      trimmedExpr.startsWith("#") ||  // 2)
      trimmedExpr.startsWith(")") ||  // 3)
      trimmedExpr.startsWith("Prefix(") ||  // 4)
      trimmedExpr.startsWith("Ontology(") ||  // 5)
      trimmedExpr.startsWith("<http") // 6

    if (discardExpression) {
      null

    } else {
      // Expand prefix abbreviations: foo:Bar --> http://foo.com/somePath#Bar
      for (prefix <- prefixes.keys) {
        val p = prefix + ":"

        if (trimmedExpr.contains(p)) {
          val v: String = "<" + prefixes.get(prefix).get
          // TODO: refine regex
          val pattern = (p + "([a-zA-Z][0-9a-zA-Z_-]*)\\b").r

          // Append ">" to all matched local parts: "foo:car" --> "foo:car>"
          trimmedExpr = pattern.replaceAllIn(trimmedExpr, m => s"${m.matched}>")
          // Expand prefix: "foo:car>" --> "http://foo.org/res#car>"
          trimmedExpr = trimmedExpr.replace(p.toCharArray, v.toCharArray)
        }
      }

      // handle default prefix e.g. :Bar --> http://foo.com/defaultPath#Bar
      // TODO: refine regex
      val pattern = ":[^/][a-zA-Z][0-9a-zA-Z_-]*".r
      val v: String = "<" + prefixes.get(FunctionalSyntaxParsing._empty).get

      if (prefixes.contains(FunctionalSyntaxParsing._empty)) {
        pattern.findAllIn(trimmedExpr).foreach(hit => {
          val full = hit.replace(":".toCharArray, v.toCharArray)
          trimmedExpr = trimmedExpr.replace(hit, full + ">")
        })
      }

      trimmedExpr
    }
  }
}
