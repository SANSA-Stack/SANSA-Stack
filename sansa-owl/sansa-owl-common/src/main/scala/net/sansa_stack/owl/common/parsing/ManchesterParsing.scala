package net.sansa_stack.owl.common.parsing

import java.io
import java.util.stream.Collectors

import scala.collection.JavaConverters._

import org.semanticweb.owlapi.model._
import org.semanticweb.owlapi.vocab.{Namespaces, OWL2Datatype, OWLFacet}
import uk.ac.manchester.cs.owl.owlapi._


/** Enum to match property characteristics */
object PropertyCharacteristic extends Enumeration {
  val Functional, InverseFunctional, Reflexive, Irreflexive, Symmetric,
  Asymmetric, Transitive = Value
}

// Simple class hierarchy to match class details
sealed abstract class ClassDetails(details: Any)

case class ClassAnnotationDetails(details: List[OWLAnnotation])
  extends ClassDetails(details)

case class ClassSubClassOfDetails(details: List[(OWLClassExpression, List[OWLAnnotation])])
  extends ClassDetails(details)

case class ClassEquivalentToDetails(details: List[(OWLClassExpression, List[OWLAnnotation])])
  extends ClassDetails(details)

case class ClassDisjointWithDetails(details: List[(OWLClassExpression, List[OWLAnnotation])])
  extends ClassDetails(details)

case class ClassDisjointUnionOfDetails(details: (List[OWLAnnotation], List[OWLClassExpression]))
  extends ClassDetails(details)

case class HasKeyDetails(details: (List[OWLAnnotation], List[OWLPropertyExpression]))

// Simple class hierarchy to match object property details
sealed abstract class ObjectPropertyDetails(details: io.Serializable)

case class ObjectPropertyAnnotationDetails(details: List[OWLAnnotation])
  extends ObjectPropertyDetails(details)

case class ObjectPropertyDomainDetails(details: List[(OWLClassExpression, List[OWLAnnotation])])
  extends ObjectPropertyDetails(details)

case class ObjectPropertyRangeDetails(details: List[(OWLClassExpression, List[OWLAnnotation])])
  extends ObjectPropertyDetails(details)

case class ObjectPropertyCharacteristicsDetails(details: List[(PropertyCharacteristic.Value, List[OWLAnnotation])])
  extends ObjectPropertyDetails(details)

case class ObjectPropertySubPropertyOfDetails(details: List[(OWLObjectPropertyExpression, List[OWLAnnotation])])
  extends ObjectPropertyDetails(details)

case class ObjectPropertyEquivalentToDetails(details: List[(OWLObjectPropertyExpression, List[OWLAnnotation])])
  extends ObjectPropertyDetails(details)

case class ObjectPropertyDisjointWithDetails(details: List[(OWLObjectPropertyExpression, List[OWLAnnotation])])
  extends ObjectPropertyDetails(details)

case class ObjectPropertyInverseOfDetails(details: List[(OWLObjectPropertyExpression, List[OWLAnnotation])])
  extends ObjectPropertyDetails(details)

case class ObjectPropertySubPropertyChainDetails(details: (List[OWLObjectPropertyExpression], List[OWLAnnotation]))
  extends ObjectPropertyDetails(details)

// Simple class hierarchy to match data property details
sealed abstract class DataPropertyDetails(details: List[io.Serializable])

case class DataPropertyAnnotationDetails(details: List[OWLAnnotation])
  extends DataPropertyDetails(details)

case class DataPropertyDomainDetails(details: List[(OWLClassExpression, List[OWLAnnotation])])
  extends DataPropertyDetails(details)

case class DataPropertyRangeDetails(details: List[(OWLDataRange, List[OWLAnnotation])])
  extends DataPropertyDetails(details)

case class DataPropertyCharacteristicsDetails(details: List[OWLAnnotation])
  extends DataPropertyDetails(details)

case class DataPropertySubPropertyOfDetails(details: List[(OWLDataPropertyExpression, List[OWLAnnotation])])
  extends DataPropertyDetails(details)

case class DataPropertyEquivalentToDetails(details: List[(OWLDataPropertyExpression, List[OWLAnnotation])])
  extends DataPropertyDetails(details)

case class DataPropertyDisjointWithDetails(details: List[(OWLDataPropertyExpression, List[OWLAnnotation])])
  extends DataPropertyDetails(details)

// Simple class hierarchy to match annotation property details
sealed abstract class AnnotationPropertyDetails(details: List[io.Serializable])

case class AnnotationPropertyAnnotationDetails(details: List[OWLAnnotation])
  extends AnnotationPropertyDetails(details)

case class AnnotationPropertyDomainDetails(details: List[(IRI, List[OWLAnnotation])])
  extends AnnotationPropertyDetails(details)

case class AnnotationPropertyRangeDetails(details: List[(IRI, List[OWLAnnotation])])
  extends AnnotationPropertyDetails(details)

case class AnnotationPropertySubPropertyOfDetails(details: List[(OWLAnnotationProperty, List[OWLAnnotation])])
  extends AnnotationPropertyDetails(details)

// Simple class hierarchy to match individual details
sealed abstract class IndividualDetails(details: List[io.Serializable])

case class IndividualAnnotationDetails(details: List[OWLAnnotation])
  extends IndividualDetails(details)

case class IndividualTypesDetails(details: List[(OWLClassExpression, List[OWLAnnotation])])
  extends IndividualDetails(details)

case class IndividualFactsDetails(details: List[((Boolean, OWLProperty, OWLPropertyAssertionObject), List[OWLAnnotation])])
  extends IndividualDetails(details)

case class IndividualSameAsDetails(details: List[(OWLIndividual, List[OWLAnnotation])])
  extends IndividualDetails(details)

case class IndividualDifferentFromDetails(details: List[(OWLIndividual, List[OWLAnnotation])])
  extends IndividualDetails(details)


class ManchesterParsing extends IRIParsing {

  /**
    * For cases where there is a comma-separated list with an arbitrary number
    * of items, but we're only interested in the items. Converts a parsed
    * sequence like
    *
    *   , item1 , item2 , item3
    *
    * (with optional white space around the commas) to a Scala list appended to
    * the accumulator list given as first argument.
    *
    * @param acc Accumulator list
    * @param parsResults Parsed results that follow the pattern
    *                    `{ whiteSpace.? ~ comma ~ whiteSpace.? ~ value }.*`
    * @tparam U Determines the kind of items that are considered, e.g. OWLLiteral, IRI, ...
    * @return A Scala list containing just the parsed items (without commas)
    */
  def unravel[U](
                  acc: List[U],
                  parsResults: List[~[~[~[Option[String], String], Option[String]], U]]
                ): List[U] =
    parsResults match {
      case List() => acc.reverse
      case _ => unravel(parsResults.head._2 :: acc, parsResults.tail)
    }

  /**
    * Does the same like unravel but considering optional annotations. The
    * pattern is
    *
    *   <U>AnnotatedList ::= [annotations] <U> { ',' [annotations] <U> }
    *
    *
    * @param resultList A list of pairs of U's and corresponding (possibly
    *                   empty) annotation lists
    * @param annotationsOption The first parsed, optional annotations block
    * @param entry The first parsed U entry
    * @param remainingParseResults Everything parsed after the first annotation
    *                              block and first U entry
    * @tparam U What kind of OWL entity is considered here; e.g. OWLClass,
    *           OWLIndividual etc
    * @return The filled resultList
    */
  def unravelAnnotatedList[U](
                               resultList: List[(U, List[OWLAnnotation])],
                               annotationsOption: Option[~[List[OWLAnnotation], String]],
                               entry: U,
                               remainingParseResults: List[~[~[~[~[Option[String], String], Option[String]], Option[~[List[OWLAnnotation], String]]], U]]
                             ): List[(U, List[OWLAnnotation])] = {

    val annotations: List[OWLAnnotation] = annotationsOption match {
      case Some(annotationsAndWhiteSpace) => annotationsAndWhiteSpace._1
      case None => List.empty
    }

    remainingParseResults match {
      case Nil =>
        // Just add current description-annotation pair and return result list
        { (entry, annotations) :: resultList }.reverse
      case _ =>
        // Add current description-annotation pair and recursively call
        // unravelAnnotationAnnotatedList on the remaining parse results
        val nextParsedItem = remainingParseResults.head
        val nextAnnsOption: Option[~[List[OWLAnnotation], String]] = nextParsedItem._1._2
        val nextEntry: U = nextParsedItem._2
        val nextRemainingParseResults = remainingParseResults.tail

        unravelAnnotatedList(
          (entry, annotations) :: resultList,
          nextAnnsOption,
          nextEntry,
          nextRemainingParseResults
        )
    }
  }

  /**
    * Converts a parsed, 'and'-separated sequence of class expressions with
    * optional 'not' determining an expression's complement. The pattern looks
    * like this:
    *
    *   [ 'not' ] expression { 'and' [ 'not' ] expression }
    *
    * @param resultList The list containing the parsed OWLClassExpressions
    * @param remainingParseResults Holds everything after the first optional
    *                              'not' and the first class expression
    * @return The resultList
    */
  def unravelConjunctionWithOptional(
                                      resultList: List[OWLClassExpression],
                                      remainingParseResults: List[~[~[~[String, String], Option[~[String, String]]], OWLClassExpression]]
                                    ): List[OWLClassExpression] = {

    val nextItem: ~[~[~[String, String], Option[~[String, String]]], OWLClassExpression] =
      remainingParseResults.head
    val notOption: Option[~[String, String]] = nextItem._1._2

    val nextCE: OWLClassExpression = notOption match {
      // there is a 'not' --> build complement of left-most class expression
      case Some(_) => new OWLObjectComplementOfImpl(nextItem._2)
      // there is no 'not' --> just return the left-most class expression
      case _ => nextItem._2
    }

    // recursive call on remaining parse results
    remainingParseResults.tail match {
      case Nil => (nextCE :: resultList).reverse
      case _ => unravelConjunctionWithOptional(
        nextCE :: resultList,
        remainingParseResults.tail)
    }
  }

  /**
    * Converts parser results of a token separated sequence (with whitespaces
    * around the token) into a list. The pattern looks like this:
    *
    *   <U> token <U> { token <U> }
    */
  def unravelWithFixedWhiteSpace[U](
                                     results: List[U],
                                     remainingParseResults: List[~[~[~[String, String], String], U]]
                                   ): List[U] =
    remainingParseResults match {
      case Nil => results.reverse
      case _ =>
        unravelWithFixedWhiteSpace(
          remainingParseResults.head._2 :: results,
          remainingParseResults.tail)
    }

  /**
    * Converts parser results of a list of annotations which themselves can be
    * annotated into a list of OWLAnnotation instances. The pattern looks like
    * this:
    *
    *   AnnotationAnnotatedList ::=
    *     [annotations] annotation { ',' [annotations] annotation }
    *
    * @param annotationsResultList The result list containing OWLAnnotation objects
    * @param annsOption The left-most (optional) annotation block
    * @param annProperty The annotation property of the left-most annotation
    * @param annValue The annotation value of the left-most annotation
    * @param remainingParseResults The parser results coming after the left-most
    *                              annotation block
    * @return annotationsResultList
    */
  def unravelAnnotationAnnotatedList(
                                      annotationsResultList: List[OWLAnnotation],
                                      annsOption: Option[~[List[OWLAnnotation], String]],
                                      annProperty: OWLAnnotationProperty,
                                      annValue: OWLAnnotationValue,
                                      remainingParseResults: List[~[~[~[~[Option[String], String], Option[String]], Option[~[List[OWLAnnotation], String]]], (OWLAnnotationProperty, OWLAnnotationValue)]]
                                    ): List[OWLAnnotation] = {

    val currentAnnotation = annsOption match {
      // There is an annotations block for the left-most annotation
      case Some(annotationsAndString) => new OWLAnnotationImpl(
        annProperty, annValue, annotationsAndString._1.asJavaCollection.stream())
      // There is no annotations block for the left-most annotation
      case None => new OWLAnnotationImpl(
        annProperty, annValue, List.empty[OWLAnnotation].asJavaCollection.stream())
    }

    remainingParseResults match {
      case Nil =>
        // Just add current annotation and return result list
        { currentAnnotation :: annotationsResultList }.reverse
      case _ =>
        // Add current annotation and recursively call unravelAnnotationAnnotatedList
        // on the remaining parse results
        val nextParsedItem = remainingParseResults.head
        val nextAnnAnnsOption: Option[~[List[OWLAnnotation], String]] =
          nextParsedItem._1._2
        val nextAnnProperty: OWLAnnotationProperty = nextParsedItem._2._1
        val nextAnnValue: OWLAnnotationValue = nextParsedItem._2._2
        val nextRemainingParseResults = remainingParseResults.tail

        unravelAnnotationAnnotatedList(
          currentAnnotation :: annotationsResultList,
          nextAnnAnnsOption,
          nextAnnProperty,
          nextAnnValue,
          nextRemainingParseResults
        )
    }
  }

  /**
    * For cases where there is a comma-separated list with an arbitrary number
    * of item pairs, but we're only interested in the item pairs. Converts a
    * parsed sequence like
    *
    *   , item1_1 item1_2 , item2_1 item2_2 , item3_1 item3_2
    *
    * to a Scala list of pairs, appended to the accumulator list given as first
    * argument.
    *
    * @param acc Accumulator list
    * @param parsResults Parsed results that follow the pattern `{ comma ~ value ~ value }.*`
    * @tparam U Determines the kind of items that are considered, e.g. OWLLiteral, IRI, ...
    * @tparam V Determines the kind of items that are considered, e.g. OWLLiteral, IRI, ...
    * @return A Scala list containing just the parsed items (without commas)
    */
  def unravelTwo[U, V](
                        acc: List[(U, V)],
                        parsResults: List[~[~[~[~[~[Option[String], String], Option[String]], U], String], V]]
                      ): List[(U, V)] = parsResults match {

    case List() => acc.reverse
    case _ => unravelTwo((parsResults.head._1._1._2, parsResults.head._2) :: acc, parsResults.tail)
  }

  def zero: Parser[Int] = "0" ^^ { _.toInt}
  def nonZero: Parser[String] = "[1-9]".r ^^ { _.toString }
  def positiveInteger: Parser[Int] = phrase(nonZero ~ digit.*) ^^ { toString(_).toInt }
  def nonNegativeInteger: Parser[Int] = phrase(zero | positiveInteger)

  def classIRI: Parser[OWLClass] = notAnXSDDatatypeURI ^^ { dataFactory.getOWLClass(_) }

  def datatype: Parser[OWLDatatype] =
    "integer" ^^ { _ => dataFactory.getOWLDatatype(OWL2Datatype.XSD_INTEGER) } |
    "decimal" ^^ { _ => dataFactory.getOWLDatatype(OWL2Datatype.XSD_DECIMAL) } |
    "float" ^^ { _ => dataFactory.getOWLDatatype(OWL2Datatype.XSD_FLOAT) } |
    "string" ^^ { _ => dataFactory.getOWLDatatype(OWL2Datatype.XSD_STRING) } |
    iri ^^ { dataFactory.getOWLDatatype(_) }

  def nodeID: Parser[String] = "_:" ~ pn_local ^^ { toString(_) }

  def individualIRI: Parser[OWLNamedIndividual] =
    notAnXSDDatatypeURI ^^ { dataFactory.getOWLNamedIndividual(_) }

  def objectPropertyIRI: Parser[OWLObjectProperty] =
    notAnXSDDatatypeURI ^^ { dataFactory.getOWLObjectProperty(_) }

  def dataPropertyIRI: Parser[OWLDataProperty] =
    notAnXSDDatatypeURI ^^ { dataFactory.getOWLDataProperty(_)}

  def annotationPropertyIRI: Parser[OWLAnnotationProperty] =
    notAnXSDDatatypeURI ^^ { dataFactory.getOWLAnnotationProperty(_) }

  def individual: Parser[OWLIndividual] =
    individualIRI | nodeID ^^ { dataFactory.getOWLAnonymousIndividual(_) }

  /** a finite sequence of characters in which " (U+22) and \ (U+5C) occur only
    * in pairs of the form \" (U+5C, U+22) and \\ (U+5C, U+5C), enclosed in a
    * pair of " (U+22) characters */
  // scalastyle:off
  def quotedString: Parser[String] =
    doubleQuote ~ "[A-Za-z0-9 _!§$%&/()=?`´*+'#:.;,^°\n\r\f\\\\<>|-]+".r ~
      doubleQuote ^^ { _._1._2.toString }
  // scalastyle:on

  def lexicalValue: Parser[String] = quotedString

  /** ('e' | 'E') ['+' | '-'] digits */
  // scalastyle:off
  def exponent: Parser[String] =
    { "e" | "E" } ~ { plus | minus }.? ~ digit.+ ^^ { toString(_) }
  // scalastyle:on

  /** @ (U+40) followed a nonempty sequence of characters matching the langtag
    * production from [BCP 47] */
  // FIXME: too general
  def languageTag: Parser[String] = "@" ~ "[A-Za-z0-9-]+".r ^^ { _._2.toString }

  def typedLiteral: Parser[OWLLiteral] =
    lexicalValue ~ circumflex ~ circumflex ~ datatype ^^ { raw =>
      dataFactory.getOWLLiteral(raw._1._1._1, raw._2) }

  def stringLiteralNoLanguage: Parser[OWLLiteral] =
    quotedString ^^ { dataFactory.getOWLLiteral(_) }

  def stringLiteralWithLanguage: Parser[OWLLiteral] =
    quotedString ~ languageTag ^^ { raw => dataFactory.getOWLLiteral(raw._1, raw._2) }

  // scalastyle:off
  def integerLiteral: Parser[OWLLiteral] =
    { plus | minus }.? ~ digit.+ ^^ { raw =>
      dataFactory.getOWLLiteral(toString(raw), OWL2Datatype.XSD_INTEGER)
    }
  // scalastyle:on

  // scalastyle:off
  def decimalLiteral: Parser[OWLLiteral] =
    { plus | minus }.? ~ digit.+ ~ dot ~ digit.+ ^^ { raw =>
      dataFactory.getOWLLiteral(toString(raw), OWL2Datatype.XSD_DECIMAL)
    }
  // scalastyle:on

  // [ '+' | '-'] ( digits ['.'digits] [exponent] | '.' digits[exponent]) ( 'f' | 'F' )
  // scalastyle:off
  def floatingPointLiteral: Parser[OWLLiteral] =
    { plus | minus }.? ~ { digit.+ ~ { dot ~ digit.+ }.? ~ exponent.? |
      dot ~ digit.+ ~ exponent.? } ~ { "f" | "F" } ^^ { raw =>
      dataFactory.getOWLLiteral(toString(raw), OWL2Datatype.XSD_FLOAT)
    }
  // scalastyle:on

  def literal: Parser[OWLLiteral] =
    typedLiteral | stringLiteralWithLanguage | stringLiteralNoLanguage |
       integerLiteral | decimalLiteral | floatingPointLiteral

  def datatypeDecl: Parser[OWLDatatype] =
    "Datatype" ~ whiteSpace.? ~ "(" ~ whiteSpace.? ~ datatype ~ whiteSpace.? ~
      ")" ^^ { _._1._1._2 }

  def classDecl: Parser[OWLClass] =
    "Class" ~ whiteSpace.? ~ "(" ~ whiteSpace.? ~ classIRI ~ whiteSpace.? ~
      ")" ^^ { _._1._1._2 }

  def objectPropertyDecl: Parser[OWLObjectProperty] =
    "ObjectProperty" ~ whiteSpace.? ~ "(" ~ whiteSpace.? ~ objectPropertyIRI ~
      whiteSpace.? ~ ")" ^^ { _._1._1._2 }

  def dataPropertyDecl: Parser[OWLDataProperty] =
    "DataProperty" ~ whiteSpace.? ~ "(" ~ whiteSpace.? ~ dataPropertyIRI ~
      whiteSpace.? ~ ")" ^^ { _._1._1._2 }

  def annotationPropertyDecl: Parser[OWLAnnotationProperty] =
    "AnnotationProperty" ~ whiteSpace.? ~ "(" ~ whiteSpace.? ~
      annotationPropertyIRI ~ whiteSpace.? ~ ")" ^^ { _._1._1._2 }

  def namedIndividualDecl: Parser[OWLNamedIndividual] =
    "NamedIndividual" ~ whiteSpace.? ~ "(" ~ whiteSpace.? ~ individualIRI ~
      whiteSpace.? ~ ")" ^^ { _._1._1._2 }

  def entity: Parser[OWLEntity] = datatypeDecl | classDecl | objectPropertyDecl |
    dataPropertyDecl | annotationPropertyDecl | namedIndividualDecl

  def annotationTarget: Parser[OWLAnnotationValue] =
    nodeID ^^ { dataFactory.getOWLAnonymousIndividual(_) } | iri | literal

  def annotation: Parser[(OWLAnnotationProperty, OWLAnnotationValue)] =
    annotationPropertyIRI ~ whiteSpace ~ annotationTarget ^^ { raw => (raw._1._1, raw._2) }

  def annotationAnnotatedList: Parser[List[OWLAnnotation]] =
    { annotations ~ whiteSpace }.? ~ annotation ~
      { whiteSpace.? ~ comma ~ whiteSpace.? ~
        { annotations ~ whiteSpace }.? ~ annotation }.* ^^
      { raw =>
        val annotationsOption: Option[~[List[OWLAnnotation], String]] = raw._1._1
        val firstAnnotationProperty: OWLAnnotationProperty = raw._1._2._1
        val firstAnnotationTarget: OWLAnnotationValue = raw._1._2._2
        val remainingParseResults: List[~[~[~[~[Option[String], String], Option[String]], Option[~[List[OWLAnnotation], String]]], (OWLAnnotationProperty, OWLAnnotationValue)]] = raw._2

        unravelAnnotationAnnotatedList(List.empty, annotationsOption,
          firstAnnotationProperty, firstAnnotationTarget, remainingParseResults)
      }

  def annotations: Parser[List[OWLAnnotation]] =
    "Annotations:" ~ whiteSpace ~ annotationAnnotatedList ^^ { _._2 }

  /** a finite sequence of characters matching the PNAME_NS production of
    * [SPARQL] and not matching any of the keyword terminals of the syntax */
  // FIXME: the 'not matching any of the keyword terminals' part not guaranteed
  def prefixName: Parser[String] = pname_ns

  // TODO: add callback mechanism to broadcast prefixes
  def prefixDeclaration: Parser[(String, String)] =
    "Prefix:" ~ whiteSpace ~ prefixName ~ whiteSpace ~ fullIRI ^^ { raw => {
      // 1) Broadcast prefix
      // 2) Add to local prefixes (if not already covered by broadcast
      prefixes.put(raw._1._1._2, raw._2.getIRIString)
      (raw._1._1._2, raw._2.getIRIString)
    }
  }

  def ontologyIRI: Parser[IRI] = iri
  def versionIRI: Parser[IRI] = iri
  def imp0rt: Parser[IRI] = "Import:" ~ whiteSpace.? ~ iri ^^ { _._2 }

  def literalList: Parser[List[OWLLiteral]] =
    literal ~ { whiteSpace.? ~ comma ~ whiteSpace.? ~ literal }.* ^^ { raw =>
      unravel(List(raw._1), raw._2)
    }

  def facet: Parser[OWLFacet] =
    "length" ^^ { _ => OWLFacet.LENGTH } |
    "minLength" ^^ { _ => OWLFacet.MIN_LENGTH } |
    "maxLength" ^^ { _ => OWLFacet.MAX_LENGTH } |
    "pattern" ^^ { _ => OWLFacet.PATTERN } |
    "langRange" ^^ { _ => OWLFacet.LANG_RANGE } |
    "<=" ^^ { _ => OWLFacet.MAX_INCLUSIVE } |
    "<" ^^ { _ => OWLFacet.MAX_EXCLUSIVE } |
    ">=" ^^ { _ => OWLFacet.MIN_INCLUSIVE } |
    ">" ^^ { _ => OWLFacet.MIN_EXCLUSIVE }

  def restrictionValue: Parser[OWLLiteral] = literal

  def datatypeRestriction: Parser[OWLDatatypeRestriction] =
    datatype ~ whiteSpace.? ~ openingBracket ~ whiteSpace.? ~ facet ~
      whiteSpace ~ restrictionValue ~ { whiteSpace.? ~ comma ~ whiteSpace.? ~
      facet ~ whiteSpace ~ restrictionValue }.* ~ whiteSpace.? ~
      closingBracket ^^ { raw =>
        val datatype: OWLDatatype = raw._1._1._1._1._1._1._1._1._1
        val firstFacet: OWLFacet = raw._1._1._1._1._1._2
        val firstFacetVal: OWLLiteral = raw._1._1._1._2
        val remainderList = raw._1._1._2

        val facetsAndValues: List[(OWLFacet, OWLLiteral)] =
          unravelTwo[OWLFacet, OWLLiteral](List((firstFacet, firstFacetVal)), remainderList)

        val facetRestrictions: List[OWLFacetRestriction] =
          facetsAndValues.map(
            facetAndValue => new OWLFacetRestrictionImpl(facetAndValue._1, facetAndValue._2))

        new OWLDatatypeRestrictionImpl(datatype, facetRestrictions.asJavaCollection)
    }

  def dataAtomic: Parser[OWLDataRange] =
    // literal list
    openingCurlyBrace ~ whiteSpace.? ~ literalList ~ whiteSpace.? ~
      closingCurlyBrace ^^ { raw =>
        new OWLDataOneOfImpl(raw._1._1._2.asJavaCollection) } |
    datatypeRestriction |
    datatype |
    // data range
    openingParen ~ whiteSpace.? ~ dataRange ~ whiteSpace.? ~ closingParen ^^ { _._1._1._2 }

  def dataPrimary: Parser[OWLDataRange] =
    { "not" ~ whiteSpace }.? ~ dataAtomic ^^ { raw =>
      raw._1 match {
        case Some(_) => new OWLDataComplementOfImpl(raw._2)
        case None => raw._2
      }
    }

  def dataConjunction: Parser[OWLDataRange] =
    dataPrimary ~ whiteSpace ~ "and" ~ whiteSpace ~ dataPrimary ~
      { whiteSpace ~ "and" ~ whiteSpace ~ dataPrimary }.* ^^ { raw =>
        new OWLDataIntersectionOfImpl(unravelWithFixedWhiteSpace[OWLDataRange](
          List(raw._1._1._1._1._1, raw._1._2), raw._2).asJavaCollection)
    } |
    dataPrimary

  def dataRange: Parser[OWLDataRange] =
    dataConjunction ~ whiteSpace ~ "or" ~ whiteSpace ~ dataConjunction ~
      { whiteSpace ~ "or" ~ whiteSpace ~ dataConjunction }.* ^^ {
      raw => new OWLDataUnionOfImpl(unravelWithFixedWhiteSpace(
        List(raw._1._1._1._1._1, raw._1._2), raw._2).asJavaCollection)
    } |
    dataConjunction

  def datatypeFrame: Parser[List[OWLAxiom]] = "Datatype:" ~ whiteSpace ~ datatype ~
    { whiteSpace ~ "Annotations:" ~ whiteSpace ~ annotationAnnotatedList }.* ~
    { whiteSpace ~ "EquivalentTo:" ~ whiteSpace ~ annotations ~ whiteSpace ~ dataRange }.? ~
    { whiteSpace ~ "Annotations:" ~ whiteSpace ~ annotationAnnotatedList }.* ^^ { raw => {
      // just to make everything as explicit as possible and not get lost...
      val datatype: OWLDatatype = raw._1._1._1._2
      val annotationLists1: List[OWLAnnotation] = raw._1._1._2.flatMap(_._2)
      val equivalencesOption: Option[~[~[~[~[~[String, String], String], List[OWLAnnotation]], String], OWLDataRange]] = raw._1._2
      val annotationLists2: List[OWLAnnotation] = raw._2.flatMap(_._2)
      val annotations = { annotationLists1 ::: annotationLists2 }

      equivalencesOption match {
        /**
          * In case an equivalent data range is given,
          * - a datatype declaration axiom and
          * - a datatype definition axiom will be created
          */
        case Some(equivalences) =>
          val dataRange: OWLDataRange = equivalences._2
          val equivalenceAnnotations = equivalences._1._1._2.asJavaCollection

          List(
            new OWLDeclarationAxiomImpl(datatype, annotations.asJavaCollection),
            new OWLDatatypeDefinitionAxiomImpl(datatype, dataRange, equivalenceAnnotations)
          )

        /**
          * In case no equivalent data range is given, we will just create a
          * dataytpe declaration
          */
        case None =>
          List(new OWLDeclarationAxiomImpl(datatype, annotations.asJavaCollection))
      }
    }
  }

  def inverseObjectProperty: Parser[OWLObjectInverseOf] =
    "inverse" ~ whiteSpace ~ objectPropertyIRI ^^ { raw => new OWLObjectInverseOfImpl(raw._2) }

  def objectPropertyExpression: Parser[OWLObjectPropertyExpression] =
    inverseObjectProperty | objectPropertyIRI

  def dataPropertyExpression: Parser[OWLDataPropertyExpression] = dataPropertyIRI

  def objectSomeValuesFrom_restriction: Parser[OWLClassExpression] =
    { objectPropertyExpression ~ whiteSpace ~ "some" ~ whiteSpace ~ primary } ^^ { raw =>
      new OWLObjectSomeValuesFromImpl(raw._1._1._1._1, raw._2)
    }

  def objectAllValuesFrom_restriction: Parser[OWLClassExpression] =
    { objectPropertyExpression ~ whiteSpace ~ "only" ~ whiteSpace ~ primary } ^^ { raw =>
      new OWLObjectAllValuesFromImpl(raw._1._1._1._1, raw._2)
    }

  def objectHasValue_restriction: Parser[OWLClassExpression] =
    { objectPropertyExpression ~ whiteSpace ~ "value" ~ whiteSpace ~ individual } ^^ { raw =>
      new OWLObjectHasValueImpl(raw._1._1._1._1, raw._2)
    }

  def objectHasSelf_restriction: Parser[OWLClassExpression] =
    { objectPropertyExpression ~ whiteSpace ~ "Self" } ^^ { raw =>
      new OWLObjectHasSelfImpl(raw._1._1)
    }

  def objectMinCardinality_restriction: Parser[OWLClassExpression] =
    // FIXME: PW: don't get it why t.f. "0|[1-9][0-9]*".r works, but not nonNegativeInteger
    { objectPropertyExpression ~ whiteSpace ~ "min" ~ whiteSpace ~
      { "0|[1-9][0-9]*".r ^^ { _.toString.toInt } } ~ { whiteSpace ~ primary }.? } ^^ { raw =>
      raw._2 match {
        case Some(whiteSpaceAndCE) =>
          dataFactory.getOWLObjectMinCardinality(
            raw._1._2, raw._1._1._1._1._1, whiteSpaceAndCE._2)
        case None =>
          dataFactory.getOWLObjectMinCardinality(raw._1._2, raw._1._1._1._1._1)
      }
    }

  def objectMaxCardinality_restriction: Parser[OWLClassExpression] =
    // FIXME: PW: don't get it why t.f. "0|[1-9][0-9]*".r works, but not nonNegativeInteger
    { objectPropertyExpression ~ whiteSpace ~ "max" ~ whiteSpace ~
      { "0|[1-9][0-9]*".r ^^ { _.toString.toInt } } ~ { whiteSpace ~ primary }.? } ^^ { raw =>
      raw._2 match {
        case Some(whiteSpaceAndCE) =>
          dataFactory.getOWLObjectMaxCardinality(
            raw._1._2, raw._1._1._1._1._1, whiteSpaceAndCE._2)
        case None =>
          dataFactory.getOWLObjectMaxCardinality(raw._1._2, raw._1._1._1._1._1)
      }
    }

  def objectExactCardinality_restriction: Parser[OWLClassExpression] =
    // FIXME: PW: don't get it why t.f. "0|[1-9][0-9]*".r works, but not nonNegativeInteger
    { objectPropertyExpression ~ whiteSpace ~ "exactly" ~ whiteSpace ~
        { "0|[1-9][0-9]*".r ^^ { _.toString.toInt } } ~ { whiteSpace ~ primary }.? } ^^ { raw => raw._2 match {
        case Some(whiteSpaceAndCE) =>
          dataFactory.getOWLObjectExactCardinality(
            raw._1._2, raw._1._1._1._1._1, whiteSpaceAndCE._2)
        case None =>
          dataFactory.getOWLObjectExactCardinality(raw._1._2, raw._1._1._1._1._1)
      }
    }

  def dataSomeValuesFrom_restriction: Parser[OWLClassExpression] =
    { dataPropertyExpression ~ whiteSpace ~ "some" ~ whiteSpace ~ dataPrimary } ^^ { raw =>
      new OWLDataSomeValuesFromImpl(raw._1._1._1._1, raw._2)
    }

  def dataAllValuesFrom_restriction: Parser[OWLClassExpression] =
    { dataPropertyExpression ~ whiteSpace ~ "only" ~ whiteSpace ~ dataPrimary } ^^ { raw =>
      new OWLDataAllValuesFromImpl(raw._1._1._1._1, raw._2)
    }

  def dataHasValue_restriction: Parser[OWLClassExpression] =
    { dataPropertyExpression ~ whiteSpace ~ "value" ~ whiteSpace ~ literal } ^^ { raw =>
      new OWLDataHasValueImpl(raw._1._1._1._1, raw._2)
    }

  def dataMinCardinality_restriction: Parser[OWLClassExpression] =
    // FIXME: In case no dataPrimary is given, this will never match since it's already covered by the obj prop case!!!
    // TODO: Establish way to check whether parsed property is already known as obj or data property
    { dataPropertyExpression ~ whiteSpace ~ "min" ~ whiteSpace ~
      { "0|[1-9][0-9]*".r ^^ { _.toString.toInt } } ~
      { whiteSpace ~ dataPrimary }.? } ^^ { raw =>
        raw._2 match {
          case Some(whiteSpaceAndDataRange) =>
            dataFactory.getOWLDataMinCardinality(
              raw._1._2, raw._1._1._1._1._1, whiteSpaceAndDataRange._2)
          case None =>
            dataFactory.getOWLDataMinCardinality(raw._1._2, raw._1._1._1._1._1)
        }
      }

  def dataMaxCardinality_restriction: Parser[OWLClassExpression] =
    // FIXME: In case no dataPrimary is given, this will never match since it's already covered by the obj prop case!!!
    // TODO: Establish way to check whether parsed property is already known as obj or data property
    { dataPropertyExpression ~ whiteSpace ~ "max" ~ whiteSpace ~
      { "0|[1-9][0-9]*".r ^^ { _.toString.toInt } } ~
      { whiteSpace ~ dataPrimary }.? } ^^ { raw =>
        raw._2 match {
          case Some(whitespaceAndDataRange) =>
            dataFactory.getOWLDataMaxCardinality(
              raw._1._2, raw._1._1._1._1._1, whitespaceAndDataRange._2)
          case None =>
            dataFactory.getOWLDataMaxCardinality(raw._1._2, raw._1._1._1._1._1)
        }
      }

  def dataExactCardinality_restriction: Parser[OWLClassExpression] =
    // FIXME: In case no dataPrimary is given, this will never match since it's already covered by the obj prop case!!!
    // TODO: Establish way to check whether parsed property is already known as obj or data property
    { dataPropertyExpression ~ whiteSpace ~ "exactly" ~ whiteSpace ~
      { "0|[1-9][0-9]*".r ^^ { _.toString.toInt } } ~
      { whiteSpace ~ dataPrimary }.? } ^^ { raw =>
        raw._2 match {
          case Some(whiteSpaceAndDataRange) =>
            dataFactory.getOWLDataExactCardinality(
              raw._1._2, raw._1._1._1._1._1, whiteSpaceAndDataRange._2)
          case None =>
            dataFactory.getOWLDataExactCardinality(raw._1._2, raw._1._1._1._1._1)
        }
      }

  def restriction: Parser[OWLClassExpression] =
    objectSomeValuesFrom_restriction |
    objectAllValuesFrom_restriction |
    objectHasValue_restriction |
    objectHasSelf_restriction |
    objectMinCardinality_restriction |
    objectMaxCardinality_restriction |
    objectExactCardinality_restriction |
    dataSomeValuesFrom_restriction |
    dataAllValuesFrom_restriction |
    dataHasValue_restriction |
    dataMinCardinality_restriction |
    dataMaxCardinality_restriction |
    dataExactCardinality_restriction

  def notAnXSDDatatypeURI: Parser[IRI] = iri ^? {
    case iri if !iri.getIRIString.startsWith(Namespaces.XSD.getPrefixIRI) => iri}

  def individualList: Parser[List[OWLIndividual]] =
    individual ~ { whiteSpace.? ~ comma ~ whiteSpace.? ~ individual }.* ^^ { raw =>
      unravel(List(raw._1), raw._2)
    }

  def atomic: Parser[OWLClassExpression] =
    classIRI |
    // individuals list
    { openingCurlyBrace ~ individualList ~ closingCurlyBrace } ^^ { raw =>
      new OWLObjectOneOfImpl(raw._1._2.asJavaCollection.stream())
    } |
    // conjunctions of disjunctions
    { openingParen ~ description ~ closingParen } ^^ { _._1._2 }

  def primary: Parser[OWLClassExpression] =
    { "not" ~ whiteSpace }.? ~ { restriction | atomic } ^^ { raw =>
      raw._1 match {
        case Some(_) => new OWLObjectComplementOfImpl(raw._2)
        case _ => raw._2
      }
    }

  def conjunction: Parser[OWLClassExpression] =
    // e.g. Cinema that hasMovie only ( hasGenre value Action )
    classIRI ~ whiteSpace ~ "that" ~ whiteSpace ~ { "not" ~ whiteSpace }.? ~
      restriction ~
      { whiteSpace ~ "and" ~ { whiteSpace ~ "not" }.? ~ restriction }.* ^^ { raw =>
        val classIRI: OWLClass = raw._1._1._1._1._1._1

        val firstRestriction: OWLClassExpression = raw._1._1._2 match {
          // check whether there is a 'not'
          case Some(_) => new OWLObjectComplementOfImpl(raw._1._2)
          case _ => raw._1._2
        }

        val restrictions = unravelConjunctionWithOptional(List(firstRestriction), raw._2)

        new OWLObjectIntersectionOfImpl(
          { classIRI :: restrictions }.asJavaCollection.stream())
    } |
    primary ~ whiteSpace ~ "and" ~ whiteSpace ~ primary ~
      { whiteSpace ~ "and" ~ whiteSpace ~ primary }.* ^^ { raw =>
        val firstCE: OWLClassExpression = raw._1._1._1._1._1
        val secondCE: OWLClassExpression = raw._1._2
        val remainingParseResults = raw._2

        val ces: List[OWLClassExpression] =
          unravelWithFixedWhiteSpace(List(firstCE, secondCE), remainingParseResults)

        new OWLObjectIntersectionOfImpl(ces.asJavaCollection.stream())
    } |
    primary

  def description: Parser[OWLClassExpression] =
    conjunction ~ whiteSpace ~ "or" ~ whiteSpace ~ conjunction ~
      { whiteSpace ~ "or" ~ whiteSpace ~ conjunction }.* ^^ { raw =>
        val ces: List[OWLClassExpression] =
          unravelWithFixedWhiteSpace(List(raw._1._1._1._1._1, raw._1._2), raw._2)

      new OWLObjectUnionOfImpl(ces.asJavaCollection.stream())
    } |
    conjunction

  def descriptionAnnotatedList: Parser[List[(OWLClassExpression, List[OWLAnnotation])]] =
    { annotations ~ whiteSpace }.? ~ description ~
      { whiteSpace.? ~ comma ~ whiteSpace.? ~ { annotations ~ whiteSpace }.? ~
        description }.* ^^ { raw =>
      unravelAnnotatedList(List.empty, raw._1._1, raw._1._2, raw._2)
    }

  def descriptionList: Parser[List[OWLClassExpression]] =
    description ~ { whiteSpace.? ~ comma ~ whiteSpace.? ~ description }.* ^^ { raw =>
      unravel(List(raw._1), raw._2)
    }

  def description2List: Parser[(OWLClassExpression, List[OWLClassExpression])] =
    description ~ whiteSpace.? ~ comma ~ whiteSpace.? ~ descriptionList ^^ { raw =>
      (raw._1._1._1._1, raw._2)
    }

  def collectClassDetails(cls: OWLClass, details: ClassDetails): List[OWLAxiom] =
    details match {
      case ClassAnnotationDetails(anns) => anns.map(ann =>
        new OWLAnnotationAssertionAxiomImpl(
          cls.getIRI,
          ann.getProperty,
          ann.getValue, ann.annotations().collect(Collectors.toList())
        )
      )
      case ClassSubClassOfDetails(exprsWAnns) => exprsWAnns.map(exprWAnns => {
          val ce: OWLClassExpression = exprWAnns._1
          val annotations: List[OWLAnnotation] = exprWAnns._2

          new OWLSubClassOfAxiomImpl(cls, ce, annotations.asJavaCollection)
        }
      )
      case ClassEquivalentToDetails(exprsWAnns) => exprsWAnns.map(exprWAnns => {
        val ce: OWLClassExpression = exprWAnns._1
        val annotations: List[OWLAnnotation] = exprWAnns._2

        new OWLEquivalentClassesAxiomImpl(
          List(cls, ce).asJavaCollection, annotations.asJavaCollection)
      })
      case ClassDisjointWithDetails(exprsWAnns) => exprsWAnns.map(exprWAnns => {
        val ce: OWLClassExpression = exprWAnns._1
        val annotations: List[OWLAnnotation] = exprWAnns._2

        new OWLDisjointClassesAxiomImpl(
          List[OWLClassExpression](cls, ce).asJavaCollection,
          annotations.asJavaCollection)
      })
      case ClassDisjointUnionOfDetails(annsAndCEs) =>
        List(new OWLDisjointUnionAxiomImpl(
          cls, annsAndCEs._2.asJavaCollection.stream(), annsAndCEs._1.asJavaCollection))
    }

  def subClassOf: Parser[List[(OWLClassExpression, List[OWLAnnotation])]] =
    "SubClassOf:" ~ whiteSpace ~ descriptionAnnotatedList ^^ { _._2 }

  def equivalentTo: Parser[List[(OWLClassExpression, List[OWLAnnotation])]] =
    "EquivalentTo:" ~ whiteSpace ~ descriptionAnnotatedList ^^ { _._2 }

  def disjointWith: Parser[List[(OWLClassExpression, List[OWLAnnotation])]] =
    "DisjointWith:" ~ whiteSpace ~ descriptionAnnotatedList ^^ { _._2 }

  def disjointUnionOf: Parser[(List[OWLAnnotation], List[OWLClassExpression])] =
    //                        List[OWLAnnotation]  (OWLClassExpression, List[OWLClassExpression])
    "DisjointUnionOf:" ~ whiteSpace ~ annotations ~ whiteSpace ~ description2List ^^ { raw =>
      (raw._1._1._2, raw._2._1 :: raw._2._2) }

  def hasKey: Parser[(List[OWLAnnotation], List[OWLPropertyExpression])] =
    "HasKey:" ~ whiteSpace ~ annotations ~ whiteSpace ~
      // FIXME: it cannot be distinguished between obj and data properties here without further information
      { objectPropertyExpression | dataPropertyExpression } ~
      { whiteSpace ~ { objectPropertyExpression | dataPropertyExpression } }.* ^^ { raw =>
        (raw._1._1._1._2, raw._1._2 :: raw._2.map(e => e._2))
      }

  def classFrame: Parser[List[OWLAxiom]] =
    "Class:" ~ whiteSpace ~ classIRI ~ {
        { whiteSpace ~ { hasKey ^^ { HasKeyDetails(_) } } } |
        {
          { whiteSpace ~ { annotations ^^ { ClassAnnotationDetails(_) } } } |
          { whiteSpace ~ { subClassOf ^^ { ClassSubClassOfDetails(_) } } } |
          { whiteSpace ~ { equivalentTo ^^ { ClassEquivalentToDetails(_) } } } |
          { whiteSpace ~ { disjointWith ^^ { ClassDisjointWithDetails(_) } } } |
          { whiteSpace ~ { disjointUnionOf ^^ { ClassDisjointUnionOfDetails(_) } } }
        }.*
      } ^^ { raw =>
        val cls = raw._1._2
        var axioms: List[OWLAxiom] = List(
          new OWLDeclarationAxiomImpl(cls, List.empty[OWLAnnotation].asJavaCollection))

        // HasKey or List of string ClassDetails pairs
        val classDetails = raw._2

        classDetails match {
          case ~(_, HasKeyDetails(annotationsAndProperties)) =>
            axioms = axioms :+ new OWLHasKeyAxiomImpl(
              cls,
              annotationsAndProperties._2.asJavaCollection,
              annotationsAndProperties._1.asJavaCollection)
          case details: List[~[String, ClassDetails]] =>
            axioms ++= details.flatMap(e => collectClassDetails(cls, e._2))
        }

        axioms
      }

  def domain: Parser[List[(OWLClassExpression, List[OWLAnnotation])]] =
    "Domain:" ~ whiteSpace ~ descriptionAnnotatedList ^^ { _._2 }

  def range: Parser[List[(OWLClassExpression, List[OWLAnnotation])]] =
    "Range:" ~ whiteSpace ~ descriptionAnnotatedList ^^ { _._2}

  def objectPropertyCharacteristic: Parser[PropertyCharacteristic.Value] =
    "Functional" ^^ { _ => PropertyCharacteristic.Functional } |
    "InverseFunctional" ^^ { _ => PropertyCharacteristic.InverseFunctional } |
    "Reflexive" ^^ { _ => PropertyCharacteristic.Reflexive } |
    "Irreflexive" ^^ { _ => PropertyCharacteristic.Irreflexive } |
    "Symmetric" ^^ { _ => PropertyCharacteristic.Symmetric } |
    "Asymmetric" ^^ { _ => PropertyCharacteristic.Asymmetric } |
    "Transitive" ^^ { _ => PropertyCharacteristic.Transitive }

  def objectPropertyCharacteristicAnnotatedList: Parser[List[(PropertyCharacteristic.Value, List[OWLAnnotation])]] =
    { annotations ~ whiteSpace }.? ~ objectPropertyCharacteristic ~
      { whiteSpace.? ~ comma ~ whiteSpace.? ~ { annotations ~ whiteSpace }.? ~
        objectPropertyCharacteristic }.* ^^ { raw =>
      unravelAnnotatedList[PropertyCharacteristic.Value](
        List.empty, raw._1._1, raw._1._2, raw._2)
    }

  def characteristics: Parser[List[(PropertyCharacteristic.Value, List[OWLAnnotation])]] =
    "Characteristics:" ~ whiteSpace ~ objectPropertyCharacteristicAnnotatedList ^^ { _._2 }

  def objectPropertyExpressionAnnotatedList: Parser[List[(OWLObjectPropertyExpression, List[OWLAnnotation])]] =
    { annotations ~ whiteSpace }.? ~ objectPropertyExpression ~
      { whiteSpace.? ~ comma ~ whiteSpace.? ~ { annotations ~ whiteSpace }.? ~
        objectPropertyExpression }.* ^^ { raw =>
      unravelAnnotatedList[OWLObjectPropertyExpression](
        List.empty, raw._1._1, raw._1._2, raw._2)
    }

  def subPropertyOf: Parser[List[(OWLObjectPropertyExpression, List[OWLAnnotation])]] =
    "SubPropertyOf:" ~ whiteSpace ~ objectPropertyExpressionAnnotatedList ^^ { _._2 }

  def objectPropertyEquivalentTo: Parser[List[(OWLObjectPropertyExpression, List[OWLAnnotation])]] =
    "EquivalentTo:" ~ whiteSpace ~ objectPropertyExpressionAnnotatedList ^^ { _._2 }

  def objectPropertyDisjointWith: Parser[List[(OWLObjectPropertyExpression, List[OWLAnnotation])]] =
    "DisjointWith:" ~ whiteSpace ~ objectPropertyExpressionAnnotatedList ^^ { _._2 }

  def inverseOf: Parser[List[(OWLObjectPropertyExpression, List[OWLAnnotation])]] =
    "InverseOf:" ~ whiteSpace ~ objectPropertyExpressionAnnotatedList ^^ { _._2 }

  def subPropertyChain: Parser[(List[OWLObjectPropertyExpression], List[OWLAnnotation])] =
    "SubPropertyChain:" ~ whiteSpace ~ annotations ~ whiteSpace ~
      objectPropertyExpression ~ whiteSpace ~ "o" ~ whiteSpace ~ objectPropertyExpression ~
      { whiteSpace ~ "o" ~ whiteSpace ~ objectPropertyExpression }.* ^^ { raw =>
      // ...readability, my ass!
      (raw._1._1._1._1._1._2 :: { raw._1._2 :: raw._2.map(_._2)},
        raw._1._1._1._1._1._1._1._2)
    }

  def objectPropertyFrame: Parser[List[OWLAxiom]] =
    "ObjectProperty:" ~ whiteSpace ~ objectPropertyIRI ~ {
      { whiteSpace ~ { annotations ^^ { ObjectPropertyAnnotationDetails(_) } } } |
      { whiteSpace ~ { domain ^^ { ObjectPropertyDomainDetails(_) } } } |
      { whiteSpace ~ { range ^^ { ObjectPropertyRangeDetails(_) } } } |
      { whiteSpace ~ { characteristics ^^ { ObjectPropertyCharacteristicsDetails(_) } } } |
      { whiteSpace ~ { subPropertyOf ^^ { ObjectPropertySubPropertyOfDetails(_) } } } |
      { whiteSpace ~ { objectPropertyEquivalentTo ^^ { ObjectPropertyEquivalentToDetails(_) } } } |
      { whiteSpace ~ { objectPropertyDisjointWith ^^ { ObjectPropertyDisjointWithDetails(_) } } } |
      { whiteSpace ~ { inverseOf ^^ { ObjectPropertyInverseOfDetails(_) } } } |
      { whiteSpace ~ { subPropertyChain ^^ { ObjectPropertySubPropertyChainDetails(_) } } }
    }.* ^^ { raw =>
      val objProperty: OWLObjectProperty = raw._1._2
      val objPropDetails: List[ObjectPropertyDetails] = raw._2.map(_._2)
      var axioms: List[OWLAxiom] = List(
        new OWLDeclarationAxiomImpl(
          objProperty, List.empty[OWLAnnotation].asJavaCollection
        )
      )

      axioms ++= objPropDetails.flatMap(d =>
        d match {
          case ObjectPropertyAnnotationDetails(details) =>
            details.map(d => new OWLAnnotationAssertionAxiomImpl(
              objProperty.getIRI,
              d.getProperty,
              d.getValue,
              d.annotations().collect(Collectors.toList())
            ))
          case ObjectPropertyDomainDetails(details) =>
            details.map(d => new OWLObjectPropertyDomainAxiomImpl(
              objProperty,
              d._1,
              d._2.asJavaCollection
            ))
          case ObjectPropertyRangeDetails(details) =>
            details.map(d => new OWLObjectPropertyRangeAxiomImpl(
              objProperty,
              d._1,
              d._2.asJavaCollection
            ))
          case ObjectPropertyCharacteristicsDetails(details) =>
            details.map(d => {
              d._1 match {
                case PropertyCharacteristic.Functional =>
                  new OWLFunctionalObjectPropertyAxiomImpl(
                    objProperty,
                    d._2.asJavaCollection
                  )
                case PropertyCharacteristic.InverseFunctional =>
                  new OWLInverseFunctionalObjectPropertyAxiomImpl(
                    objProperty,
                    d._2.asJavaCollection
                  )
                case PropertyCharacteristic.Reflexive =>
                  new OWLReflexiveObjectPropertyAxiomImpl(
                    objProperty,
                    d._2.asJavaCollection
                  )
                case PropertyCharacteristic.Irreflexive =>
                  new OWLIrreflexiveObjectPropertyAxiomImpl(
                    objProperty,
                    d._2.asJavaCollection
                  )
                case PropertyCharacteristic.Symmetric =>
                  new OWLSymmetricObjectPropertyAxiomImpl(
                    objProperty,
                    d._2.asJavaCollection
                  )
                case PropertyCharacteristic.Asymmetric =>
                  new OWLAsymmetricObjectPropertyAxiomImpl(
                    objProperty,
                    d._2.asJavaCollection
                  )
                case PropertyCharacteristic.Transitive =>
                  new OWLTransitiveObjectPropertyAxiomImpl(
                    objProperty,
                    d._2.asJavaCollection
                  )
              }
            })
          case ObjectPropertySubPropertyOfDetails(details) =>
            details.map(d => new OWLSubObjectPropertyOfAxiomImpl(
              objProperty,
              d._1,
              d._2.asJavaCollection
            ))
          case ObjectPropertyEquivalentToDetails(details) =>
            details.map(d => new OWLEquivalentObjectPropertiesAxiomImpl(
              List(
                objProperty,
                d._1
              ).asJavaCollection,
              d._2.asJavaCollection
            ))
          case ObjectPropertyDisjointWithDetails(details) =>
            details.map(d => new OWLDisjointObjectPropertiesAxiomImpl(
              List(
                objProperty,
                d._1
              ).asJavaCollection,
              d._2.asJavaCollection
            ))
          case ObjectPropertyInverseOfDetails(details) =>
            details.map(d => new OWLInverseObjectPropertiesAxiomImpl(
              objProperty,
              d._1,
              d._2.asJavaCollection
            ))
          case ObjectPropertySubPropertyChainDetails(details) =>
            List(new OWLSubPropertyChainAxiomImpl(
              details._1.asJava,
              objProperty,
              details._2.asJava
            ))
        }
      )

      axioms
    }

  def dataPropertyDomain: Parser[List[(OWLClassExpression, List[OWLAnnotation])]] =
    "Domain:" ~ whiteSpace ~ descriptionAnnotatedList ^^ { _._2 }

  def dataRangeAnnotatedList: Parser[List[(OWLDataRange, List[OWLAnnotation])]] =
    { annotations ~ whiteSpace }.? ~ dataRange ~
      { whiteSpace.? ~ comma ~ whiteSpace.? ~
        { annotations ~ whiteSpace }.? ~ dataRange }.* ^^ { raw =>
      unravelAnnotatedList(List.empty, raw._1._1, raw._1._2, raw._2)
    }

  def dataPropertyRange: Parser[List[(OWLDataRange, List[OWLAnnotation])]] =
    "Range:" ~ whiteSpace ~ dataRangeAnnotatedList ^^ { _._2 }

  def dataPropertyCharacteristics: Parser[List[OWLAnnotation]] =
    "Characteristics:" ~ whiteSpace ~ annotations ~ whiteSpace ~ "Functional" ^^ { _._1._1._2 }

  def dataPropertyExpressionAnnotatedList: Parser[List[(OWLDataPropertyExpression, List[OWLAnnotation])]] =
    { annotations ~ whiteSpace }.? ~ dataPropertyExpression ~
      { whiteSpace.? ~ comma ~ whiteSpace.? ~
        { annotations ~ whiteSpace }.? ~ dataPropertyExpression }.* ^^ { raw =>
      unravelAnnotatedList(List.empty, raw._1._1, raw._1._2, raw._2)
    }

  def dataPropertySubPropertyOf: Parser[List[(OWLDataPropertyExpression, List[OWLAnnotation])]] =
    "SubPropertyOf:" ~ whiteSpace ~ dataPropertyExpressionAnnotatedList ^^ { _._2 }

  def dataPropertyEquivalentTo: Parser[List[(OWLDataPropertyExpression, List[OWLAnnotation])]] =
    "EquivalentTo:" ~ whiteSpace ~ dataPropertyExpressionAnnotatedList ^^ { _._2 }

  def dataPropertyDisjointWith: Parser[List[(OWLDataPropertyExpression, List[OWLAnnotation])]] =
    "DisjointWith:" ~ whiteSpace ~ dataPropertyExpressionAnnotatedList ^^ { _._2 }

  def dataPropertyFrame: Parser[List[OWLAxiom]] =
    "DataProperty:" ~ whiteSpace ~ dataPropertyIRI ~ {
      { whiteSpace ~ { annotations ^^ { DataPropertyAnnotationDetails(_) } } } |
      { whiteSpace ~ { dataPropertyDomain ^^ { DataPropertyDomainDetails(_) } } } |
      { whiteSpace ~ { dataPropertyRange ^^ { DataPropertyRangeDetails(_) } } } |
      { whiteSpace ~ { dataPropertyCharacteristics ^^ { DataPropertyCharacteristicsDetails(_) } } } |
      { whiteSpace ~ { dataPropertySubPropertyOf ^^ { DataPropertySubPropertyOfDetails(_) } } } |
      { whiteSpace ~ { dataPropertyEquivalentTo ^^ { DataPropertyEquivalentToDetails(_) } } } |
      { whiteSpace ~ { dataPropertyDisjointWith ^^ { DataPropertyDisjointWithDetails(_) } } }
    }.* ^^ { raw =>
      val dataProperty: OWLDataProperty = raw._1._2
      val dataPropDetails: List[DataPropertyDetails] = raw._2.map(_._2)
      var axioms: List[OWLAxiom] = List(
        new OWLDeclarationAxiomImpl(
          dataProperty, List.empty[OWLAnnotation].asJavaCollection
        )
      )

      axioms ++= dataPropDetails.flatMap(d =>
        d match {
          case DataPropertyAnnotationDetails(details) => details.map(d =>
            new OWLAnnotationAssertionAxiomImpl(
              dataProperty.getIRI,
              d.getProperty,
              d.getValue,
              d.annotations().collect(Collectors.toList())
            )
          )
          case DataPropertyDomainDetails(details) => details.map(d =>
            new OWLDataPropertyDomainAxiomImpl(
              dataProperty,
              d._1,
              d._2.asJavaCollection
            )
          )
          case DataPropertyRangeDetails(details) => details.map(d =>
            new OWLDataPropertyRangeAxiomImpl(
              dataProperty,
              d._1,
              d._2.asJavaCollection
            )
          )
          case DataPropertyCharacteristicsDetails(details) =>
            List(
              new OWLFunctionalDataPropertyAxiomImpl(dataProperty, details.asJavaCollection)
            )
          case DataPropertySubPropertyOfDetails(details) =>
            details.map(d =>
              new OWLSubDataPropertyOfAxiomImpl(
                dataProperty,
                d._1,
                d._2.asJavaCollection
              )
            )
          case DataPropertyEquivalentToDetails(details) =>
            details.map(d =>
              new OWLEquivalentDataPropertiesAxiomImpl(
                List(dataProperty, d._1).asJavaCollection,
                d._2.asJavaCollection
              )
            )
          case DataPropertyDisjointWithDetails(details) =>
            details.map(d =>
              new OWLDisjointDataPropertiesAxiomImpl(
                List(dataProperty, d._1).asJavaCollection,
                d._2.asJavaCollection
              )
            )
        }
      )
      axioms
    }

  def iriAnnotatedList: Parser[List[(IRI, List[OWLAnnotation])]] =
    { annotations ~ whiteSpace }.? ~ iri ~
      { whiteSpace.? ~ comma ~ whiteSpace.? ~ { annotations ~ whiteSpace }.? ~ iri }.* ^^ { raw =>
      unravelAnnotatedList(List.empty, raw._1._1, raw._1._2, raw._2)
    }

  def annotationPropertyDomain: Parser[List[(IRI, List[OWLAnnotation])]] =
    "Domain:" ~ whiteSpace ~ iriAnnotatedList ^^ { _._2 }

  def annotationPropertyRange: Parser[List[(IRI, List[OWLAnnotation])]] =
    "Range:" ~ whiteSpace ~ iriAnnotatedList ^^ { _._2 }

  def annotationPropertyIRIAnnotatedList: Parser[List[(OWLAnnotationProperty, List[OWLAnnotation])]] =
    { annotations ~ whiteSpace }.? ~ annotationPropertyIRI ~
      { whiteSpace.? ~ comma ~ whiteSpace.? ~ { annotations ~ whiteSpace }.? ~
        annotationPropertyIRI }.* ^^ { raw =>
      unravelAnnotatedList(List.empty, raw._1._1, raw._1._2, raw._2)
    }

  def annotationPropertySubPropertyOf: Parser[List[(OWLAnnotationProperty, List[OWLAnnotation])]] =
    "SubPropertyOf:" ~ whiteSpace ~ annotationPropertyIRIAnnotatedList ^^ { _._2 }

  /**
    * Since the specs (https://www.w3.org/TR/owl2-manchester-syntax/#annotationPropertyFrame)
    * say an annotation property frame should look like this
    *
    *   annotationPropertyFrame ::=
    *       'AnnotationProperty:' annotationPropertyIRI {
    *           'Annotations:' annotationAnnotatedList } |
    *           'Domain:' IRIAnnotatedList |
    *           'Range:' IRIAnnotatedList |
    *           'SubPropertyOf:' annotationPropertyIRIAnnotatedList
    *
    * (which means either annotations, or one domain, or one range, or one
    * subproperty of part) but give an example that violates this rule:
    *
    *    AnnotationProperty: creator
    *        Annotations: ...
    *        Domain: Person ,...
    *        Range: integer ,...
    *        SubPropertyOf: initialCreator ,...
    *
    * I assume spec should be
    *
    *   annotationPropertyFrame ::=
    *       'AnnotationProperty:' annotationPropertyIRI {
    *           'Annotations:' annotationAnnotatedList |
    *           'Domain:' IRIAnnotatedList |
    *           'Range:' IRIAnnotatedList |
    *           'SubPropertyOf:' annotationPropertyIRIAnnotatedList
    *        }
    */
  def annotationPropertyFrame: Parser[List[OWLAxiom]] =
    "AnnotationProperty:" ~ whiteSpace ~ annotationPropertyIRI ~ {
      { whiteSpace ~ { annotations ^^ { AnnotationPropertyAnnotationDetails(_) } } } |
      { whiteSpace ~ { annotationPropertyDomain ^^ { AnnotationPropertyDomainDetails(_) } } } |
      { whiteSpace ~ { annotationPropertyRange ^^ { AnnotationPropertyRangeDetails(_) } } } |
      { whiteSpace ~ { annotationPropertySubPropertyOf ^^ { AnnotationPropertySubPropertyOfDetails(_) } } }
    }.* ^^ { raw =>
      val annotationProperty: OWLAnnotationProperty = raw._1._2
      val annotationPropDetails: List[AnnotationPropertyDetails] = raw._2.map(_._2)
      var axioms: List[OWLAxiom] = List(
        new OWLDeclarationAxiomImpl(
          annotationProperty, List.empty[OWLAnnotation].asJavaCollection
        )
      )

      axioms ++= annotationPropDetails.flatMap {
        case AnnotationPropertyAnnotationDetails(details) =>
          details.map(d =>
            new OWLAnnotationAssertionAxiomImpl(
              annotationProperty.getIRI,
              d.getProperty,
              d.getValue,
              d.annotations().collect(Collectors.toList())
            )
          )
        case AnnotationPropertyDomainDetails(details) =>
          details.map(d =>
            new OWLAnnotationPropertyDomainAxiomImpl(
              annotationProperty,
              d._1,
              d._2.asJavaCollection
            )
          )
        case AnnotationPropertyRangeDetails(details) =>
          details.map(d =>
            new OWLAnnotationPropertyRangeAxiomImpl(
              annotationProperty,
              d._1,
              d._2.asJavaCollection
            )
          )
        case AnnotationPropertySubPropertyOfDetails(details) =>
          details.map(d =>
            new OWLSubAnnotationPropertyOfAxiomImpl(
              annotationProperty,
              d._1,
              d._2.asJavaCollection
            )
          )
      }

      axioms
    }

  def individualAnnotations: Parser[List[OWLAnnotation]] =
    "Annotations:" ~ whiteSpace ~ annotationAnnotatedList ^^ { _._2 }

  def individualTypes: Parser[List[(OWLClassExpression, List[OWLAnnotation])]] =
    "Types:" ~ whiteSpace ~ descriptionAnnotatedList ^^ { _._2 }

  def objectPropertyFact: Parser[(OWLObjectProperty, OWLIndividual)] =
    objectPropertyIRI ~ whiteSpace ~ individual ^^ { raw => (raw._1._1, raw._2) }

  def dataPropertyFact: Parser[(OWLDataProperty, OWLLiteral)] =
    dataPropertyIRI ~ whiteSpace ~ literal ^^ { raw => (raw._1._1, raw._2)}

  def fact: Parser[(Boolean, OWLProperty, OWLPropertyAssertionObject)] =
    { "not" ~ whiteSpace }.? ~ {
      objectPropertyFact | dataPropertyFact
    } ^^ { raw => raw._1 match {
      case Some(_) => (false, raw._2._1, raw._2._2)
      case None => (true, raw._2._1, raw._2._2)
    }}

  def factAnnotatedList: Parser[List[((Boolean, OWLProperty, OWLPropertyAssertionObject), List[OWLAnnotation])]] =
    { annotations ~ whiteSpace }.? ~ fact ~ {
      whiteSpace.? ~ comma ~ whiteSpace.? ~ { annotations ~ whiteSpace }.? ~ fact
    }.* ^^ { raw =>
      unravelAnnotatedList(List.empty, raw._1._1, raw._1._2, raw._2)
    }

  def individualFacts: Parser[List[((Boolean, OWLProperty, OWLPropertyAssertionObject), List[OWLAnnotation])]] =
    "Facts:" ~ whiteSpace ~ factAnnotatedList ^^ { _._2 }

  def individualAnnotatedList: Parser[List[(OWLIndividual, List[OWLAnnotation])]] =
    { annotations ~ whiteSpace }.? ~ individual ~ {
      whiteSpace.? ~ comma ~ whiteSpace.? ~ { annotations ~ whiteSpace }.? ~ individual
    }.* ^^ { raw =>
      unravelAnnotatedList(List.empty, raw._1._1, raw._1._2, raw._2)
    }

  def individualSameAs: Parser[List[(OWLIndividual, List[OWLAnnotation])]] =
    "SameAs:" ~ whiteSpace ~ individualAnnotatedList ^^ { _._2 }

  def individualDifferentFrom: Parser[List[(OWLIndividual, List[OWLAnnotation])]] =
    "DifferentFrom:" ~ whiteSpace ~ individualAnnotatedList ^^ { _._2 }

  def individualFrame: Parser[List[OWLAxiom]] =
    "Individual:" ~ whiteSpace ~ individualIRI ~ {
      { whiteSpace ~ { individualAnnotations ^^ { IndividualAnnotationDetails(_) } } } |
      { whiteSpace ~ { individualTypes ^^ { IndividualTypesDetails(_) } } } |
      { whiteSpace ~ { individualFacts ^^ { IndividualFactsDetails(_) } } } |
      { whiteSpace ~ { individualSameAs ^^ { IndividualSameAsDetails(_) } } } |
      { whiteSpace ~ { individualDifferentFrom ^^ { IndividualDifferentFromDetails(_) } } }
    }.* ^^ { raw =>
      val indiv: OWLNamedIndividual = raw._1._2
      val indivDetails: List[IndividualDetails] = raw._2.map(_._2)
      var axioms: List[OWLAxiom] = List(
        new OWLDeclarationAxiomImpl(indiv, List.empty[OWLAnnotation].asJavaCollection)
      )

      axioms ++= indivDetails.flatMap {
        case IndividualAnnotationDetails(details) => details.map(d =>
          new OWLAnnotationAssertionAxiomImpl(
            indiv.getIRI,
            d.getProperty,
            d.getValue,
            d.annotations().collect(Collectors.toList())
          )
        )
        case IndividualTypesDetails(details) => details.map(d =>
          new OWLClassAssertionAxiomImpl(
            indiv,
            d._1,
            d._2.asJavaCollection
          )
        )
        case IndividualFactsDetails(details) => details.map(d => {
          val positiveAssertion: Boolean = d._1._1
          val prop = d._1._2
          val value = d._1._3
          val annotations = d._2

          if (positiveAssertion) {
            prop match {
              case prop: OWLObjectProperty =>
                new OWLObjectPropertyAssertionAxiomImpl(
                  indiv,
                  prop,
                  value.asInstanceOf[OWLIndividual],
                  annotations.asJavaCollection
                )
              case prop: OWLDataProperty =>
                new OWLDataPropertyAssertionAxiomImpl(
                  indiv,
                  prop,
                  value.asInstanceOf[OWLLiteral],
                  annotations.asJavaCollection
                )
            }
          } else {
            prop match {
              case prop: OWLObjectProperty =>
                new OWLNegativeObjectPropertyAssertionAxiomImpl(
                  indiv,
                  prop,
                  value.asInstanceOf[OWLIndividual],
                  annotations.asJavaCollection
                )
              case prop: OWLDataProperty =>
                new OWLNegativeDataPropertyAssertionAxiomImpl(
                  indiv,
                  prop,
                  value.asInstanceOf[OWLLiteral],
                  annotations.asJavaCollection
                )
            }
          }
        })
        case IndividualSameAsDetails(details) => details.map(d =>
          new OWLSameIndividualAxiomImpl(
            List(indiv, d._1).asJavaCollection,
            d._2.asJavaCollection
          )
        )
        case IndividualDifferentFromDetails(details) => details.map(d =>
          new OWLDifferentIndividualsAxiomImpl(
            List(indiv, d._1).asJavaCollection,
            d._2.asJavaCollection
          )
        )
      }

      axioms
    }

  def equivalentClasses: Parser[List[OWLAxiom]] =
    "EquivalentClasses:" ~ whiteSpace ~ annotations ~ whiteSpace ~ description2List ^^ { raw =>
      val annotations: List[OWLAnnotation] = raw._1._1._2
      val descriptions: List[OWLClassExpression] = raw._2._1 :: raw._2._2
      List(
        new OWLEquivalentClassesAxiomImpl(
          descriptions.asJavaCollection, annotations.asJavaCollection)
      )
    }

  def disjointClasses: Parser[List[OWLAxiom]] =
    "DisjointClasses:" ~ whiteSpace ~ annotations ~ whiteSpace ~ description2List ^^ { raw =>
      val annotations: List[OWLAnnotation] = raw._1._1._2
      val descriptions: List[OWLClassExpression] = raw._2._1 :: raw._2._2

      List(
        new OWLDisjointClassesAxiomImpl(
          descriptions.asJavaCollection, annotations.asJavaCollection)
      )
    }

  def objectPropertyExpressionList: Parser[List[OWLObjectPropertyExpression]] =
    objectPropertyExpression ~
      { whiteSpace.? ~ comma ~ whiteSpace.? ~ objectPropertyExpression }.*  ^^ { raw =>
      raw._1 :: raw._2.map(_._2)
    }

  def objectPropertyExpression2List: Parser[List[OWLObjectPropertyExpression]] =
    objectPropertyExpression ~ whiteSpace.? ~ comma ~ whiteSpace.? ~
      objectPropertyExpressionList ^^ { raw => raw._1._1._1._1 :: raw._2 }

  def equivalentObjectProperties: Parser[List[OWLAxiom]] =
    "EquivalentProperties:" ~ whiteSpace ~ annotations ~ whiteSpace ~
      objectPropertyExpression2List ^^ { raw =>
      List(
        new OWLEquivalentObjectPropertiesAxiomImpl(
          raw._2.asJavaCollection,
          raw._1._1._2.asJavaCollection
        )
      )
    }

  def disjointObjectProperties: Parser[List[OWLAxiom]] =
    "DisjointProperties:" ~ whiteSpace ~ annotations ~ whiteSpace ~
      objectPropertyExpression2List ^^ { raw =>
      List(
        new OWLDisjointObjectPropertiesAxiomImpl(
          raw._2.asJavaCollection,
          raw._1._1._2.asJavaCollection
        )
      )
    }

  def dataPropertyExpressionList: Parser[List[OWLDataPropertyExpression]] =
    dataPropertyExpression ~
      { whiteSpace.? ~ comma ~ whiteSpace.? ~ dataPropertyExpression }.* ^^ { raw =>
      raw._1 :: raw._2.map(_._2)
    }

  def dataProperty2List: Parser[List[OWLDataPropertyExpression]] =
    dataPropertyExpression ~ whiteSpace.? ~ comma ~ whiteSpace.? ~
      dataPropertyExpressionList ^^ { raw => raw._1._1._1._1 :: raw._2 }

  def equivalentDataProperties: Parser[List[OWLAxiom]] =
    "EquivalentProperties:" ~ whiteSpace ~ annotations ~ whiteSpace ~
      dataProperty2List ^^ { raw =>
      List(
        new OWLEquivalentDataPropertiesAxiomImpl(
          raw._2.asJavaCollection,
          raw._1._1._2.asJavaCollection
        )
      )
    }

  def disjointDataProperties: Parser[List[OWLAxiom]] =
    "DisjointProperties:" ~ whiteSpace ~ annotations ~ whiteSpace ~
      dataProperty2List ^^ { raw =>
      List(
        new OWLDisjointDataPropertiesAxiomImpl(
          raw._2.asJavaCollection,
          raw._1._1._2.asJavaCollection
        )
      )
    }

  def individual2List: Parser[List[OWLIndividual]] =
    individual ~ whiteSpace.? ~ comma ~ whiteSpace.? ~ individualList ^^ { raw =>
      raw._1._1._1._1 :: raw._2
    }

  def sameIndividual: Parser[List[OWLAxiom]] =
    "SameIndividual:" ~ whiteSpace ~ annotations ~ whiteSpace ~
      individual2List ^^ { raw =>
      List(
        new OWLSameIndividualAxiomImpl(
          raw._2.asJavaCollection,
          raw._1._1._2.asJavaCollection
        )
      )
    }

  def differentIndividuals: Parser[List[OWLAxiom]] =
    "DifferentIndividuals:" ~ whiteSpace ~ annotations ~ whiteSpace ~
      individual2List ^^ { raw =>
      List(
        new OWLDifferentIndividualsAxiomImpl(
          raw._2.asJavaCollection,
          raw._1._1._2.asJavaCollection
        )
      )
    }

  // FIXME: equivalentDataProperties and disjointDataProperties will never match since already matched by equivalentObjectProperties and disjointObjectProperties, respectively
  def misc: Parser[List[OWLAxiom]] =
    equivalentClasses | disjointClasses | equivalentObjectProperties |
      disjointObjectProperties | equivalentDataProperties |
      disjointDataProperties | sameIndividual | differentIndividuals

  def frame: Parser[List[OWLAxiom]] = datatypeFrame | classFrame |
    objectPropertyFrame | dataPropertyFrame | annotationPropertyFrame |
    individualFrame | misc

//  def ontology: Parser[List[OWLAxiom]] = "Ontology: " ~ { ontologyIRI ~ versionIRI.? }.? ~
//    { imp0rt }.* ~ annotations.* ~ frame.* ^^ { _ => "" }  // TODO: return something meaningful
//  def ontologyDocument: Parser[String] = prefixDeclaration.* ~ ontology ^^ {
//    _ => "" }  // TODO: return something meaningful
}


object ManchesterParser extends ManchesterParsing {
  def parseFrame(frameStr: String): List[OWLAxiom] = checkParsed(frame, frameStr)

  def checkParsed[U](fn: Parser[U], input: String): U = {
    parse(fn, input) match {
      case Success(matched: U, _) => matched
      case Failure(msg, _) => throw ParserException(msg)
      case Error(msg, _) => throw ParserException(msg)
    }
  }
}
