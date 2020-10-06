package net.sansa_stack.owl.common.parsing

import scala.collection.mutable
import scala.util.matching.Regex
import scala.util.parsing.combinator.RegexParsers

import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.model.{IRI, OWLDataFactory}
import org.semanticweb.owlapi.vocab.Namespaces



/**
  * Trait for parsing IRIs
  */
trait IRIParsing extends RegexParsers {
  override def skipWhitespace: Boolean = false
  override val whiteSpace: Regex = "\\s+".r

  val dataFactory: OWLDataFactory = OWLManager.getOWLDataFactory
  val prefixes: mutable.Map[String, String] = mutable.Map(
    "rdf" -> "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "rdfs" -> "http://www.w3.org/2000/01/rdf-schema#",
    "xsd" -> "http://www.w3.org/2001/XMLSchema#",
    "owl" -> "http://www.w3.org/2002/07/owl#"
  )

  def toString(input: Any): String = {
    input match {
      case input: String => input
      case ~(first, second) => toString(first) + toString(second)
      case List() => ""
      case head :: tail => toString(head) + toString(tail)
      case Some(value) => toString(value)
      case None => ""
      case _ => throw new RuntimeException(
        "Case not covered: " + input.toString + ": " + input.getClass)
    }
  }

  /*
   * Low level patterns
   */
  def alpha: Parser[String] = "[a-zA-Z]".r ^^ { _.toString }
  def digit: Parser[String] = "[0-9]".r ^^ { _.toString }
  def hexDigit: Parser[String] = "[0-9a-fA-F]".r ^^ { _.toString }
  def plus: Parser[String] = "+"
  def minus: Parser[String] = "-"
  def dot: Parser[String] = "."
  def at: Parser[String] = "@"
  def dash: Parser[String] = "-"
  def underscore: Parser[String] = "_"
  def tilde: Parser[String] = "~"
  def openingBracket: Parser[String] = "["
  def closingBracket: Parser[String] = "]"
  def slash: Parser[String] = "/"
  def percent: Parser[String] = "%"
  def exclamationMark: Parser[String] = "!"
  def dollar: Parser[String] = "$"
  def ampersand: Parser[String] = "&"
  def quote: Parser[String] = "'"
  def doubleQuote: Parser[String] = "\""
  def openingParen: Parser[String] = "("
  def closingParen: Parser[String] = ")"
  def asterisk: Parser[String] = "*"
  def comma: Parser[String] = ","
  def semicolon: Parser[String] = ";"
  def equalsSign: Parser[String] = "="
  def colon: Parser[String] = ":".r ^^ { _.toString }
  def questionmark: Parser[String] = "?"
  def hashSign: Parser[String] = "#"
  def circumflex: Parser[String] = "^"
  def openingCurlyBrace: Parser[String] = "{"
  def closingCurlyBrace: Parser[String] = "}"
  def openingAngleBracket: Parser[String] = "<"
  def closingAngleBracket: Parser[String] = ">"
  def doubleSlash: Parser[String] = slash ~ slash ^^ { toString(_) }

  /*
   * IRI patterns
   * Sources:
   * - http://www.ietf.org/rfc/rfc3987.txt
   * - https://www.w3.org/TR/2008/REC-rdf-sparql-query-20080115/
   */
  def scheme: Parser[String] =
    alpha ~ { alpha | digit | plus | minus | dot }.* ^^ { toString(_) }

  def iuserinfo: Parser[String] =
    {iunreserved | pctEncoded | subDelims | colon}.* ^^ { toString(_) }

  def ipv6Block: Parser[String] = { repN(4, hexDigit) | repN(3, hexDigit) |
    repN(2, hexDigit) | repN(1, hexDigit) } ^^ { toString(_) }

  def ipv6address: Parser[String] =
    /*
     * 6( h16 ":" ) ls32
     *   --> 8 ipv6 blocks (separated by colons)
     */
    ipv6Block ~ colon ~ ipv6Block ~ colon ~ ipv6Block ~ colon ~ ipv6Block ~
      colon ~ ipv6Block ~ colon ~ ipv6Block ~ colon ~ ipv6Block ~ colon ~
      ipv6Block ^^ { toString(_)} |
      /*
       * "::" 5( h16 ":" ) ls32
       *   --> :: + 7 ipv6 blocks (separated by colons)
       */
      colon ~ colon ~ ipv6Block ~ colon ~ ipv6Block ~ colon ~ ipv6Block ~ colon ~
        ipv6Block ~ colon ~ ipv6Block ~ colon ~ ipv6Block ~ colon ~
        ipv6Block ^^ { toString(_) } |
      /*
       * [               h16 ] "::" 4( h16 ":" ) ls32
       *   --> :: + 6 ipv6 blocks (separated by colons)
       */
      colon ~ colon ~ ipv6Block ~ colon ~ ipv6Block ~ colon ~ ipv6Block ~ colon ~
        ipv6Block ~ colon ~ ipv6Block ~ colon ~ ipv6Block ^^ { toString(_) } |
      /*
       * [               h16 ] "::" 4( h16 ":" ) ls32
       *   --> 1 ipv6 block + :: + 6 ipv6 blocks (separated by colons)
       */
      ipv6Block ~ colon ~ colon ~ ipv6Block ~ colon ~ ipv6Block ~ colon ~
        ipv6Block ~ colon ~ ipv6Block ~ colon ~ ipv6Block ~ colon ~
        ipv6Block ^^ { toString(_) } |
      /*
       * [ *1( h16 ":" ) h16 ] "::" 3( h16 ":" ) ls32
       *   --> :: + 5 ipv6 blocks (separated by colons)
       */
      colon ~ colon ~ ipv6Block ~ colon ~ ipv6Block ~ colon ~ ipv6Block ~ colon ~
        ipv6Block ~ colon ~ ipv6Block ^^ { toString(_) } |
      /*
       * [ *1( h16 ":" ) h16 ] "::" 3( h16 ":" ) ls32
       *   --> 1 ipv6 block + :: + 5 ipv6 blocks (separated by colons)
       */
      ipv6Block ~ colon ~ colon ~ ipv6Block ~ colon ~ ipv6Block ~ colon ~
        ipv6Block ~ colon ~ ipv6Block ~ colon ~ ipv6Block ^^ { toString(_) } |
      /*
       * [ *1( h16 ":" ) h16 ] "::" 3( h16 ":" ) ls32
       *   --> 2 ipv6 blocks (separated by colons) + :: + 5 ipv6 blocks (separated by colons)
       */
      ipv6Block ~ colon ~ ipv6Block ~ colon ~ colon ~ ipv6Block ~ colon ~
        ipv6Block ~ colon ~ ipv6Block ~ colon ~ ipv6Block ~ colon ~ ipv6Block ^^
        { toString(_) } |
      /*
       * [ *2( h16 ":" ) h16 ] "::" 2( h16 ":" ) ls32
       *   --> :: + 4 ipv6 blocks (separated by colons)
       */
      colon ~ colon ~ ipv6Block ~ colon ~ ipv6Block ~ colon ~ ipv6Block ~ colon ~
        ipv6Block ^^ { toString(_) } |
      /*
       * [ *2( h16 ":" ) h16 ] "::" 2( h16 ":" ) ls32
       *   --> 1 ipv6 block + :: + 4 ipv6 blocks (separated by colons)
       */
      ipv6Block ~ colon ~ colon ~ ipv6Block ~ colon ~ ipv6Block ~ colon ~
        ipv6Block ~ colon ~ ipv6Block ^^ { toString(_) } |
      /*
       * [ *2( h16 ":" ) h16 ] "::" 2( h16 ":" ) ls32
       *   --> 2 ipv6 blocks (separated by colons) + :: + 4 ipv6 blocks (separated by colons)
       */
      ipv6Block ~ colon ~ ipv6Block ~ colon ~ colon ~ ipv6Block ~ colon ~
        ipv6Block ~ colon ~ ipv6Block ~ colon ~ ipv6Block ^^ { toString(_) } |
      /*
       * [ *2( h16 ":" ) h16 ] "::" 2( h16 ":" ) ls32
       *   --> 3 ipv6 block (separated by colons) + :: + 4 ipv6 blocks (separated by colons)
       */
      ipv6Block ~ colon ~ ipv6Block ~ colon ~ ipv6Block ~ colon ~ colon ~
        ipv6Block ~ colon ~ ipv6Block ~ colon ~ ipv6Block ~ colon ~ ipv6Block ^^
        { toString(_) } |
      /*
       * [ *3( h16 ":" ) h16 ] "::"    h16 ":"   ls32
       *   --> :: 3 ipv6 blocks (separated by colons)
       */
      colon ~ colon ~ ipv6Block ~ colon ~ ipv6Block ~ colon ~ ipv6Block ^^
        { toString(_) } |
      /*
       * [ *3( h16 ":" ) h16 ] "::"    h16 ":"   ls32
       *   --> 1 ipv6 block + :: 3 ipv6 blocks (separated by colons)
       */
      ipv6Block ~ colon ~ colon ~ ipv6Block ~ colon ~ ipv6Block ~ colon ~
        ipv6Block ^^ { toString(_) } |
      /*
       * [ *3( h16 ":" ) h16 ] "::"    h16 ":"   ls32
       *   --> 2 ipv6 blocks (separated by colons) + :: 3 ipv6 blocks (separated by colons)
       */
      ipv6Block ~ colon ~ ipv6Block ~ colon ~ colon ~ ipv6Block ~ colon ~
        ipv6Block ~ colon ~ ipv6Block ^^ { toString(_) } |
      /*
       * [ *3( h16 ":" ) h16 ] "::"    h16 ":"   ls32
       *   --> 3 ipv6 blocks (separated by colons) + :: 3 ipv6 blocks (separated by colons)
       */
      ipv6Block ~ colon ~ ipv6Block ~ colon ~ ipv6Block ~ colon ~ colon ~
        ipv6Block ~ colon ~ ipv6Block ~ colon ~ ipv6Block ^^ { toString(_) } |
      /*
       * [ *3( h16 ":" ) h16 ] "::"    h16 ":"   ls32
       *   --> 4 ipv6 blocks (separated by colons) + :: 3 ipv6 blocks (separated by colons)
       */
      ipv6Block ~ colon ~ ipv6Block ~ colon ~ ipv6Block ~ colon ~ ipv6Block ~
        colon ~ colon ~ ipv6Block ~ colon ~ ipv6Block ~ colon ~ ipv6Block ^^
        { toString(_) } |
      /*
       * [ *4( h16 ":" ) h16 ] "::"              ls32
       *   --> :: + 2 ipv6 blocks (separated by colons)
       */
      colon ~ colon ~ ipv6Block ~ colon ~ ipv6Block ^^ { toString(_) } |
      /*
       * [ *4( h16 ":" ) h16 ] "::"              ls32
       *   --> 1 ipv6 block + :: + 2 ipv6 blocks (separated by colons)
       */
      ipv6Block ~ colon ~ colon ~ ipv6Block ~ colon ~ ipv6Block ^^ { toString(_) } |
      /*
       * [ *4( h16 ":" ) h16 ] "::"              ls32
       *   --> 2 ipv6 blocks (separated by colons) + :: + 2 ipv6 blocks (separated by colons)
       */
      ipv6Block ~ colon ~ ipv6Block ~ colon ~ colon ~ ipv6Block ~ colon ~
        ipv6Block ^^ { toString(_) } |
      /*
       * [ *4( h16 ":" ) h16 ] "::"              ls32
       *   --> 3 ipv6 blocks (separated by colons) + :: + 2 ipv6 blocks (separated by colons)
       */
      ipv6Block ~ colon ~ ipv6Block ~ colon ~ ipv6Block ~ colon ~ colon ~
        ipv6Block ~ colon ~ ipv6Block ^^ { toString(_) } |
      /*
       * [ *4( h16 ":" ) h16 ] "::"              ls32
       *   --> 4 ipv6 blocks (separated by colons) + :: + 2 ipv6 blocks (separated by colons)
       */
      ipv6Block ~ colon ~ ipv6Block ~ colon ~ ipv6Block ~ colon ~ ipv6Block ~
        colon ~ colon ~ ipv6Block ~ colon ~ ipv6Block ^^ { toString(_) } |
      /*
       * [ *4( h16 ":" ) h16 ] "::"              ls32
       *   --> 5 ipv6 blocks (separated by colons) + :: + 2 ipv6 blocks (separated by colons)
       */
      ipv6Block ~ colon ~ ipv6Block ~ colon ~ ipv6Block ~ colon ~ ipv6Block ~
        colon ~ ipv6Block ~ colon ~ colon ~ ipv6Block ~ colon ~ ipv6Block ^^
        { toString(_) } |
      /*
       * [ *5( h16 ":" ) h16 ] "::"              h16
       *   --> :: + 1 ipv6 block
       */
      colon ~ colon ~ ipv6Block ^^ { toString(_) } |
      /*
       * [ *5( h16 ":" ) h16 ] "::"              h16
       *   --> 1 ipv6 block + :: + 1 ipv6 block
       */
      ipv6Block ~ colon ~ colon ~ ipv6Block ^^ { toString(_) } |
      /*
       * [ *5( h16 ":" ) h16 ] "::"              h16
       *   --> 2 ipv6 blocks (separated by colons) + :: + 1 ipv6 block
       */
      ipv6Block ~ colon ~ ipv6Block ~ colon ~ colon ~ ipv6Block ^^ { toString(_) } |
      /*
       * [ *5( h16 ":" ) h16 ] "::"              h16
       *   --> 3 ipv6 blocks (separated by colons) + :: + 1 ipv6 block
       */
      ipv6Block ~ colon ~ ipv6Block ~ colon ~ ipv6Block ~ colon ~ colon ~
        ipv6Block ^^ { toString(_) } |
      /*
       * [ *5( h16 ":" ) h16 ] "::"              h16
       *   --> 4 ipv6 blocks (separated by colons) + :: + 1 ipv6 block
       */
      ipv6Block ~ colon ~ ipv6Block ~ colon ~ ipv6Block ~ colon ~ ipv6Block ~
        colon ~ colon ~ ipv6Block ^^ { toString(_) } |
      /*
       * [ *5( h16 ":" ) h16 ] "::"              h16
       *   --> 5 ipv6 blocks (separated by colons) + :: + 1 ipv6 block
       */
      ipv6Block ~ colon ~ ipv6Block ~ colon ~ ipv6Block ~ colon ~ ipv6Block ~
        colon ~ ipv6Block ~ colon ~ colon ~ ipv6Block ^^ { toString(_) } |
      /*
       * [ *5( h16 ":" ) h16 ] "::"              h16
       *   --> 6 ipv6 blocks (separated by colons) + :: + 1 ipv6 block
       */
      ipv6Block ~ colon ~ ipv6Block ~ colon ~ ipv6Block ~ colon ~ ipv6Block ~
        colon ~ ipv6Block ~ colon ~ ipv6Block ~ colon ~ colon ~ ipv6Block ^^
        { toString(_) } |
      /*
       * [ *6( h16 ":" ) h16 ] "::"
       *   --> ::
       */
      colon ~ colon ^^ { toString(_) } |
      /*
       * [ *6( h16 ":" ) h16 ] "::"
       *   --> 1 ipv6 block + ::
       */
      ipv6Block ~ colon ~ colon ^^ { toString(_) } |
      /*
       * [ *6( h16 ":" ) h16 ] "::"
       *   --> 2 ipv6 blocks (separated by colons) + ::
       */
      ipv6Block ~ colon ~ ipv6Block ~ colon ~ colon ^^ { toString(_) } |
      /*
       * [ *6( h16 ":" ) h16 ] "::"
       *   --> 3 ipv6 blocks (separated by colon) + ::
       */
      ipv6Block ~ colon ~ ipv6Block ~ colon ~ ipv6Block ~ colon ~ colon ^^
        { toString(_) } |
      /*
       * [ *6( h16 ":" ) h16 ] "::"
       *   --> 4 ipv6 blocks (separated by colons) + ::
       */
      ipv6Block ~ colon ~ ipv6Block ~ colon ~ ipv6Block ~ colon ~ ipv6Block ~
        colon ~ colon ^^ { toString(_) } |
      /*
       * [ *6( h16 ":" ) h16 ] "::"
       *   --> 5 ipv6 blocks (separated by coloons) + ::
       */
      ipv6Block ~ colon ~ ipv6Block ~ colon ~ ipv6Block ~ colon ~ ipv6Block ~
        colon ~ ipv6Block ~ colon ~ colon ^^ { toString(_) } |
      /*
       * [ *6( h16 ":" ) h16 ] "::"
       *   --> 6 ipv6 blocks (separated by colons) + ::
       */
      ipv6Block ~ colon ~ ipv6Block ~ colon ~ ipv6Block ~ colon ~ ipv6Block ~
        colon ~ ipv6Block ~ colon ~ ipv6Block ~ colon ~ colon ^^ { toString(_) } |
      /*
       * [ *6( h16 ":" ) h16 ] "::"
       *   --> 7 ipv6 blocks (separated by colons) + ::
       */
      ipv6Block ~ colon ~ ipv6Block ~ colon ~ ipv6Block ~ colon ~ ipv6Block ~
        colon ~ ipv6Block ~ colon ~ ipv6Block ~ colon ~ ipv6Block ~ colon ~
        colon ^^ { toString(_) }

  def ipLiteral: Parser[String] =
    openingBracket ~ ipv6address ~ closingBracket ^^ { toString(_) }

  def tenToNinentynine: Parser[String] =
    "[1-9]".r ~ digit ^^ { raw => raw._1 + raw._2 }

  def onehundredToOnehundredninetynine: Parser[String] =
    "1" ~ digit ~ digit ^^ { raw => raw._1._1 + raw._1._2 + raw._2 }

  def twohundredToTwohundredfourtynine: Parser[String] =
    "2" ~ "[0-4]".r ~ digit ^^ { raw => raw._1._1 + raw._1._2 + raw._2 }

  def twohundredfiftyToTwohundredfiftyfive: Parser[String] =
    "25" ~ "[0-5]".r ^^ {raw => raw._1 + raw._2 }

  def decOctet: Parser[String] =
    twohundredfiftyToTwohundredfiftyfive | // 250-255
      twohundredToTwohundredfourtynine |  // 200-249
      onehundredToOnehundredninetynine |  // 100-199
      tenToNinentynine |  // 10-99
      digit  // 0-9

  def ipv4address: Parser[String] = decOctet ~ dot ~ decOctet ~ dot ~
    decOctet ~ dot ~ decOctet ^^ { toString(_) }

  // FIXME: skipped upper code points for now: x10000-1FFFD, x20000-2FFFD, ...
  // x30000-3FFFD, x40000-4FFFD, x50000-5FFFD, x60000-6FFFD, x70000-7FFFD,
  // x80000-8FFFD, x90000-9FFFD, xA0000-AFFFD, xB0000-BFFFD, xC0000-CFFFD,
  // xD0000-DFFFD, xE1000-EFFFD
  // scalastyle:off
  def ucschar: Parser[String] =
    "[\u00A0-\uD7FF]".r |
    "[\uF900-\uFDCF]".r |
    "[\uFDF0-\uFFEF]".r ^^ { _.toString }
  // scalastyle:on

  def iunreserved: Parser[String] =
    alpha | digit | dash | dot | underscore | tilde | ucschar

  def pctEncoded: Parser[String] = percent ~ hexDigit ~ hexDigit ^^ { toString(_) }

  def subDelims: Parser[String] = exclamationMark | dollar | ampersand |
    quote | openingParen | closingParen | asterisk | plus | comma |
    semicolon | equalsSign

  // scalastyle:off
  def iregName: Parser[String] = { iunreserved | pctEncoded | subDelims }.+ ^^ { toString(_) }
  // scalastyle:on

  def ihost: Parser[String] = ipLiteral | ipv4address | iregName

  def port: Parser[String] = digit.* ^^ { _.reduce(_ + _) }

  /** [ iuserinfo "@" ] ihost [ ":" port ] */
  def iauthorty: Parser[String] =
    { iuserinfo ~ at }.? ~ ihost ~ { colon ~ port }.? ^^ { toString(_) }

  def ipchar: Parser[String] =
    iunreserved | pctEncoded | subDelims | colon | at ^^ { toString(_) }

  def isegment: Parser[String] = ipchar.* ^^ { toString(_) }

  def ipathAbempty: Parser[String] = { slash ~ isegment }.* ^^ { toString(_) }

  // non-zero-length segment
  // scalastyle:off
  def isegmentNz: Parser[String] = ipchar.+ ^^ { toString(_) }
  // scalastyle:on

  def ipathAbsolute: Parser[String] =
    slash ~ { isegmentNz ~ { slash ~ isegment }.* }.? ^^ { toString(_) }

  def ipathRootless: Parser[String] = isegmentNz ~ { slash ~ isegment }.* ^^ { toString(_) }

  // ipath-empty    = 0<ipchar>
  def ipathEmpty: Parser[String] = repN(0, ipchar) ^^ { toString(_) }

  def ihierPart: Parser[String] = doubleSlash ~ iauthorty ~
    { ipathAbempty | ipathAbsolute | ipathRootless | ipathEmpty } ^^ { toString(_) }

  // FIXME: skipped upper code points: xF0000-FFFFD, x100000-10FFFD
  // scalastyle:off
  def iprivate: Parser[String] = "[\uE000-\uF8FF]".r ^^ { _.toString }
  // scalastyle:on

  def iquery: Parser[String] =
    { ipchar | iprivate | slash | questionmark }.* ^^ { toString(_) }

  def ifragment: Parser[String] = { ipchar | slash | questionmark }.* ^^ { toString(_) }

  def fullIRI: Parser[IRI] =
    openingAngleBracket ~ { scheme ~ colon ~ ihierPart ~ { questionmark ~ iquery }.? ~
      { hashSign ~ ifragment}.? } ~ closingAngleBracket ^^ { raw => {
      val quotedIRIStr = toString(raw)  // i.e. with angle brackets
      val strLen = quotedIRIStr.length

      //                      strip off angle brackets
      IRI.create(quotedIRIStr.substring(1, strLen-1))
    }}

  // Definitions from https://www.w3.org/TR/2008/REC-rdf-sparql-query-20080115/

  // [A-Z] | [a-z] | [#x00C0-#x00D6] | [#x00D8-#x00F6] | [#x00F8-#x02FF] |
  //   [#x0370-#x037D] | [#x037F-#x1FFF] | [#x200C-#x200D] | [#x2070-#x218F] |
  //   [#x2C00-#x2FEF] | [#x3001-#xD7FF] | [#xF900-#xFDCF] | [#xFDF0-#xFFFD] |
  //   [#x10000-#xEFFFF]
  // scalastyle:off
  def pn_chars_base: Parser[String] = alpha |  // [A-Z] | [a-z]
    "[\u00C0-\u00D6]".r ^^ { _.toString } |  // [#x00C0-#x00D6]
    "[\u00D8-\u00F6]".r ^^ { _.toString } |  // [#x00D8-#x00F6]
    "[\u00F8-\u02FF]".r ^^ { _.toString } |  // [#x00F8-#x02FF]
    "[\u0370-\u037D]".r ^^ { _.toString } |  // [#x0370-#x037D]
    "[\u037F-\u1FFF]".r ^^ { _.toString } |  // [#x037F-#x1FFF]
    "[\u200C-\u200D]".r ^^ { _.toString } |  // [#x200C-#x200D]
    "[\u2070-\u218F]".r ^^ { _.toString } |  // [#x2070-#x218F]
    "[\u2C00-\u2FEF]".r ^^ { _.toString } |  // [#x2C00-#x2FEF]
    "[\u3001-\uD7FF]".r ^^ { _.toString } |  // [#x3001-#xD7FF]
    "[\uF900-\uFDCF]".r ^^ { _.toString } |  // [#xF900-#xFDCF]
    "[\uFDF0-\uFFFD]".r ^^ { _.toString }    // [#xFDF0-#xFFFD]
  // TODO: [#x10000-#xEFFFF]
  // scalastyle:on

  // PN_CHARS_BASE | '_'
  def pn_chars_u: Parser[String] = pn_chars_base | underscore

  // PN_CHARS_U | '-' | [0-9] | #x00B7 | [#x0300-#x036F] | [#x203F-#x2040]
  // scalastyle:off
  def pn_chars: Parser[String] = pn_chars_u | dash | digit | "\u00B7" |
    "[\u0300-\u036F]".r ^^ { _.toString } |
    "[\u203F-\u2040]".r ^^ { _.toString }
  // scalastyle:on

  // PN_CHARS_BASE ((PN_CHARS|'.')* PN_CHARS)?
  // FIXME
  //  def pn_prefix: Parser[String] = pn_chars_base ~ { { pn_chars | dot }.* ~ pn_chars }.? ^^ { toString(_) }
  def pn_prefix: Parser[String] = pn_chars_base ~ { pn_chars | dot }.* ^^ { toString(_) }

  // PNAME_NS ::= PN_PREFIX? ':'
  def pname_ns: Parser[String] = pn_prefix.? ~ colon ^^ { raw => toString(raw._1) }

  // ( PN_CHARS_U | [0-9] ) ((PN_CHARS|'.')* PN_CHARS)?
  // FIXME
//    def pn_local: Parser[String] = { pn_chars_u | digit } ~
//      { { pn_chars | dot }.* ~ pn_chars }.? ^^ { toString(_) }
//  def pn_local: Parser[String] = { pn_chars_u | digit } ~ { pn_chars | dot }.* ^^ { toString(_) }
  def pn_local: Parser[String] = pn_local_non_digit_start | pn_local_at_least_one_non_digit_char

  def pn_local_non_digit_start: Parser[String] =
    pn_chars_u ~ { pn_chars | dot }.* ^^ { toString(_) }

  def pn_local_at_least_one_non_digit_char: Parser[String] =
    { pn_chars_u | digit } ~ { pn_chars | dot }.* ~ pn_chars ~
      { pn_chars | dot }.* ^^ { toString(_) }

  // = PNAME_LN ::= PNAME_NS PN_LOCAL
  def abbreviatedIRI: Parser[IRI] = pname_ns ~ pn_local ^^ { raw => {
      // TODO: add warning or do something meaningful in case prefix is unknown
      val prefix = prefixes.getOrElse(raw._1.replace(":", ""), raw._1)
      IRI.create(prefix, raw._2.toString)
    }
  }

  // TODO: check whether these are specific for Manchester OWL syntax
  private val dtypeBuiltinKeywords: List[String] = List(
    "integer", "decimal", "float", "string"
  )

  val keywords: List[String] = List("EquivalentTo", "DisjointWith",
    "SubClassOf", "DisjointUnionOf", "HasKey", "Annotations", "Domain", "Range")

  def notAManchesterOWLKeyword: Parser[String] = pn_local ^? {
    case iriStr if !keywords.contains(iriStr) => iriStr
  }

  def simpleIRI: Parser[IRI] = notAManchesterOWLKeyword ^^ { raw =>
    /* Add default prefix if it is defined. */
    if (dtypeBuiltinKeywords.contains(raw)) {
      IRI.create(Namespaces.XSD.getPrefixIRI, raw)
    } else {
      prefixes.get("") match {
        case Some(defaultPrefix) => IRI.create(defaultPrefix, raw)
        case None => IRI.create(raw)
      }
    }
  }

  def iri: Parser[IRI] = fullIRI | abbreviatedIRI | simpleIRI
}
