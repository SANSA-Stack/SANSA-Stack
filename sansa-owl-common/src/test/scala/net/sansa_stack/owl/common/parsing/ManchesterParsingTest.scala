package net.sansa_stack.owl.common.parsing

import org.scalatest.FunSuite
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.formats.{ManchesterSyntaxDocumentFormat, TurtleDocumentFormat}
import org.semanticweb.owlapi.model.{IRI, OWLAnnotation, OWLAxiom, OWLClassExpression, OWLDataRange, OWLDocumentFormatImpl, OWLFacetRestriction, OWLObjectInverseOf, OWLObjectProperty, OWLObjectPropertyExpression}
import org.semanticweb.owlapi.vocab.{Namespaces, OWL2Datatype, OWLFacet, XSDVocabulary}
import uk.ac.manchester.cs.owl.owlapi._

import scala.collection.JavaConversions.asJavaCollection
import scala.collection.JavaConverters.{asJavaCollectionConverter, _}


class ManchesterParsingTest extends FunSuite {
  def p = ManchesterParser
  val df = OWLManager.getOWLDataFactory
  val noAnnotations = List.empty[OWLAnnotation]

  def setupParserPrefixes = {
    p.prefixes.clear()
    p.prefixes.put("", "http://ex.com/default#")
    p.prefixes.put("foo", "http://ex.com/foo#")
    p.prefixes.put("bar", "http://ex.com/bar#")
    p.prefixes.put("xsd", "http://www.w3.org/2001/XMLSchema#")
    p.prefixes.put("rdfs", "http://www.w3.org/2000/01/rdf-schema#")
  }

  def clearParserPrefixes = p.prefixes.clear()

  test("The scheme parser should work correctly") {
    val fn = p.scheme
    val scheme1 = "http"
    assert(p.checkParsed(fn, scheme1) == scheme1)

    val scheme2 = "https"
    assert(p.checkParsed(fn, scheme2) == scheme2)

    val scheme3 = "git+ssh"
    assert(p.checkParsed(fn, scheme3) == scheme3)
  }

  test("The user info parser should work correctly") {
    val fn = p.iuserinfo
    val ui1 = "user_123"
    assert(p.checkParsed(fn, ui1) == ui1)

    val ui2 = "user_123:pw%35$"
    assert(p.checkParsed(fn, ui2) == ui2)
  }

  test("The IPv6 address parser should work correctly") {
    val fn = p.ipv6address

    val a01 = "::"
    assert(p.checkParsed(fn, a01) == a01)

    val a02 = "1234::"
    assert(p.checkParsed(fn, a02) == a02)

    val a03 = "1234:5678::"
    assert(p.checkParsed(fn, a03) == a03)

    val a04 = "1234:5678:90ab::"
    assert(p.checkParsed(fn, a04) == a04)

    val a05 = "1234:5678:90ab:cdef::"
    assert(p.checkParsed(fn, a05) == a05)

    val a06 = "1234:5678:90ab:cdef:fedc::"
    assert(p.checkParsed(fn, a06) == a06)

    val a07 = "1234:5678:90ab:cdef:fedc:ba09::"
    assert(p.checkParsed(fn, a07) == a07)

    val a08 = "1234:5678:90ab:cdef:fedc:ba09:8765::"
    assert(p.checkParsed(fn, a08) == a08)

    val a09 = "::1"
    assert(p.checkParsed(fn, a09) == a09)

    val a10 = "::23"
    assert(p.checkParsed(fn, a10) == a10)

    val a11 = "::10ff"
    assert(p.checkParsed(fn, a11) == a11)

    val a12 = "1234::10ff"
    assert(p.checkParsed(fn, a12) == a12)

    val a13 = "1234:5678::10ff"
    assert(p.checkParsed(fn, a13) == a13)

    val a14 = "1234:5678:90ab::10ff"
    assert(p.checkParsed(fn, a14) == a14)

    val a15 = "1234:5678:90ab:cdef::10ff"
    assert(p.checkParsed(fn, a15) == a15)

    val a16 = "1234:5678:90ab:cdef:fedc::10ff"
    assert(p.checkParsed(fn, a16) == a16)

    val a17 = "1234:5678:90ab:cdef:fedc:ba09::10ff"
    assert(p.checkParsed(fn, a17) == a17)

    val a18 = "::11ee:10ff"
    assert(p.checkParsed(fn, a18) == a18)

    val a19 = "1234::11ee:10ff"
    assert(p.checkParsed(fn, a19) == a19)

    val a20 = "1234:5678::11ee:10ff"
    assert(p.checkParsed(fn, a20) == a20)

    val a21 = "1234:5678:90ab::11ee:10ff"
    assert(p.checkParsed(fn, a21) == a21)

    val a22 = "1234:5678:90ab:cdef::11ee:10ff"
    assert(p.checkParsed(fn, a22) == a22)

    val a23 = "1234:5678:90ab:cdef:fedc::11ee:10ff"
    assert(p.checkParsed(fn, a23) == a23)

    val a24 = "::12dd:11ee:10ff"
    assert(p.checkParsed(fn, a24) == a24)

    val a25 = "1234::12dd:11ee:10ff"
    assert(p.checkParsed(fn, a25) == a25)

    val a26 = "1234:5678::12dd:11ee:10ff"
    assert(p.checkParsed(fn, a26) == a26)

    val a27 = "1234:5678:90ab::12dd:11ee:10ff"
    assert(p.checkParsed(fn, a27) == a27)

    val a28 = "1234:5678:90ab:cdef::12dd:11ee:10ff"
    assert(p.checkParsed(fn, a28) == a28)

    val a29 = "::13cc:12dd:11ee:10ff"
    assert(p.checkParsed(fn, a29) == a29)

    val a30 = "1234::13cc:12dd:11ee:10ff"
    assert(p.checkParsed(fn, a30) == a30)

    val a31 = "1234:5678::13cc:12dd:11ee:10ff"
    assert(p.checkParsed(fn, a31) == a31)

    val a32 = "1234:5678:90ab::13cc:12dd:11ee:10ff"
    assert(p.checkParsed(fn, a32) == a32)

    val a33 = "::14bb:13cc:12dd:11ee:10ff"
    assert(p.checkParsed(fn, a33) == a33)

    val a34 = "1234::14bb:13cc:12dd:11ee:10ff"
    assert(p.checkParsed(fn, a34) == a34)

    val a35 = "1234:5678::14bb:13cc:12dd:11ee:10ff"
    assert(p.checkParsed(fn, a35) == a35)

    val a36 = "::15aa:14bb:13cc:12dd:11ee:10ff"
    assert(p.checkParsed(fn, a36) == a36)

    val a37 = "1234::15aa:14bb:13cc:12dd:11ee:10ff"
    assert(p.checkParsed(fn, a37) == a37)

    val a38 = "1799:1600:15aa:14bb:13cc:12dd:11ee:10ff"
    assert(p.checkParsed(fn, a38) == a38)
  }

  test("The IPv4 address parser should work correctly") {
    val fn = p.ipv4address
    val a01 = "1.2.3.4"
    assert(p.checkParsed(fn, a01) == a01)

    val a02 = "12.34.56.78"
    assert(p.checkParsed(fn, a02) == a02)

    val a03 = "112.123.134.145"
    assert(p.checkParsed(fn, a03) == a03)

    val a04 = "212.223.234.245"
    assert(p.checkParsed(fn, a04) == a04)

    val a05 = "0.0.0.0"
    assert(p.checkParsed(fn, a05) == a05)

    val a06 = "255.255.255.255"
    assert(p.checkParsed(fn, a06) == a06)
  }

  def quoted(iriStr: String) = "<" + iriStr + ">"

  /**
    * URL test cases taken from https://mathiasbynens.be/demo/url-regex
    */
  test("The IRI parser should work correctly") {
    var iri = "http://foo.com/blah_blah"
    assert(p.checkParsed(p.fullIRI, quoted(iri)) == IRI.create(iri))

    iri = "http://foo.com/blah_blah/"
    assert(p.checkParsed(p.fullIRI, quoted(iri)) == IRI.create(iri))

    iri = "http://foo.com/blah_blah_(wikipedia)"
    assert(p.checkParsed(p.fullIRI, quoted(iri)) == IRI.create(iri))

    iri = "http://foo.com/blah_blah_(wikipedia)_(again)"
    assert(p.checkParsed(p.fullIRI, quoted(iri)) == IRI.create(iri))

    iri = "http://www.example.com/wpstyle/?p=364"
    assert(p.checkParsed(p.fullIRI, quoted(iri)) == IRI.create(iri))

    iri = "https://www.example.com/foo/?bar=baz&inga=42&quux"
    assert(p.checkParsed(p.fullIRI, quoted(iri)) == IRI.create(iri))

    iri = "http://✪df.ws/123"
    assert(p.checkParsed(p.fullIRI, quoted(iri)) == IRI.create(iri))

    iri = "http://userid:password@example.com:8080"
    assert(p.checkParsed(p.fullIRI, quoted(iri)) == IRI.create(iri))

    iri = "http://userid:password@example.com:8080/"
    assert(p.checkParsed(p.fullIRI, quoted(iri)) == IRI.create(iri))

    iri = "http://userid@example.com"
    assert(p.checkParsed(p.fullIRI, quoted(iri)) == IRI.create(iri))

    iri = "http://userid@example.com/"
    assert(p.checkParsed(p.fullIRI, quoted(iri)) == IRI.create(iri))

    iri = "http://userid@example.com:8080"
    assert(p.checkParsed(p.fullIRI, quoted(iri)) == IRI.create(iri))

    iri = "http://userid@example.com:8080/"
    assert(p.checkParsed(p.fullIRI, quoted(iri)) == IRI.create(iri))

    iri = "http://userid:password@example.com"
    assert(p.checkParsed(p.fullIRI, quoted(iri)) == IRI.create(iri))

    iri= "http://userid:password@example.com/"
    assert(p.checkParsed(p.fullIRI, quoted(iri)) == IRI.create(iri))

    iri = "http://142.42.1.1/"
    assert(p.checkParsed(p.fullIRI, quoted(iri)) == IRI.create(iri))

    iri = "http://142.42.1.1:8080/"
    assert(p.checkParsed(p.fullIRI, quoted(iri)) == IRI.create(iri))

    iri = "http://➡.ws/䨹"
    assert(p.checkParsed(p.fullIRI, quoted(iri)) == IRI.create(iri))

    iri = "http://⌘.ws"
    assert(p.checkParsed(p.fullIRI, quoted(iri)) == IRI.create(iri))

    iri = "http://⌘.ws/"
    assert(p.checkParsed(p.fullIRI, quoted(iri)) == IRI.create(iri))

    iri = "http://foo.com/blah_(wikipedia)#cite-1"
    assert(p.checkParsed(p.fullIRI, quoted(iri)) == IRI.create(iri))

    iri = "http://foo.com/blah_(wikipedia)_blah#cite-1"
    assert(p.checkParsed(p.fullIRI, quoted(iri)) == IRI.create(iri))

    iri = "http://foo.com/unicode_(✪)_in_parens"
    assert(p.checkParsed(p.fullIRI, quoted(iri)) == IRI.create(iri))

    iri = "http://foo.com/(something)?after=parens"
    assert(p.checkParsed(p.fullIRI, quoted(iri)) == IRI.create(iri))

    iri = "http://☺.damowmow.com/"
    assert(p.checkParsed(p.fullIRI, quoted(iri)) == IRI.create(iri))

    iri = "http://code.google.com/events/#&product=browser"
    assert(p.checkParsed(p.fullIRI, quoted(iri)) == IRI.create(iri))

    iri = "http://j.mp"
    assert(p.checkParsed(p.fullIRI, quoted(iri)) == IRI.create(iri))

    iri = "ftp://foo.bar/baz"
    assert(p.checkParsed(p.fullIRI, quoted(iri)) == IRI.create(iri))

    iri = "http://foo.bar/?q=Test%20URL-encoded%20stuff"
    assert(p.checkParsed(p.fullIRI, quoted(iri)) == IRI.create(iri))

    iri = "http://مثال.إختبار"
    assert(p.checkParsed(p.fullIRI, quoted(iri)) == IRI.create(iri))

    iri = "http://例子.测试"
    assert(p.checkParsed(p.fullIRI, quoted(iri)) == IRI.create(iri))

    iri = "http://उदाहरण.परीक्षा"
    assert(p.checkParsed(p.fullIRI, quoted(iri)) == IRI.create(iri))

    iri = "http://-.~_!$&'()*+,;=:%40:80%2f::::::@example.com"
    assert(p.checkParsed(p.fullIRI, quoted(iri)) == IRI.create(iri))

    iri = "http://1337.net"
    assert(p.checkParsed(p.fullIRI, quoted(iri)) == IRI.create(iri))

    iri = "http://a.b-c.de"
    assert(p.checkParsed(p.fullIRI, quoted(iri)) == IRI.create(iri))

    iri = "http://223.255.255.254"
    assert(p.checkParsed(p.fullIRI, quoted(iri)) == IRI.create(iri))

    iri = "http://"
    try {
      p.checkParsed(p.fullIRI, quoted(iri))
      fail("Parsed invalid URI " + quoted(iri))
    } catch {
      case e: ParserException => "OK"
    }

    // FIXME: following test will fail
//    iri = "http://."
//    try {
//      p.checkParsed(p.fullIRI, quoted(iri))
//      fail("Parsed invalid URI " + quoted(iri))
//    } catch {
//      case e: ParserException => "OK"
//    }

    // FIXME: following test will fail
//    iri = "http://.."
//    try {
//      p.checkParsed(p.fullIRI, quoted(iri))
//      fail("Parsed invalid URI " + quoted(iri))
//    } catch {
//      case e: ParserException => "OK"
//    }

    // FIXME: following test will fail
//    iri = "http://../"
//    try {
//      p.checkParsed(p.fullIRI, quoted(iri))
//      fail("Parsed invalid URI " + quoted(iri))
//    } catch {
//      case e: ParserException => "OK"
//    }

    iri = "http://?"
    try {
      p.checkParsed(p.fullIRI, quoted(iri))
      fail("Parsed invalid URI " + quoted(iri))
    } catch {
      case e: ParserException => "OK"
    }

    iri = "http://??"
    try {
      p.checkParsed(p.fullIRI, quoted(iri))
      fail("Parsed invalid URI " + quoted(iri))
    } catch {
      case e: ParserException => "OK"
    }

    iri = "http://??/"
    try {
      p.checkParsed(p.fullIRI, quoted(iri))
      fail("Parsed invalid URI " + quoted(iri))
    } catch {
      case e: ParserException => "OK"
    }

    iri = "http://#"
    try {
      p.checkParsed(p.fullIRI, quoted(iri))
      fail("Parsed invalid URI " + quoted(iri))
    } catch {
      case e: ParserException => "OK"
    }

    iri = "http://##"
    try {
      p.checkParsed(p.fullIRI, quoted(iri))
      fail("Parsed invalid URI " + quoted(iri))
    } catch {
      case e: ParserException => "OK"
    }

    iri = "http://##/"
    try {
      p.checkParsed(p.fullIRI, quoted(iri))
      fail("Parsed invalid URI " + quoted(iri))
    } catch {
      case e: ParserException => "OK"
    }

    // FIXME: following test not covered due to whitespace settings
//    iri = "http://foo.bar?q=Spaces should be encoded"
//    try {
//      p.checkParsed(p.fullIRI, quoted(iri))
//      fail("Parsed invalid URI " + quoted(iri))
//    } catch {
//      case e: ParserException => "OK"
//    }

    iri = "//"
    try {
      p.checkParsed(p.fullIRI, quoted(iri))
      fail("Parsed invalid URI " + quoted(iri))
    } catch {
      case e: ParserException => "OK"
    }

    iri = "//a"
    try {
      p.checkParsed(p.fullIRI, quoted(iri))
      fail("Parsed invalid URI " + quoted(iri))
    } catch {
      case e: ParserException => "OK"
    }

    iri = "///a"
    try {
      p.checkParsed(p.fullIRI, quoted(iri))
      fail("Parsed invalid URI " + quoted(iri))
    } catch {
      case e: ParserException => "OK"
    }

    iri = "///"
    try {
      p.checkParsed(p.fullIRI, quoted(iri))
      fail("Parsed invalid URI " + quoted(iri))
    } catch {
      case e: ParserException => "OK"
    }

    iri = "http:///a"
    try {
      p.checkParsed(p.fullIRI, quoted(iri))
      fail("Parsed invalid URI " + quoted(iri))
    } catch {
      case e: ParserException => "OK"
    }

    iri = "foo.com"
    try {
      p.checkParsed(p.fullIRI, quoted(iri))
      fail("Parsed invalid URI " + quoted(iri))
    } catch {
      case e: ParserException => "OK"
    }

    // FIXME: following test will fail
//    iri = "rdar://1234"
//    try {
//      p.checkParsed(p.fullIRI, quoted(iri))
//      fail("Parsed invalid URI " + quoted(iri))
//    } catch {
//      case e: ParserException => "OK"
//    }

    // FIXME: following test will fail
//    iri = "h://test"
//    try {
//      p.checkParsed(p.fullIRI, quoted(iri))
//      fail("Parsed invalid URI " + quoted(iri))
//    } catch {
//      case e: ParserException => "OK"
//    }

    iri = "http://"
    try {
      p.checkParsed(p.fullIRI, quoted(iri))
      fail("Parsed invalid URI " + quoted(iri))
    } catch {
      case e: ParserException => "OK"
    }

    iri = "://"
    try {
      p.checkParsed(p.fullIRI, quoted(iri))
      fail("Parsed invalid URI " + quoted(iri))
    } catch {
      case e: ParserException => "OK"
    }

    // FIXME: following test will fail
//    iri = "http://foo.bar/foo(bar)baz quux"
//    try {
//      p.checkParsed(p.fullIRI, quoted(iri))
//      fail("Parsed invalid URI " + quoted(iri))
//    } catch {
//      case e: ParserException => "OK"
//    }

    // FIXME: follwing test will fail
//    iri = "ftps://foo.bar/"
//    try {
//      p.checkParsed(p.fullIRI, quoted(iri))
//      fail("Parsed invalid URI " + quoted(iri))
//    } catch {
//      case e: ParserException => "OK"
//    }

    // FIXME: following test will fail
//    iri = "http://-error-.invalid/"
//    try {
//      p.checkParsed(p.fullIRI, quoted(iri))
//      fail("Parsed invalid URI " + quoted(iri))
//    } catch {
//      case e: ParserException => "OK"
//    }

    // FIXME: following test will fail
//    iri = "http://a.b--c.de/"
//    try {
//      p.checkParsed(p.fullIRI, quoted(iri))
//      fail("Parsed invalid URI " + quoted(iri))
//    } catch {
//      case e: ParserException => "OK"
//    }

    // FIXME: following test will fail
//    iri = "http://-a.b.co"
//    try {
//      p.checkParsed(p.fullIRI, quoted(iri))
//      fail("Parsed invalid URI " + quoted(iri))
//    } catch {
//      case e: ParserException => "OK"
//    }

    // FIXME: following test will fail
//    iri = "http://a.b-.co"
//    try {
//      p.checkParsed(p.fullIRI, quoted(iri))
//      fail("Parsed invalid URI " + quoted(iri))
//    } catch {
//      case e: ParserException => "OK"
//    }

    // FIXME: following test will fail
//    iri = "http://0.0.0.0"
//    try {
//      p.checkParsed(p.fullIRI, quoted(iri))
//      fail("Parsed invalid URI " + quoted(iri))
//    } catch {
//      case e: ParserException => "OK"
//    }

    // FIXME: following test will fail
//    iri = "http://10.1.1.0"
//    try {
//      p.checkParsed(p.fullIRI, quoted(iri))
//      fail("Parsed invalid URI " + quoted(iri))
//    } catch {
//      case e: ParserException => "OK"
//    }

    // FIXME: following test will fail
//    iri = "http://10.1.1.255"
//    try {
//      p.checkParsed(p.fullIRI, quoted(iri))
//      fail("Parsed invalid URI " + quoted(iri))
//    } catch {
//      case e: ParserException => "OK"
//    }

    // FIXME: following test will fail
//    iri = "http://224.1.1.1"
//    try {
//      p.checkParsed(p.fullIRI, quoted(iri))
//      fail("Parsed invalid URI " + quoted(iri))
//    } catch {
//      case e: ParserException => "OK"
//    }

    iri = "http://1.1.1.1.1"
    try {
      p.checkParsed(p.fullIRI, quoted(iri))
      fail("Parsed invalid URI " + quoted(iri))
    } catch {
      case e: ParserException => "OK"
    }

    // FIXME: following test will fail
//    iri = "http://123.123.123"
//    try {
//      p.checkParsed(p.fullIRI, quoted(iri))
//      fail("Parsed invalid URI " + quoted(iri))
//    } catch {
//      case e: ParserException => "OK"
//    }

    // FIXME: following test will fail
//    iri = "http://3628126748"
//    try {
//      p.checkParsed(p.fullIRI, quoted(iri))
//      fail("Parsed invalid URI " + quoted(iri))
//    } catch {
//      case e: ParserException => "OK"
//    }

    // FIXME: following test will fail
//    iri = "http://.www.foo.bar/"
//    try {
//      p.checkParsed(p.fullIRI, quoted(iri))
//      fail("Parsed invalid URI " + quoted(iri))
//    } catch {
//      case e: ParserException => "OK"
//    }

    // FIXME: following test will fail
//    iri = "http://www.foo.bar./"
//    try {
//      p.checkParsed(p.fullIRI, quoted(iri))
//      fail("Parsed invalid URI " + quoted(iri))
//    } catch {
//      case e: ParserException => "OK"
//    }

    // FIXME: following test will fail
//    iri = "http://.www.foo.bar./"
//    try {
//      p.checkParsed(p.fullIRI, quoted(iri))
//      fail("Parsed invalid URI " + quoted(iri))
//    } catch {
//      case e: ParserException => "OK"
//    }

    // FIXME: following test will fail
//    iri = "http://10.1.1.1"
//    try {
//      p.checkParsed(p.fullIRI, quoted(iri))
//      fail("Parsed invalid URI " + quoted(iri))
//    } catch {
//      case e: ParserException => "OK"
//    }

    // FIXME: following test will fail
//    iri = "http://10.1.1.254"
//    try {
//      p.checkParsed(p.fullIRI, quoted(iri))
//      fail("Parsed invalid URI " + quoted(iri))
//    } catch {
//      case e: ParserException => "OK"
//    }
  }

  test("The abbreviated IRI parser should work correctly") {
    val prefix1Abbr = "ns1"
    val prefix1 = "http://ex.com/some/path#"

    val prefix2Abbr = "ns2"
    val prefix2 = "http://dbpedia.org/resource/"

    val defaultPrefix = "http://dl-learner.org/whatever#"
    p.prefixes.put(prefix1Abbr, prefix1)
    p.prefixes.put(prefix2Abbr, prefix2)
    p.prefixes.put("", defaultPrefix)

    var localPart = "someLocalPart"
    assert(p.checkParsed(p.abbreviatedIRI, prefix1Abbr + ":" + localPart) ==
      IRI.create(prefix1, localPart))

    localPart = "anotherLocalPart123.xyz"
    assert(p.checkParsed(p.abbreviatedIRI, prefix2Abbr + ":" + localPart) ==
      IRI.create(prefix2, localPart))

    localPart = "yetAnother"
    assert(p.checkParsed(p.abbreviatedIRI, ":" + localPart) ==
      IRI.create(defaultPrefix, localPart))

    p.prefixes.clear()
  }

  test("The non-negatve integer parser should work correctly") {
    assert(p.checkParsed(p.nonNegativeInteger, "0") == 0)
    assert(p.checkParsed(p.nonNegativeInteger, "3") == 3)
    assert(p.checkParsed(p.nonNegativeInteger, "3456") == 3456)

    try {
      p.checkParsed(p.nonNegativeInteger, "-23")
      fail("Parses negative integers")
    } catch {
      case _: ParserException => "OK"
    }

    try {
      p.checkParsed(p.nonNegativeInteger, "023")
      fail("Parses integers with leading 0")
    } catch {
      case _: ParserException => "OK"
    }
  }

  test("The datatype parser should work correctly") {
    p.prefixes.put("xsd", "http://www.w3.org/2001/XMLSchema#")

    assert(p.checkParsed(p.datatype, "xsd:nonNegativeInteger") ==
      df.getOWLDatatype(OWL2Datatype.XSD_NON_NEGATIVE_INTEGER))
    assert(p.checkParsed(p.datatype, "integer") ==
      df.getOWLDatatype(OWL2Datatype.XSD_INTEGER))
    assert(p.checkParsed(p.datatype, "decimal") ==
      df.getOWLDatatype(OWL2Datatype.XSD_DECIMAL))
    assert(p.checkParsed(p.datatype, "float") ==
      df.getOWLDatatype(OWL2Datatype.XSD_FLOAT))
    assert(p.checkParsed(p.datatype, "string") ==
      df.getOWLDatatype(OWL2Datatype.XSD_STRING))

    p.prefixes.clear()
  }

  test("The typed literal parser should work correctly") {
    p.prefixes.put("xsd", "http://www.w3.org/2001/XMLSchema#")

    assert(p.checkParsed(p.typedLiteral, "\"23\"^^xsd:integer") ==
      df.getOWLLiteral("23", OWL2Datatype.XSD_INTEGER))

    assert(p.checkParsed(p.typedLiteral, "\"lala\"^^string") ==
      df.getOWLLiteral("lala", OWL2Datatype.XSD_STRING))

    val lexValue = "la la \\!§$%&/()=?°^*'+#_-:.;,><|"
    assert(p.checkParsed(p.typedLiteral, "\"" + lexValue + "\"^^string") ==
      df.getOWLLiteral(lexValue, OWL2Datatype.XSD_STRING))

    p.prefixes.clear()
  }

  test("The parser for string literals with language tag should work correctly") {
    assert(p.checkParsed(p.stringLiteralWithLanguage, "\"foo\"@en") ==
      df.getOWLLiteral("foo", "en"))
    assert(p.checkParsed(p.stringLiteralWithLanguage, "\"bar\"@sgn-CH-DE") ==
      df.getOWLLiteral("bar", "sgn-CH-DE"))
  }

  test("The integer literal parser should work correctly") {
    var intVal = "0"
    assert(p.checkParsed(p.integerLiteral, intVal) ==
      df.getOWLLiteral(intVal, OWL2Datatype.XSD_INTEGER))

    intVal = "+23"
    assert(p.checkParsed(p.integerLiteral, intVal) ==
      df.getOWLLiteral(intVal, OWL2Datatype.XSD_INTEGER))

    intVal = "-34"
    assert(p.checkParsed(p.integerLiteral, intVal) ==
      df.getOWLLiteral(intVal, OWL2Datatype.XSD_INTEGER))
  }

  test("The decimal literal parser should work correctly") {
    var decVal = "0.2345678"
    assert(p.checkParsed(p.decimalLiteral, decVal) ==
      df.getOWLLiteral(decVal, OWL2Datatype.XSD_DECIMAL))

    decVal = "+123.456"
    assert(p.checkParsed(p.decimalLiteral, decVal) ==
      df.getOWLLiteral(decVal, OWL2Datatype.XSD_DECIMAL))

    decVal = "-9876.54"
    assert(p.checkParsed(p.decimalLiteral, decVal) ==
      df.getOWLLiteral(decVal, OWL2Datatype.XSD_DECIMAL))
  }

  test("The floating point literal parser should work correctly") {
    var floatVal = "23f"
    assert(p.checkParsed(p.floatingPointLiteral, floatVal) ==
      df.getOWLLiteral(floatVal, OWL2Datatype.XSD_FLOAT))

    floatVal = "+23F"
    assert(p.checkParsed(p.floatingPointLiteral, floatVal) ==
      df.getOWLLiteral(floatVal, OWL2Datatype.XSD_FLOAT))

    floatVal = "-23F"
    assert(p.checkParsed(p.floatingPointLiteral, floatVal) ==
      df.getOWLLiteral(floatVal, OWL2Datatype.XSD_FLOAT))

    floatVal = "-23.678f"
    assert(p.checkParsed(p.floatingPointLiteral, floatVal) ==
      df.getOWLLiteral(floatVal, OWL2Datatype.XSD_FLOAT))

    floatVal = "+23.678e8F"
    assert(p.checkParsed(p.floatingPointLiteral, floatVal) ==
      df.getOWLLiteral(floatVal, OWL2Datatype.XSD_FLOAT))

    floatVal = "+23.678E8f"
    assert(p.checkParsed(p.floatingPointLiteral, floatVal) ==
      df.getOWLLiteral(floatVal, OWL2Datatype.XSD_FLOAT))

    floatVal = ".456F"
    assert(p.checkParsed(p.floatingPointLiteral, floatVal) ==
      df.getOWLLiteral(floatVal, OWL2Datatype.XSD_FLOAT))

    floatVal = "+.456f"
    assert(p.checkParsed(p.floatingPointLiteral, floatVal) ==
      df.getOWLLiteral(floatVal, OWL2Datatype.XSD_FLOAT))

    floatVal = "-.456F"
    assert(p.checkParsed(p.floatingPointLiteral, floatVal) ==
      df.getOWLLiteral(floatVal, OWL2Datatype.XSD_FLOAT))

    floatVal = "-.456e23f"
    assert(p.checkParsed(p.floatingPointLiteral, floatVal) ==
      df.getOWLLiteral(floatVal, OWL2Datatype.XSD_FLOAT))

    floatVal = "+.456E32F"
    assert(p.checkParsed(p.floatingPointLiteral, floatVal) ==
      df.getOWLLiteral(floatVal, OWL2Datatype.XSD_FLOAT))

    floatVal = "+23.678e-8F"
    assert(p.checkParsed(p.floatingPointLiteral, floatVal) ==
      df.getOWLLiteral(floatVal, OWL2Datatype.XSD_FLOAT))

    floatVal = "+23.678E-8f"
    assert(p.checkParsed(p.floatingPointLiteral, floatVal) ==
      df.getOWLLiteral(floatVal, OWL2Datatype.XSD_FLOAT))

    floatVal = "-.456e-23f"
    assert(p.checkParsed(p.floatingPointLiteral, floatVal) ==
      df.getOWLLiteral(floatVal, OWL2Datatype.XSD_FLOAT))

    floatVal = "+.456E-32F"
    assert(p.checkParsed(p.floatingPointLiteral, floatVal) ==
      df.getOWLLiteral(floatVal, OWL2Datatype.XSD_FLOAT))
  }

  test("The annotations parser should work correctly") {
    p.prefixes.put("bar", "http://ex.com/bar#")
    p.prefixes.put("", "http://ex.com/default#")
    p.prefixes.put("foo", "http://ex.com/foo#")
    p.prefixes.put("owl", "http://www.w3.org/2002/07/owl#")
    p.prefixes.put("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#")
    p.prefixes.put("rdfs", "http://www.w3.org/2000/01/rdf-schema#")
    p.prefixes.put("xml", "http://www.w3.org/XML/1998/namespace")
    p.prefixes.put("xsd", "http://www.w3.org/2001/XMLSchema#")

    var annotationsStr =
      """Annotations:
        |    bar:hasTitle "Title",
        |    description "A longer
        |description running over
        |several lines",
        |    foo:hasName "Name"
      """.stripMargin

    val annProp1 =
      df.getOWLAnnotationProperty(IRI.create("http://ex.com/bar#hasTitle"))
    val annProp2 =
      df.getOWLAnnotationProperty(IRI.create("http://ex.com/default#description"))
    val annProp3 =
      df.getOWLAnnotationProperty(IRI.create("http://ex.com/foo#hasName"))

    val annVal1 = df.getOWLLiteral("Title")
    val annVal2 = df.getOWLLiteral("""A longer
                                     |description running over
                                     |several lines""".stripMargin)
    val annVal3 = df.getOWLLiteral("Name")

    val emptyJavaList = List.empty[OWLAnnotation].asJavaCollection

    var parsed: List[OWLAnnotation] = p.checkParsed(p.annotations, annotationsStr)
    assert(parsed.contains(new OWLAnnotationImpl(annProp1, annVal1, emptyJavaList.stream())))
    assert(parsed.contains(new OWLAnnotationImpl(annProp2, annVal2, emptyJavaList.stream())))
    assert(parsed.contains(new OWLAnnotationImpl(annProp3, annVal3, emptyJavaList.stream())))

    annotationsStr =
      """Annotations:
        |    Annotations:
        |        bar:prop1 "Property 1",
        |        foo:prop1 "23"
        |    bar:label "First annotation",
        |
        |    Annotations:
        |        bar:prop2 "Property 2",
        |        foo:prop2 "42"
        |    bar:label "Second annotation"
      """.stripMargin

    /* List(
     *     Annotation(
     *         Annotation(
     *             <http://ex.com/bar#prop1> "Property 1"^^xsd:string
     *         )
     *         Annotation(
     *             <http://ex.com/foo#prop1> "23"^^xsd:string
     *         )
     *         <http://ex.com/bar#label> "First annotation"^^xsd:string
     *     ),
     *
     *     Annotation(
     *         Annotation(
     *             <http://ex.com/bar#prop2> "Property 2"^^xsd:string
     *         )
     *         Annotation(
     *             <http://ex.com/foo#prop2> "42"^^xsd:string
     *         )
     *         <http://ex.com/bar#label> "Second annotation"^^xsd:string
     *     )
     * )
     *
     */
    parsed = p.checkParsed(p.annotations, annotationsStr)
    assert(parsed.length == 2)

    val firstOuterAnnotation = parsed(0)
    assert(firstOuterAnnotation.annotations().count() == 2)
    assert(firstOuterAnnotation.getProperty ==
      new OWLAnnotationPropertyImpl(IRI.create("http://ex.com/bar#label")))

    val secondOuterAnnotation = parsed(1)
    assert(secondOuterAnnotation.annotations().count() == 2)
    assert(secondOuterAnnotation.getProperty ==
      new OWLAnnotationPropertyImpl(IRI.create("http://ex.com/bar#label")))
  }

  test("The datatype frame parser should work correctly") {
    p.prefixes.clear()
    p.prefixes.put("", "http://ex.com/default#")
    p.prefixes.put("bar", "http://ex.com/bar#")

    val noAnnotations = List.empty[OWLAnnotation].asJavaCollection

    var dtypeFrameStr = """Datatype: string"""
    var parsed = p.checkParsed(p.datatypeFrame, dtypeFrameStr)

    var expectedAxiom: OWLAxiom = new OWLDeclarationAxiomImpl(
      df.getStringOWLDatatype,
      noAnnotations
    )
    assert(parsed.length == 1)
    assert(parsed.contains(expectedAxiom))

    var prefix = "http://ex.com/default#"
    dtypeFrameStr =
      """Datatype: string
        |    Annotations:
        |        comment "Just to check the annotations"
        |    Annotations:
        |        title "Blah"
      """.stripMargin
    parsed = p.checkParsed(p.datatypeFrame, dtypeFrameStr)
    expectedAxiom = new OWLDeclarationAxiomImpl(
      df.getStringOWLDatatype,
      List(
        df.getOWLAnnotation(
          df.getOWLAnnotationProperty(prefix + "comment"),
          df.getOWLLiteral("Just to check the annotations")
        ),
        df.getOWLAnnotation(
          df.getOWLAnnotationProperty(prefix + "title"),
          df.getOWLLiteral("Blah")
        )
      ).asJavaCollection
    )
    assert(parsed.length == 1)
    assert(parsed.contains(expectedAxiom))

    prefix = "http://ex.com/default#"
    dtypeFrameStr =
      """Datatype: string
        |    Annotations:
        |        comment "Just to check the annotations",
        |        title "Blah"
      """.stripMargin
    parsed = p.checkParsed(p.datatypeFrame, dtypeFrameStr)
    expectedAxiom = new OWLDeclarationAxiomImpl(
      df.getStringOWLDatatype,
      List(
        df.getOWLAnnotation(
          df.getOWLAnnotationProperty(prefix + "comment"),
          df.getOWLLiteral("Just to check the annotations")
        ),
        df.getOWLAnnotation(
          df.getOWLAnnotationProperty(prefix + "title"),
          df.getOWLLiteral("Blah")
        )
      ).asJavaCollection
    )
    assert(parsed.length == 1)
    assert(parsed.contains(expectedAxiom))

    prefix = "http://ex.com/whatever#"
    dtypeFrameStr =
      """Datatype: <http://ex.com/whatever#test1>
        |   Annotations:
        |     <http://ex.com/whatever#comment> "String or integer",
        |     <http://ex.com/whatever#title> "StringInt"
        |   EquivalentTo:
        |       Annotations:
        |           <http://ex.com/whatever#comment2> "String or integer",
        |           <http://ex.com/whatever#title2> "StringInt"
        |       string or integer
        |
      """.stripMargin
    parsed = p.checkParsed(p.datatypeFrame, dtypeFrameStr)

    expectedAxiom = new OWLDeclarationAxiomImpl(
      df.getOWLDatatype(prefix + "test1"),
      List(
        df.getOWLAnnotation(
          df.getOWLAnnotationProperty(prefix + "comment"),
          df.getOWLLiteral("String or integer")
        ),
        df.getOWLAnnotation(
          df.getOWLAnnotationProperty(prefix + "title"),
          df.getOWLLiteral("StringInt")
        )
      ).asJavaCollection
    )
    assert(parsed.length == 2)
    assert(parsed.contains(expectedAxiom))

    val ranges = List(df.getStringOWLDatatype, df.getIntegerOWLDatatype)
    val dataRange = df.getOWLDataUnionOf(ranges.asJavaCollection)
    expectedAxiom = new OWLDatatypeDefinitionAxiomImpl(
      df.getOWLDatatype(prefix + "test1"),
      dataRange,
      List(
        df.getOWLAnnotation(
          df.getOWLAnnotationProperty(prefix + "comment2"),
          df.getOWLLiteral("String or integer")
        ),
        df.getOWLAnnotation(
          df.getOWLAnnotationProperty(prefix + "title2"),
          df.getOWLLiteral("StringInt")
        )
      ).asJavaCollection
    )
    assert(parsed.contains(expectedAxiom))

    p.prefixes.clear()
  }

  test("The prefix declaration parser should work correctly") {
    p.prefixes.clear()

    var prefixDeclString = "Prefix: : <http://ex.com/default#>"
    p.checkParsed(p.prefixDeclaration, prefixDeclString)

    assert(p.prefixes.size == 1)
    assert(p.prefixes("") == "http://ex.com/default#")

    prefixDeclString = "Prefix: bar: <http://ex.com/bar#>"
    p.checkParsed(p.prefixDeclaration, prefixDeclString)

    assert(p.prefixes.size == 2)
    assert(p.prefixes("bar") == "http://ex.com/bar#")

    prefixDeclString = "Prefix: foo: <http://ex.com/foo#>"
    p.checkParsed(p.prefixDeclaration, prefixDeclString)

    assert(p.prefixes.size == 3)
    assert(p.prefixes("foo") == "http://ex.com/foo#")

    p.prefixes.clear()
  }

  test("The data type restriction parser should work correctly") {
    val int = df.getOWLDatatype(OWL2Datatype.XSD_INTEGER)

    var dtypeRestrStr = "integer [>= 0]"
    var facetRestr1 = new OWLFacetRestrictionImpl(OWLFacet.MIN_INCLUSIVE, df.getOWLLiteral(0))
    var dtypeRestr = new OWLDatatypeRestrictionImpl(
      int, List[OWLFacetRestriction](facetRestr1).asJavaCollection)
    assert(p.checkParsed(p.datatypeRestriction, dtypeRestrStr) == dtypeRestr)

    dtypeRestrStr = "integer [>= 0,< 100]"
    facetRestr1 = new OWLFacetRestrictionImpl(OWLFacet.MIN_INCLUSIVE, df.getOWLLiteral(0))
    var facetRestr2 = new OWLFacetRestrictionImpl(OWLFacet.MAX_EXCLUSIVE, df.getOWLLiteral(100))
    dtypeRestr = new OWLDatatypeRestrictionImpl(
      int, List[OWLFacetRestriction](facetRestr1, facetRestr2).asJavaCollection)
    assert(p.checkParsed(p.datatypeRestriction, dtypeRestrStr) == dtypeRestr)

    dtypeRestrStr = "integer [ >= 0, < 100 ]"
    assert(p.checkParsed(p.datatypeRestriction, dtypeRestrStr) == dtypeRestr)

  }

  test("The atomic data parser should work correctly") {
    var atomicDataStr = "integer"
    var dataRange: OWLDataRange = df.getIntegerOWLDatatype
    assert(p.checkParsed(p.dataAtomic, atomicDataStr) == dataRange)

    atomicDataStr = "{1,2,3}"
    val values = List(df.getOWLLiteral(1), df.getOWLLiteral(2), df.getOWLLiteral(3))
    dataRange = new OWLDataOneOfImpl(values.asJavaCollection)
    assert(p.checkParsed(p.dataAtomic, atomicDataStr) == dataRange)

    atomicDataStr = "{1, 2, 3}"
    assert(p.checkParsed(p.dataAtomic, atomicDataStr) == dataRange)

    atomicDataStr = "{ 1, 2, 3 }"
    assert(p.checkParsed(p.dataAtomic, atomicDataStr) == dataRange)

    atomicDataStr = "(integer or string)"
    val ranges: List[OWLDataRange] = List(df.getIntegerOWLDatatype, df.getStringOWLDatatype)
    dataRange = new OWLDataUnionOfImpl(ranges.asJavaCollection)
    assert(p.checkParsed(p.dataAtomic, atomicDataStr) == dataRange)

    atomicDataStr = "( integer or string )"
    assert(p.checkParsed(p.dataAtomic, atomicDataStr) == dataRange)
  }

  test("The primary data parser should work correctly") {
    var dataPrimaryStr = "integer"
    var dataRange: OWLDataRange = df.getIntegerOWLDatatype
    assert(p.checkParsed(p.dataPrimary, dataPrimaryStr) == dataRange)

    dataPrimaryStr = "not integer"
    dataRange = new OWLDataComplementOfImpl(dataRange)
    assert(p.checkParsed(p.dataPrimary, dataPrimaryStr) == dataRange)
  }

  test("The data conjunction parser should work correctly") {
    var dataConjunctionStr = "integer"
    var dataRange: OWLDataRange = df.getIntegerOWLDatatype
    assert(p.checkParsed(p.dataConjunction, dataConjunctionStr) == dataRange)

    dataConjunctionStr = "integer and decimal"
    var ranges = List[OWLDataRange](df.getIntegerOWLDatatype, df.getOWLDatatype(OWL2Datatype.XSD_DECIMAL))
    dataRange = new OWLDataIntersectionOfImpl(ranges.asJavaCollection)
    assert(p.checkParsed(p.dataConjunction, dataConjunctionStr) == dataRange)

    dataConjunctionStr = "integer and decimal and float"
    ranges = df.getFloatOWLDatatype :: ranges
    dataRange = new OWLDataIntersectionOfImpl(ranges.asJavaCollection)
    assert(p.checkParsed(p.dataConjunction, dataConjunctionStr) == dataRange)
  }

  test("The inverse object property parser should work correctly") {
    p.prefixes.clear()
    p.prefixes.put("", "http://ex.com/default#")
    p.prefixes.put("bar", "http://ex.com/bar#")

    var invPropStr = "inverse someProp"
    var prop = df.getOWLObjectProperty(IRI.create("http://ex.com/default#someProp"))
    var inverse: OWLObjectInverseOf = df.getOWLObjectInverseOf(prop)
    assert(p.checkParsed(p.inverseObjectProperty, invPropStr) == inverse)

    invPropStr = "inverse bar:someProp"
    prop = df.getOWLObjectProperty(IRI.create("http://ex.com/bar#someProp"))
    inverse = df.getOWLObjectInverseOf(prop)
    assert(p.checkParsed(p.inverseObjectProperty, invPropStr) == inverse)

    invPropStr = "inverse <http://ex.com/whatever#prop>"
    prop = df.getOWLObjectProperty(IRI.create("http://ex.com/whatever#prop"))
    inverse = df.getOWLObjectInverseOf(prop)
    assert(p.checkParsed(p.inverseObjectProperty, invPropStr) == inverse)

    p.prefixes.clear()
  }

  test("The ObjectSomeValuesFrom restriction parser should work correctly") {
    p.prefixes.clear()
    p.prefixes.put("", "http://ex.com/default#")
    p.prefixes.put("bar", "http://ex.com/bar#")

    var restrStr = "objProp some Cls"
    var prop = df.getOWLObjectProperty(IRI.create("http://ex.com/default#objProp"))
    var cls = df.getOWLClass(IRI.create("http://ex.com/default#Cls"))
    var restr: OWLClassExpression = df.getOWLObjectSomeValuesFrom(prop, cls)
    assert(p.checkParsed(p.restriction, restrStr) == restr)

    restrStr = "bar:objProp some bar:Cls"
    prop = df.getOWLObjectProperty(IRI.create("http://ex.com/bar#objProp"))
    cls = df.getOWLClass(IRI.create("http://ex.com/bar#Cls"))
    restr = df.getOWLObjectSomeValuesFrom(prop, cls)
    assert(p.checkParsed(p.restriction, restrStr) == restr)

    restrStr = "<http://ex.com/foo#objProp> some <http://ex.com/foo#Cls>"
    prop = df.getOWLObjectProperty(IRI.create("http://ex.com/foo#objProp"))
    cls = df.getOWLClass(IRI.create("http://ex.com/foo#Cls"))
    restr = df.getOWLObjectSomeValuesFrom(prop, cls)
    assert(p.checkParsed(p.restriction, restrStr) == restr)

    p.prefixes.clear()
  }

  test("The ObjectAllValuesFrom restriction parser should work correctly") {
    p.prefixes.clear()
    p.prefixes.put("", "http://ex.com/default#")
    p.prefixes.put("bar", "http://ex.com/bar#")

    var restrStr = "objProp only Cls"
    var prop = df.getOWLObjectProperty(IRI.create("http://ex.com/default#objProp"))
    var cls = df.getOWLClass(IRI.create("http://ex.com/default#Cls"))
    var restr: OWLClassExpression = df.getOWLObjectAllValuesFrom(prop, cls)
    assert(p.checkParsed(p.restriction, restrStr) == restr)

    restrStr = "bar:objProp only bar:Cls"
    prop = df.getOWLObjectProperty(IRI.create("http://ex.com/bar#objProp"))
    cls = df.getOWLClass(IRI.create("http://ex.com/bar#Cls"))
    restr = df.getOWLObjectAllValuesFrom(prop, cls)
    assert(p.checkParsed(p.restriction, restrStr) == restr)

    restrStr = "<http://ex.com/foo#objProp> only <http://ex.com/foo#Cls>"
    prop = df.getOWLObjectProperty(IRI.create("http://ex.com/foo#objProp"))
    cls = df.getOWLClass(IRI.create("http://ex.com/foo#Cls"))
    restr = df.getOWLObjectAllValuesFrom(prop, cls)
    assert(p.checkParsed(p.restriction, restrStr) == restr)

    p.prefixes.clear()
  }

  test("The ObjectHasValue restriction parser should work correctly") {
    p.prefixes.clear()
    p.prefixes.put("", "http://ex.com/default#")
    p.prefixes.put("bar", "http://ex.com/bar#")

    var restrStr = "objProp value someIndividual"
    var prop = df.getOWLObjectProperty(IRI.create("http://ex.com/default#objProp"))
    var indiv = df.getOWLNamedIndividual(IRI.create("http://ex.com/default#someIndividual"))
    var restr: OWLClassExpression = df.getOWLObjectHasValue(prop, indiv)
    assert(p.checkParsed(p.restriction, restrStr) == restr)

    restrStr = "bar:objProp value bar:someIndividual"
    prop = df.getOWLObjectProperty(IRI.create("http://ex.com/bar#objProp"))
    indiv = df.getOWLNamedIndividual(IRI.create("http://ex.com/bar#someIndividual"))
    restr = df.getOWLObjectHasValue(prop, indiv)
    assert(p.checkParsed(p.restriction, restrStr) == restr)

    restrStr = "<http://ex.com/foo#objProp> value <http://ex.com/foo#someIndividual>"
    prop = df.getOWLObjectProperty(IRI.create("http://ex.com/foo#objProp"))
    indiv = df.getOWLNamedIndividual(IRI.create("http://ex.com/foo#someIndividual"))
    restr = df.getOWLObjectHasValue(prop, indiv)
    assert(p.checkParsed(p.restriction, restrStr) == restr)

    p.prefixes.clear()
  }

  test("The ObjectHasSelf restriction parser should work correctly") {
    p.prefixes.clear()
    p.prefixes.put("", "http://ex.com/default#")
    p.prefixes.put("bar", "http://ex.com/bar#")

    var restrStr = "objProp Self"
    var prop = df.getOWLObjectProperty(IRI.create("http://ex.com/default#objProp"))
    var restr: OWLClassExpression = df.getOWLObjectHasSelf(prop)
    assert(p.checkParsed(p.restriction, restrStr) == restr)

    restrStr = "bar:objProp Self"
    prop = df.getOWLObjectProperty(IRI.create("http://ex.com/bar#objProp"))
    restr = df.getOWLObjectHasSelf(prop)
    assert(p.checkParsed(p.restriction, restrStr) == restr)

    restrStr = "<http://ex.com/foo#objProp> Self"
    prop = df.getOWLObjectProperty(IRI.create("http://ex.com/foo#objProp"))
    restr = df.getOWLObjectHasSelf(prop)
    assert(p.checkParsed(p.restriction, restrStr) == restr)

    p.prefixes.clear()
  }

  test("The ObjectMinCardinality restriction parser should work correctly") {
    p.prefixes.clear()
    p.prefixes.put("", "http://ex.com/default#")
    p.prefixes.put("bar", "http://ex.com/bar#")

    var restrStr = "objProp min 3"
    var prop = df.getOWLObjectProperty(IRI.create("http://ex.com/default#objProp"))
    var restr: OWLClassExpression = df.getOWLObjectMinCardinality(3, prop)
    assert(p.checkParsed(p.restriction, restrStr) == restr)

    restrStr = "bar:objProp min 3"
    prop = df.getOWLObjectProperty(IRI.create("http://ex.com/bar#objProp"))
    restr = df.getOWLObjectMinCardinality(3, prop)
    assert(p.checkParsed(p.restriction, restrStr) == restr)

    restrStr = "<http://ex.com/foo#objProp> min 3"
    prop = df.getOWLObjectProperty(IRI.create("http://ex.com/foo#objProp"))
    restr = df.getOWLObjectMinCardinality(3, prop)
    assert(p.checkParsed(p.restriction, restrStr) == restr)


    restrStr = "objProp min 3 Cls"
    prop = df.getOWLObjectProperty(IRI.create("http://ex.com/default#objProp"))
    var cls = df.getOWLClass(IRI.create("http://ex.com/default#Cls"))
    restr = df.getOWLObjectMinCardinality(3, prop, cls)
    assert(p.checkParsed(p.restriction, restrStr) == restr)

    restrStr = "bar:objProp min 3 bar:Cls"
    prop = df.getOWLObjectProperty(IRI.create("http://ex.com/bar#objProp"))
    cls = df.getOWLClass(IRI.create("http://ex.com/bar#Cls"))
    restr = df.getOWLObjectMinCardinality(3, prop, cls)
    assert(p.checkParsed(p.restriction, restrStr) == restr)

    restrStr = "<http://ex.com/foo#objProp> min 3 <http://ex.com/foo#Cls>"
    prop = df.getOWLObjectProperty(IRI.create("http://ex.com/foo#objProp"))
    cls = df.getOWLClass(IRI.create("http://ex.com/foo#Cls"))
    restr = df.getOWLObjectMinCardinality(3, prop, cls)
    assert(p.checkParsed(p.restriction, restrStr) == restr)

    p.prefixes.clear()
  }

  test("The ObjectMaxCardinality restriction parser should work correctly") {
    p.prefixes.clear()
    p.prefixes.put("", "http://ex.com/default#")
    p.prefixes.put("bar", "http://ex.com/bar#")

    var restrStr = "objProp max 3"
    var prop = df.getOWLObjectProperty(IRI.create("http://ex.com/default#objProp"))
    var restr: OWLClassExpression = df.getOWLObjectMaxCardinality(3, prop)
    assert(p.checkParsed(p.restriction, restrStr) == restr)

    restrStr = "bar:objProp max 3"
    prop = df.getOWLObjectProperty(IRI.create("http://ex.com/bar#objProp"))
    restr = df.getOWLObjectMaxCardinality(3, prop)
    assert(p.checkParsed(p.restriction, restrStr) == restr)

    restrStr = "<http://ex.com/foo#objProp> max 3"
    prop = df.getOWLObjectProperty(IRI.create("http://ex.com/foo#objProp"))
    restr = df.getOWLObjectMaxCardinality(3, prop)
    assert(p.checkParsed(p.restriction, restrStr) == restr)


    restrStr = "objProp max 3 Cls"
    prop = df.getOWLObjectProperty(IRI.create("http://ex.com/default#objProp"))
    var cls = df.getOWLClass(IRI.create("http://ex.com/default#Cls"))
    restr = df.getOWLObjectMaxCardinality(3, prop, cls)
    assert(p.checkParsed(p.restriction, restrStr) == restr)

    restrStr = "bar:objProp max 3 bar:Cls"
    prop = df.getOWLObjectProperty(IRI.create("http://ex.com/bar#objProp"))
    cls = df.getOWLClass(IRI.create("http://ex.com/bar#Cls"))
    restr = df.getOWLObjectMaxCardinality(3, prop, cls)
    assert(p.checkParsed(p.restriction, restrStr) == restr)

    restrStr = "<http://ex.com/foo#objProp> max 3 <http://ex.com/foo#Cls>"
    prop = df.getOWLObjectProperty(IRI.create("http://ex.com/foo#objProp"))
    cls = df.getOWLClass(IRI.create("http://ex.com/foo#Cls"))
    restr = df.getOWLObjectMaxCardinality(3, prop, cls)
    assert(p.checkParsed(p.restriction, restrStr) == restr)

    p.prefixes.clear()
  }

  test("The ObjectExactCardinality restriction parser should work correctly") {
    p.prefixes.clear()
    p.prefixes.put("", "http://ex.com/default#")
    p.prefixes.put("bar", "http://ex.com/bar#")

    var restrStr = "objProp exactly 3"
    var prop = df.getOWLObjectProperty(IRI.create("http://ex.com/default#objProp"))
    var restr: OWLClassExpression = df.getOWLObjectExactCardinality(3, prop)
    assert(p.checkParsed(p.restriction, restrStr) == restr)

    restrStr = "bar:objProp exactly 3"
    prop = df.getOWLObjectProperty(IRI.create("http://ex.com/bar#objProp"))
    restr = df.getOWLObjectExactCardinality(3, prop)
    assert(p.checkParsed(p.restriction, restrStr) == restr)

    restrStr = "<http://ex.com/foo#objProp> exactly 3"
    prop = df.getOWLObjectProperty(IRI.create("http://ex.com/foo#objProp"))
    restr = df.getOWLObjectExactCardinality(3, prop)
    assert(p.checkParsed(p.restriction, restrStr) == restr)


    restrStr = "objProp exactly 3 Cls"
    prop = df.getOWLObjectProperty(IRI.create("http://ex.com/default#objProp"))
    var cls = df.getOWLClass(IRI.create("http://ex.com/default#Cls"))
    restr = df.getOWLObjectExactCardinality(3, prop, cls)
    assert(p.checkParsed(p.restriction, restrStr) == restr)

    restrStr = "bar:objProp exactly 3 bar:Cls"
    prop = df.getOWLObjectProperty(IRI.create("http://ex.com/bar#objProp"))
    cls = df.getOWLClass(IRI.create("http://ex.com/bar#Cls"))
    restr = df.getOWLObjectExactCardinality(3, prop, cls)
    assert(p.checkParsed(p.restriction, restrStr) == restr)

    restrStr = "<http://ex.com/foo#objProp> exactly 3 <http://ex.com/foo#Cls>"
    prop = df.getOWLObjectProperty(IRI.create("http://ex.com/foo#objProp"))
    cls = df.getOWLClass(IRI.create("http://ex.com/foo#Cls"))
    restr = df.getOWLObjectExactCardinality(3, prop, cls)
    assert(p.checkParsed(p.restriction, restrStr) == restr)

    p.prefixes.clear()
  }

  test("The DataSomeValuesFrom restriction parser should work correctly") {
    p.prefixes.clear()
    p.prefixes.put("", "http://ex.com/default#")
    p.prefixes.put("bar", "http://ex.com/bar#")

    var restrStr = "dataProp some not { 1, 2, 3 }"
    var dataProp = df.getOWLDataProperty(IRI.create("http://ex.com/default#dataProp"))
    var literals = List(df.getOWLLiteral(1), df.getOWLLiteral(2), df.getOWLLiteral(3))
    var dataRange = df.getOWLDataComplementOf(
      df.getOWLDataOneOf(literals.asJavaCollection.stream()))
    var restr = df.getOWLDataSomeValuesFrom(dataProp, dataRange)
    assert(p.checkParsed(p.restriction, restrStr) == restr)

    restrStr = "bar:dataProp some not { 1, 2, 3 }"
    dataProp = df.getOWLDataProperty(IRI.create("http://ex.com/bar#dataProp"))
    restr = df.getOWLDataSomeValuesFrom(dataProp, dataRange)
    assert(p.checkParsed(p.restriction, restrStr) == restr)

    restrStr = "<http://ex.com/foo#dataProp> some not { 1, 2, 3 }"
    dataProp = df.getOWLDataProperty(IRI.create("http://ex.com/foo#dataProp"))
    restr = df.getOWLDataSomeValuesFrom(dataProp, dataRange)
    assert(p.checkParsed(p.restriction, restrStr) == restr)

    p.prefixes.clear()
  }

  test("The DataAllValuesFrom restriction parser should work correctly") {
    p.prefixes.clear()
    p.prefixes.put("", "http://ex.com/default#")
    p.prefixes.put("bar", "http://ex.com/bar#")

    var restrStr = "dataProp only not { 1, 2, 3 }"
    var dataProp = df.getOWLDataProperty(IRI.create("http://ex.com/default#dataProp"))
    var literals = List(df.getOWLLiteral(1), df.getOWLLiteral(2), df.getOWLLiteral(3))
    var dataRange = df.getOWLDataComplementOf(
      df.getOWLDataOneOf(literals.asJavaCollection.stream()))
    var restr = df.getOWLDataAllValuesFrom(dataProp, dataRange)
    assert(p.checkParsed(p.restriction, restrStr) == restr)

    restrStr = "bar:dataProp only not { 1, 2, 3 }"
    dataProp = df.getOWLDataProperty(IRI.create("http://ex.com/bar#dataProp"))
    restr = df.getOWLDataAllValuesFrom(dataProp, dataRange)
    assert(p.checkParsed(p.restriction, restrStr) == restr)

    restrStr = "<http://ex.com/foo#dataProp> only not { 1, 2, 3 }"
    dataProp = df.getOWLDataProperty(IRI.create("http://ex.com/foo#dataProp"))
    restr = df.getOWLDataAllValuesFrom(dataProp, dataRange)
    assert(p.checkParsed(p.restriction, restrStr) == restr)

    p.prefixes.clear()
  }

  test("The DataHasValue restriction parser should work correctly") {
    p.prefixes.clear()
    p.prefixes.put("", "http://ex.com/default#")
    p.prefixes.put("bar", "http://ex.com/bar#")

    var restrStr = "dataProp value 23"
    var dataProp = df.getOWLDataProperty(IRI.create("http://ex.com/default#dataProp"))
    var literal = df.getOWLLiteral(23)
    var restr = df.getOWLDataHasValue(dataProp, literal)
    assert(p.checkParsed(p.restriction, restrStr) == restr)

    restrStr = "bar:dataProp value 23 "
    dataProp = df.getOWLDataProperty(IRI.create("http://ex.com/bar#dataProp"))
    restr = df.getOWLDataHasValue(dataProp, literal)
    assert(p.checkParsed(p.restriction, restrStr) == restr)

    restrStr = "<http://ex.com/foo#dataProp> value 23"
    dataProp = df.getOWLDataProperty(IRI.create("http://ex.com/foo#dataProp"))
    restr = df.getOWLDataHasValue(dataProp, literal)
    assert(p.checkParsed(p.restriction, restrStr) == restr)

    p.prefixes.clear()
  }

  /**
    * Unfortunately this cannot be tested with the restriction parser since
    * the tests will always match the more general object cardinality parsers
    * without primary, i.e. sth like 'someObjProp min 3', 'someObjProp max 3',
    * or 'someObjProp exactly 3'
    */
  test("The DataMinCardinality restriction parser should work correctly") {
    p.prefixes.clear()
    p.prefixes.put("", "http://ex.com/default#")
    p.prefixes.put("bar", "http://ex.com/bar#")

    var restrStr = "dataProp min 3"
    var dataProp = df.getOWLDataProperty(IRI.create("http://ex.com/default#dataProp"))
    var restr = df.getOWLDataMinCardinality(3, dataProp)
    assert(p.checkParsed(p.dataMinCardinality_restriction, restrStr) == restr)

    restrStr = "bar:dataProp min 3"
    dataProp = df.getOWLDataProperty(IRI.create("http://ex.com/bar#dataProp"))
    restr = df.getOWLDataMinCardinality(3, dataProp)
    assert(p.checkParsed(p.dataMinCardinality_restriction, restrStr) == restr)

    restrStr = "<http://ex.com/foo#dataProp> min 3"
    dataProp = df.getOWLDataProperty(IRI.create("http://ex.com/foo#dataProp"))
    restr = df.getOWLDataMinCardinality(3, dataProp)
    assert(p.checkParsed(p.dataMinCardinality_restriction, restrStr) == restr)


    restrStr = "dataProp min 3 string"
    dataProp = df.getOWLDataProperty(IRI.create("http://ex.com/default#dataProp"))
    restr = df.getOWLDataMinCardinality(3, dataProp, df.getStringOWLDatatype)
    assert(p.checkParsed(p.dataMinCardinality_restriction, restrStr) == restr)

    restrStr = "bar:dataProp min 3 string"
    dataProp = df.getOWLDataProperty(IRI.create("http://ex.com/bar#dataProp"))
    restr = df.getOWLDataMinCardinality(3, dataProp, df.getStringOWLDatatype)
    assert(p.checkParsed(p.dataMinCardinality_restriction, restrStr) == restr)

    restrStr = "<http://ex.com/foo#dataProp> min 3 string"
    dataProp = df.getOWLDataProperty(IRI.create("http://ex.com/foo#dataProp"))
    restr = df.getOWLDataMinCardinality(3, dataProp, df.getStringOWLDatatype)
    assert(p.checkParsed(p.dataMinCardinality_restriction, restrStr) == restr)

    p.prefixes.clear()
  }

  /**
    * Unfortunately this cannot be tested with the restriction parser since
    * the tests will always match the more general object cardinality parsers
    * without primary, i.e. sth like 'someObjProp min 3', 'someObjProp max 3',
    * or 'someObjProp exactly 3'
    */
  test("The DataMaxCardinality restriction parser should work correctly") {
    p.prefixes.clear()
    p.prefixes.put("", "http://ex.com/default#")
    p.prefixes.put("bar", "http://ex.com/bar#")

    var restrStr = "dataProp max 3"
    var dataProp = df.getOWLDataProperty(IRI.create("http://ex.com/default#dataProp"))
    var restr = df.getOWLDataMaxCardinality(3, dataProp)
    assert(p.checkParsed(p.dataMaxCardinality_restriction, restrStr) == restr)

    restrStr = "bar:dataProp max 3"
    dataProp = df.getOWLDataProperty(IRI.create("http://ex.com/bar#dataProp"))
    restr = df.getOWLDataMaxCardinality(3, dataProp)
    assert(p.checkParsed(p.dataMaxCardinality_restriction, restrStr) == restr)

    restrStr = "<http://ex.com/foo#dataProp> max 3"
    dataProp = df.getOWLDataProperty(IRI.create("http://ex.com/foo#dataProp"))
    restr = df.getOWLDataMaxCardinality(3, dataProp)
    assert(p.checkParsed(p.dataMaxCardinality_restriction, restrStr) == restr)


    restrStr = "dataProp max 3 string"
    dataProp = df.getOWLDataProperty(IRI.create("http://ex.com/default#dataProp"))
    restr = df.getOWLDataMaxCardinality(3, dataProp, df.getStringOWLDatatype)
    assert(p.checkParsed(p.dataMaxCardinality_restriction, restrStr) == restr)

    restrStr = "bar:dataProp max 3 string"
    dataProp = df.getOWLDataProperty(IRI.create("http://ex.com/bar#dataProp"))
    restr = df.getOWLDataMaxCardinality(3, dataProp, df.getStringOWLDatatype)
    assert(p.checkParsed(p.dataMaxCardinality_restriction, restrStr) == restr)

    restrStr = "<http://ex.com/foo#dataProp> max 3 string"
    dataProp = df.getOWLDataProperty(IRI.create("http://ex.com/foo#dataProp"))
    restr = df.getOWLDataMaxCardinality(3, dataProp, df.getStringOWLDatatype)
    assert(p.checkParsed(p.dataMaxCardinality_restriction, restrStr) == restr)

    p.prefixes.clear()
  }

  /**
    * Unfortunately this cannot be tested with the restriction parser since
    * the tests will always match the more general object cardinality parsers
    * without primary, i.e. sth like 'someObjProp min 3', 'someObjProp max 3',
    * or 'someObjProp exactly 3'
    */
  test("The DataExactCardinality restriction parser should work correctly") {
    p.prefixes.clear()
    p.prefixes.put("", "http://ex.com/default#")
    p.prefixes.put("bar", "http://ex.com/bar#")

    var restrStr = "dataProp exactly 3"
    var dataProp = df.getOWLDataProperty(IRI.create("http://ex.com/default#dataProp"))
    var restr = df.getOWLDataExactCardinality(3, dataProp)
    assert(p.checkParsed(p.dataExactCardinality_restriction, restrStr) == restr)

    restrStr = "bar:dataProp exactly 3"
    dataProp = df.getOWLDataProperty(IRI.create("http://ex.com/bar#dataProp"))
    restr = df.getOWLDataExactCardinality(3, dataProp)
    assert(p.checkParsed(p.dataExactCardinality_restriction, restrStr) == restr)

    restrStr = "<http://ex.com/foo#dataProp> exactly 3"
    dataProp = df.getOWLDataProperty(IRI.create("http://ex.com/foo#dataProp"))
    restr = df.getOWLDataExactCardinality(3, dataProp)
    assert(p.checkParsed(p.dataExactCardinality_restriction, restrStr) == restr)


    restrStr = "dataProp exactly 3 string"
    dataProp = df.getOWLDataProperty(IRI.create("http://ex.com/default#dataProp"))
    restr = df.getOWLDataExactCardinality(3, dataProp, df.getStringOWLDatatype)
    assert(p.checkParsed(p.dataExactCardinality_restriction, restrStr) == restr)

    restrStr = "bar:dataProp exactly 3 string"
    dataProp = df.getOWLDataProperty(IRI.create("http://ex.com/bar#dataProp"))
    restr = df.getOWLDataExactCardinality(3, dataProp, df.getStringOWLDatatype)
    assert(p.checkParsed(p.dataExactCardinality_restriction, restrStr) == restr)

    restrStr = "<http://ex.com/foo#dataProp> exactly 3 string"
    dataProp = df.getOWLDataProperty(IRI.create("http://ex.com/foo#dataProp"))
    restr = df.getOWLDataExactCardinality(3, dataProp, df.getStringOWLDatatype)
    assert(p.checkParsed(p.dataExactCardinality_restriction, restrStr) == restr)

    p.prefixes.clear()
  }

  test("The description parser should work correctly") {
    // 'description' = disjunction of conjunctions
    p.prefixes.clear()
    p.prefixes.put("", "http://ex.com/default#")

    var descrString = "Cls1"
    val ce1 = df.getOWLClass("http://ex.com/default#Cls1")
    var descr: OWLClassExpression = ce1
    assert(p.checkParsed(p.description, descrString) == descr)

    descrString = "Cls1 or Cls2"
    val ce2 = df.getOWLClass("http://ex.com/default#Cls2")
    descr = df.getOWLObjectUnionOf(List(ce1, ce2).asJavaCollection.stream())
    assert(p.checkParsed(p.description, descrString) == descr)

    descrString = "Cls1 or Cls2 or Cls3"
    val ce3 = df.getOWLClass("http://ex.com/default#Cls3")
    descr = df.getOWLObjectUnionOf(List(ce1, ce2, ce3).asJavaCollection.stream())
    assert(p.checkParsed(p.description, descrString) == descr)

    p.prefixes.clear()
  }

  test("The EquivalentTo part of the class frame parser should work correctly") {
    p.prefixes.clear()
    p.prefixes.put("", "http://ex.com/default#")
    p.prefixes.put("bar", "http://ex.com/bar#")
    p.prefixes.put("rdfs", "http://www.w3.org/2000/01/rdf-schema#")

    var eqToStr =
      """EquivalentTo:
        |   SomeClass
      """.stripMargin
    var ce: OWLClassExpression = df.getOWLClass("http://ex.com/default#SomeClass")
    var resultList: List[(OWLClassExpression, List[OWLAnnotation])] =
      List((ce, List.empty))
    assert(p.checkParsed(p.equivalentTo, eqToStr) == resultList)

    eqToStr =
      """EquivalentTo:
        |   bar:SomeClass
      """.stripMargin
    ce = df.getOWLClass("http://ex.com/bar#SomeClass")
    resultList = List((ce, List.empty))
    assert(p.checkParsed(p.equivalentTo, eqToStr) == resultList)

    eqToStr =
      """EquivalentTo:
        |   <http://ex.com/whatever#SomeClass>
      """.stripMargin
    ce = df.getOWLClass("http://ex.com/whatever#SomeClass")
    resultList = List((ce, List.empty))
    assert(p.checkParsed(p.equivalentTo, eqToStr) == resultList)

    eqToStr =
      """EquivalentTo:
        |   someProp some SomeClass
      """.stripMargin
    ce = df.getOWLClass("http://ex.com/default#SomeClass")
    var prop: OWLObjectProperty = df.getOWLObjectProperty("http://ex.com/default#someProp")
    ce = df.getOWLObjectSomeValuesFrom(prop, ce)
    resultList = List((ce, List.empty))
    assert(p.checkParsed(p.equivalentTo, eqToStr) == resultList)

    eqToStr =
      """EquivalentTo:
        |   bar:someProp some bar:SomeClass
      """.stripMargin
    ce = df.getOWLClass("http://ex.com/bar#SomeClass")
    prop = df.getOWLObjectProperty("http://ex.com/bar#someProp")
    ce = df.getOWLObjectSomeValuesFrom(prop, ce)
    resultList = List((ce, List.empty))
    assert(p.checkParsed(p.equivalentTo, eqToStr) == resultList)

    eqToStr =
      """EquivalentTo:
        |   <http://ex.com/whatever#someProp> some <http://ex.com/whatever#SomeClass>
      """.stripMargin
    ce = df.getOWLClass("http://ex.com/whatever#SomeClass")
    prop = df.getOWLObjectProperty("http://ex.com/whatever#someProp")
    ce = df.getOWLObjectSomeValuesFrom(prop, ce)
    resultList = List((ce, List.empty))
    assert(p.checkParsed(p.equivalentTo, eqToStr) == resultList)

    eqToStr =
      """EquivalentTo:
        |   someProp exactly 23 SomeClass
      """.stripMargin
    ce = df.getOWLClass("http://ex.com/default#SomeClass")
    prop = df.getOWLObjectProperty("http://ex.com/default#someProp")
    ce = df.getOWLObjectExactCardinality(23, prop, ce)
    resultList = List((ce, List.empty))
    assert(p.checkParsed(p.equivalentTo, eqToStr) == resultList)

//    eqToStr =
//      """EquivalentTo:
//        |   someProp exactly 1 SomeClass
//      """.stripMargin
//    ce = df.getOWLClass("http://ex.com/whatever#SomeClass")
//    prop = df.getOWLObjectProperty("http://ex.com/whatever#someProp")
//    ce = df.getOWLObjectSomeValuesFrom(prop, ce)
//    resultList = List((ce, List.empty))
//    assert(p.checkParsed(p.equivalentTo, eqToStr) == resultList)

    p.prefixes.clear()
  }

  test("The HasKey parser should work correctly") {
    p.prefixes.clear()
    p.prefixes.put("", "http://ex.com/default#")
    p.prefixes.put("bar", "http://ex.com/bar#")
    p.prefixes.put("rdfs", "http://www.w3.org/2000/01/rdf-schema#")

    val noAnnotations = List.empty[OWLAnnotation].asJavaCollection

    var prefix = "http://ex.com/default#"
    var hasKeyStr =
      """HasKey:
        |    Annotations:
        |        rdfs:label "Don't understand why there has to be an annotation here"
        |    objProp dProp
      """.stripMargin
    var parsed = p.checkParsed(p.hasKey, hasKeyStr)
    var annotation = new OWLAnnotationImpl(
      df.getOWLAnnotationProperty(Namespaces.RDFS.getPrefixIRI, "label"),
      df.getOWLLiteral("Don't understand why there has to be an annotation here"),
      noAnnotations.stream()
    )
    var properties = List(
      df.getOWLObjectProperty(prefix, "objProp"),
      df.getOWLObjectProperty(prefix, "dProp")
    )
    assert(parsed == (List(annotation), properties))


    prefix = "http://ex.com/bar#"
    hasKeyStr =
      """HasKey:
        |    Annotations:
        |        rdfs:label "Don't understand why there has to be an annotation here"
        |    bar:objProp bar:dProp
      """.stripMargin
    parsed = p.checkParsed(p.hasKey, hasKeyStr)
    annotation = new OWLAnnotationImpl(
      df.getOWLAnnotationProperty(Namespaces.RDFS.getPrefixIRI, "label"),
      df.getOWLLiteral("Don't understand why there has to be an annotation here"),
      noAnnotations.stream()
    )
    properties = List(
      df.getOWLObjectProperty(prefix, "objProp"),
      df.getOWLObjectProperty(prefix, "dProp")
    )
    assert(parsed == (List(annotation), properties))

    prefix = "http://ex.com/whatever#"
    hasKeyStr =
      """HasKey:
        |    Annotations:
        |        rdfs:label "Don't understand why there has to be an annotation here"
        |    <http://ex.com/whatever#objProp> <http://ex.com/whatever#dProp>
      """.stripMargin
    parsed = p.checkParsed(p.hasKey, hasKeyStr)
    annotation = new OWLAnnotationImpl(
      df.getOWLAnnotationProperty(Namespaces.RDFS.getPrefixIRI, "label"),
      df.getOWLLiteral("Don't understand why there has to be an annotation here"),
      noAnnotations.stream()
    )
    properties = List(
      df.getOWLObjectProperty(prefix, "objProp"),
      df.getOWLObjectProperty(prefix, "dProp")
    )
    assert(parsed == (List(annotation), properties))

    prefix = "http://ex.com/whatever#"
    hasKeyStr =
      """HasKey:
        |    Annotations:
        |        rdfs:label "Don't understand why there has to be an annotation here"
        |    <http://ex.com/whatever#objProp> <http://ex.com/whatever#dProp>
        |    <http://ex.com/whatever#anotherProp>
      """.stripMargin
    parsed = p.checkParsed(p.hasKey, hasKeyStr)
    annotation = new OWLAnnotationImpl(
      df.getOWLAnnotationProperty(Namespaces.RDFS.getPrefixIRI, "label"),
      df.getOWLLiteral("Don't understand why there has to be an annotation here"),
      noAnnotations.stream()
    )
    properties = List(
      df.getOWLObjectProperty(prefix, "objProp"),
      df.getOWLObjectProperty(prefix, "dProp"),
      df.getOWLObjectProperty(prefix, "anotherProp")
    )
    assert(parsed == (List(annotation), properties))

    prefix = "http://ex.com/whatever#"
    hasKeyStr =
      """HasKey:
        |    Annotations:
        |        rdfs:label "Don't understand why there has to be an annotation here"
        |    <http://ex.com/whatever#objProp>
      """.stripMargin
    parsed = p.checkParsed(p.hasKey, hasKeyStr)
    annotation = new OWLAnnotationImpl(
      df.getOWLAnnotationProperty(Namespaces.RDFS.getPrefixIRI, "label"),
      df.getOWLLiteral("Don't understand why there has to be an annotation here"),
      noAnnotations.stream()
    )
    properties = List(
      df.getOWLObjectProperty(prefix, "objProp")
    )
    assert(parsed == (List(annotation), properties))

    p.prefixes.clear()
  }

  def debugSave(axioms: List[OWLAxiom], format: OWLDocumentFormatImpl, filePath: String) = {
    val man = OWLManager.createOWLOntologyManager()
    val ont = man.createOntology(IRI.create("file://" + filePath))
    ont.addAxioms(axioms.asJavaCollection)
    man.saveOntology(ont, format)
  }

  test("The class frame parser should work correctly") {
    p.prefixes.clear()
    p.prefixes.put("", "http://ex.com/default#")
    p.prefixes.put("rdfs", "http://www.w3.org/2000/01/rdf-schema#")

    val noAnnotations = List.empty[OWLAnnotation].asJavaCollection
    var classFrameStr =
      """Class: SomeClass
        |    Annotations:
        |        comment "Some class",
        |        rdfs:label "Some class"
        |    SubClassOf:
        |        someProp exactly 1
        |    EquivalentTo:
        |        SomeOtherClass,
        |        YetAnotherClass
        |    DisjointWith:
        |        ADifferentClass,
        |        AnotherDifferentClass
        |    DisjointUnionOf:
        |        Annotations:
        |           rdfs:label "A union of small, medium and large"
        |        Small, Medium, Large
      """.stripMargin

    var parsed: List[OWLAxiom] = p.checkParsed(p.classFrame, classFrameStr)
//    debugSave(parsed, new TurtleDocumentFormat, "/tmp/owl_trials/ont.ttl")
    var prefix = "http://ex.com/default#"
    var cls = df.getOWLClass(prefix + "SomeClass")

    var annAxiom = new OWLAnnotationAssertionAxiomImpl(
      cls.getIRI,
      df.getOWLAnnotationProperty(prefix, "comment"),
      df.getOWLLiteral("Some class"),
      noAnnotations
    )
    assert(parsed.contains(annAxiom))

    annAxiom = new OWLAnnotationAssertionAxiomImpl(
      cls.getIRI,
      df.getOWLAnnotationProperty(Namespaces.RDFS.getPrefixIRI, "label"),
      df.getOWLLiteral("Some class"),
      noAnnotations
    )
    assert(parsed.contains(annAxiom))

    var subClassOfAxiom = new OWLSubClassOfAxiomImpl(
      cls,
      df.getOWLObjectExactCardinality(1, df.getOWLObjectProperty(prefix + "someProp")),
      noAnnotations
    )
    assert(parsed.contains(subClassOfAxiom))

    var equivClsAxiom = new OWLEquivalentClassesAxiomImpl(
      List(
        cls,
        df.getOWLClass(prefix + "SomeOtherClass")).asJavaCollection,
      noAnnotations)
    assert(parsed.contains(equivClsAxiom))

    equivClsAxiom = new OWLEquivalentClassesAxiomImpl(
      List(
        cls,
        df.getOWLClass(prefix + "YetAnotherClass")).asJavaCollection,
      noAnnotations)
    assert(parsed.contains(equivClsAxiom))

    var disjointWithAxiom = new OWLDisjointClassesAxiomImpl(
      List(
        cls,
        df.getOWLClass(prefix + "ADifferentClass")
      ).asJavaCollection,
      noAnnotations)
    assert(parsed.contains(disjointWithAxiom))

    disjointWithAxiom = new OWLDisjointClassesAxiomImpl(
      List(
        cls,
        df.getOWLClass(prefix + "AnotherDifferentClass")
      ).asJavaCollection,
      noAnnotations)
    assert(parsed.contains(disjointWithAxiom))

    var annotations = List(
      new OWLAnnotationImpl(
        df.getOWLAnnotationProperty(Namespaces.RDFS.getPrefixIRI, "label"),
        df.getOWLLiteral("A union of small, medium and large"),
        noAnnotations.stream())
    )
    var disjUnionAxiom = new OWLDisjointUnionAxiomImpl(
      cls,
      List[OWLClassExpression](
        df.getOWLClass(prefix + "Small"), df.getOWLClass(prefix + "Medium"),
        df.getOWLClass(prefix + "Large")
      ).asJavaCollection.stream(),
      annotations
    )
    assert(parsed.contains(disjUnionAxiom))

    classFrameStr =
      """Class: SomeClass
        |    HasKey:
        |        Annotations:
        |            rdfs:label "Don't understand why there has to be an annotation here"
        |        objProp dProp
      """.stripMargin
    parsed = p.checkParsed(p.classFrame, classFrameStr)
    annotations = List(
      new OWLAnnotationImpl(
        df.getOWLAnnotationProperty(Namespaces.RDFS.getPrefixIRI, "label"),
        df.getOWLLiteral("Don't understand why there has to be an annotation here"),
        noAnnotations.stream()
      )
    )
    val properties = List(
      df.getOWLObjectProperty(prefix + "objProp"),
      df.getOWLObjectProperty(prefix + "dProp")
    )
    val hasKeyAxiom = new OWLHasKeyAxiomImpl(cls, properties, annotations)
    assert(parsed.contains(hasKeyAxiom))

    p.prefixes.clear()
  }

  test("The object property domain parser should work correctly") {
    p.prefixes.clear()
    p.prefixes.put("", "http://ex.com/default#")
    p.prefixes.put("bar", "http://ex.com/bar#")
    p.prefixes.put("rdfs", "http://www.w3.org/2000/01/rdf-schema#")

    val noAnnotations = List.empty[OWLAnnotation].asJavaCollection

    var prefix = "http://ex.com/default#"
    var domainStr =
      """Domain:
        |    Annotations:
        |        label "Domain ABC",
        |        comment "Some comment"
        |    prop exactly 23 Whatever
      """.stripMargin
    var parsed = p.checkParsed(p.domain, domainStr)

    var expectedAnnotations = List(
      df.getOWLAnnotation(
        df.getOWLAnnotationProperty(prefix + "label"),
        df.getOWLLiteral("Domain ABC"),
        noAnnotations
      ),
      df.getOWLAnnotation(
        df.getOWLAnnotationProperty(prefix + "comment"),
        df.getOWLLiteral("Some comment"),
        noAnnotations
      )
    )
    var expectedDomain: OWLClassExpression =
      df.getOWLObjectExactCardinality(
        23,
        df.getOWLObjectProperty(prefix + "prop"),
        df.getOWLClass(prefix + "Whatever")
      )
    assert(parsed.length == 1)
    assert(parsed(0)._2 == expectedAnnotations)
    assert(parsed(0)._1 == expectedDomain)


    prefix = "http://ex.com/bar#"
    domainStr =
      """Domain:
        |    Annotations:
        |        bar:label "Domain ABC",
        |        bar:comment "Some comment"
        |    bar:prop exactly 23 bar:Whatever
      """.stripMargin
    parsed = p.checkParsed(p.domain, domainStr)

    expectedAnnotations = List(
      df.getOWLAnnotation(
        df.getOWLAnnotationProperty(prefix + "label"),
        df.getOWLLiteral("Domain ABC"),
        noAnnotations
      ),
      df.getOWLAnnotation(
        df.getOWLAnnotationProperty(prefix + "comment"),
        df.getOWLLiteral("Some comment"),
        noAnnotations
      )
    )
    expectedDomain =
      df.getOWLObjectExactCardinality(
        23,
        df.getOWLObjectProperty(prefix + "prop"),
        df.getOWLClass(prefix + "Whatever")
      )
    assert(parsed.length == 1)
    assert(parsed(0)._2 == expectedAnnotations)
    assert(parsed(0)._1 == expectedDomain)


    prefix = "http://ex.com/blah#"
    domainStr =
      """Domain:
        |    Annotations:
        |        <http://ex.com/blah#label> "Domain ABC",
        |        <http://ex.com/blah#comment> "Some comment"
        |    <http://ex.com/blah#prop> exactly 23 <http://ex.com/blah#Whatever>
      """.stripMargin
    parsed = p.checkParsed(p.domain, domainStr)

    expectedAnnotations = List(
      df.getOWLAnnotation(
        df.getOWLAnnotationProperty(prefix + "label"),
        df.getOWLLiteral("Domain ABC"),
        noAnnotations
      ),
      df.getOWLAnnotation(
        df.getOWLAnnotationProperty(prefix + "comment"),
        df.getOWLLiteral("Some comment"),
        noAnnotations
      )
    )
    expectedDomain =
      df.getOWLObjectExactCardinality(
        23,
        df.getOWLObjectProperty(prefix + "prop"),
        df.getOWLClass(prefix + "Whatever")
      )
    assert(parsed.length == 1)
    assert(parsed(0)._2 == expectedAnnotations)
    assert(parsed(0)._1 == expectedDomain)

    p.prefixes.clear()
  }

  test("The object property frame parser should work correctly") {
    // I'm well aware that a property cannot have all the properties made up in the test
    p.prefixes.clear()
    p.prefixes.put("", "http://ex.com/default#")
    p.prefixes.put("rdfs", "http://www.w3.org/2000/01/rdf-schema#")

    val noAnnotations = List.empty[OWLAnnotation].asJavaCollection

    var prefix = "http://ex.com/default#"
    var objProp = df.getOWLObjectProperty(prefix + "objProp")
    var objPropFrameStr =
      """ObjectProperty: objProp
        |    Annotations:
        |        comment "Some comment",
        |        label "Object Property XYZ"
        |    Domain:
        |        anotherProp exactly 23
        |    Range:
        |        yetAnother some Whatever
        |    Characteristics:
        |        Functional, InverseFunctional, Reflexive, Irreflexive,
        |        Annotations:
        |            comment "Yes, this object property is asymmetric"
        |        Asymmetric,
        |        Symmetric, Transitive
        |    SubPropertyOf:
        |        anotherProp,
        |        Annotations:
        |            comment "Some comment"
        |        inverse yetAnotherProp
        |    EquivalentTo:
        |        andYetAnotherProp,
        |        Annotations:
        |            comment "Some comment again"
        |        inverse yetAnotherProp
        |    DisjointWith:
        |        aDisjointProp,
        |        Annotations:
        |            comment "Comment my ass!"
        |        inverse anotherDisjointProp
        |    InverseOf:
        |        anInverseProp,
        |        Annotations:
        |            comment "Lalalala"
        |        inverse inverseOfInverseProp
        |    SubPropertyChain:
        |        Annotations:
        |            comment "Check, 1, 2"
        |        chainProp1 o chainProp2 o
        |          inverse inverseChainProp3 o chainProp4
      """.stripMargin
    var parsed: List[OWLAxiom] = p.checkParsed(p.objectPropertyFrame, objPropFrameStr)

    var expectedAxiom: OWLAxiom = new OWLAnnotationAssertionAxiomImpl(
      objProp.getIRI,
      df.getOWLAnnotationProperty(prefix + "comment"),
      df.getOWLLiteral("Some comment"),
      noAnnotations
    )
    assert(parsed.contains(expectedAxiom))

    expectedAxiom = new OWLAnnotationAssertionAxiomImpl(
      objProp.getIRI,
      df.getOWLAnnotationProperty(prefix + "label"),
      df.getOWLLiteral("Object Property XYZ"),
      noAnnotations
    )
    assert(parsed.contains(expectedAxiom))

    expectedAxiom = new OWLFunctionalObjectPropertyAxiomImpl(
      objProp,
      noAnnotations
    )
    assert(parsed.contains(expectedAxiom))

    expectedAxiom = new OWLInverseFunctionalObjectPropertyAxiomImpl(
      objProp,
      noAnnotations
    )
    assert(parsed.contains(expectedAxiom))

    expectedAxiom = new OWLReflexiveObjectPropertyAxiomImpl(
      objProp,
      noAnnotations
    )
    assert(parsed.contains(expectedAxiom))

    expectedAxiom = new OWLIrreflexiveObjectPropertyAxiomImpl(
      objProp,
      noAnnotations
    )
    assert(parsed.contains(expectedAxiom))

    expectedAxiom = new OWLAsymmetricObjectPropertyAxiomImpl(
      objProp,
      List(df.getOWLAnnotation(
        df.getOWLAnnotationProperty(prefix + "comment"),
        df.getOWLLiteral("Yes, this object property is asymmetric"),
        noAnnotations
      )).asJavaCollection
    )
    assert(parsed.contains(expectedAxiom))

    expectedAxiom = new OWLSymmetricObjectPropertyAxiomImpl(
      objProp,
      noAnnotations
    )
    assert(parsed.contains(expectedAxiom))

    expectedAxiom = new OWLTransitiveObjectPropertyAxiomImpl(
      objProp,
      noAnnotations
    )
    assert(parsed.contains(expectedAxiom))

    expectedAxiom = new OWLSubObjectPropertyOfAxiomImpl(
      objProp,
      df.getOWLObjectProperty(prefix + "anotherProp"),
      noAnnotations
    )
    assert(parsed.contains(expectedAxiom))

    expectedAxiom = new OWLSubObjectPropertyOfAxiomImpl(
      objProp,
      df.getOWLObjectInverseOf(
        df.getOWLObjectProperty(prefix + "yetAnotherProp")
      ),
      List(df.getOWLAnnotation(
        df.getOWLAnnotationProperty(prefix + "comment"),
        df.getOWLLiteral("Some comment")
      )).asJavaCollection
    )
    assert(parsed.contains(expectedAxiom))

    expectedAxiom = new OWLEquivalentObjectPropertiesAxiomImpl(
      List(
        objProp,
        df.getOWLObjectProperty(prefix + "andYetAnotherProp")
      ).asJavaCollection,
      noAnnotations
    )
    assert(parsed.contains(expectedAxiom))

    expectedAxiom = new OWLEquivalentObjectPropertiesAxiomImpl(
      List(
        objProp,
        df.getOWLObjectInverseOf(
          df.getOWLObjectProperty(prefix + "yetAnotherProp")
        )
      ).asJavaCollection,
      List(df.getOWLAnnotation(
        df.getOWLAnnotationProperty(prefix + "comment"),
        df.getOWLLiteral("Some comment again")
      )).asJavaCollection
    )
    assert(parsed.contains(expectedAxiom))

    expectedAxiom = new OWLDisjointObjectPropertiesAxiomImpl(
      List(
        objProp,
        df.getOWLObjectProperty(prefix + "aDisjointProp")
      ).asJavaCollection,
      noAnnotations
    )
    assert(parsed.contains(expectedAxiom))

    expectedAxiom = new OWLDisjointObjectPropertiesAxiomImpl(
      List(
        objProp,
        df.getOWLObjectInverseOf(
          df.getOWLObjectProperty(prefix + "anotherDisjointProp")
        )
      ).asJavaCollection,
      List(df.getOWLAnnotation(
        df.getOWLAnnotationProperty(prefix + "comment"),
        df.getOWLLiteral("Comment my ass!")
      )).asJavaCollection
    )
    assert(parsed.contains(expectedAxiom))

    expectedAxiom = new OWLInverseObjectPropertiesAxiomImpl(
      objProp,
      df.getOWLObjectProperty(prefix + "anInverseProp"),
      noAnnotations
    )
    assert(parsed.contains(expectedAxiom))

    expectedAxiom = new OWLInverseObjectPropertiesAxiomImpl(
      objProp,
      df.getOWLObjectInverseOf(
        df.getOWLObjectProperty(prefix + "inverseOfInverseProp")
      ),
      List(df.getOWLAnnotation(
        df.getOWLAnnotationProperty(prefix + "comment"),
        df.getOWLLiteral("Lalalala")
      )).asJavaCollection
    )
    assert(parsed.contains(expectedAxiom))

    expectedAxiom = new OWLSubPropertyChainAxiomImpl(
      List(
        df.getOWLObjectProperty(prefix + "chainProp1"),
        df.getOWLObjectProperty(prefix + "chainProp2"),
        df.getOWLObjectInverseOf(
          df.getOWLObjectProperty(prefix + "inverseChainProp3")
        ),
        df.getOWLObjectProperty(prefix + "chainProp4")
      ).asJava,
      objProp,
      List(df.getOWLAnnotation(
        df.getOWLAnnotationProperty(prefix + "comment"),
        df.getOWLLiteral("Check, 1, 2")
      )).asJavaCollection
    )
    assert(parsed.contains(expectedAxiom))

//    debugSave(parsed, new ManchesterSyntaxDocumentFormat, "/tmp/owl_trials/ont.owl")

    p.prefixes.clear()
  }

  test("The object property characteristics parser should work correctly") {
    // I'm well aware that a property cannot have all the properties made up in the test
    p.prefixes.clear()
    p.prefixes.put("", "http://ex.com/default#")
    p.prefixes.put("bar", "http://ex.com/bar#")
    p.prefixes.put("rdfs", "http://www.w3.org/2000/01/rdf-schema#")

    var prefix = "http://ex.com/default#"
    var characteristicsStr =
      """Characteristics:
        |    Annotations:
        |        comment "The property is functional",
        |        title "Functionality characteristic"
        |    Functional,
        |
        |    Annotations:
        |        comment "The property is inverse functional",
        |        title "Inverse functionality characteristic"
        |    InverseFunctional,
        |
        |    Annotations:
        |        comment "The property is reflexive",
        |        title "Reflexivity characteristic"
        |    Reflexive,
        |
        |    Annotations:
        |        comment "The property is irreflexive",
        |        title "Irreflexivity characteristic"
        |    Irreflexive,
        |
        |    Annotations:
        |        comment "The property is symmetric",
        |        title "Symmetry characteristic"
        |    Symmetric,
        |
        |    Annotations:
        |        comment "The property is asymmetric",
        |        title "Asymmetry characteristic"
        |    Asymmetric,
        |
        |    Annotations:
        |        comment "The property is transitive",
        |        title "Transitivity characteristic"
        |    Transitive
      """.stripMargin
    var parsed: List[(PropertyCharacteristic.Value, List[OWLAnnotation])] =
      p.checkParsed(p.characteristics, characteristicsStr)

    var annotations = List(
      df.getOWLAnnotation(
        df.getOWLAnnotationProperty(prefix + "comment"),
        df.getOWLLiteral("The property is functional")
      ),
      df.getOWLAnnotation(
        df.getOWLAnnotationProperty(prefix + "title"),
        df.getOWLLiteral("Functionality characteristic")
      )
    )
    assert(parsed.contains((PropertyCharacteristic.Functional, annotations)))

    annotations = List(
      df.getOWLAnnotation(
        df.getOWLAnnotationProperty(prefix + "comment"),
        df.getOWLLiteral("The property is inverse functional")
      ),
      df.getOWLAnnotation(
        df.getOWLAnnotationProperty(prefix + "title"),
        df.getOWLLiteral("Inverse functionality characteristic")
      )
    )
    assert(parsed.contains((PropertyCharacteristic.InverseFunctional, annotations)))

    annotations = List(
      df.getOWLAnnotation(
        df.getOWLAnnotationProperty(prefix + "comment"),
        df.getOWLLiteral("The property is reflexive")
      ),
      df.getOWLAnnotation(
        df.getOWLAnnotationProperty(prefix + "title"),
        df.getOWLLiteral("Reflexivity characteristic")
      )
    )
    assert(parsed.contains((PropertyCharacteristic.Reflexive, annotations)))

    annotations = List(
      df.getOWLAnnotation(
        df.getOWLAnnotationProperty(prefix + "comment"),
        df.getOWLLiteral("The property is irreflexive")
      ),
      df.getOWLAnnotation(
        df.getOWLAnnotationProperty(prefix + "title"),
        df.getOWLLiteral("Irreflexivity characteristic")
      )
    )
    assert(parsed.contains((PropertyCharacteristic.Irreflexive, annotations)))

    annotations = List(
      df.getOWLAnnotation(
        df.getOWLAnnotationProperty(prefix + "comment"),
        df.getOWLLiteral("The property is symmetric")
      ),
      df.getOWLAnnotation(
        df.getOWLAnnotationProperty(prefix + "title"),
        df.getOWLLiteral("Symmetry characteristic")
      )
    )
    assert(parsed.contains((PropertyCharacteristic.Symmetric, annotations)))

    annotations = List(
      df.getOWLAnnotation(
        df.getOWLAnnotationProperty(prefix + "comment"),
        df.getOWLLiteral("The property is asymmetric")
      ),
      df.getOWLAnnotation(
        df.getOWLAnnotationProperty(prefix + "title"),
        df.getOWLLiteral("Asymmetry characteristic")
      )
    )
    assert(parsed.contains((PropertyCharacteristic.Asymmetric, annotations)))

    annotations = List(
      df.getOWLAnnotation(
        df.getOWLAnnotationProperty(prefix + "comment"),
        df.getOWLLiteral("The property is transitive")
      ),
      df.getOWLAnnotation(
        df.getOWLAnnotationProperty(prefix + "title"),
        df.getOWLLiteral("Transitivity characteristic")
      )
    )
    assert(parsed.contains((PropertyCharacteristic.Transitive, annotations)))


    prefix = "http://ex.com/bar#"
    characteristicsStr =
      """Characteristics:
        |    Annotations:
        |        bar:comment "The property is functional",
        |        bar:title "Functionality characteristic"
        |    Functional,
        |
        |    Annotations:
        |        bar:comment "The property is inverse functional",
        |        bar:title "Inverse functionality characteristic"
        |    InverseFunctional,
        |
        |    Annotations:
        |        bar:comment "The property is reflexive",
        |        bar:title "Reflexivity characteristic"
        |    Reflexive,
        |
        |    Annotations:
        |        bar:comment "The property is irreflexive",
        |        bar:title "Irreflexivity characteristic"
        |    Irreflexive,
        |
        |    Annotations:
        |        bar:comment "The property is symmetric",
        |        bar:title "Symmetry characteristic"
        |    Symmetric,
        |
        |    Annotations:
        |        bar:comment "The property is asymmetric",
        |        bar:title "Asymmetry characteristic"
        |    Asymmetric,
        |
        |    Annotations:
        |        bar:comment "The property is transitive",
        |        bar:title "Transitivity characteristic"
        |    Transitive
      """.stripMargin
    parsed = p.checkParsed(p.characteristics, characteristicsStr)

    annotations = List(
      df.getOWLAnnotation(
        df.getOWLAnnotationProperty(prefix + "comment"),
        df.getOWLLiteral("The property is functional")
      ),
      df.getOWLAnnotation(
        df.getOWLAnnotationProperty(prefix + "title"),
        df.getOWLLiteral("Functionality characteristic")
      )
    )
    assert(parsed.contains((PropertyCharacteristic.Functional, annotations)))

    annotations = List(
      df.getOWLAnnotation(
        df.getOWLAnnotationProperty(prefix + "comment"),
        df.getOWLLiteral("The property is inverse functional")
      ),
      df.getOWLAnnotation(
        df.getOWLAnnotationProperty(prefix + "title"),
        df.getOWLLiteral("Inverse functionality characteristic")
      )
    )
    assert(parsed.contains((PropertyCharacteristic.InverseFunctional, annotations)))

    annotations = List(
      df.getOWLAnnotation(
        df.getOWLAnnotationProperty(prefix + "comment"),
        df.getOWLLiteral("The property is reflexive")
      ),
      df.getOWLAnnotation(
        df.getOWLAnnotationProperty(prefix + "title"),
        df.getOWLLiteral("Reflexivity characteristic")
      )
    )
    assert(parsed.contains((PropertyCharacteristic.Reflexive, annotations)))

    annotations = List(
      df.getOWLAnnotation(
        df.getOWLAnnotationProperty(prefix + "comment"),
        df.getOWLLiteral("The property is irreflexive")
      ),
      df.getOWLAnnotation(
        df.getOWLAnnotationProperty(prefix + "title"),
        df.getOWLLiteral("Irreflexivity characteristic")
      )
    )
    assert(parsed.contains((PropertyCharacteristic.Irreflexive, annotations)))

    annotations = List(
      df.getOWLAnnotation(
        df.getOWLAnnotationProperty(prefix + "comment"),
        df.getOWLLiteral("The property is symmetric")
      ),
      df.getOWLAnnotation(
        df.getOWLAnnotationProperty(prefix + "title"),
        df.getOWLLiteral("Symmetry characteristic")
      )
    )
    assert(parsed.contains((PropertyCharacteristic.Symmetric, annotations)))

    annotations = List(
      df.getOWLAnnotation(
        df.getOWLAnnotationProperty(prefix + "comment"),
        df.getOWLLiteral("The property is asymmetric")
      ),
      df.getOWLAnnotation(
        df.getOWLAnnotationProperty(prefix + "title"),
        df.getOWLLiteral("Asymmetry characteristic")
      )
    )
    assert(parsed.contains((PropertyCharacteristic.Asymmetric, annotations)))

    annotations = List(
      df.getOWLAnnotation(
        df.getOWLAnnotationProperty(prefix + "comment"),
        df.getOWLLiteral("The property is transitive")
      ),
      df.getOWLAnnotation(
        df.getOWLAnnotationProperty(prefix + "title"),
        df.getOWLLiteral("Transitivity characteristic")
      )
    )
    assert(parsed.contains((PropertyCharacteristic.Transitive, annotations)))


    prefix = "http://ex.com/whatever#"
    characteristicsStr =
      """Characteristics:
        |    Annotations:
        |        <http://ex.com/whatever#comment> "The property is functional",
        |        <http://ex.com/whatever#title> "Functionality characteristic"
        |    Functional,
        |
        |    Annotations:
        |        <http://ex.com/whatever#comment> "The property is inverse functional",
        |        <http://ex.com/whatever#title> "Inverse functionality characteristic"
        |    InverseFunctional,
        |
        |    Annotations:
        |        <http://ex.com/whatever#comment> "The property is reflexive",
        |        <http://ex.com/whatever#title> "Reflexivity characteristic"
        |    Reflexive,
        |
        |    Annotations:
        |        <http://ex.com/whatever#comment> "The property is irreflexive",
        |        <http://ex.com/whatever#title> "Irreflexivity characteristic"
        |    Irreflexive,
        |
        |    Annotations:
        |        <http://ex.com/whatever#comment> "The property is symmetric",
        |        <http://ex.com/whatever#title> "Symmetry characteristic"
        |    Symmetric,
        |
        |    Annotations:
        |        <http://ex.com/whatever#comment> "The property is asymmetric",
        |        <http://ex.com/whatever#title> "Asymmetry characteristic"
        |    Asymmetric,
        |
        |    Annotations:
        |        <http://ex.com/whatever#comment> "The property is transitive",
        |        <http://ex.com/whatever#title> "Transitivity characteristic"
        |    Transitive
      """.stripMargin
    parsed = p.checkParsed(p.characteristics, characteristicsStr)

    annotations = List(
      df.getOWLAnnotation(
        df.getOWLAnnotationProperty(prefix + "comment"),
        df.getOWLLiteral("The property is functional")
      ),
      df.getOWLAnnotation(
        df.getOWLAnnotationProperty(prefix + "title"),
        df.getOWLLiteral("Functionality characteristic")
      )
    )
    assert(parsed.contains((PropertyCharacteristic.Functional, annotations)))

    annotations = List(
      df.getOWLAnnotation(
        df.getOWLAnnotationProperty(prefix + "comment"),
        df.getOWLLiteral("The property is inverse functional")
      ),
      df.getOWLAnnotation(
        df.getOWLAnnotationProperty(prefix + "title"),
        df.getOWLLiteral("Inverse functionality characteristic")
      )
    )
    assert(parsed.contains((PropertyCharacteristic.InverseFunctional, annotations)))

    annotations = List(
      df.getOWLAnnotation(
        df.getOWLAnnotationProperty(prefix + "comment"),
        df.getOWLLiteral("The property is reflexive")
      ),
      df.getOWLAnnotation(
        df.getOWLAnnotationProperty(prefix + "title"),
        df.getOWLLiteral("Reflexivity characteristic")
      )
    )
    assert(parsed.contains((PropertyCharacteristic.Reflexive, annotations)))

    annotations = List(
      df.getOWLAnnotation(
        df.getOWLAnnotationProperty(prefix + "comment"),
        df.getOWLLiteral("The property is irreflexive")
      ),
      df.getOWLAnnotation(
        df.getOWLAnnotationProperty(prefix + "title"),
        df.getOWLLiteral("Irreflexivity characteristic")
      )
    )
    assert(parsed.contains((PropertyCharacteristic.Irreflexive, annotations)))

    annotations = List(
      df.getOWLAnnotation(
        df.getOWLAnnotationProperty(prefix + "comment"),
        df.getOWLLiteral("The property is symmetric")
      ),
      df.getOWLAnnotation(
        df.getOWLAnnotationProperty(prefix + "title"),
        df.getOWLLiteral("Symmetry characteristic")
      )
    )
    assert(parsed.contains((PropertyCharacteristic.Symmetric, annotations)))

    annotations = List(
      df.getOWLAnnotation(
        df.getOWLAnnotationProperty(prefix + "comment"),
        df.getOWLLiteral("The property is asymmetric")
      ),
      df.getOWLAnnotation(
        df.getOWLAnnotationProperty(prefix + "title"),
        df.getOWLLiteral("Asymmetry characteristic")
      )
    )
    assert(parsed.contains((PropertyCharacteristic.Asymmetric, annotations)))

    annotations = List(
      df.getOWLAnnotation(
        df.getOWLAnnotationProperty(prefix + "comment"),
        df.getOWLLiteral("The property is transitive")
      ),
      df.getOWLAnnotation(
        df.getOWLAnnotationProperty(prefix + "title"),
        df.getOWLLiteral("Transitivity characteristic")
      )
    )
    assert(parsed.contains((PropertyCharacteristic.Transitive, annotations)))

    p.prefixes.clear()
  }
  // --------------------------------------------------------------------------

  test("The data property frame parser should work correctly") {
    p.prefixes.clear()
    p.prefixes.put("", "http://ex.com/default#")
    p.prefixes.put("bar", "http://ex.com/bar#")
    p.prefixes.put("xsd", "http://www.w3.org/2001/XMLSchema#")
    p.prefixes.put("rdfs", "http://www.w3.org/2000/01/rdf-schema#")

    val noAnnotations = List.empty[OWLAnnotation].asJavaCollection

    var prefix = "http://ex.com/default#"
    var dataProp = df.getOWLDataProperty(prefix + "prop")
    var dataPropFrameStr =
      """DataProperty: prop
        |    Annotations:
        |        comment "Some comment",
        |        label "Data Property XYZ"
        |    Domain:
        |        anotherProp exactly 23
        |    Range:
        |        xsd:nonNegativeInteger,
        |        Annotations:
        |           comment "...or more general"
        |        xsd:integer,
        |        Annotations:
        |           comment "...or even more general"
        |        xsd:decimal,
        |        Annotations:
        |           comment "We could go even further",
        |           title "The actual range"
        |        xsd:decimal or string
        |    Characteristics:
        |        Annotations:
        |            comment "This is a functional data property"
        |        Functional
        |    SubPropertyOf:
        |        superProp1,
        |        superProp2
        |    EquivalentTo:
        |        equivProp1,
        |        Annotations:
        |            comment "Whatever"
        |        equivProp2
        |    DisjointWith:
        |        disjProp1, disjProp2
      """.stripMargin
    var parsed = p.checkParsed(p.dataPropertyFrame, dataPropFrameStr)

    var expectedAxiom: OWLAxiom = new OWLAnnotationAssertionAxiomImpl(
      dataProp.getIRI,
      df.getOWLAnnotationProperty(prefix + "comment"),
      df.getOWLLiteral("Some comment"),
      noAnnotations
    )
    assert(parsed.contains(expectedAxiom))

    expectedAxiom = new OWLAnnotationAssertionAxiomImpl(
      dataProp.getIRI,
      df.getOWLAnnotationProperty(prefix + "label"),
      df.getOWLLiteral("Data Property XYZ"),
      noAnnotations
    )
    assert(parsed.contains(expectedAxiom))

    expectedAxiom = new OWLDataPropertyDomainAxiomImpl(
      dataProp,
      df.getOWLObjectExactCardinality(
        23,
        df.getOWLObjectProperty(prefix + "anotherProp")
      ),
      noAnnotations
    )
    assert(parsed.contains(expectedAxiom))

    expectedAxiom = new OWLDataPropertyRangeAxiomImpl(
      dataProp,
      df.getOWLDatatype(XSDVocabulary.NON_NEGATIVE_INTEGER),
      noAnnotations
    )
    assert(parsed.contains(expectedAxiom))

    expectedAxiom = new OWLDataPropertyRangeAxiomImpl(
      dataProp,
      df.getOWLDatatype(XSDVocabulary.INTEGER),
      List(
        df.getOWLAnnotation(
          df.getOWLAnnotationProperty(prefix + "comment"),
          df.getOWLLiteral("...or more general")
        )
      ).asJavaCollection
    )
    assert(parsed.contains(expectedAxiom))

    expectedAxiom = new OWLDataPropertyRangeAxiomImpl(
      dataProp,
      df.getOWLDatatype(XSDVocabulary.DECIMAL),
      List(
        df.getOWLAnnotation(
          df.getOWLAnnotationProperty(prefix + "comment"),
          df.getOWLLiteral("...or even more general")
        )
      ).asJavaCollection
    )
    assert(parsed.contains(expectedAxiom))

    expectedAxiom = new OWLDataPropertyRangeAxiomImpl(
      dataProp,
      df.getOWLDataUnionOf(
        List(
          df.getOWLDatatype(XSDVocabulary.DECIMAL),
          df.getOWLDatatype(XSDVocabulary.STRING)
        ).asJavaCollection
      ),
      List(
        df.getOWLAnnotation(
          df.getOWLAnnotationProperty(prefix + "comment"),
          df.getOWLLiteral("We could go even further")
        ),
        df.getOWLAnnotation(
          df.getOWLAnnotationProperty(prefix + "title"),
          df.getOWLLiteral("The actual range")
        )
      ).asJavaCollection
    )
    assert(parsed.contains(expectedAxiom))

    expectedAxiom = new OWLFunctionalDataPropertyAxiomImpl(
      dataProp,
      List(
        df.getOWLAnnotation(
          df.getOWLAnnotationProperty(prefix + "comment"),
          df.getOWLLiteral("This is a functional data property")
        )
      ).asJavaCollection
    )
    assert(parsed.contains(expectedAxiom))

    expectedAxiom = new OWLSubDataPropertyOfAxiomImpl(
      dataProp,
      df.getOWLDataProperty(prefix + "superProp1"),
      noAnnotations
    )
    assert(parsed.contains(expectedAxiom))

    expectedAxiom = new OWLSubDataPropertyOfAxiomImpl(
      dataProp,
      df.getOWLDataProperty(prefix + "superProp2"),
      noAnnotations
    )
    assert(parsed.contains(expectedAxiom))

    expectedAxiom = new OWLEquivalentDataPropertiesAxiomImpl(
      List(dataProp, df.getOWLDataProperty(prefix + "equivProp1")).asJavaCollection,
      noAnnotations
    )
    assert(parsed.contains(expectedAxiom))

    expectedAxiom = new OWLEquivalentDataPropertiesAxiomImpl(
      List(dataProp, df.getOWLDataProperty(prefix + "equivProp2")).asJavaCollection,
      List(
        df.getOWLAnnotation(
          df.getOWLAnnotationProperty(prefix + "comment"),
          df.getOWLLiteral("Whatever")
        )
      ).asJavaCollection
    )
    assert(parsed.contains(expectedAxiom))

    expectedAxiom = new OWLDisjointDataPropertiesAxiomImpl(
      List(dataProp, df.getOWLDataProperty(prefix + "disjProp1")).asJavaCollection,
      noAnnotations
    )
    assert(parsed.contains(expectedAxiom))

    expectedAxiom = new OWLDisjointDataPropertiesAxiomImpl(
      List(dataProp, df.getOWLDataProperty(prefix + "disjProp2")).asJavaCollection,
      noAnnotations
    )
    assert(parsed.contains(expectedAxiom))

    p.prefixes.clear()
  }

  test("The annotation property frame parser should work correctly") {
    p.prefixes.clear()
    p.prefixes.put("", "http://ex.com/default#")
    p.prefixes.put("bar", "http://ex.com/bar#")
    p.prefixes.put("xsd", "http://www.w3.org/2001/XMLSchema#")
    p.prefixes.put("rdfs", "http://www.w3.org/2000/01/rdf-schema#")

    val noAnnotations = List.empty[OWLAnnotation].asJavaCollection

    var prefix = "http://ex.com/default#"
    var annProp = df.getOWLAnnotationProperty(prefix + "prop")
    var annPropFrameStr =
      """AnnotationProperty: prop
        |    Annotations:
        |        comment "A comment",
        |        title "Annotation Property XYZ"
      """.stripMargin
    var parsed = p.checkParsed(p.annotationPropertyFrame, annPropFrameStr)

    var expectedAxiom: OWLAxiom = new OWLDeclarationAxiomImpl(annProp, noAnnotations)
    assert(parsed.contains(expectedAxiom))

    expectedAxiom = new OWLAnnotationAssertionAxiomImpl(
      annProp.getIRI,
      df.getOWLAnnotationProperty(prefix + "comment"),
      df.getOWLLiteral("A comment"),
      noAnnotations
    )
    assert(parsed.contains(expectedAxiom))

    expectedAxiom = new OWLAnnotationAssertionAxiomImpl(
      annProp.getIRI,
      df.getOWLAnnotationProperty(prefix + "title"),
      df.getOWLLiteral("Annotation Property XYZ"),
      noAnnotations
    )
    assert(parsed.contains(expectedAxiom))

    prefix = "http://ex.com/default#"
    annProp = df.getOWLAnnotationProperty(prefix + "prop")
    annPropFrameStr =
      """AnnotationProperty: prop
        |    Annotations:
        |        comment "A comment"
        |    Annotations:
        |        title "Annotation Property XYZ"
      """.stripMargin
    parsed = p.checkParsed(p.annotationPropertyFrame, annPropFrameStr)

    expectedAxiom = new OWLDeclarationAxiomImpl(annProp, noAnnotations)
    assert(parsed.contains(expectedAxiom))

    expectedAxiom = new OWLAnnotationAssertionAxiomImpl(
      annProp.getIRI,
      df.getOWLAnnotationProperty(prefix + "comment"),
      df.getOWLLiteral("A comment"),
      noAnnotations
    )
    assert(parsed.contains(expectedAxiom))

    expectedAxiom = new OWLAnnotationAssertionAxiomImpl(
      annProp.getIRI,
      df.getOWLAnnotationProperty(prefix + "title"),
      df.getOWLLiteral("Annotation Property XYZ"),
      noAnnotations
    )
    assert(parsed.contains(expectedAxiom))

    prefix = "http://ex.com/default#"
    annProp = df.getOWLAnnotationProperty(prefix + "prop")
    annPropFrameStr =
      """AnnotationProperty: prop
        |    Annotations:
        |        label "Whatever"
        |    Domain:
        |        Annotations:
        |            comment "Some comment"
        |        someIRI
        |    Range:
        |        anotherIRI
        |    SubPropertyOf:
        |        Annotations:
        |            comment "Blah"
        |        anotherProp
      """.stripMargin
    parsed = p.checkParsed(p.annotationPropertyFrame, annPropFrameStr)

    expectedAxiom = new OWLAnnotationAssertionAxiomImpl(
      annProp.getIRI,
      df.getOWLAnnotationProperty(prefix + "label"),
      df.getOWLLiteral("Whatever"),
      noAnnotations
    )
    assert(parsed.contains(expectedAxiom))

    expectedAxiom = new OWLAnnotationPropertyDomainAxiomImpl(
      annProp,
      IRI.create(prefix + "someIRI"),
      List(
        df.getOWLAnnotation(
          df.getOWLAnnotationProperty(prefix + "comment"),
          df.getOWLLiteral("Some comment")
        )
      ).asJavaCollection
    )
    assert(parsed.contains(expectedAxiom))

    parsed = p.checkParsed(p.annotationPropertyFrame, annPropFrameStr)

    expectedAxiom = new OWLAnnotationPropertyRangeAxiomImpl(
      annProp,
      IRI.create(prefix + "anotherIRI"),
      noAnnotations
    )
    assert(parsed.contains(expectedAxiom))

    expectedAxiom = new OWLSubAnnotationPropertyOfAxiomImpl(
      annProp,
      df.getOWLAnnotationProperty(prefix + "anotherProp"),
      List(
        df.getOWLAnnotation(
          df.getOWLAnnotationProperty(prefix + "comment"),
          df.getOWLLiteral("Blah"),
          noAnnotations
        )
      ).asJavaCollection
    )
    assert(parsed.contains(expectedAxiom))

    p.prefixes.clear
  }

  test("The annotation property domain parser should work correctly") {
    p.prefixes.clear()
    p.prefixes.put("", "http://ex.com/default#")
    p.prefixes.put("bar", "http://ex.com/bar#")
    p.prefixes.put("xsd", "http://www.w3.org/2001/XMLSchema#")
    p.prefixes.put("rdfs", "http://www.w3.org/2000/01/rdf-schema#")
    val noAnnotations = List.empty[OWLAnnotation]

    var prefix = "http://ex.com/default#"
    var domainStr = """Domain: SomeClass"""
    var expected = List((
      IRI.create(prefix + "SomeClass"),
      noAnnotations
    ))
    assert(p.checkParsed(p.annotationPropertyDomain, domainStr) == expected)

    domainStr =
      """Domain:
        |    Annotations:
        |        comment "Some comment"
        |    SomeClass""".stripMargin
    expected = List((
      IRI.create(prefix + "SomeClass"),
      List(new OWLAnnotationImpl(
        df.getOWLAnnotationProperty(prefix + "comment"),
        df.getOWLLiteral("Some comment"),
        noAnnotations.asJavaCollection.stream()
      ))
    ))
    assert(p.checkParsed(p.annotationPropertyDomain, domainStr) == expected)

    p.prefixes.clear()
  }

  test("The individual frame parser should work correctly") {
    setupParserPrefixes

    var prefix = "http://ex.com/default#"
    var indiv = df.getOWLNamedIndividual(prefix + "someIndiv")
    var individualFrameStr =
      """Individual: someIndiv
        |    Annotations:
        |        comment "Some comment"
        |    Types:
        |        Annotations:
        |            comment "A plain class"
        |        ClassABC,
        |
        |        Annotations:
        |            comment "A class expression"
        |        objProp min 3,
        |        anotherProp some RangeClass
        |    Facts:
        |        Annotations:
        |            comment "Object property fact"
        |        objProp anotherIndiv,
        |        Annotations:
        |            comment "Data property fact"
        |        dataProp 23,
        |        Annotations:
        |            comment "Negative object property fact"
        |        not objProp anUnrelatedIndiv
        |    SameAs:
        |        Annotations:
        |            comment "Whatever"
        |        sameIndiv, sameAgainIndiv
        |    DifferentFrom:
        |        aDifferentIndiv
      """.stripMargin
    var parsed = p.checkParsed(p.individualFrame, individualFrameStr)
    var expectedAxiom: OWLAxiom = new OWLDeclarationAxiomImpl(
      indiv,
      noAnnotations
    )
    assert(parsed.contains(expectedAxiom))

    expectedAxiom = new OWLAnnotationAssertionAxiomImpl(
      indiv.getIRI,
      df.getOWLAnnotationProperty(prefix + "comment"),
      df.getOWLLiteral("Some comment"),
      noAnnotations
    )
    assert(parsed.contains(expectedAxiom))

    expectedAxiom = new OWLClassAssertionAxiomImpl(
      indiv,
      df.getOWLClass(prefix + "ClassABC"),
      List(
        df.getOWLAnnotation(
          df.getOWLAnnotationProperty(prefix + "comment"),
          df.getOWLLiteral("A plain class")
        )
      ).asJavaCollection
    )
    assert(parsed.contains(expectedAxiom))

    expectedAxiom = new OWLClassAssertionAxiomImpl(
      indiv,
      df.getOWLObjectMinCardinality(
        3,
        df.getOWLObjectProperty(prefix + "objProp")
      ),
      List(
        df.getOWLAnnotation(
          df.getOWLAnnotationProperty(prefix + "comment"),
          df.getOWLLiteral("A class expression")
        )
      ).asJavaCollection
    )
    assert(parsed.contains(expectedAxiom))

    expectedAxiom = new OWLClassAssertionAxiomImpl(
      indiv,
      df.getOWLObjectSomeValuesFrom(
        df.getOWLObjectProperty(prefix + "anotherProp"),
        df.getOWLClass(prefix + "RangeClass")
      ),
      noAnnotations
    )
    assert(parsed.contains(expectedAxiom))

    expectedAxiom = new OWLObjectPropertyAssertionAxiomImpl(
      indiv,
      df.getOWLObjectProperty(prefix + "objProp"),
      df.getOWLNamedIndividual(prefix + "anotherIndiv"),
      List(
        df.getOWLAnnotation(
          df.getOWLAnnotationProperty(prefix + "comment"),
          df.getOWLLiteral("Object property fact")
        )
      ).asJavaCollection
    )
    assert(parsed.contains(expectedAxiom))

    expectedAxiom = new OWLDataPropertyAssertionAxiomImpl(
      indiv,
      df.getOWLDataProperty(prefix + "dataProp"),
      df.getOWLLiteral(23),
      List(
        df.getOWLAnnotation(
          df.getOWLAnnotationProperty(prefix + "comment"),
          df.getOWLLiteral("Data property fact")
        )
      ).asJavaCollection
    )
    assert(parsed.contains(expectedAxiom))

    expectedAxiom = new OWLNegativeObjectPropertyAssertionAxiomImpl(
      indiv,
      df.getOWLObjectProperty(prefix + "objProp"),
      df.getOWLNamedIndividual(prefix + "anUnrelatedIndiv"),
      List(
        df.getOWLAnnotation(
          df.getOWLAnnotationProperty(prefix + "comment"),
          df.getOWLLiteral("Negative object property fact")
        )
      ).asJavaCollection
    )
    assert(parsed.contains(expectedAxiom))

    expectedAxiom = new OWLSameIndividualAxiomImpl(
      List(
        indiv,
        df.getOWLNamedIndividual(prefix + "sameIndiv")
      ).asJavaCollection,
      List(
        df.getOWLAnnotation(
          df.getOWLAnnotationProperty(prefix + "comment"),
          df.getOWLLiteral("Whatever")
        )
      ).asJavaCollection
    )
    assert(parsed.contains(expectedAxiom))

    expectedAxiom = new OWLSameIndividualAxiomImpl(
      List(
        indiv,
        df.getOWLNamedIndividual(prefix + "sameAgainIndiv")
      ).asJavaCollection,
      noAnnotations
    )
    assert(parsed.contains(expectedAxiom))

    expectedAxiom = new OWLDifferentIndividualsAxiomImpl(
      List(
        indiv,
        df.getOWLNamedIndividual(prefix + "aDifferentIndiv")
      ).asJavaCollection,
      noAnnotations
    )
    assert(parsed.contains(expectedAxiom))

    individualFrameStr =
      """Individual: foo:indivA
        |
        |    Types:
        |        bar:Cls1
        |
        |    Facts:
        |     bar:objProp1  foo:indivB,
        |     bar:dataProp1  "ABCD",
        |      not  bar:dataProp2  23
        |
        |    SameAs:
        |        foo:sameAsIndivA
        |
        |    DifferentFrom:
        |        foo:indivB
        |
      """.stripMargin
    parsed = p.checkParsed(p.individualFrame, individualFrameStr)

    p.prefixes.clear()
  }

  test("The disjoint classes parser should work correctly") {
    setupParserPrefixes

    val prefix = "http://ex.com/default#"
    var disjointClassesStr =
      """DisjointClasses:
        |    Annotations:
        |        comment "Required annotation"
        |    prop min 2, Class2,
      """.stripMargin
    var parsed = p.checkParsed(p.disjointClasses, disjointClassesStr)
    var expectedAxiom = new OWLDisjointClassesAxiomImpl(
      List(
        df.getOWLObjectMinCardinality(
          2,
          df.getOWLObjectProperty(prefix + "prop")
        ),
        df.getOWLClass(prefix + "Class2")
      ).asJavaCollection,
      List(
        df.getOWLAnnotation(
          df.getOWLAnnotationProperty(prefix + "comment"),
          df.getOWLLiteral("Required annotation")
        )
      ).asJavaCollection
    )
    assert(parsed.contains(expectedAxiom))

    clearParserPrefixes
  }

  test("The equivalent classes parser should work correctly") {
    setupParserPrefixes

    val prefix = "http://ex.com/default#"
    var equivClassesStr =
      """EquivalentClasses:
        |    Annotations:
        |        comment "Required annotation"
        |    prop min 2, Class2,
      """.stripMargin
    var parsed = p.checkParsed(p.equivalentClasses, equivClassesStr)
    var expectedAxiom = new OWLEquivalentClassesAxiomImpl(
      List(
        df.getOWLObjectMinCardinality(
          2,
          df.getOWLObjectProperty(prefix + "prop")
        ),
        df.getOWLClass(prefix + "Class2")
      ).asJavaCollection,
      List(
        df.getOWLAnnotation(
          df.getOWLAnnotationProperty(prefix + "comment"),
          df.getOWLLiteral("Required annotation")
        )
      ).asJavaCollection
    )
    assert(parsed.contains(expectedAxiom))

    clearParserPrefixes
  }

  test("The equivalent object properties parser should work correctly") {
    setupParserPrefixes

    val prefix = "http://ex.com/default#"
    var equivPropertiesStr =
      """EquivalentProperties:
        |    Annotations:
        |        comment "Required annotation"
        |    prop1, prop2, prop3
      """.stripMargin
    var parsed = p.checkParsed(p.equivalentObjectProperties, equivPropertiesStr)
    var expectedAxiom = new OWLEquivalentObjectPropertiesAxiomImpl(
      List(
        df.getOWLObjectProperty(prefix + "prop1"),
        df.getOWLObjectProperty(prefix + "prop2"),
        df.getOWLObjectProperty(prefix + "prop3")
      ).asJavaCollection,
      List(
        df.getOWLAnnotation(
          df.getOWLAnnotationProperty(prefix + "comment"),
          df.getOWLLiteral("Required annotation")
        )
      ).asJavaCollection
    )
    assert(parsed.contains(expectedAxiom))

    clearParserPrefixes
  }

  test("The disjoint object properties parser should work correctly") {
    setupParserPrefixes

    val prefix = "http://ex.com/default#"
    var disjPropertiesStr =
      """DisjointProperties:
        |    Annotations:
        |        comment "Required annotation"
        |    prop1, prop2, prop3
      """.stripMargin
    var parsed = p.checkParsed(p.disjointObjectProperties, disjPropertiesStr)
    var expectedAxiom = new OWLDisjointObjectPropertiesAxiomImpl(
      List(
        df.getOWLObjectProperty(prefix + "prop1"),
        df.getOWLObjectProperty(prefix + "prop2"),
        df.getOWLObjectProperty(prefix + "prop3")
      ).asJavaCollection,
      List(
        df.getOWLAnnotation(
          df.getOWLAnnotationProperty(prefix + "comment"),
          df.getOWLLiteral("Required annotation")
        )
      ).asJavaCollection
    )
    assert(parsed.contains(expectedAxiom))

    clearParserPrefixes
  }

  test("The equivalent data properties parser should work correctly") {
    setupParserPrefixes

    val prefix = "http://ex.com/default#"
    var equivPropertiesStr =
      """EquivalentProperties:
        |    Annotations:
        |        comment "Required annotation"
        |    prop1, prop2, prop3
      """.stripMargin
    var parsed = p.checkParsed(p.equivalentDataProperties, equivPropertiesStr)
    var expectedAxiom = new OWLEquivalentDataPropertiesAxiomImpl(
      List(
        df.getOWLDataProperty(prefix + "prop1"),
        df.getOWLDataProperty(prefix + "prop2"),
        df.getOWLDataProperty(prefix + "prop3")
      ).asJavaCollection,
      List(
        df.getOWLAnnotation(
          df.getOWLAnnotationProperty(prefix + "comment"),
          df.getOWLLiteral("Required annotation")
        )
      ).asJavaCollection
    )
    assert(parsed.contains(expectedAxiom))

    clearParserPrefixes
  }

  test("The disjoint data properties parser should work correctly") {
    setupParserPrefixes

    val prefix = "http://ex.com/default#"
    var disjPropertiesStr =
      """DisjointProperties:
        |    Annotations:
        |        comment "Required annotation"
        |    prop1, prop2, prop3
      """.stripMargin
    var parsed = p.checkParsed(p.disjointDataProperties, disjPropertiesStr)
    var expectedAxiom = new OWLDisjointDataPropertiesAxiomImpl(
      List(
        df.getOWLDataProperty(prefix + "prop1"),
        df.getOWLDataProperty(prefix + "prop2"),
        df.getOWLDataProperty(prefix + "prop3")
      ).asJavaCollection,
      List(
        df.getOWLAnnotation(
          df.getOWLAnnotationProperty(prefix + "comment"),
          df.getOWLLiteral("Required annotation")
        )
      ).asJavaCollection
    )
    assert(parsed.contains(expectedAxiom))

    clearParserPrefixes
  }

  test("The same individual parser should work correctly") {
    setupParserPrefixes

    val prefix = "http://ex.com/default#"
    var sameIndivStr =
      """SameIndividual:
        |    Annotations:
        |        comment "Required annotation"
        |    indiv1, indiv2, indiv3
      """.stripMargin
    var parsed = p.checkParsed(p.sameIndividual, sameIndivStr)
    var expectedAxiom = new OWLSameIndividualAxiomImpl(
      List(
        df.getOWLNamedIndividual(prefix + "indiv1"),
        df.getOWLNamedIndividual(prefix + "indiv2"),
        df.getOWLNamedIndividual(prefix + "indiv3")
      ).asJavaCollection,
      List(
        df.getOWLAnnotation(
          df.getOWLAnnotationProperty(prefix + "comment"),
          df.getOWLLiteral("Required annotation")
        )
      ).asJavaCollection
    )
    assert(parsed.contains(expectedAxiom))

    clearParserPrefixes
  }

  test("The different individuals parser should work correctly") {
    setupParserPrefixes

    val prefix = "http://ex.com/default#"
    var differentIndivsStr =
      """DifferentIndividuals:
        |    Annotations:
        |        comment "Required annotation"
        |    indiv1, indiv2, indiv3
      """.stripMargin
    var parsed = p.checkParsed(p.differentIndividuals, differentIndivsStr)
    var expectedAxiom = new OWLDifferentIndividualsAxiomImpl(
      List(
        df.getOWLNamedIndividual(prefix + "indiv1"),
        df.getOWLNamedIndividual(prefix + "indiv2"),
        df.getOWLNamedIndividual(prefix + "indiv3")
      ).asJavaCollection,
      List(
        df.getOWLAnnotation(
          df.getOWLAnnotationProperty(prefix + "comment"),
          df.getOWLLiteral("Required annotation")
        )
      ).asJavaCollection
    )
    assert(parsed.contains(expectedAxiom))

    clearParserPrefixes
  }
}
