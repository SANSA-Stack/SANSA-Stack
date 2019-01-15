package net.sansa_stack.ml.common.outliers.vandalismdetection.feature.extraction

import org.scalatest.FunSuite

import net.sansa_stack.ml.common.outliers.vandalismdetection.feature.extraction.Character._

class CharacterTests extends FunSuite {

  val text = """{"type":"item","id":"Q15","labels":{"en":{"language":"en","value":"Africa"}},"descriptions":{"en":{"language":"en","value":"A continent"}},"aliases":[],"claims":[],"sitelinks":[]}"""

  test("upper case ratio should match") {
    val ratio = uppercaseRationCharacter(text)
    assert(ratio == 0.01675977653631285)
  }

  test("lower case ratio should match") {
    val ratio = lowercaseRationCharacter(text)
    assert(ratio == 0.547486033519553)
  }

  test("alpha numeric ratio should match") {
    val ratio = alphanumericRationCharacter(text)
    assert(ratio == 0.5754189944134078)
  }

  test("ASCII ratio should match") {
    val ratio = ASCIIRationCharacter(text)
    assert(ratio == 1.0)
  }

  test("bracket ratio should match") {
    val ratio = bracketRationCharacter(text)
    assert(ratio == 0.0893854748603352)
  }

  test("digits ratio should match") {
    val ratio = digitsRationCharacter(text)
    assert(ratio == 0.0111731843575419)
  }

  test("latin ratio should match") {
    val ratio = latinCharacter(text)
    assert(ratio == 0.5642458100558659)
  }

  test("white space ratio should match") {
    val ratio = whiteSpaceCharacter(text)
    assert(ratio == 0.00558659217877095)
  }

  test("punct ratio should match") {
    val ratio = punctCharacter(text)
    assert(ratio == 0.41899441340782123)
  }

  test("long character sequence ratio should match") {
    val ratio = longCharacterSequenceCharacter(text)
    assert(ratio == 2.0)
  }

  test("arabic ratio should match") {
    val ratio = arabicRationCharacter(text)
    assert(ratio == 0.0)
  }

  test("bengali ratio should match") {
    val ratio = bengaliRationCharacter(text)
    assert(ratio == 0.0)
  }

  test("brahmi ratio should match") {
    val ratio = brahmiRationCharacter(text)
    assert(ratio == 0.0)
  }

  test("cyrillic ratio should match") {
    val ratio = cyrillicRationCharacter(text)
    assert(ratio == 0.0)
  }

  test("han ratio should match") {
    val ratio = hanRatioCharacter(text)
    assert(ratio == 0.0)
  }

  test("malays ratio should match") {
    val ratio = malaysRatioCharacter(text)
    assert(ratio == 0.0)
  }

  test("tamil ratio should match") {
    val ratio = tamilRatioCharacter(text)
    assert(ratio == 0.0)
  }

  test("telugu ratio should match") {
    val ratio = teluguRatioCharacter(text)
    assert(ratio == 0.0)
  }

  test("symbol ratio should match") {
    val ratio = symbolCharacter(text)
    assert(ratio == 0.1787709497206704)
  }

  test("alphabets ratio should match") {
    val ratio = alphabetsRationCharacter(text)
    assert(ratio == 0.5642458100558659)
  }

  test("visible ratio should match") {
    val ratio = visibleRationCharacter(text)
    assert(ratio == 0.994413407821229)
  }

  test("printable ratio should match") {
    val ratio = printableRationCharacter(text)
    assert(ratio == 1.0)
  }

  test("blank ratio should match") {
    val ratio = blankRationCharacter(text)
    assert(ratio == 0.00558659217877095)
  }

  test("control ratio should match") {
    val ratio = controlRationCharacter(text)
    assert(ratio == 0.0)
  }

  test("hexa ratio should match") {
    val ratio = hexaRationCharacter(text)
    assert(ratio == 0.2122905027932961)
  }

  test("character ratio values should match") {
    val exeptedValues = Seq(0.0168, 0.5475, 0.5754,
      1.0, 0.0894, 0.0112, 0.5642, 0.0056, 0.419,
      2.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
      0.1788, 0.5642, 0.9944, 1.0, 0.0056, 0.0, 0.2123).toArray
    val values = characterFeatures(text)
    assert(values.sameElements(exeptedValues))
  }

}
