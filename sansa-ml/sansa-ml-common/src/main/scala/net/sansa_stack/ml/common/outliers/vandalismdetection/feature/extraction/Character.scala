package net.sansa_stack.ml.common.outliers.vandalismdetection.feature.extraction

import net.sansa_stack.ml.common.outliers.vandalismdetection.feature.Utils._

object Character extends Serializable {

  def characterFeatures(StrValue: String): Array[Double] = {

    // Reference for this :  http://www.mpi.nl/corpus/html/elan/apa.html
    var RatioValues = new Array[Double](25)

    // 1.Double result Value for uppercase Ration
    val uppercase = uppercaseRationCharacter(StrValue)
    if (!uppercase.isNaN()) {
      RatioValues(0) = roundDouble(uppercase)
    } else {
      RatioValues(0) = 0.0
    }
    // 2.Double result Value for lowerCase Ratio
    val lowerCase = lowercaseRationCharacter(StrValue)
    if (!lowerCase.isNaN()) {
      RatioValues(1) = roundDouble(lowerCase)
    } else {
      RatioValues(1) = 0.0

    }
    // 3.Double result Value for  Alphanumeric Ratio
    val Alphanumeric = alphanumericRationCharacter(StrValue)
    if (!Alphanumeric.isNaN()) {
      RatioValues(2) = roundDouble(Alphanumeric)
    } else {
      RatioValues(2) = 0.0
    }
    // 4.Double result Value for ASCII Ratio
    val ASCII = ASCIIRationCharacter(StrValue)
    if (!ASCII.isNaN()) {
      RatioValues(3) = roundDouble(ASCII)
    } else {
      RatioValues(3) = 0.0
    }
    // 5.Double result Value for Bracket Ratio
    val Bracket = bracketRationCharacter(StrValue)
    if (!Bracket.isNaN()) {
      RatioValues(4) = roundDouble(Bracket)

    } else {
      RatioValues(4) = 0.0
    }

    // 6.Double result Value for Digits Ratio
    val Digits = digitsRationCharacter(StrValue)
    if (!Digits.isNaN()) {
      RatioValues(5) = roundDouble(Digits)
    } else {
      RatioValues(5) = 0.0
    }
    // 7.Double result Value for Latin Ratio
    val Latin = latinCharacter(StrValue)
    if (!Latin.isNaN()) {
      RatioValues(6) = roundDouble(Latin)
    } else {

      RatioValues(6) = 0.0
    }
    // 8.Double result Value for WhiteSpace Ratio
    val WhiteSpace = whiteSpaceCharacter(StrValue)
    if (!WhiteSpace.isNaN()) {
      RatioValues(7) = roundDouble(WhiteSpace)
    } else {
      RatioValues(7) = 0.0
    }
    // 9.Double result Value for punc Ratio
    val punc = punctCharacter(StrValue)
    if (!punc.isNaN()) {
      RatioValues(8) = roundDouble(punc)
    } else {
      RatioValues(8) = 0.0
    }
    // 10. Integer to Double result Value for LongCharacterSequence (1 integer)
    val LongCharacterSequence = longCharacterSequenceCharacter(StrValue)
    if (!LongCharacterSequence.isNaN()) {
      RatioValues(9) = LongCharacterSequence
    } else {
      RatioValues(9) = 0.0
    }

    // 11.Double result Value for ArabicCharacter
    val ArabicCharacter = arabicRationCharacter(StrValue)
    if (!ArabicCharacter.isNaN()) {
      RatioValues(10) = roundDouble(ArabicCharacter)
    } else {
      RatioValues(10)
    }

    // 12.Double result Value for Bengali
    val Bengali = bengaliRationCharacter(StrValue)
    if (!Bengali.isNaN()) {
      RatioValues(11) = roundDouble(Bengali)

    } else {
      RatioValues(11) = 0.0
    }

    // 13.Double result Value for Brahmi
    val Brahmi = brahmiRationCharacter(StrValue)
    if (!Brahmi.isNaN()) {
      RatioValues(12) = roundDouble(Brahmi)

    } else {
      RatioValues(12) = 0.0
    }

    // 14.Double result Value for Cyrillic
    val Cyrillic = cyrillicRationCharacter(StrValue)
    if (!Cyrillic.isNaN()) {
      RatioValues(13) = roundDouble(Cyrillic)

    } else {
      RatioValues(13) = 0.0
    }
    // 15.Double result Value for Han
    val Han = hanRatioCharacter(StrValue)
    if (!Han.isNaN()) {
      RatioValues(14) = roundDouble(Han)

    } else {
      RatioValues(14) = 0.0
    }

    // 16.Double result Value for Malysia
    val Malysia = malaysRatioCharacter(StrValue)
    if (!Malysia.isNaN()) {
      RatioValues(15) = roundDouble(Malysia)
    } else {
      RatioValues(15) = 0.0
    }

    // 17.Double result Value for Tami
    val Tami = tamilRatioCharacter(StrValue)
    if (!Tami.isNaN()) {
      RatioValues(16) = roundDouble(Tami)
    } else {
      RatioValues(16) = 0.0
    }
    // 18.Double result Value for Telugu
    val Telugu = teluguRatioCharacter(StrValue)
    if (!Telugu.isNaN()) {
      RatioValues(17) = roundDouble(Telugu)

    } else {
      RatioValues(17) = 0.0
    }
    // 19.Double result Value for  Symbol
    val Symbol = symbolCharacter(StrValue)
    if (!Symbol.isNaN()) {
      RatioValues(18) = roundDouble(Symbol)

    } else {
      RatioValues(18) = 0.0
    }
    // 20. Double Alphabets Ration:
    val Alphabets = alphabetsRationCharacter(StrValue)
    if (!Alphabets.isNaN()) {
      RatioValues(19) = roundDouble(Alphabets)
    } else {
      RatioValues(19) = 0.0
    }
    // 21. Double AVisible character Ratio:
    val Visible = visibleRationCharacter(StrValue)
    if (!Visible.isNaN()) {
      RatioValues(20) = roundDouble(Visible)
    } else {

      RatioValues(20) = 0.0
    }

    // 22. Double Printable character Ratio:
    val Printable = printableRationCharacter(StrValue)
    if (!Printable.isNaN()) {
      RatioValues(21) = roundDouble(Printable)
    } else {
      RatioValues(21) = 0.0
    }

    // 23.Double Blank character Ratio:
    val Blank = blankRationCharacter(StrValue)
    if (!Blank.isNaN()) {
      RatioValues(22) = roundDouble(Blank)
    } else {
      RatioValues(22) = 0.0

    }

    // 24.Double A control character:
    val Control = controlRationCharacter(StrValue)
    if (!Control.isNaN()) {
      RatioValues(23) = roundDouble(Control)
    } else {
      RatioValues(23) = 0.0
    }

    // 25. Double A hexadecimal digit :
    val hexadecimal = hexaRationCharacter(StrValue)
    if (!hexadecimal.isNaN()) {
      RatioValues(24) = roundDouble(hexadecimal)
    } else {
      RatioValues(24) = 0.0
    }

    RatioValues
  }

  // 1.Uppercase Ratio:
  def uppercaseRationCharacter(str: String): Double =
    extractCharacterRatio(str, "\\p{javaUpperCase}")

  def lowercaseRationCharacter(str: String): Double =
    extractCharacterRatio(str, "\\p{javaLowerCase}")

  // 3.Alphanumeric
  def alphanumericRationCharacter(str: String): Double =
    extractCharacterRatio(str, "\\p{Alnum}")

  // 4.ASCII
  def ASCIIRationCharacter(str: String): Double =
    extractCharacterRatio(str, "\\p{ASCII}")

  // 5.Bracket
  def bracketRationCharacter(str: String): Double =
    extractCharacterRatio(str, "\\(|\\)|\\}|\\{|\\[|\\]")

  // 6.Digits
  def digitsRationCharacter(str: String): Double =
    extractCharacterRatio(str, "\\d")

  // 7.Latin
  def latinCharacter(str: String): Double =
    extractCharacterRatio(str, "\\p{IsLatin}")

  // 8.WhiteSpace
  def whiteSpaceCharacter(str: String): Double =
    extractCharacterRatio(str, "\\s")

  // 9.Punct
  def punctCharacter(str: String): Double =
    extractCharacterRatio(str, "\\p{Punct}")

  // 10.Long character sequence:
  def longCharacterSequenceCharacter(str: String): Double = {
    var text: String = str
    var maxlength: Integer = null
    if (str != null) {
      maxlength = 0
      var prevCharacter: Char = 'a'
      var prevPosision = 0
      text = text.trim()
      var i: Integer = 0
      for (i <- 0 until text.length) {
        val Currcharacter: Char = text.charAt(i)
        if (i > 0 && prevCharacter != Currcharacter) {

          if (i - prevPosision > maxlength) {
            maxlength = i - prevPosision
          }
          prevPosision = i
        }
        prevCharacter = Currcharacter
      }

      if (i > 0) {
        if (i - prevPosision > maxlength) {
          maxlength = prevPosision
        }
      }
    }

    val result: Double = maxlength.toDouble
    result

  }

  // 11.ARabic Ratio:
  def arabicRationCharacter(str: String): Double =
    extractCharacterRatio(str, "\\p{IsArabic}")

  // 12. Bengali Ratio
  def bengaliRationCharacter(str: String): Double =
    extractCharacterRatio(str, "\\p{IsBengali}")

  // 13.Brahmi Ratio
  def brahmiRationCharacter(str: String): Double =
    extractCharacterRatio(str, "\\p{IsBrahmi}")

  // 14.Cyrillic Ratio
  def cyrillicRationCharacter(str: String): Double =
    extractCharacterRatio(str, "\\p{IsCyrillic}")

  // 15.HanRatio
  def hanRatioCharacter(str: String): Double =
    extractCharacterRatio(str, "\\p{IsHan}")

  // 16.Malaysian Ratio:
  def malaysRatioCharacter(str: String): Double =
    extractCharacterRatio(str, "\\p{IsMalayalam}")

  // 17.Tamil Ratio:
  def tamilRatioCharacter(str: String): Double =
    extractCharacterRatio(str, "\\p{IsTamil}")

  // 18.Telugu Ration:
  def teluguRatioCharacter(str: String): Double =
    extractCharacterRatio(str, "\\p{IsTelugu}")

  // 19.Symbols Ratio :
  def symbolCharacter(str: String): Double =
    extractCharacterRatio(str, "[#$%&@+-_+*/]*")

  // 20.Alphabets Ratio :
  def alphabetsRationCharacter(str: String): Double =
    extractCharacterRatio(str, "\\p{Alpha}")

  // 21.A visible character Ratio:
  def visibleRationCharacter(str: String): Double =
    extractCharacterRatio(str, "\\p{Graph}")

  // 22.A printable character
  def printableRationCharacter(str: String): Double =
    extractCharacterRatio(str, "\\p{Print}")

  // 23.A Black(it is different from White space) character Ratio
  def blankRationCharacter(str: String): Double =
    extractCharacterRatio(str, "\\p{Blank}")

  // 24.Control character  Ratio
  def controlRationCharacter(str: String): Double =
    extractCharacterRatio(str, "\\p{Cntrl}")

  // 25.HexaDecimal character Ratio
  def hexaRationCharacter(str: String): Double =
    extractCharacterRatio(str, "\\p{XDigit}")
}
