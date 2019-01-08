package net.sansa_stack.ml.common.outliers.vandalismdetection.feature.extraction

import net.sansa_stack.ml.common.outliers.vandalismdetection.feature.extraction.FacilitiesClass._

object CharactersFeatures extends Serializable {

  def vectorCharactersFeature(StrValue: String): Array[Double] = {

    // Reference for this :  http://www.mpi.nl/corpus/html/elan/apa.html
    var ratioValues = new Array[Double](25) //  Index is Important here

    // 1.Double result Value for uppercase Ration
    val uppercase = UppercaseRation_Character(StrValue)
    if (!uppercase.isNaN()) {
      ratioValues(0) = roundDouble(uppercase)
    }
    // 2.Double result Value for lowerCase Ratio
    val lowerCase = LowercaseRation_Character(StrValue)
    if (!lowerCase.isNaN()) {
      ratioValues(1) = roundDouble(lowerCase)
    }
    // 3.Double result Value for  Alphanumeric Ratio
    val Alphanumeric = AlphanumericRation_Character(StrValue)
    if (!Alphanumeric.isNaN()) {
      ratioValues(2) = roundDouble(Alphanumeric)
    }
    // 4.Double result Value for ASCII Ratio
    val ASCII = ASCIIRation_Character(StrValue)
    if (!ASCII.isNaN()) {
      ratioValues(3) = roundDouble(ASCII)
    }
    // 5.Double result Value for Bracket Ratio
    val Bracket = BracketRation_Character(StrValue)
    if (!Bracket.isNaN()) {
      ratioValues(4) = roundDouble(Bracket)

    }
    // 6.Double result Value for Digits Ratio
    val Digits = DigitsRation_Character(StrValue)
    if (!Digits.isNaN()) {
      ratioValues(5) = roundDouble(Digits)
    }
    // 7.Double result Value for Latin Ratio
    val Latin = Latin_Character(StrValue)
    if (!Latin.isNaN()) {
      ratioValues(6) = roundDouble(Latin)
    }
    // 8.Double result Value for WhiteSpace Ratio
    val WhiteSpace = WhiteSpace_Character(StrValue)
    if (!WhiteSpace.isNaN()) {
      ratioValues(7) = roundDouble(WhiteSpace)
    }
    // 9.Double result Value for punc Ratio
    val punc = Punct_Character(StrValue)
    if (!punc.isNaN()) {
      ratioValues(8) = roundDouble(punc)
    }
    // 10. Integer to Double result Value for LongCharacterSequence (1 integer)
    val LongCharacterSequence = Longcharactersequence_Character(StrValue)
    if (!LongCharacterSequence.isNaN()) {
      ratioValues(9) = LongCharacterSequence
    }

    // 11.Double result Value for ArabicCharacter
    val ArabicCharacter = ArabicRation_Character(StrValue)
    if (!ArabicCharacter.isNaN()) {
      ratioValues(10) = roundDouble(ArabicCharacter)
    }

    // 12.Double result Value for Bengali
    val Bengali = BengaliRation_Character(StrValue)
    if (!Bengali.isNaN()) {
      ratioValues(11) = roundDouble(Bengali)

    }

    // 13.Double result Value for Brahmi
    val Brahmi = BrahmiRation_Character(StrValue)
    if (!Brahmi.isNaN()) {
      ratioValues(12) = roundDouble(Brahmi)
    }

    // 14.Double result Value for Cyrillic
    val Cyrillic = CyrillicRation_Character(StrValue)
    if (!Cyrillic.isNaN()) {
      ratioValues(13) = roundDouble(Cyrillic)
    }
    // 15.Double result Value for Han
    val Han = HanRatio_Character(StrValue)
    if (!Han.isNaN()) {
      ratioValues(14) = roundDouble(Han)
    }

    // 16.Double result Value for Malysia
    val Malysia = MalaysRatio_Character(StrValue)
    if (!Malysia.isNaN()) {
      ratioValues(15) = roundDouble(Malysia)
    }

    // 17.Double result Value for Tami
    val Tami = TamilRatio_Character(StrValue)
    if (!Tami.isNaN()) {
      ratioValues(16) = roundDouble(Tami)
    }
    // 18.Double result Value for Telugu
    val Telugu = TeluguRatio_Character(StrValue)
    if (!Telugu.isNaN()) {
      ratioValues(17) = roundDouble(Telugu)

    }
    // 19.Double result Value for  Symbol
    val Symbol = Symbol_Character(StrValue)
    if (!Symbol.isNaN()) {
      ratioValues(18) = roundDouble(Symbol)

    }
    // 20. Double Alphabets Ration:
    val Alphabets = AlphaBetsRation_Character(StrValue)
    if (!Alphabets.isNaN()) {
      ratioValues(19) = roundDouble(Alphabets)
    }
    // 21. Double AVisible character Ratio:
    val Visible = VisibleRation_Character(StrValue)
    if (!Visible.isNaN()) {
      ratioValues(20) = roundDouble(Visible)
    }

    // 22. Double Printable character Ratio:
    val Printable = PrintableRation_Character(StrValue)
    if (!Printable.isNaN()) {
      ratioValues(21) = roundDouble(Printable)
    }

    // 23.Double Blank character Ratio:
    val Blank = BlankRation_Character(StrValue)
    if (!Blank.isNaN()) {
      ratioValues(22) = roundDouble(Blank)
    }

    // 24.Double A control character:
    val Control = ControlRation_Character(StrValue)
    if (!Control.isNaN()) {
      ratioValues(23) = roundDouble(Control)
    }
    // 25. Double A hexadecimal digit :
    val hexadecimal = HexaRation_Character(StrValue)
    if (!hexadecimal.isNaN()) {
      ratioValues(24) = roundDouble(hexadecimal)
    }

    ratioValues
  }

  // 1.Uppercase Ratio:
  def UppercaseRation_Character(str: String): Double =
    extractCharacterRatio(str, "\\p{javaUpperCase}")

  def LowercaseRation_Character(str: String): Double =
    extractCharacterRatio(str, "\\p{javaLowerCase}")

  // 3.Alphanumeric
  def AlphanumericRation_Character(str: String): Double =
    extractCharacterRatio(str, "\\p{Alnum}")

  // 4.ASCII
  def ASCIIRation_Character(str: String): Double =
    extractCharacterRatio(str, "\\p{ASCII}")

  // 5.Bracket
  def BracketRation_Character(str: String): Double =
    extractCharacterRatio(str, "\\(|\\)|\\}|\\{|\\[|\\]")

  // 6.Digits
  def DigitsRation_Character(str: String): Double =
    extractCharacterRatio(str, "\\d")

  // 7.Latin
  def Latin_Character(str: String): Double =
    extractCharacterRatio(str, "\\p{IsLatin}")

  // 8.WhiteSpace
  def WhiteSpace_Character(str: String): Double =
    extractCharacterRatio(str, "\\s")

  // 9.Punct
  def Punct_Character(str: String): Double =
    extractCharacterRatio(str, "\\p{Punct}")

  // 10.Long character sequence:
  def Longcharactersequence_Character(str: String): Double = {
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
  def ArabicRation_Character(str: String): Double =
    extractCharacterRatio(str, "\\p{IsArabic}")

  // 12. Bengali Ratio
  def BengaliRation_Character(str: String): Double =
    extractCharacterRatio(str, "\\p{IsBengali}")

  // 13.Brahmi Ratio
  def BrahmiRation_Character(str: String): Double =
    extractCharacterRatio(str, "\\p{IsBrahmi}")

  // 14.Cyrillic Ratio
  def CyrillicRation_Character(str: String): Double =
    extractCharacterRatio(str, "\\p{IsCyrillic}")

  // 15.HanRatio
  def HanRatio_Character(str: String): Double =
    extractCharacterRatio(str, "\\p{IsHan}")

  // 16.Malaysian Ratio:
  def MalaysRatio_Character(str: String): Double =
    extractCharacterRatio(str, "\\p{IsMalayalam}")

  // 17.Tamil Ratio:
  def TamilRatio_Character(str: String): Double =
    extractCharacterRatio(str, "\\p{IsTamil}")

  // 18.Telugu Ration:
  def TeluguRatio_Character(str: String): Double =
    extractCharacterRatio(str, "\\p{IsTelugu}")

  // 19.Symbols Ratio :
  def Symbol_Character(str: String): Double =
    extractCharacterRatio(str, "[#$%&@+-_+*/]*")

  // 20.Alphabets Ratio :
  def AlphaBetsRation_Character(str: String): Double =
    extractCharacterRatio(str, "\\p{Alpha}")

  // 21.A visible character Ratio:
  def VisibleRation_Character(str: String): Double =
    extractCharacterRatio(str, "\\p{Graph}")

  // 22.A printable character
  def PrintableRation_Character(str: String): Double =
    extractCharacterRatio(str, "\\p{Print}")

  // 23.A Black(it is different from White space) character Ratio
  def BlankRation_Character(str: String): Double =
    extractCharacterRatio(str, "\\p{Blank}")

  // 24.Control character  Ratio
  def ControlRation_Character(str: String): Double =
    extractCharacterRatio(str, "\\p{Cntrl}")

  // 25.HexaDecimal character Ratio
  def HexaRation_Character(str: String): Double =
    extractCharacterRatio(str, "\\p{XDigit}")
}

