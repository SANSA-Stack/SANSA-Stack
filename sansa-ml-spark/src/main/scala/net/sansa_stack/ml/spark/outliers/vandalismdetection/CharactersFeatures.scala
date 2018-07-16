package net.sansa_stack.ml.spark.outliers.vandalismdetection

import org.apache.spark.ml.linalg.{ Vector, Vectors }
import java.util.regex.{ Pattern, Matcher }

class CharactersFeatures extends Serializable {

  def RoundDouble(va: Double): Double = {

    val rounded: Double = Math.round(va * 10000).toDouble / 10000

    rounded

  }

  def Vector_Characters_Feature(StrValue: String): Array[Double] = {

    // Reference for this :  http://www.mpi.nl/corpus/html/elan/apa.html
    var RatioValues = new Array[Double](25) //  Index is Important here
    val characterFeature_OBJ = new CharactersFeatures()

    //1.Double result Value for uppercase Ration
    val uppercase = characterFeature_OBJ.UppercaseRation_Character(StrValue)
    if (!uppercase.isNaN()) {
      RatioValues(0) = RoundDouble(uppercase)
    }
    //2.Double result Value for lowerCase Ratio
    val lowerCase = characterFeature_OBJ.LowercaseRation_Character(StrValue)
    if (!lowerCase.isNaN()) {
      RatioValues(1) = RoundDouble(lowerCase)
    }
    //3.Double result Value for  Alphanumeric Ratio
    val Alphanumeric = characterFeature_OBJ.AlphanumericRation_Character(StrValue)
    if (!Alphanumeric.isNaN()) {
      RatioValues(2) = RoundDouble(Alphanumeric)
    }
    //4.Double result Value for ASCII Ratio
    val ASCII = characterFeature_OBJ.ASCIIRation_Character(StrValue)
    if (!ASCII.isNaN()) {
      RatioValues(3) = RoundDouble(ASCII)
    }
    //5.Double result Value for Bracket Ratio
    val Bracket = characterFeature_OBJ.BracketRation_Character(StrValue)
    if (!Bracket.isNaN()) {
      RatioValues(4) = RoundDouble(Bracket)

    }
    //6.Double result Value for Digits Ratio
    val Digits = characterFeature_OBJ.DigitsRation_Character(StrValue)
    if (!Digits.isNaN()) {
      RatioValues(5) = RoundDouble(Digits)
    }
    //7.Double result Value for Latin Ratio
    val Latin = characterFeature_OBJ.Latin_Character(StrValue)
    if (!Latin.isNaN()) {
      RatioValues(6) = RoundDouble(Latin)
    }
    //8.Double result Value for WhiteSpace Ratio
    val WhiteSpace = characterFeature_OBJ.WhiteSpace_Character(StrValue)
    if (!WhiteSpace.isNaN()) {
      RatioValues(7) = RoundDouble(WhiteSpace)
    }
    //9.Double result Value for punc Ratio
    val punc = characterFeature_OBJ.Punct_Character(StrValue)
    if (!punc.isNaN()) {
      RatioValues(8) = RoundDouble(punc)
    }
    //10. Integer to Double result Value for LongCharacterSequence (1 integer)
    val LongCharacterSequence = characterFeature_OBJ.Longcharactersequence_Character(StrValue)
    if (!LongCharacterSequence.isNaN()) {
      RatioValues(9) = LongCharacterSequence
    }

    //11.Double result Value for ArabicCharacter
    val ArabicCharacter = characterFeature_OBJ.ArabicRation_Character(StrValue)
    if (!ArabicCharacter.isNaN()) {
      RatioValues(10) = RoundDouble(ArabicCharacter)
    }

    //12.Double result Value for Bengali
    val Bengali = characterFeature_OBJ.BengaliRation_Character(StrValue)
    if (!Bengali.isNaN()) {
      RatioValues(11) = RoundDouble(Bengali)

    }

    //13.Double result Value for Brahmi
    val Brahmi = characterFeature_OBJ.BrahmiRation_Character(StrValue)
    if (!Brahmi.isNaN()) {
      RatioValues(12) = RoundDouble(Brahmi)

    }

    //14.Double result Value for Cyrillic
    val Cyrillic = characterFeature_OBJ.CyrillicRation_Character(StrValue)
    if (!Cyrillic.isNaN()) {
      RatioValues(13) = RoundDouble(Cyrillic)

    }
    //15.Double result Value for Han
    val Han = characterFeature_OBJ.HanRatio_Character(StrValue)
    if (!Han.isNaN()) {
      RatioValues(14) = RoundDouble(Han)

    }

    //16.Double result Value for Malysia
    val Malysia = characterFeature_OBJ.MalaysRatio_Character(StrValue)
    if (!Malysia.isNaN()) {
      RatioValues(15) = RoundDouble(Malysia)
    }

    //17.Double result Value for Tami
    val Tami = characterFeature_OBJ.TamilRatio_Character(StrValue)
    if (!Tami.isNaN()) {
      RatioValues(16) = RoundDouble(Tami)
    }
    //18.Double result Value for Telugu
    val Telugu = characterFeature_OBJ.TeluguRatio_Character(StrValue)
    if (!Telugu.isNaN()) {
      RatioValues(17) = RoundDouble(Telugu)

    }
    //19.Double result Value for  Symbol
    val Symbol = characterFeature_OBJ.Symbol_Character(StrValue)
    if (!Symbol.isNaN()) {
      RatioValues(18) = RoundDouble(Symbol)

    }
    //20. Double Alphabets Ration:
    val Alphabets = characterFeature_OBJ.AlphaBetsRation_Character(StrValue)
    if (!Alphabets.isNaN()) {
      RatioValues(19) = RoundDouble(Alphabets)
    }
    //21. Double AVisible character Ratio:
    val Visible = characterFeature_OBJ.VisibleRation_Character(StrValue)
    if (!Visible.isNaN()) {
      RatioValues(20) = RoundDouble(Visible)
    }

    //22. Double Printable character Ratio:
    val Printable = characterFeature_OBJ.PrintableRation_Character(StrValue)
    if (!Printable.isNaN()) {
      RatioValues(21) = RoundDouble(Printable)
    }

    //23.Double Blank character Ratio:
    val Blank = characterFeature_OBJ.BlankRation_Character(StrValue)
    if (!Blank.isNaN()) {
      RatioValues(22) = RoundDouble(Blank)
    }

    //24.Double A control character:
    val Control = characterFeature_OBJ.ControlRation_Character(StrValue)
    if (!Control.isNaN()) {
      RatioValues(23) = RoundDouble(Control)
    }

    //25. Double A hexadecimal digit :
    val hexadecimal = characterFeature_OBJ.HexaRation_Character(StrValue)
    if (!hexadecimal.isNaN()) {
      RatioValues(24) = RoundDouble(hexadecimal)
    }
    //    val FacilityOBJ = new FacilitiesClass()
    //    val vector_Values = FacilityOBJ.ToVector(RatioValues)

    RatioValues
  }
  // Character Features: ------ start calculation the Ratio for character:
  def characterRatio(str: String, pattern: Pattern): Double = {
    var charRatio: Double = -1.0;
    if (str != null) {
      val tem: String = pattern.matcher(str).replaceAll("")
      val digits: Double = str.length() - tem.length()
      charRatio = digits / str.length().toDouble
    }
    charRatio
  }
  //1.Uppercase Ratio:
  def UppercaseRation_Character(str: String): Double = {
    val pattern: Pattern = Pattern.compile("\\p{javaUpperCase}")
    val result: Double = characterRatio(str, pattern)
    result
  }
  def LowercaseRation_Character(str: String): Double = {
    val pattern: Pattern = Pattern.compile("\\p{javaLowerCase}")
    val result: Double = characterRatio(str, pattern)
    result
  }
  //3.Alphanumeric
  def AlphanumericRation_Character(str: String): Double = {
    val pattern: Pattern = Pattern.compile("\\p{Alnum}")
    val result: Double = characterRatio(str, pattern)
    result
  }

  //4.ASCII
  def ASCIIRation_Character(str: String): Double = {
    val pattern: Pattern = Pattern.compile("\\p{ASCII}")
    val result: Double = characterRatio(str, pattern)
    result
  }
  //5.Bracket
  def BracketRation_Character(str: String): Double = {
    val pattern: Pattern = Pattern.compile("\\(|\\)|\\}|\\{|\\[|\\]")

    val result: Double = characterRatio(str, pattern)
    result
  }
  //6.Digits
  def DigitsRation_Character(str: String): Double = {
    val pattern: Pattern = Pattern.compile("\\d")
    val result: Double = characterRatio(str, pattern)
    result
  }
  //7.Latin
  def Latin_Character(str: String): Double = {
    val pattern: Pattern = Pattern.compile("\\p{IsLatin}")
    val result: Double = characterRatio(str, pattern)
    result
  }
  //8.WhiteSpace
  def WhiteSpace_Character(str: String): Double = {
    val pattern: Pattern = Pattern.compile("\\s")
    val result: Double = characterRatio(str, pattern)
    result
  }
  //9.Punct
  def Punct_Character(str: String): Double = {
    val pattern: Pattern = Pattern.compile("\\p{Punct}")
    val result: Double = characterRatio(str, pattern)
    result
  }
  //10.Long character sequence:
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

  //11.ARabic Ratio:
  def ArabicRation_Character(str: String): Double = {
    val pattern: Pattern = Pattern.compile("\\p{IsArabic}")
    val result: Double = characterRatio(str, pattern)
    result
  }
  //12. Bengali Ratio
  def BengaliRation_Character(str: String): Double = {
    val pattern: Pattern = Pattern.compile("\\p{IsBengali}")
    val result: Double = characterRatio(str, pattern)
    result
  }
  //13.Brahmi Ratio
  def BrahmiRation_Character(str: String): Double = {
    val pattern: Pattern = Pattern.compile("\\p{IsBrahmi}")
    val result: Double = characterRatio(str, pattern)
    result
  }
  //14.Cyrillic Ratio
  def CyrillicRation_Character(str: String): Double = {
    val pattern: Pattern = Pattern.compile("\\p{IsCyrillic}")
    val result: Double = characterRatio(str, pattern)
    result
  }
  //15.HanRatio
  def HanRatio_Character(str: String): Double = {
    val pattern: Pattern = Pattern.compile("\\p{IsHan}")
    val result: Double = characterRatio(str, pattern)
    result
  }

  //16.Malaysian Ratio:
  def MalaysRatio_Character(str: String): Double = {
    val pattern: Pattern = Pattern.compile("\\p{IsMalayalam}")
    val result: Double = characterRatio(str, pattern)
    result
  }
  //17.Tamil Ratio:
  def TamilRatio_Character(str: String): Double = {
    val pattern: Pattern = Pattern.compile("\\p{IsTamil}")
    val result: Double = characterRatio(str, pattern)
    result
  }
  //18.Telugu Ration:
  def TeluguRatio_Character(str: String): Double = {
    val pattern: Pattern = Pattern.compile("\\p{IsTelugu}")
    val result: Double = characterRatio(str, pattern)
    result
  }

  //19.Symbols Ratio :
  def Symbol_Character(str: String): Double = {
    val pattern: Pattern = Pattern.compile("[#$%&@+-_+*/]*")
    val result: Double = characterRatio(str, pattern)
    result
  }
  //20.Alphabets Ratio :
  def AlphaBetsRation_Character(str: String): Double = {
    val pattern: Pattern = Pattern.compile("\\p{Alpha}")
    val result: Double = characterRatio(str, pattern)
    result
  }
  //21.A visible character Ratio:
  def VisibleRation_Character(str: String): Double = {
    val pattern: Pattern = Pattern.compile("\\p{Graph}")
    val result: Double = characterRatio(str, pattern)
    result
  }
  //22.A printable character
  def PrintableRation_Character(str: String): Double = {
    val pattern: Pattern = Pattern.compile("\\p{Print}")
    val result: Double = characterRatio(str, pattern)
    result
  }

  //23.A Black(it is different from White space) character Ratio
  def BlankRation_Character(str: String): Double = {
    val pattern: Pattern = Pattern.compile("\\p{Blank}")
    val result: Double = characterRatio(str, pattern)
    result
  }

  //24.Control character  Ratio
  def ControlRation_Character(str: String): Double = {
    val pattern: Pattern = Pattern.compile("\\p{Cntrl}")
    val result: Double = characterRatio(str, pattern)
    result
  }

  //25.HexaDecimal character Ratio
  def HexaRation_Character(str: String): Double = {
    val pattern: Pattern = Pattern.compile("\\p{XDigit}")
    val result: Double = characterRatio(str, pattern)
    result
  }
  // Character features: ------ End calculation the Ratio for character:

}