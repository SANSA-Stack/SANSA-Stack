package net.sansa_stack.ml.spark.outliers.vandalismdetection

import Array._
import java.util.{Arrays, ArrayList, Collections, HashMap}
import java.util.regex.{Matcher, Pattern}
import java.util.List
import com.google.common.base.Splitter
import java.io.{BufferedReader, ByteArrayInputStream, FileReader}
import org.apache.jena.graph.Triple
import org.apache.jena.rdf.model.ModelFactory
import org.apache.commons.lang3.StringUtils

object VandalismDetection extends Serializable {

//Build Vector Features extraction Values:
  def build_VectorFeature(TRP: String): String = {

    var trp: Array[String] = TRP.split("::", 3)
    var OB = trp(2)
    var StrValue = OB
    var FeatureValues = new ArrayList[Double]()
    val x = -1.0
    val xx = ""
    var VectorString =TRP
    VectorString = VectorString + "::["
    // Features for character:
    //1.
    val uppercase = UppercaseRation_Character(StrValue)
    if (!uppercase.isNaN()) {
      FeatureValues.add(uppercase)
      VectorString = VectorString + uppercase.toString() + ","
    } else {
      FeatureValues.add(x)

      VectorString = VectorString + xx + ","
    }
    //2.
    val lowerCase = LowercaseRation_Character(StrValue)
    if (!lowerCase.isNaN()) {
      FeatureValues.add(lowerCase)
      VectorString = VectorString + lowerCase.toString() + ","

    } else {
      FeatureValues.add(x)
      VectorString = VectorString + xx + ","

    }
    //3.
    val Alphanumeric = AlphanumericRation_Character(StrValue)
    if (!Alphanumeric.isNaN()) {
      FeatureValues.add(Alphanumeric)
      VectorString = VectorString + Alphanumeric.toString() + ","

    } else {
      FeatureValues.add(x)
      VectorString = VectorString + xx + ","

    }

    //4.
    val ASCII = ASCIIRation_Character(StrValue)
    if (!ASCII.isNaN()) {
      FeatureValues.add(ASCII)
      VectorString = VectorString + ASCII.toString() + ","

    } else {
      FeatureValues.add(x)
      VectorString = VectorString + xx + ","

    }
    //5.
    val Bracket = BracketRation_Character(StrValue)
    if (!Bracket.isNaN()) {
      FeatureValues.add(Bracket)
      VectorString = VectorString + Bracket.toString() + ","

    } else {
      FeatureValues.add(x)
      VectorString = VectorString + xx + ","

    }

    //6.
    val Digits = DigitsRation_Character(StrValue)
    if (!Digits.isNaN()) {
      FeatureValues.add(Digits)
      VectorString = VectorString + Digits.toString() + ","

    } else {
      FeatureValues.add(x)
      VectorString = VectorString + xx + ","

    }

    //7.
    val Latin = Latin_Character(StrValue)
    if (!Latin.isNaN()) {
      FeatureValues.add(Latin)
      VectorString = VectorString + Latin.toString() + ","

    } else {
      FeatureValues.add(x)
      VectorString = VectorString + xx + ","

    }

    //8.

    val WhiteSpace = WhiteSpace_Character(StrValue)
    if (!WhiteSpace.isNaN()) {
      FeatureValues.add(WhiteSpace)
      VectorString = VectorString + WhiteSpace.toString() + ","

    } else {
      FeatureValues.add(x)
      VectorString = VectorString + xx + ","

    }
    //9.
    val punc = Punct_Character(StrValue)
    if (!punc.isNaN()) {
      FeatureValues.add(punc)
      VectorString = VectorString + punc.toString() + ","

    } else {
      FeatureValues.add(x)
      VectorString = VectorString + xx + ","

    }
    //10.
    val LongCharacterSequence = Longcharactersequence_Character(StrValue)
    if (!LongCharacterSequence.isNaN()) {
      FeatureValues.add(LongCharacterSequence)
      VectorString = VectorString + LongCharacterSequence.toString() + ","

    } else {
      FeatureValues.add(x)
      VectorString = VectorString + xx + ","

    }

    //11.
    val ArabicCharacter = ArabicRation_Character(StrValue)
    if (!ArabicCharacter.isNaN()) {
      FeatureValues.add(ArabicCharacter)
      VectorString = VectorString + ArabicCharacter.toString() + ","

    } else {
      FeatureValues.add(x)
      VectorString = VectorString + xx + ","

    }

    //12.
    val Bengali = BengaliRation_Character(StrValue)
    if (!Bengali.isNaN()) {
      FeatureValues.add(Bengali)
      VectorString = VectorString + Bengali.toString() + ","

    } else {
      FeatureValues.add(x)
      VectorString = VectorString + xx + ","

    }

    //13.
    val Brahmi = BrahmiRation_Character(StrValue)
    if (!Brahmi.isNaN()) {
      FeatureValues.add(Brahmi)
      VectorString = VectorString + Brahmi.toString() + ","

    } else {
      FeatureValues.add(x)
      VectorString = VectorString + xx + ","

    }

    //14.
    val Cyrillic = CyrillicRation_Character(StrValue)
    if (!Cyrillic.isNaN()) {
      FeatureValues.add(Cyrillic)
      VectorString = VectorString + Cyrillic.toString() + ","

    } else {
      FeatureValues.add(x)
      VectorString = VectorString + xx + ","

    }
    //15.
    val Han = HanRatio_Character(StrValue)
    if (!Han.isNaN()) {
      FeatureValues.add(Han)
      VectorString = VectorString + Han.toString() + ","

    } else {
      FeatureValues.add(x)
      VectorString = VectorString + xx + ","

    }

    //16.
    val Malysia = MalaysRatio_Character(StrValue)
    if (Malysia.isNaN()) {
      FeatureValues.add(Malysia)
      VectorString = VectorString + Malysia.toString() + ","

    } else {
      FeatureValues.add(x)
      VectorString = VectorString + xx + ","

    }

    //17.
    val Tami = TamilRatio_Character(StrValue)
    if (!Tami.isNaN()) {
      FeatureValues.add(Tami)
      VectorString = VectorString + Tami.toString() + ","

    } else {
      FeatureValues.add(x)
      VectorString = VectorString + xx + ","

    }

    //18.
    val Telugu = TeluguRatio_Character(StrValue)
    if (!Telugu.isNaN()) {
      FeatureValues.add(Telugu)
      VectorString = VectorString + Telugu.toString() + ","

    } else {
      FeatureValues.add(x)
      VectorString = VectorString + xx + ","

    }
    //19.==1. WOrd Feature
    val LanguageWord = LanguageWordRatio_Character(StrValue)
    if (!LanguageWord.isNaN()) {
      FeatureValues.add(LanguageWord)
      VectorString = VectorString + LanguageWord.toString() + ","

    } else {
      FeatureValues.add(x)
      VectorString = VectorString + xx + ","

    }

    //20.2  Boolean case:

    //21.3
    val UpperCaseWord = UppercaseWordRation(StrValue)
    if (!UpperCaseWord.isNaN()) {
      FeatureValues.add(UpperCaseWord)
      VectorString = VectorString + UpperCaseWord.toString() + ","

    } else {
      FeatureValues.add(x)
      VectorString = VectorString + xx + ","

    }

    //22.4
    val LowerCaseWord = LowercaseWordRation(StrValue)
    if (!LowerCaseWord.isNaN()) {
      FeatureValues.add(LowerCaseWord)
      VectorString = VectorString + LowerCaseWord.toString() + ","

    } else {
      FeatureValues.add(x)
      VectorString = VectorString + xx + ","

    }

    //23.5 Boolean Case

    //24.6  Integer Case

    val LongWord = LongestWord(StrValue)
    if (LongWord != null) {
      val castedValue = LongWord.toDouble
      FeatureValues.add(castedValue)
      VectorString = VectorString + castedValue.toString() + ","

    } else {
      FeatureValues.add(x)
      VectorString = VectorString + xx + ","

    }

    //25.7
    val BadWord = BadWordRation(StrValue)
    if (!BadWord.isNaN()) {
      FeatureValues.add(BadWord)
      VectorString = VectorString + BadWord.toString() + ","

    } else {
      FeatureValues.add(x)
      VectorString = VectorString + xx + ","

    }

    //26.8  Bad word Boolean case

    //27.9 Ban word Boolean case

    //28.10 Ban word
    val BanWord = BanWordRation(StrValue)
    if (!BanWord.isNaN()) {
      FeatureValues.add(BanWord)
      VectorString = VectorString + BanWord.toString() + ","

    } else {
      FeatureValues.add(x)
      VectorString = VectorString + xx + ","

    }

    //29.11  Contain language word  Boolean Case

    //30.12 Male Name Word  Boolean Case

    //31.13 Female Name WOrd  Boolean Case

    var Size = FeatureValues.size().toInt
    var Result_Feature_Value_Vector = Array.ofDim[Double](Size)

    for (a <- 0 until Size - 1) {
      Result_Feature_Value_Vector +:= FeatureValues.get(a).toDouble
    }
    VectorString + "]"
  }

  // This function for RDFXML case.
  def arrayListTOstring(Arraylistval: ArrayList[Triple]): String = {
    val str = Arraylistval.get(0).toString()
    str
  }
  // This function for RDFXML case.
  def RecordParse(record: String, prefixes: String): ArrayList[Triple] = {
    var triples = new ArrayList[Triple]()
    var model = ModelFactory.createDefaultModel();
    val modelText = "<?xml version=\"1.0\" encoding=\"utf-8\" ?> \n" + prefixes + record + "</rdf:RDF>"
    model.read(new ByteArrayInputStream(modelText.getBytes()), "http://example.org");
    val iter = model.listStatements()
    while (iter.hasNext()) {
      var triple = iter.next().asTriple()
      triples.add(triple)
    }
    triples
  }
  // This function for RDFXML case.
  def prefixParse(line: String): ArrayList[String] = {
    var temp = line
    val prefixRegex = Pattern.compile("xmlns\\s*:\\s*[a-zA-Z][a-zA-Z0-9_]*\\s*=\\s*([\"'])(?:(?=(\\\\?))\\2.)*?\\1")
    var matcher = prefixRegex.matcher(temp)
    var vars = new ArrayList[String]()
    while (matcher.find()) {
      var x = temp.substring(matcher.start(), matcher.end())
      vars.add(x.toString())
      temp = temp.substring(0, matcher.start()) + temp.substring(matcher.end(), temp.length())
      matcher = prefixRegex.matcher(temp)
    }
    vars
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
  //2.lower Ratio:
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
    val pattern: Pattern = Pattern.compile("\\(|\\)")
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

  // Character MISC:
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
  //14.Cyrillic
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

  //16.MalaysianRatio:
  def MalaysRatio_Character(str: String): Double = {
    val pattern: Pattern = Pattern.compile("\\p{IsMalayalam}")
    val result: Double = characterRatio(str, pattern)
    result
  }
  //17.TamilRAtio:
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
  // Character features: ------ End calculation the Ratio for character:

  // Words features: ------ start calculation the Ratio for Words:
  // Word Features
  def WordRatio(str: String, pattern: Pattern): Double = {

    val pattern_splitting: Pattern = Pattern.compile("\\s+")
    var wordRatio: Double = -1.0;
    val words: Array[String] = pattern_splitting.split(str.trim())
    if (words.length > 0) {
      wordRatio = 0;
    }
    val matcher: Matcher = pattern.matcher("")
    for (word <- words) {
      var strword = word.trim()
      if (!strword.equals("") && matcher.reset(strword).matches()) {
        wordRatio = wordRatio + 1
      }
    }
    wordRatio = wordRatio / words.length
    wordRatio

  }
  //1.Language Words Ratio :
  val regex_LanguageWordRatio: String = "(a(frikaa?ns|lbanian?|lemanha|ng(lais|ol)|ra?b(e?|[ei]c|ian?|isc?h)|rmenian?|ssamese|azeri|z[e\\u0259]rba(ijani?|ycan(ca)?|yjan)|\\u043d\\u0433\\u043b\\u0438\\u0439\\u0441\\u043a\\u0438\\u0439)|b(ahasa( (indonesia|jawa|malaysia|melayu))?|angla|as(k|qu)e|[aeo]ng[ao]?li|elarusian?|okm\\u00e5l|osanski|ra[sz]il(ian?)?|ritish( kannada)?|ulgarian?)|c(ebuano|hina|hinese( simplified)?|zech|roat([eo]|ian?)|atal[a\\u00e0]n?|\\u0440\\u043f\\u0441\\u043a\\u0438|antonese)|[c\\u010d](esky|e[s\\u0161]tina)\r\n|d(an(isc?h|sk)|e?uts?ch)|e(esti|ll[hi]nika|ng(els|le(ski|za)|lisc?h)|spa(g?[n\\u00f1]h?i?ol|nisc?h)|speranto|stonian|usk[ae]ra)|f(ilipino|innish|ran[c\\u00e7](ais|e|ez[ao])|ren[cs]h|arsi|rancese)|g(al(ego|ician)|uja?rati|ree(ce|k)|eorgian|erman[ay]?|ilaki)|h(ayeren|ebrew|indi|rvatski|ungar(y|ian))|i(celandic|ndian?|ndonesian?|ngl[e\\u00ea]se?|ngilizce|tali(ano?|en(isch)?))|ja(pan(ese)?|vanese)|k(a(nn?ada|zakh)|hmer|o(rean?|sova)|urd[i\\u00ee])|l(at(in[ao]?|vi(an?|e[s\\u0161]u))|ietuvi[u\\u0173]|ithuanian?)|m(a[ck]edon(ian?|ski)|agyar|alay(alam?|sian?)?|altese|andarin|arathi|elayu|ontenegro|ongol(ian?)|yanmar)|n(e(d|th)erlands?|epali|orw(ay|egian)|orsk( bokm[a\\u00e5]l)?|ynorsk)|o(landese|dia)|p(ashto|ersi?an?|ol(n?isc?h|ski)|or?tugu?[e\\u00ea]se?(( d[eo])? brasil(eiro)?| ?\\(brasil\\))?|unjabi)|r(om[a\\u00e2i]ni?[a\\u0103]n?|um(ano|\\u00e4nisch)|ussi([ao]n?|sch))|s(anskrit|erbian|imple english|inha?la|lov(ak(ian?)?|en\\u0161?[c\\u010d]ina|en(e|ij?an?)|uomi)|erbisch|pagnolo?|panisc?h|rbeska|rpski|venska|c?wedisc?h|hqip)|t(a(galog|mil)|elugu|hai(land)?|i[e\\u1ebf]ng vi[e\\u1ec7]t|[u\\u00fc]rk([c\\u00e7]e|isc?h|i\\u015f|ey))|u(rdu|zbek)|v(alencia(no?)?|ietnamese)|welsh|(\\u0430\\u043d\\u0433\\u043b\\u0438\\u0438\\u0441|[k\\u043a]\\u0430\\u043b\\u043c\\u044b\\u043a\\u0441|[k\\u043a]\\u0430\\u0437\\u0430\\u0445\\u0441|\\u043d\\u0435\\u043c\\u0435\\u0446|[p\\u0440]\\u0443\\u0441\\u0441|[y\\u0443]\\u0437\\u0431\\u0435\\u043a\\u0441)\\u043a\\u0438\\u0439( \\u044f\\u0437\\u044b\\u043a)??|\\u05e2\\u05d1\\u05e8\\u05d9\\u05ea|[k\\u043a\\u049b](\\u0430\\u0437\\u0430[\\u043a\\u049b]\\u0448\\u0430|\\u044b\\u0440\\u0433\\u044b\\u0437\\u0447\\u0430|\\u0438\\u0440\\u0438\\u043b\\u043b)|\\u0443\\u043a\\u0440\\u0430\\u0457\\u043d\\u0441\\u044c\\u043a(\\u0430|\\u043e\\u044e)|\\u0431(\\u0435\\u043b\\u0430\\u0440\\u0443\\u0441\\u043a\\u0430\\u044f|\\u044a\\u043b\\u0433\\u0430\\u0440\\u0441\\u043a\\u0438( \\u0435\\u0437\\u0438\\u043a)?)|\\u03b5\\u03bb\\u03bb[\\u03b7\\u03b9]\\u03bd\\u03b9\\u03ba(\\u03ac|\\u03b1)|\\u10e5\\u10d0\\u10e0\\u10d7\\u10e3\\u10da\\u10d8|\\u0939\\u093f\\u0928\\u094d\\u0926\\u0940|\\u0e44\\u0e17\\u0e22|[m\\u043c]\\u043e\\u043d\\u0433\\u043e\\u043b(\\u0438\\u0430)?|([c\\u0441]\\u0440\\u043f|[m\\u043c]\\u0430\\u043a\\u0435\\u0434\\u043e\\u043d)\\u0441\\u043a\\u0438|\\u0627\\u0644\\u0639\\u0631\\u0628\\u064a\\u0629|\\u65e5\\u672c\\u8a9e|\\ud55c\\uad6d(\\ub9d0|\\uc5b4)|\\u200c\\u0939\\u093f\\u0928\\u0926\\u093c\\u093f|\\u09ac\\u09be\\u0982\\u09b2\\u09be|\\u0a2a\\u0a70\\u0a1c\\u0a3e\\u0a2c\\u0a40|\\u092e\\u0930\\u093e\\u0920\\u0940|\\u0c95\\u0ca8\\u0ccd\\u0ca8\\u0ca1|\\u0627\\u064f\\u0631\\u062f\\u064f\\u0648|\\u0ba4\\u0bae\\u0bbf\\u0bb4\\u0bcd|\\u0c24\\u0c46\\u0c32\\u0c41\\u0c17\\u0c41|\\u0a97\\u0ac1\\u0a9c\\u0ab0\\u0abe\\u0aa4\\u0ac0|\\u0641\\u0627\\u0631\\u0633\\u06cc|\\u067e\\u0627\\u0631\\u0633\\u06cc|\\u0d2e\\u0d32\\u0d2f\\u0d3e\\u0d33\\u0d02|\\u067e\\u069a\\u062a\\u0648|\\u1019\\u103c\\u1014\\u103a\\u1019\\u102c\\u1018\\u102c\\u101e\\u102c|\\u4e2d\\u6587(\\u7b80\\u4f53|\\u7e41\\u9ad4)?|\\u4e2d\\u6587\\uff08(\\u7b80\\u4f53?|\\u7e41\\u9ad4)\\uff09|\\u7b80\\u4f53|\\u7e41\\u9ad4)";
  val pattern_LanguageWordRatio: Pattern = Pattern.compile(regex_LanguageWordRatio);
  def LanguageWordRatio_Character(str: String): Double = {
    val result: Double = WordRatio(str, pattern_LanguageWordRatio)
    result
  }

  //2. Contain language word :
  val regex_ContainLanguageWord: String = "(^|\\n)([ei]n )??(a(frikaa?ns|lbanian?|lemanha|ng(lais|ol)|ra?b(e?|[ei]c|ian?|isc?h)|rmenian?|ssamese|azeri|z[e\\u0259]rba(ijani?|ycan(ca)?|yjan)|\\u043d\\u0433\\u043b\\u0438\\u0439\\u0441\\u043a\\u0438\\u0439)|b(ahasa( (indonesia|jawa|malaysia|melayu))?|angla|as(k|qu)e|[aeo]ng[ao]?li|elarusian?|okm\\u00e5l|osanski|ra[sz]il(ian?)?|ritish( kannada)?|ulgarian?)|c(ebuano|hina|hinese( simplified)?|zech|roat([eo]|ian?)|atal[a\\u00e0]n?|\\u0440\\u043f\\u0441\\u043a\\u0438|antonese)|[c\\u010d](esky|e[s\\u0161]tina)\r\n|d(an(isc?h|sk)|e?uts?ch)|e(esti|ll[hi]nika|ng(els|le(ski|za)|lisc?h)|spa(g?[n\\u00f1]h?i?ol|nisc?h)|speranto|stonian|usk[ae]ra)|f(ilipino|innish|ran[c\\u00e7](ais|e|ez[ao])|ren[cs]h|arsi|rancese)|g(al(ego|ician)|uja?rati|ree(ce|k)|eorgian|erman[ay]?|ilaki)|h(ayeren|ebrew|indi|rvatski|ungar(y|ian))|i(celandic|ndian?|ndonesian?|ngl[e\\u00ea]se?|ngilizce|tali(ano?|en(isch)?))|ja(pan(ese)?|vanese)|k(a(nn?ada|zakh)|hmer|o(rean?|sova)|urd[i\\u00ee])|l(at(in[ao]?|vi(an?|e[s\\u0161]u))|ietuvi[u\\u0173]|ithuanian?)|m(a[ck]edon(ian?|ski)|agyar|alay(alam?|sian?)?|altese|andarin|arathi|elayu|ontenegro|ongol(ian?)|yanmar)|n(e(d|th)erlands?|epali|orw(ay|egian)|orsk( bokm[a\\u00e5]l)?|ynorsk)|o(landese|dia)|p(ashto|ersi?an?|ol(n?isc?h|ski)|or?tugu?[e\\u00ea]se?(( d[eo])? brasil(eiro)?| ?\\(brasil\\))?|unjabi)|r(om[a\\u00e2i]ni?[a\\u0103]n?|um(ano|\\u00e4nisch)|ussi([ao]n?|sch))|s(anskrit|erbian|imple english|inha?la|lov(ak(ian?)?|en\\u0161?[c\\u010d]ina|en(e|ij?an?)|uomi)|erbisch|pagnolo?|panisc?h|rbeska|rpski|venska|c?wedisc?h|hqip)|t(a(galog|mil)|elugu|hai(land)?|i[e\\u1ebf]ng vi[e\\u1ec7]t|[u\\u00fc]rk([c\\u00e7]e|isc?h|i\\u015f|ey))|u(rdu|zbek)|v(alencia(no?)?|ietnamese)|welsh|(\\u0430\\u043d\\u0433\\u043b\\u0438\\u0438\\u0441|[k\\u043a]\\u0430\\u043b\\u043c\\u044b\\u043a\\u0441|[k\\u043a]\\u0430\\u0437\\u0430\\u0445\\u0441|\\u043d\\u0435\\u043c\\u0435\\u0446|[p\\u0440]\\u0443\\u0441\\u0441|[y\\u0443]\\u0437\\u0431\\u0435\\u043a\\u0441)\\u043a\\u0438\\u0439( \\u044f\\u0437\\u044b\\u043a)??|\\u05e2\\u05d1\\u05e8\\u05d9\\u05ea|[k\\u043a\\u049b](\\u0430\\u0437\\u0430[\\u043a\\u049b]\\u0448\\u0430|\\u044b\\u0440\\u0433\\u044b\\u0437\\u0447\\u0430|\\u0438\\u0440\\u0438\\u043b\\u043b)|\\u0443\\u043a\\u0440\\u0430\\u0457\\u043d\\u0441\\u044c\\u043a(\\u0430|\\u043e\\u044e)|\\u0431(\\u0435\\u043b\\u0430\\u0440\\u0443\\u0441\\u043a\\u0430\\u044f|\\u044a\\u043b\\u0433\\u0430\\u0440\\u0441\\u043a\\u0438( \\u0435\\u0437\\u0438\\u043a)?)|\\u03b5\\u03bb\\u03bb[\\u03b7\\u03b9]\\u03bd\\u03b9\\u03ba(\\u03ac|\\u03b1)|\\u10e5\\u10d0\\u10e0\\u10d7\\u10e3\\u10da\\u10d8|\\u0939\\u093f\\u0928\\u094d\\u0926\\u0940|\\u0e44\\u0e17\\u0e22|[m\\u043c]\\u043e\\u043d\\u0433\\u043e\\u043b(\\u0438\\u0430)?|([c\\u0441]\\u0440\\u043f|[m\\u043c]\\u0430\\u043a\\u0435\\u0434\\u043e\\u043d)\\u0441\\u043a\\u0438|\\u0627\\u0644\\u0639\\u0631\\u0628\\u064a\\u0629|\\u65e5\\u672c\\u8a9e|\\ud55c\\uad6d(\\ub9d0|\\uc5b4)|\\u200c\\u0939\\u093f\\u0928\\u0926\\u093c\\u093f|\\u09ac\\u09be\\u0982\\u09b2\\u09be|\\u0a2a\\u0a70\\u0a1c\\u0a3e\\u0a2c\\u0a40|\\u092e\\u0930\\u093e\\u0920\\u0940|\\u0c95\\u0ca8\\u0ccd\\u0ca8\\u0ca1|\\u0627\\u064f\\u0631\\u062f\\u064f\\u0648|\\u0ba4\\u0bae\\u0bbf\\u0bb4\\u0bcd|\\u0c24\\u0c46\\u0c32\\u0c41\\u0c17\\u0c41|\\u0a97\\u0ac1\\u0a9c\\u0ab0\\u0abe\\u0aa4\\u0ac0|\\u0641\\u0627\\u0631\\u0633\\u06cc|\\u067e\\u0627\\u0631\\u0633\\u06cc|\\u0d2e\\u0d32\\u0d2f\\u0d3e\\u0d33\\u0d02|\\u067e\\u069a\\u062a\\u0648|\\u1019\\u103c\\u1014\\u103a\\u1019\\u102c\\u1018\\u102c\\u101e\\u102c|\\u4e2d\\u6587(\\u7b80\\u4f53|\\u7e41\\u9ad4)?|\\u4e2d\\u6587\\uff08(\\u7b80\\u4f53?|\\u7e41\\u9ad4)\\uff09|\\u7b80\\u4f53|\\u7e41\\u9ad4)( language)??($|\\n)";
  val pattern_ContainLanguageWord: Pattern = Pattern.compile(regex_ContainLanguageWord);
  val matcher_ContainLanguageWord: Matcher = pattern_ContainLanguageWord.matcher("");
  def ContainLanguageWord(str: String): Boolean = {

    var text: String = str
    var result: Boolean = false
    if (text != null) {
      text = text.trim()
      text = text.toLowerCase()
      result = matcher_ContainLanguageWord.reset(text).matches()
    }

    result
  }

  //3. Upper case word Ratio:
  def UppercaseWordRation(str: String): Double = {
    val pattern: Pattern = Pattern.compile("\\p{Lu}.*")
    val result: Double = WordRatio(str, pattern)
    result
  }

  //4.  Lower case word Ratio:
  def LowercaseWordRation(str: String): Double = {
    val pattern: Pattern = Pattern.compile("[\\p{L}&&[^\\p{Lu}]].*")
    val result: Double = WordRatio(str, pattern)
    result
  }
  //5.word Contain URL :
  val pattern_WordContainURL: Pattern = Pattern.compile("\\b(https?:\\/\\/|www\\.)\\S{10}.*", Pattern.CASE_INSENSITIVE
    | Pattern.UNICODE_CASE | Pattern.DOTALL | Pattern.CANON_EQ)
  val matcher_WordContainURL: Matcher = pattern_WordContainURL.matcher("");
  def ContainURLWord(str: String): Boolean = {

    var text: String = str
    var result: Boolean = false
    if (text != null) {
      text = text.trim()
      text = text.toLowerCase()
      result = matcher_WordContainURL.reset(text).matches()
    }

    result
  }

  //6. Longest Word
  val pattern_longestWord: Pattern = Pattern.compile("\\p{IsAlphabetic}+", Pattern.CASE_INSENSITIVE
    | Pattern.UNICODE_CASE | Pattern.DOTALL | Pattern.CANON_EQ);
  val matcher_longestWord: Matcher = pattern_WordContainURL.matcher("");
  def LongestWord(str: String): Integer = {

    var max: Integer = null
    var text: String = str
    if (text != null) {
      max = 0;
      text = text.trim()
      matcher_longestWord.reset(text)
      while (matcher_longestWord.find()) {

        var lenght: Integer = matcher_longestWord.end() - matcher_longestWord.start()
        if (lenght > max) {
          max = lenght
        }

      }
    }

    max
  }
  //7. Bad Word : It is Ok
  val luisVonAhnWordlist: Array[String] =
    Array("abbo", "abo",
      "abortion", "abuse", "addict", "addicts", "adult", "africa",
      "african", "alla", "allah", "alligatorbait", "amateur", "american",
      "anal", "analannie", "analsex", "angie", "angry", "anus", "arab",
      "arabs", "areola", "argie", "aroused", "arse", "arsehole", "asian",
      "ass", "assassin", "assassinate", "assassination", "assault",
      "assbagger", "assblaster", "assclown", "asscowboy", "asses",
      "assfuck", "assfucker", "asshat", "asshole", "assholes", "asshore",
      "assjockey", "asskiss", "asskisser", "assklown", "asslick",
      "asslicker", "asslover", "assman", "assmonkey", "assmunch",
      "assmuncher", "asspacker", "asspirate", "asspuppies", "assranger",
      "asswhore", "asswipe", "athletesfoot", "attack", "australian",
      "babe", "babies", "backdoor", "backdoorman", "backseat", "badfuck",
      "balllicker", "balls", "ballsack", "banging", "baptist",
      "barelylegal", "barf", "barface", "barfface", "bast", "bastard ",
      "bazongas", "bazooms", "beaner", "beast", "beastality", "beastial",
      "beastiality", "beatoff", "beat-off", "beatyourmeat", "beaver",
      "bestial", "bestiality", "bi", "biatch", "bible", "bicurious",
      "bigass", "bigbastard", "bigbutt", "bigger", "bisexual",
      "bi-sexual", "bitch", "bitcher", "bitches", "bitchez", "bitchin",
      "bitching", "bitchslap", "bitchy", "biteme", "black", "blackman",
      "blackout", "blacks", "blind", "blow", "blowjob", "boang", "bogan",
      "bohunk", "bollick", "bollock", "bomb", "bombers", "bombing",
      "bombs", "bomd", "bondage", "boner", "bong", "boob", "boobies",
      "boobs", "booby", "boody", "boom", "boong", "boonga", "boonie",
      "booty", "bootycall", "bountybar", "bra", "brea5t", "breast",
      "breastjob", "breastlover", "breastman", "brothel", "bugger",
      "buggered", "buggery", "bullcrap", "bulldike", "bulldyke",
      "bullshit", "bumblefuck", "bumfuck", "bunga", "bunghole", "buried",
      "burn", "butchbabes", "butchdike", "butchdyke", "butt", "buttbang",
      "butt-bang", "buttface", "buttfuck", "butt-fuck", "buttfucker",
      "butt-fucker", "buttfuckers", "butt-fuckers", "butthead",
      "buttman", "buttmunch", "buttmuncher", "buttpirate", "buttplug",
      "buttstain", "byatch", "cacker", "cameljockey", "cameltoe",
      "canadian", "cancer", "carpetmuncher", "carruth", "catholic",
      "catholics", "cemetery", "chav", "cherrypopper", "chickslick",
      "children's", "chin", "chinaman", "chinamen", "chinese", "chink",
      "chinky", "choad", "chode", "christ", "christian", "church",
      "cigarette", "cigs", "clamdigger", "clamdiver", "clit", "clitoris",
      "clogwog", "cocaine", "cock", "cockblock", "cockblocker",
      "cockcowboy", "cockfight", "cockhead", "cockknob", "cocklicker",
      "cocklover", "cocknob", "cockqueen", "cockrider", "cocksman",
      "cocksmith", "cocksmoker", "cocksucer", "cocksuck ", "cocksucked ",
      "cocksucker", "cocksucking", "cocktail", "cocktease", "cocky",
      "cohee", "coitus", "color", "colored", "coloured", "commie",
      "communist", "condom", "conservative", "conspiracy", "coolie",
      "cooly", "coon", "coondog", "copulate", "cornhole", "corruption",
      "cra5h", "crabs", "crack", "crackpipe", "crackwhore",
      "crack-whore", "crap", "crapola", "crapper", "crappy", "crash",
      "creamy", "crime", "crimes", "criminal", "criminals", "crotch",
      "crotchjockey", "crotchmonkey", "crotchrot", "cum", "cumbubble",
      "cumfest", "cumjockey", "cumm", "cummer", "cumming", "cumquat",
      "cumqueen", "cumshot", "cunilingus", "cunillingus", "cunn",
      "cunnilingus", "cunntt", "cunt", "cunteyed", "cuntfuck",
      "cuntfucker", "cuntlick ", "cuntlicker ", "cuntlicking ",
      "cuntsucker", "cybersex", "cyberslimer", "dago", "dahmer",
      "dammit", "damn", "damnation", "damnit", "darkie", "darky",
      "datnigga", "dead", "deapthroat", "death", "deepthroat",
      "defecate", "dego", "demon", "deposit", "desire", "destroy",
      "deth", "devil", "devilworshipper", "dick", "dickbrain",
      "dickforbrains", "dickhead", "dickless", "dicklick", "dicklicker",
      "dickman", "dickwad", "dickweed", "diddle", "die", "died", "dies",
      "dike", "dildo", "dingleberry", "dink", "dipshit", "dipstick",
      "dirty", "disease", "diseases", "disturbed", "dive", "dix",
      "dixiedike", "dixiedyke", "doggiestyle", "doggystyle", "dong",
      "doodoo", "doo-doo", "doom", "dope", "dragqueen", "dragqween",
      "dripdick", "drug", "drunk", "drunken", "dumb", "dumbass",
      "dumbbitch", "dumbfuck", "dyefly", "dyke", "easyslut", "eatballs",
      "eatme", "eatpussy", "ecstacy", "ejaculate", "ejaculated",
      "ejaculating ", "ejaculation", "enema", "enemy", "erect",
      "erection", "ero", "escort", "ethiopian", "ethnic", "european",
      "evl", "excrement", "execute", "executed", "execution",
      "executioner", "explosion", "facefucker", "faeces", "fag",
      "fagging", "faggot", "fagot", "failed", "failure", "fairies",
      "fairy", "faith", "fannyfucker", "fart", "farted ", "farting ",
      "farty ", "fastfuck", "fat", "fatah", "fatass", "fatfuck",
      "fatfucker", "fatso", "fckcum", "fear", "feces", "felatio ",
      "felch", "felcher", "felching", "fellatio", "feltch", "feltcher",
      "feltching", "fetish", "fight", "filipina", "filipino",
      "fingerfood", "fingerfuck ", "fingerfucked ", "fingerfucker ",
      "fingerfuckers", "fingerfucking ", "fire", "firing", "fister",
      "fistfuck", "fistfucked ", "fistfucker ", "fistfucking ",
      "fisting", "flange", "flasher", "flatulence", "floo", "flydie",
      "flydye", "fok", "fondle", "footaction", "footfuck", "footfucker",
      "footlicker", "footstar", "fore", "foreskin", "forni", "fornicate",
      "foursome", "fourtwenty", "fraud", "freakfuck", "freakyfucker",
      "freefuck", "fu", "fubar", "fuc", "fucck", "fuck", "fucka",
      "fuckable", "fuckbag", "fuckbuddy", "fucked", "fuckedup", "fucker",
      "fuckers", "fuckface", "fuckfest", "fuckfreak", "fuckfriend",
      "fuckhead", "fuckher", "fuckin", "fuckina", "fucking",
      "fuckingbitch", "fuckinnuts", "fuckinright", "fuckit", "fuckknob",
      "fuckme ", "fuckmehard", "fuckmonkey", "fuckoff", "fuckpig",
      "fucks", "fucktard", "fuckwhore", "fuckyou", "fudgepacker",
      "fugly", "fuk", "fuks", "funeral", "funfuck", "fungus", "fuuck",
      "gangbang", "gangbanged ", "gangbanger", "gangsta", "gatorbait",
      "gay", "gaymuthafuckinwhore", "gaysex ", "geez", "geezer", "geni",
      "genital", "german", "getiton", "gin", "ginzo", "gipp", "girls",
      "givehead", "glazeddonut", "gob", "god", "godammit", "goddamit",
      "goddammit", "goddamn", "goddamned", "goddamnes", "goddamnit",
      "goddamnmuthafucker", "goldenshower", "gonorrehea", "gonzagas",
      "gook", "gotohell", "goy", "goyim", "greaseball", "gringo", "groe",
      "gross", "grostulation", "gubba", "gummer", "gun", "gyp", "gypo",
      "gypp", "gyppie", "gyppo", "gyppy", "hamas", "handjob", "hapa",
      "harder", "hardon", "harem", "headfuck", "headlights", "hebe",
      "heeb", "hell", "henhouse", "heroin", "herpes", "heterosexual",
      "hijack", "hijacker", "hijacking", "hillbillies", "hindoo",
      "hiscock", "hitler", "hitlerism", "hitlerist", "hiv", "ho", "hobo",
      "hodgie", "hoes", "hole", "holestuffer", "homicide", "homo",
      "homobangers", "homosexual", "honger", "honk", "honkers", "honkey",
      "honky", "hook", "hooker", "hookers", "hooters", "hore", "hork",
      "horn", "horney", "horniest", "horny", "horseshit", "hosejob",
      "hoser", "hostage", "hotdamn", "hotpussy", "hottotrot", "hummer",
      "husky", "hussy", "hustler", "hymen", "hymie", "iblowu", "idiot",
      "ikey", "illegal", "incest", "insest", "intercourse",
      "interracial", "intheass", "inthebuff", "israel", "israeli",
      "israel's", "italiano", "itch", "jackass", "jackoff", "jackshit",
      "jacktheripper", "jade", "jap", "japanese", "japcrap", "jebus",
      "jeez", "jerkoff", "jesus", "jesuschrist", "jew", "jewish", "jiga",
      "jigaboo", "jigg", "jigga", "jiggabo", "jigger ", "jiggy", "jihad",
      "jijjiboo", "jimfish", "jism", "jiz ", "jizim", "jizjuice",
      "jizm ", "jizz", "jizzim", "jizzum", "joint", "juggalo", "jugs",
      "junglebunny", "kaffer", "kaffir", "kaffre", "kafir", "kanake",
      "kid", "kigger", "kike", "kill", "killed", "killer", "killing",
      "kills", "kink", "kinky", "kissass", "kkk", "knife", "knockers",
      "kock", "kondum", "koon", "kotex", "krap", "krappy", "kraut",
      "kum", "kumbubble", "kumbullbe", "kummer", "kumming", "kumquat",
      "kums", "kunilingus", "kunnilingus", "kunt", "ky", "kyke",
      "lactate", "laid", "lapdance", "latin", "lesbain", "lesbayn",
      "lesbian", "lesbin", "lesbo", "lez", "lezbe", "lezbefriends",
      "lezbo", "lezz", "lezzo", "liberal", "libido", "licker", "lickme",
      "lies", "limey", "limpdick", "limy", "lingerie", "liquor",
      "livesex", "loadedgun", "lolita", "looser", "loser", "lotion",
      "lovebone", "lovegoo", "lovegun", "lovejuice", "lovemuscle",
      "lovepistol", "loverocket", "lowlife", "lsd", "lubejob", "lucifer",
      "luckycammeltoe", "lugan", "lynch", "macaca", "mad", "mafia",
      "magicwand", "mams", "manhater", "manpaste", "marijuana",
      "mastabate", "mastabater", "masterbate", "masterblaster",
      "mastrabator", "masturbate", "masturbating", "mattressprincess",
      "meatbeatter", "meatrack", "meth", "mexican", "mgger", "mggor",
      "mickeyfinn", "mideast", "milf", "minority", "mockey", "mockie",
      "mocky", "mofo", "moky", "moles", "molest", "molestation",
      "molester", "molestor", "moneyshot", "mooncricket", "mormon",
      "moron", "moslem", "mosshead", "mothafuck", "mothafucka",
      "mothafuckaz", "mothafucked ", "mothafucker", "mothafuckin",
      "mothafucking ", "mothafuckings", "motherfuck", "motherfucked",
      "motherfucker", "motherfuckin", "motherfucking", "motherfuckings",
      "motherlovebone", "muff", "muffdive", "muffdiver", "muffindiver",
      "mufflikcer", "mulatto", "muncher", "munt", "murder", "murderer",
      "muslim", "naked", "narcotic", "nasty", "nastybitch", "nastyho",
      "nastyslut", "nastywhore", "nazi", "necro", "negro", "negroes",
      "negroid", "negro's", "nig", "niger", "nigerian", "nigerians",
      "nigg", "nigga", "niggah", "niggaracci", "niggard", "niggarded",
      "niggarding", "niggardliness", "niggardliness's", "niggardly",
      "niggards", "niggard's", "niggaz", "nigger", "niggerhead",
      "niggerhole", "niggers", "nigger's", "niggle", "niggled",
      "niggles", "niggling", "nigglings", "niggor", "niggur", "niglet",
      "nignog", "nigr", "nigra", "nigre", "nip", "nipple", "nipplering",
      "nittit", "nlgger", "nlggor", "nofuckingway", "nook", "nookey",
      "nookie", "noonan", "nooner", "nude", "nudger", "nuke",
      "nutfucker", "nymph", "ontherag", "oral", "orga", "orgasim ",
      "orgasm", "orgies", "orgy", "osama", "paki", "palesimian",
      "palestinian", "pansies", "pansy", "panti", "panties", "payo",
      "pearlnecklace", "peck", "pecker", "peckerwood", "pee", "peehole",
      "pee-pee", "peepshow", "peepshpw", "pendy", "penetration", "peni5",
      "penile", "penis", "penises", "penthouse", "period", "perv",
      "phonesex", "phuk", "phuked", "phuking", "phukked", "phukking",
      "phungky", "phuq", "pi55", "picaninny", "piccaninny", "pickaninny",
      "piker", "pikey", "piky", "pimp", "pimped", "pimper", "pimpjuic",
      "pimpjuice", "pimpsimp", "pindick", "piss", "pissed", "pisser",
      "pisses ", "pisshead", "pissin ", "pissing", "pissoff ", "pistol",
      "pixie", "pixy", "playboy", "playgirl", "pocha", "pocho",
      "pocketpool", "pohm", "polack", "pom", "pommie", "pommy", "poo",
      "poon", "poontang", "poop", "pooper", "pooperscooper", "pooping",
      "poorwhitetrash", "popimp", "porchmonkey", "porn", "pornflick",
      "pornking", "porno", "pornography", "pornprincess", "pot",
      "poverty", "premature", "pric", "prick", "prickhead", "primetime",
      "propaganda", "pros", "prostitute", "protestant", "pu55i", "pu55y",
      "pube", "pubic", "pubiclice", "pud", "pudboy", "pudd", "puddboy",
      "puke", "puntang", "purinapricness", "puss", "pussie", "pussies",
      "pussy", "pussycat", "pussyeater", "pussyfucker", "pussylicker",
      "pussylips", "pussylover", "pussypounder", "pusy", "quashie",
      "queef", "queer", "quickie", "quim", "ra8s", "rabbi", "racial",
      "racist", "radical", "radicals", "raghead", "randy", "rape",
      "raped", "raper", "rapist", "rearend", "rearentry", "rectum",
      "redlight", "redneck", "reefer", "reestie", "refugee", "reject",
      "remains", "rentafuck", "republican", "rere", "retard", "retarded",
      "ribbed", "rigger", "rimjob", "rimming", "roach", "robber",
      "roundeye", "rump", "russki", "russkie", "sadis", "sadom",
      "samckdaddy", "sandm", "sandnigger", "satan", "scag", "scallywag",
      "scat", "schlong", "screw", "screwyou", "scrotum", "scum", "semen",
      "seppo", "servant", "sex", "sexed", "sexfarm", "sexhound",
      "sexhouse", "sexing", "sexkitten", "sexpot", "sexslave", "sextogo",
      "sextoy", "sextoys", "sexual", "sexually", "sexwhore", "sexy",
      "sexymoma", "sexy-slim", "shag", "shaggin", "shagging", "shat",
      "shav", "shawtypimp", "sheeney", "shhit", "shinola", "shit",
      "shitcan", "shitdick", "shite", "shiteater", "shited", "shitface",
      "shitfaced", "shitfit", "shitforbrains", "shitfuck", "shitfucker",
      "shitfull", "shithapens", "shithappens", "shithead", "shithouse",
      "shiting", "shitlist", "shitola", "shitoutofluck", "shits",
      "shitstain", "shitted", "shitter", "shitting", "shitty ", "shoot",
      "shooting", "shortfuck", "showtime", "sick", "sissy", "sixsixsix",
      "sixtynine", "sixtyniner", "skank", "skankbitch", "skankfuck",
      "skankwhore", "skanky", "skankybitch", "skankywhore", "skinflute",
      "skum", "skumbag", "slant", "slanteye", "slapper", "slaughter",
      "slav", "slave", "slavedriver", "sleezebag", "sleezeball",
      "slideitin", "slime", "slimeball", "slimebucket", "slopehead",
      "slopey", "slopy", "slut", "sluts", "slutt", "slutting", "slutty",
      "slutwear", "slutwhore", "smack", "smackthemonkey", "smut",
      "snatch", "snatchpatch", "snigger", "sniggered", "sniggering",
      "sniggers", "snigger's", "sniper", "snot", "snowback",
      "snownigger", "sob", "sodom", "sodomise", "sodomite", "sodomize",
      "sodomy", "sonofabitch", "sonofbitch", "sooty", "sos", "soviet",
      "spaghettibender", "spaghettinigger", "spank", "spankthemonkey",
      "sperm", "spermacide", "spermbag", "spermhearder", "spermherder",
      "spic", "spick", "spig", "spigotty", "spik", "spit", "spitter",
      "splittail", "spooge", "spreadeagle", "spunk", "spunky", "squaw",
      "stagg", "stiffy", "strapon", "stringer", "stripclub", "stroke",
      "stroking", "stupid", "stupidfuck", "stupidfucker", "suck",
      "suckdick", "sucker", "suckme", "suckmyass", "suckmydick",
      "suckmytit", "suckoff", "suicide", "swallow", "swallower",
      "swalow", "swastika", "sweetness", "syphilis", "taboo", "taff",
      "tampon", "tang", "tantra", "tarbaby", "tard", "teat", "terror",
      "terrorist", "teste", "testicle", "testicles", "thicklips",
      "thirdeye", "thirdleg", "threesome", "threeway", "timbernigger",
      "tinkle", "tit", "titbitnipply", "titfuck", "titfucker",
      "titfuckin", "titjob", "titlicker", "titlover", "tits", "tittie",
      "titties", "titty", "tnt", "toilet", "tongethruster", "tongue",
      "tonguethrust", "tonguetramp", "tortur", "torture", "tosser",
      "towelhead", "trailertrash", "tramp", "trannie", "tranny",
      "transexual", "transsexual", "transvestite", "triplex",
      "trisexual", "trojan", "trots", "tuckahoe", "tunneloflove", "turd",
      "turnon", "twat", "twink", "twinkie", "twobitwhore", "uck", "uk",
      "unfuckable", "upskirt", "uptheass", "upthebutt", "urinary",
      "urinate", "urine", "usama", "uterus", "vagina", "vaginal",
      "vatican", "vibr", "vibrater", "vibrator", "vietcong", "violence",
      "virgin", "virginbreaker", "vomit", "vulva", "wab", "wank",
      "wanker", "wanking", "waysted", "weapon", "weenie", "weewee",
      "welcher", "welfare", "wetb", "wetback", "wetspot", "whacker",
      "whash", "whigger", "whiskey", "whiskeydick", "whiskydick", "whit",
      "whitenigger", "whites", "whitetrash", "whitey", "whiz", "whop",
      "whore", "whorefucker", "whorehouse", "wigger", "willie",
      "williewanker", "willy", "wn", "wog", "women's", "wop", "wtf",
      "wuss", "wuzzie", "xtc", "xxx", "yankee", "yellowman", "zigabo",
      "zipperhead")

  val tokens: List[String] = new ArrayList[String](Arrays.asList(luisVonAhnWordlist: _*))
  val patternString: String = StringUtils.join(tokens, "|")
  val pattern_badWord: Pattern = Pattern.compile(patternString)

  def BadWordRation(str: String): Double = {
    var results: Double = 0.0
    var text: String = str
    if (text != null) {
      text = text.toLowerCase()
      results = WordRatio(text, pattern_badWord)
    }
    results

  }

  //8. Contain Bad Word:It is ok
  val tokens_containbadword: List[String] = new ArrayList[String](Arrays.asList(luisVonAhnWordlist: _*))
  val patternString_containBadword: String = ".*\\b(" + StringUtils.join(tokens_containbadword, "|") + ")\\b.*"
  val pattern_containBadword: Pattern = Pattern.compile(patternString, Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE | Pattern.DOTALL | Pattern.CANON_EQ)
  val matcher_ContainBadWord: Matcher = pattern_containBadword.matcher("");
  def containBadWord_word(str: String): Boolean = {
    var results: Boolean = false
    var text: String = str
    if (text != null) {
      results = matcher_ContainBadWord.reset(text).matches();
    }

    results

  }

  //9.Ban Builder Word:It is OK
  val BanBuilderWordlist: Array[String] = Array("$#!+", "$1ut", "$h1t",
    "$hit", "$lut", "'ho", "'hobag", "a$$", "anal", "anus", "ass",
    "assmunch", "b1tch", "ballsack", "bastard", "beaner",
    "beastiality", "biatch", "beeyotch", "bitch", "bitchy", "blow job",
    "blow me", "blowjob", "bollock", "bollocks", "bollok", "boner",
    "boob", "bugger", "buttplug", "c-0-c-k", "c-o-c-k", "c-u-n-t",
    "c.0.c.k", "c.o.c.k.", "c.u.n.t", "jerk", "jackoff", "jackhole",
    "j3rk0ff", "homo", "hom0", "hobag", "hell", "h0mo", "h0m0",
    "goddamn", "goddammit", "godamnit", "god damn", "ghey", "ghay",
    "gfy", "gay", "fudgepacker", "fudge packer", "fuckwad", "fucktard",
    "fuckoff", "fucker", "fuck-tard", "fuck off", "fuck", "fellatio",
    "fellate", "felching", "felcher", "felch", "fartknocker", "fart",
    "fannybandit", "fanny bandit", "faggot", "fagg", "fag", "f.u.c.k",
    "f-u-c-k", "f u c k", "dyke", "douchebag", "douche", "douch3",
    "doosh", "dildo", "dike", "dick", "damnit", "damn", "dammit",
    "d1ldo", "d1ld0", "d1ck", "d0uche", "d0uch3", "cunt", "cumstain",
    "cum", "crap", "coon", "cock", "clitoris", "clit", "cl1t", "cawk",
    "c0ck", "jerk0ff", "jerkoff", "jizz", "knob end", "knobend",
    "labia", "lmfao", "moolie", "muff", "nigga", "nigger",
    "p.u.s.s.y.", "penis", "piss", "piss-off", "pissoff", "prick",
    "pube", "pussy", "queer", "retard", "retarded", "s hit", "s-h-1-t",
    "s-h-i-t", "s.h.i.t.", "scrotum", "sex", "sh1t", "shit", "slut",
    "smegma", "t1t", "tard", "terd", "tit", "tits", "titties", "turd",
    "twat", "vag", "vagina", "wank", "wetback", "whore", "whoreface",
    "F*ck", "sh*t", "pu$$y", "p*ssy", "diligaf", "wtf", "stfu",
    "fu*ck", "fack", "shite", "fxck", "sh!t", "@sshole", "assh0le",
    "assho!e", "a$$hole", "a$$h0le", "a$$h0!e", "a$$h01e", "assho1e",
    "wh0re", "f@g", "f@gg0t", "f@ggot", "motherf*cker", "mofo",
    "cuntlicker", "cuntface", "dickbag", "douche waffle", "jizz bag",
    "cockknocker", "beatch", "fucknut", "nucking futs", "mams",
    "carpet muncher", "ass munch", "ass hat", "cunny", "quim",
    "clitty", "fuck wad", "kike", "spic", "wop", "chink", "wet back",
    "mother humper", "feltch", "feltcher", "FvCk", "ahole", "nads",
    "spick", "douchey", "Bullturds", "gonads", "bitch", "butt",
    "fellatio", "lmao", "s-o-b", "spunk", "he11", "jizm", "jism",
    "bukkake", "shiz", "wigger", "gook", "ritard", "reetard",
    "masterbate", "masturbate", "goatse", "masterbating",
    "masturbating", "hitler", "nazi", "tubgirl", "GTFO", "FOAD",
    "r-tard", "rtard", "hoor", "g-spot", "gspot", "vulva", "assmaster",
    "viagra", "Phuck", "frack", "fuckwit", "assbang", "assbanged",
    "assbangs", "asshole", "assholes", "asswipe", "asswipes", "b1tch",
    "bastards", "bitched", "bitches", "blow jobs", "boners",
    "bullshit", "bullshits", "bullshitted", "cameltoe", "camel toe",
    "camel toes", "chinc", "chincs", "chink", "chode", "chodes",
    "clit", "clits", "cocks", "coons", "cumming", "cunts", "d1ck",
    "dickhead", "dickheads", "doggie-style", "dildos", "douchebags",
    "dumass", "dumb ass", "dumbasses", "dykes", "f-u-c-k", "faggit",
    "fags", "fucked", "fucker", "fuckface", "fucks", "godamnit",
    "gooks", "humped", "humping", "jackass", "jap", "japs", "jerk off",
    "jizzed", "kikes", "knobend", "kooch", "kooches", "kootch",
    "mother fucker", "mother fuckers", "motherfucking", "niggah",
    "niggas", "niggers", "p.u.s.s.y.", "porch monkey", "porch monkeys",
    "pussies", "queers", "rim job", "rim jobs", "sand nigger",
    "sand niggers", "s0b", "shitface", "shithead", "shits", "shitted",
    "s.o.b.", "spik", "spiks", "twats", "whack off", "whores",
    "zoophile", "m-fucking", "mthrfucking", "muthrfucking",
    "mutherfucking", "mutherfucker", "mtherfucker", "mthrfucker",
    "mthrf*cker", "whorehopper", "maternal copulator", "(!)",
    "whoralicious", "whorealicious", "( Y )", "(@ Y @)", "(. Y .)",
    "aeolus", "Analprobe", "Areola", "areole", "aryan", "arian",
    "asses", "assfuck", "azazel", "baal", "Babes", "bang", "banger",
    "Barf", "bawdy", "Beardedclam", "beater", "Beaver", "beer",
    "bigtits", "bimbo", "Blew", "blow", "blowjobs", "blowup", "bod",
    "bodily", "boink", "Bone", "boned", "bong", "Boobies", "Boobs",
    "booby", "booger", "Bookie", "Booky", "bootee", "bootie", "Booty",
    "Booze", "boozer", "boozy", "bosom", "bosomy", "bowel", "bowels",
    "bra", "Brassiere", "breast", "breasts", "bung", "babe", "bush",
    "buttfuck", "cocaine", "kinky", "klan", "panties", "pedophile",
    "pedophilia", "pedophiliac", "punkass", "queaf", "rape",
    "scantily", "essohbee", "shithouse", "smut", "snatch", "toots",
    "doggie style", "anorexia", "bulimia", "bulimiic", "burp", "busty",
    "Buttfucker", "caca", "cahone", "Carnal", "Carpetmuncher",
    "cervix", "climax", "Cocain", "Cocksucker", "Coital", "coke",
    "commie", "condom", "corpse", "Coven", "Crabs", "crack",
    "Crackwhore", "crappy", "cuervo", "Cummin", "Cumshot", "cumshots",
    "Cunnilingus", "dago", "dagos", "damned", "dick-ish", "dickish",
    "Dickweed", "anorexic", "prostitute", "marijuana", "LSD", "PCP",
    "diddle", "dawgie-style", "dimwit", "dingle", "doofus", "dopey",
    "douche", "Drunk", "Dummy", "Ejaculate", "enlargement", "erect",
    "erotic", "exotic", "extacy", "Extasy", "faerie", "faery",
    "fagged", "fagot", "Fairy", "fisted", "fisting", "Fisty", "floozy",
    "fondle", "foobar", "foreskin", "frigg", "frigga", "fubar",
    "Fucking", "fuckup", "ganja", "gays", "glans", "godamn", "goddam",
    "Goldenshower", "gonad", "gonads", "Handjob", "hebe", "hemp",
    "heroin", "herpes", "hijack", "Hiv", "Homey", "Honky", "hooch",
    "hookah", "Hooker", "Hootch", "hooter", "hooters", "hump", "hussy",
    "hymen", "inbred", "incest", "injun", "jerked", "Jiz", "Jizm",
    "horny", "junkie", "junky", "kill", "kkk", "kraut", "kyke", "lech",
    "leper", "lesbians", "lesbos", "Lez", "Lezbian", "lezbians",
    "Lezbo", "Lezbos", "Lezzie", "Lezzies", "Lezzy", "loin", "loins",
    "lube", "Lust", "lusty", "Massa", "Masterbation", "Masturbation",
    "maxi", "Menses", "Menstruate", "Menstruation", "meth", "molest",
    "moron", "Motherfucka", "Motherfucker", "murder", "Muthafucker",
    "nad", "naked", "napalm", "Nappy", "nazism", "negro", "niggle",
    "nimrod", "ninny", "Nipple", "nooky", "Nympho", "Opiate", "opium",
    "oral", "orally", "organ", "orgasm", "orgies", "orgy", "ovary",
    "ovum", "ovums", "Paddy", "pantie", "panty", "Pastie", "pasty",
    "Pecker", "pedo", "pee", "Peepee", "Penetrate", "Penetration",
    "penial", "penile", "perversion", "peyote", "phalli", "Phallic",
    "Pillowbiter", "pimp", "pinko", "pissed", "pms", "polack", "porn",
    "porno", "pornography", "pot", "potty", "prig", "prude", "pubic",
    "pubis", "punky", "puss", "Queef", "quicky", "Racist", "racy",
    "raped", "Raper", "rapist", "raunch", "rectal", "rectum", "rectus",
    "reefer", "reich", "revue", "risque", "rum", "rump", "sadism",
    "sadist", "satan", "scag", "schizo", "screw", "Screwed", "scrog",
    "Scrot", "Scrote", "scrud", "scum", "seaman", "seamen", "seduce",
    "semen", "sex_story", "sexual", "Shithole", "Shitter", "shitty",
    "s*o*b", "sissy", "skag", "slave", "sleaze", "sleazy", "sluts",
    "smutty", "sniper", "snuff", "sodom", "souse", "soused", "sperm",
    "spooge", "Stab", "steamy", "Stiffy", "stoned", "strip", "Stroke",
    "whacking off", "suck", "sucked", "sucking", "tampon", "tawdry",
    "teat", "teste", "testee", "testes", "Testis", "thrust", "thug",
    "tinkle", "Titfuck", "titi", "titty", "whacked off", "toke",
    "tramp", "trashy", "tush", "undies", "unwed", "urinal", "urine",
    "uterus", "uzi", "valium", "virgin", "vixen", "vodka", "vomit",
    "voyeur", "vulgar", "wad", "wazoo", "wedgie", "weed", "weenie",
    "weewee", "weiner", "weirdo", "wench", "whitey", "whiz", "Whored",
    "Whorehouse", "Whoring", "womb", "woody", "x-rated", "xxx",
    "B@lls", "yeasty", "yobbo", "sumofabiatch", "doggy-style",
    "doggy style", "wang", "dong", "d0ng", "w@ng", "wh0reface",
    "wh0ref@ce", "wh0r3f@ce", "tittyfuck", "tittyfucker",
    "tittiefucker", "cockholster", "cockblock", "gai", "gey", "faig",
    "faigt", "a55", "a55hole", "gae", "corksucker", "rumprammer",
    "slutdumper", "niggaz", "muthafuckaz", "gigolo", "pussypounder",
    "herp", "herpy", "transsexual", "gender dysphoria", "orgasmic",
    "cunilingus", "anilingus", "dickdipper", "dickwhipper",
    "dicksipper", "dickripper", "dickflipper", "dickzipper", "homoey",
    "queero", "freex", "cunthunter", "shamedame", "slutkiss",
    "shiteater", "slut devil", "fuckass", "fucka$$", "clitorus",
    "assfucker", "dillweed", "cracker", "teabagging", "shitt", "azz",
    "fuk", "fucknugget", "cuntlick", "g@y", "@ss", "beotch")

  val tokens_banBuilder: List[String] = new ArrayList[String](Arrays.asList(BanBuilderWordlist: _*))
  val patternString_banBuilder: String = ".*\\b(" + StringUtils.join(tokens_banBuilder, "|") + ")\\b.*"
  val pattern_banBuilder: Pattern = Pattern.compile(patternString_banBuilder, Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE | Pattern.DOTALL | Pattern.CANON_EQ)
  val matcher_BanBuilder: Matcher = pattern_banBuilder.matcher("");
  def BanBuilderWordlist_word(str: String): Boolean = {

    var results: Boolean = false
    var text: String = str
    if (text != null) {
      results = matcher_BanBuilder.reset(text).matches();
    }

    results

  }
  //10 Ban word Ratio:
  val tokens_ban: List[String] = new ArrayList[String](Arrays.asList(BanBuilderWordlist: _*))
  val patternString_ban: String = StringUtils.join(tokens_ban, "|")
  val pattern_banWord: Pattern = Pattern.compile(patternString_ban)

  def BanWordRation(str: String): Double = {
    var results: Double = 0.0
    var text: String = str
    if (text != null) {
      text = text.toLowerCase()
      results = WordRatio(text, pattern_banWord)
    }
    results

  }

  //11.Contain language word:It is ok
  val regex_containLanguageWord: String = ".*(a(frikaa?ns|lbanian?|lemanha|ng(lais|ol)|ra?b(e?|[ei]c|ian?|isc?h)|rmenian?|ssamese|azeri|z[e\\u0259]rba(ijani?|ycan(ca)?|yjan)|\\u043d\\u0433\\u043b\\u0438\\u0439\\u0441\\u043a\\u0438\\u0439)|b(ahasa( (indonesia|jawa|malaysia|melayu))?|angla|as(k|qu)e|[aeo]ng[ao]?li|elarusian?|okm\\u00e5l|osanski|ra[sz]il(ian?)?|ritish( kannada)?|ulgarian?)|c(ebuano|hina|hinese( simplified)?|zech|roat([eo]|ian?)|atal[a\\u00e0]n?|\\u0440\\u043f\\u0441\\u043a\\u0438|antonese)|[c\\u010d](esky|e[s\\u0161]tina)\r\n|d(an(isc?h|sk)|e?uts?ch)|e(esti|ll[hi]nika|ng(els|le(ski|za)|lisc?h)|spa(g?[n\\u00f1]h?i?ol|nisc?h)|speranto|stonian|usk[ae]ra)|f(ilipino|innish|ran[c\\u00e7](ais|e|ez[ao])|ren[cs]h|arsi|rancese)|g(al(ego|ician)|uja?rati|ree(ce|k)|eorgian|erman[ay]?|ilaki)|h(ayeren|ebrew|indi|rvatski|ungar(y|ian))|i(celandic|ndian?|ndonesian?|ngl[e\\u00ea]se?|ngilizce|tali(ano?|en(isch)?))|ja(pan(ese)?|vanese)|k(a(nn?ada|zakh)|hmer|o(rean?|sova)|urd[i\\u00ee])|l(at(in[ao]?|vi(an?|e[s\\u0161]u))|ietuvi[u\\u0173]|ithuanian?)|m(a[ck]edon(ian?|ski)|agyar|alay(alam?|sian?)?|altese|andarin|arathi|elayu|ontenegro|ongol(ian?)|yanmar)|n(e(d|th)erlands?|epali|orw(ay|egian)|orsk( bokm[a\\u00e5]l)?|ynorsk)|o(landese|dia)|p(ashto|ersi?an?|ol(n?isc?h|ski)|or?tugu?[e\\u00ea]se?(( d[eo])? brasil(eiro)?| ?\\(brasil\\))?|unjabi)|r(om[a\\u00e2i]ni?[a\\u0103]n?|um(ano|\\u00e4nisch)|ussi([ao]n?|sch))|s(anskrit|erbian|imple english|inha?la|lov(ak(ian?)?|en\\u0161?[c\\u010d]ina|en(e|ij?an?)|uomi)|erbisch|pagnolo?|panisc?h|rbeska|rpski|venska|c?wedisc?h|hqip)|t(a(galog|mil)|elugu|hai(land)?|i[e\\u1ebf]ng vi[e\\u1ec7]t|[u\\u00fc]rk([c\\u00e7]e|isc?h|i\\u015f|ey))|u(rdu|zbek)|v(alencia(no?)?|ietnamese)|welsh|(\\u0430\\u043d\\u0433\\u043b\\u0438\\u0438\\u0441|[k\\u043a]\\u0430\\u043b\\u043c\\u044b\\u043a\\u0441|[k\\u043a]\\u0430\\u0437\\u0430\\u0445\\u0441|\\u043d\\u0435\\u043c\\u0435\\u0446|[p\\u0440]\\u0443\\u0441\\u0441|[y\\u0443]\\u0437\\u0431\\u0435\\u043a\\u0441)\\u043a\\u0438\\u0439( \\u044f\\u0437\\u044b\\u043a)??|\\u05e2\\u05d1\\u05e8\\u05d9\\u05ea|[k\\u043a\\u049b](\\u0430\\u0437\\u0430[\\u043a\\u049b]\\u0448\\u0430|\\u044b\\u0440\\u0433\\u044b\\u0437\\u0447\\u0430|\\u0438\\u0440\\u0438\\u043b\\u043b)|\\u0443\\u043a\\u0440\\u0430\\u0457\\u043d\\u0441\\u044c\\u043a(\\u0430|\\u043e\\u044e)|\\u0431(\\u0435\\u043b\\u0430\\u0440\\u0443\\u0441\\u043a\\u0430\\u044f|\\u044a\\u043b\\u0433\\u0430\\u0440\\u0441\\u043a\\u0438( \\u0435\\u0437\\u0438\\u043a)?)|\\u03b5\\u03bb\\u03bb[\\u03b7\\u03b9]\\u03bd\\u03b9\\u03ba(\\u03ac|\\u03b1)|\\u10e5\\u10d0\\u10e0\\u10d7\\u10e3\\u10da\\u10d8|\\u0939\\u093f\\u0928\\u094d\\u0926\\u0940|\\u0e44\\u0e17\\u0e22|[m\\u043c]\\u043e\\u043d\\u0433\\u043e\\u043b(\\u0438\\u0430)?|([c\\u0441]\\u0440\\u043f|[m\\u043c]\\u0430\\u043a\\u0435\\u0434\\u043e\\u043d)\\u0441\\u043a\\u0438|\\u0627\\u0644\\u0639\\u0631\\u0628\\u064a\\u0629|\\u65e5\\u672c\\u8a9e|\\ud55c\\uad6d(\\ub9d0|\\uc5b4)|\\u200c\\u0939\\u093f\\u0928\\u0926\\u093c\\u093f|\\u09ac\\u09be\\u0982\\u09b2\\u09be|\\u0a2a\\u0a70\\u0a1c\\u0a3e\\u0a2c\\u0a40|\\u092e\\u0930\\u093e\\u0920\\u0940|\\u0c95\\u0ca8\\u0ccd\\u0ca8\\u0ca1|\\u0627\\u064f\\u0631\\u062f\\u064f\\u0648|\\u0ba4\\u0bae\\u0bbf\\u0bb4\\u0bcd|\\u0c24\\u0c46\\u0c32\\u0c41\\u0c17\\u0c41|\\u0a97\\u0ac1\\u0a9c\\u0ab0\\u0abe\\u0aa4\\u0ac0|\\u0641\\u0627\\u0631\\u0633\\u06cc|\\u067e\\u0627\\u0631\\u0633\\u06cc|\\u0d2e\\u0d32\\u0d2f\\u0d3e\\u0d33\\u0d02|\\u067e\\u069a\\u062a\\u0648|\\u1019\\u103c\\u1014\\u103a\\u1019\\u102c\\u1018\\u102c\\u101e\\u102c|\\u4e2d\\u6587(\\u7b80\\u4f53|\\u7e41\\u9ad4)?|\\u4e2d\\u6587\\uff08(\\u7b80\\u4f53?|\\u7e41\\u9ad4)\\uff09|\\u7b80\\u4f53|\\u7e41\\u9ad4).*";
  val pattern_forContainLanguageWord: Pattern = Pattern.compile(regex_containLanguageWord);
  val matcher_containLanguageWord: Matcher = pattern_forContainLanguageWord.matcher("");
  def containLanguageBadWord_word(str: String): Boolean = {
    var results: Boolean = false
    var text: String = str
    if (text != null) {
      text = text.trim();
      text = text.toLowerCase();
      results = matcher_containLanguageWord.reset(text).matches();
    }
    results
  }

  //12. Male Names: It is ok
  val MaleNames: Array[String] = Array("AARON", "ADAM", "ADRIAN",
    "ALAN", "ALBERT", "ALBERTO", "ALEX", "ALEXANDER", "ALFRED",
    "ALFREDO", "ALLAN", "ALLEN", "ALVIN", "ANDRE", "ANDREW", "ANDY",
    "ANGEL", "ANTHONY", "ANTONIO", "ARMANDO", "ARNOLD", "ARTHUR",
    "BARRY", "BEN", "BENJAMIN", "BERNARD", "BILL", "BILLY", "BOB",
    "BOBBY", "BRAD", "BRADLEY", "BRANDON", "BRENT", "BRETT", "BRIAN",
    "BRUCE", "BRYAN", "BYRON", "CALVIN", "CARL", "CARLOS", "CASEY",
    "CECIL", "CHAD", "CHARLES", "CHARLIE", "CHESTER", "CHRIS",
    "CHRISTIAN", "CHRISTOPHER", "CLARENCE", "CLAUDE", "CLAYTON",
    "CLIFFORD", "CLIFTON", "CLINTON", "CLYDE", "CODY", "COREY", "CORY",
    "CRAIG", "CURTIS", "DALE", "DAN", "DANIEL", "DANNY", "DARRELL",
    "DARREN", "DARRYL", "DARYL", "DAVE", "DAVID", "DEAN", "DENNIS",
    "DEREK", "DERRICK", "DON", "DONALD", "DOUGLAS", "DUANE", "DUSTIN",
    "DWAYNE", "DWIGHT", "EARL", "EDDIE", "EDGAR", "EDUARDO", "EDWARD",
    "EDWIN", "ELMER", "ENRIQUE", "ERIC", "ERIK", "ERNEST", "EUGENE",
    "EVERETT", "FELIX", "FERNANDO", "FLOYD", "FRANCIS", "FRANCISCO",
    "FRANK", "FRANKLIN", "FRED", "FREDDIE", "FREDERICK", "GABRIEL",
    "GARY", "GENE", "GEORGE", "GERALD", "GILBERT", "GLEN", "GLENN",
    "GORDON", "GREG", "GREGORY", "GUY", "HAROLD", "HARRY", "HARVEY",
    "HECTOR", "HENRY", "HERBERT", "HERMAN", "HOWARD", "HUGH", "IAN",
    "ISAAC", "IVAN", "JACK", "JACOB", "JAIME", "JAMES", "JAMIE",
    "JARED", "JASON", "JAVIER", "JAY", "JEFF", "JEFFERY", "JEFFREY",
    "JEREMY", "JEROME", "JERRY", "JESSE", "JESSIE", "JESUS", "JIM",
    "JIMMIE", "JIMMY", "JOE", "JOEL", "JOHN", "JOHNNIE", "JOHNNY",
    "JON", "JONATHAN", "JORDAN", "JORGE", "JOSE", "JOSEPH", "JOSHUA",
    "JUAN", "JULIAN", "JULIO", "JUSTIN", "KARL", "KEITH", "KELLY",
    "KEN", "KENNETH", "KENT", "KEVIN", "KIRK", "KURT", "KYLE", "LANCE",
    "LARRY", "LAWRENCE", "LEE", "LEO", "LEON", "LEONARD", "LEROY",
    "LESLIE", "LESTER", "LEWIS", "LLOYD", "LONNIE", "LOUIS", "LUIS",
    "MANUEL", "MARC", "MARCUS", "MARIO", "MARION", "MARK", "MARSHALL",
    "MARTIN", "MARVIN", "MATHEW", "MATTHEW", "MAURICE", "MAX",
    "MELVIN", "MICHAEL", "MICHEAL", "MIGUEL", "MIKE", "MILTON",
    "MITCHELL", "MORRIS", "NATHAN", "NATHANIEL", "NEIL", "NELSON",
    "NICHOLAS", "NORMAN", "OSCAR", "PATRICK", "PAUL", "PEDRO", "PERRY",
    "PETER", "PHILIP", "PHILLIP", "RAFAEL", "RALPH", "RAMON",
    "RANDALL", "RANDY", "RAUL", "RAY", "RAYMOND", "REGINALD", "RENE",
    "RICARDO", "RICHARD", "RICK", "RICKY", "ROBERT", "ROBERTO",
    "RODNEY", "ROGER", "ROLAND", "RON", "RONALD", "RONNIE", "ROSS",
    "ROY", "RUBEN", "RUSSELL", "RYAN", "SALVADOR", "SAM", "SAMUEL",
    "SCOTT", "SEAN", "SERGIO", "SETH", "SHANE", "SHAWN", "SIDNEY",
    "STANLEY", "STEPHEN", "STEVE", "STEVEN", "TED", "TERRANCE",
    "TERRENCE", "TERRY", "THEODORE", "THOMAS", "TIM", "TIMOTHY",
    "TODD", "TOM", "TOMMY", "TONY", "TRACY", "TRAVIS", "TROY", "TYLER",
    "TYRONE", "VERNON", "VICTOR", "VINCENT", "VIRGIL", "WADE",
    "WALLACE", "WALTER", "WARREN", "WAYNE", "WESLEY", "WILLARD",
    "WILLIAM", "WILLIE", "ZACHARY")

  val tokens_maleName: List[String] = new ArrayList[String](Arrays.asList(MaleNames: _*))
  val patternString_MaleName: String = ".*\\b(" + StringUtils.join(tokens_maleName, "|") + ")\\b.*"
  val pattern_MaleName: Pattern = Pattern.compile(patternString_MaleName, Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE | Pattern.DOTALL | Pattern.CANON_EQ);
  val matcher_MaleName: Matcher = pattern_MaleName.matcher("");

  def MaleName_word(str: String): Boolean = {

    var results: Boolean = false
    var text: String = str
    if (text != null) {
      results = matcher_MaleName.reset(text).matches();
    }

    results

  }

  //13. Female Names: It is ok
  val FemaleNames: Array[String] = Array("AGNES", "ALICE",
    "ALICIA", "ALLISON", "ALMA", "AMANDA", "AMBER", "AMY", "ANA",
    "ANDREA", "ANGELA", "ANITA", "ANN", "ANNA", "ANNE", "ANNETTE",
    "ANNIE", "APRIL", "ARLENE", "ASHLEY", "AUDREY", "BARBARA",
    "BEATRICE", "BECKY", "BERNICE", "BERTHA", "BESSIE", "BETH",
    "BETTY", "BEVERLY", "BILLIE", "BOBBIE", "BONNIE", "BRANDY",
    "BRENDA", "BRITTANY", "CARLA", "CARMEN", "CAROL", "CAROLE",
    "CAROLINE", "CAROLYN", "CARRIE", "CASSANDRA", "CATHERINE", "CATHY",
    "CHARLENE", "CHARLOTTE", "CHERYL", "CHRISTINA", "CHRISTINE",
    "CHRISTY", "CINDY", "CLAIRE", "CLARA", "CLAUDIA", "COLLEEN",
    "CONNIE", "CONSTANCE", "COURTNEY", "CRYSTAL", "CYNTHIA", "DAISY",
    "DANA", "DANIELLE", "DARLENE", "DAWN", "DEANNA", "DEBBIE",
    "DEBORAH", "DEBRA", "DELORES", "DENISE", "DIANA", "DIANE",
    "DIANNE", "DOLORES", "DONNA", "DORA", "DORIS", "DOROTHY", "EDITH",
    "EDNA", "EILEEN", "ELAINE", "ELEANOR", "ELIZABETH", "ELLA",
    "ELLEN", "ELSIE", "EMILY", "EMMA", "ERICA", "ERIKA", "ERIN",
    "ESTHER", "ETHEL", "EVA", "EVELYN", "FELICIA", "FLORENCE",
    "FRANCES", "GAIL", "GEORGIA", "GERALDINE", "GERTRUDE", "GINA",
    "GLADYS", "GLENDA", "GLORIA", "GRACE", "GWENDOLYN", "HAZEL",
    "HEATHER", "HEIDI", "HELEN", "HILDA", "HOLLY", "IDA", "IRENE",
    "IRMA", "JACKIE", "JACQUELINE", "JAMIE", "JANE", "JANET", "JANICE",
    "JEAN", "JEANETTE", "JEANNE", "JENNIE", "JENNIFER", "JENNY",
    "JESSICA", "JESSIE", "JILL", "JO", "JOAN", "JOANN", "JOANNE",
    "JOSEPHINE", "JOY", "JOYCE", "JUANITA", "JUDITH", "JUDY", "JULIA",
    "JULIE", "JUNE", "KAREN", "KATHERINE", "KATHLEEN", "KATHRYN",
    "KATHY", "KATIE", "KATRINA", "KAY", "KELLY", "KIM", "KIMBERLY",
    "KRISTEN", "KRISTIN", "KRISTINA", "LAURA", "LAUREN", "LAURIE",
    "LEAH", "LENA", "LEONA", "LESLIE", "LILLIAN", "LILLIE", "LINDA",
    "LISA", "LOIS", "LORETTA", "LORI", "LORRAINE", "LOUISE", "LUCILLE",
    "LUCY", "LYDIA", "LYNN", "MABEL", "MAE", "MARCIA", "MARGARET",
    "MARGIE", "MARIA", "MARIAN", "MARIE", "MARILYN", "MARION",
    "MARJORIE", "MARLENE", "MARSHA", "MARTHA", "MARY", "MATTIE",
    "MAUREEN", "MAXINE", "MEGAN", "MELANIE", "MELINDA", "MELISSA",
    "MICHELE", "MICHELLE", "MILDRED", "MINNIE", "MIRIAM", "MISTY",
    "MONICA", "MYRTLE", "NANCY", "NAOMI", "NATALIE", "NELLIE",
    "NICOLE", "NINA", "NORA", "NORMA", "OLGA", "PAMELA", "PATRICIA",
    "PATSY", "PAULA", "PAULINE", "PEARL", "PEGGY", "PENNY", "PHYLLIS",
    "PRISCILLA", "RACHEL", "RAMONA", "REBECCA", "REGINA", "RENEE",
    "RHONDA", "RITA", "ROBERTA", "ROBIN", "ROSA", "ROSE", "ROSEMARY",
    "RUBY", "RUTH", "SALLY", "SAMANTHA", "SANDRA", "SARA", "SARAH",
    "SHANNON", "SHARON", "SHEILA", "SHELLY", "SHERRI", "SHERRY",
    "SHIRLEY", "SONIA", "STACEY", "STACY", "STELLA", "STEPHANIE",
    "SUE", "SUSAN", "SUZANNE", "SYLVIA", "TAMARA", "TAMMY", "TANYA",
    "TARA", "TERESA", "TERRI", "TERRY", "THELMA", "THERESA", "TIFFANY",
    "TINA", "TONI", "TONYA", "TRACEY", "TRACY", "VALERIE", "VANESSA",
    "VELMA", "VERA", "VERONICA", "VICKI", "VICKIE", "VICTORIA",
    "VIOLA", "VIOLET", "VIRGINIA", "VIVIAN", "WANDA", "WENDY",
    "WILLIE", "WILMA", "YOLANDA", "YVONNE")

  val tokens_FemaleName: List[String] = new ArrayList[String](Arrays.asList(FemaleNames: _*))
  val patternString_FemaleName: String = ".*\\b(" + StringUtils.join(tokens_FemaleName, "|") + ")\\b.*"
  val pattern_FeMaleName: Pattern = Pattern.compile(patternString_FemaleName, Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE | Pattern.DOTALL | Pattern.CANON_EQ);
  val matcher_FeMaleName: Matcher = pattern_FeMaleName.matcher("");

  def FemaleName_word(str: String): Boolean = {

    var results: Boolean = false
    var text: String = str
    if (text != null) {
      results = matcher_FeMaleName.reset(text).matches();
    }

    results

  }

  // Words features: ------ End calculation the Ratio for Words:
  // sentence feature:------Start calculation:

  // sentence feature:--------End calculation:
  //Extract ID revision from tag Tag:
  def Get_ID_Revision(str: String): ArrayList[String] = {

    val IDRevision_Step1: ArrayList[String] = new ArrayList[String]()
    val IDRevision: ArrayList[String] = new ArrayList[String]()
    val inputID: CharSequence = str
    val pattStr_id: String = "<revision><id>.*<timestamp>"
    val p_id: Pattern = Pattern.compile(pattStr_id)
    val m_id: Matcher = p_id.matcher(inputID)
    while ((m_id.find())) {
      val repID: String = m_id.group()
      IDRevision_Step1.add(repID)
    }
    val inputID2: CharSequence = IDRevision_Step1.get(0).toString()
    val pattStr_id2: String = "<revision><id>.*</id>"
    val p_id2: Pattern = Pattern.compile(pattStr_id2)
    val m_id2: Matcher = p_id2.matcher(inputID2)
    while ((m_id2.find())) {
      val repID2: String = m_id2.group()
      val str1 = repID2.replace("<revision><id>", "")
      val str2 = str1.replace("</id>", "").trim()

      IDRevision.add(str2)
    }

    IDRevision
  }
  //Extract TimeStampe value from  Tag:
  def Get_TIMEStamp(str: String): ArrayList[String] = {

    val TimeStamp: ArrayList[String] = new ArrayList[String]()
    val inputTime: CharSequence = str
    val pattStr_time: String = "<timestamp>.*</timestamp>"
    val p_time: Pattern = Pattern.compile(pattStr_time)
    val m_time: Matcher = p_time.matcher(inputTime)
    while ((m_time.find())) {
      val repTime: String = m_time.group()
      val cleaned_value1 = repTime.replace("<timestamp>", "")
      val cleaned_value2 = cleaned_value1.replace("</timestamp>", "").trim()
      TimeStamp.add(cleaned_value2)
    }
    TimeStamp
  }
  // extract Json tage from the string object
  def Get_Json_Revision(str: String): ArrayList[String] = {

    val JsonRevision: ArrayList[String] = new ArrayList[String]()
    val inputJsonStr: CharSequence = str
    val pattStr_JsonStr: String = "</format>.*<sha1>"
    val p_JsonStr: Pattern = Pattern.compile(pattStr_JsonStr)
    val m_Jsonstr: Matcher = p_JsonStr.matcher(inputJsonStr)
    while ((m_Jsonstr.find())) {
      val replabels: String = m_Jsonstr.group()
      val cleaned_value1 = replabels.replace("{", "")
      val cleaned_value2 = cleaned_value1.replace("}", "")
      val cleaned_value3 = cleaned_value2.replace("\"", "")
      val cleaned_value4 = cleaned_value3.replace("</format><textxml:space=preserve>", "")
      val cleaned_value5 = cleaned_value4.replace("<sha1>", "")
      JsonRevision.add(cleaned_value5)
    }

    JsonRevision

  }

  //extract Item ID from Json string
  def Get_Item_ID_FromJson(str: String): String = {

    val ID_Item_FromJason: ArrayList[String] = new ArrayList[String]()
    var x = ""
    val mystr = cleaner(str)
    val inputJsonStr: CharSequence = mystr
    val pattStr_JsonStr: String = "id:.*,labels"
    val p_JsonStr: Pattern = Pattern.compile(pattStr_JsonStr)
    val m_Jsonstr: Matcher = p_JsonStr.matcher(inputJsonStr)
    if ((m_Jsonstr.find())) {
      val replabels: String = m_Jsonstr.group()
      val cleaned_value1 = replabels.replace(",labels", "")
      val cleaned_value2 = cleaned_value1.replace("id:", "")
      ID_Item_FromJason.add(cleaned_value2)
      x = cleaned_value2

    } else {

      x = "NA"
    }
    x

  }

  // extract Pairs Labels values from Json String
  def Get_Labels_FromJson(str: String): ArrayList[String] = {

    val list_pairs_fromlabels: ArrayList[String] = new ArrayList[String]()
    val inputJsonStr: CharSequence = str
    val pattStr_Label: String = "labels:.*,descriptions"
    val p_label: Pattern = Pattern.compile(pattStr_Label)
    val m_label: Matcher = p_label.matcher(inputJsonStr)
    while ((m_label.find())) {
      val replabels: String = m_label.group()
      val cleaned_value1 = replabels.replace("labels:", "").trim()
      val cleaned_value2 = cleaned_value1.replace(",descriptions", "").trim()
      list_pairs_fromlabels.add(cleaned_value2)

    }
    list_pairs_fromlabels
  }
  //extract Description from Json File
  def Get_Descri_FromJson(str: String): ArrayList[String] = {

    val list_pairs_fromDescrip: ArrayList[String] = new ArrayList[String]()
    val inputDescription: CharSequence = str
    val pattStr_desc: String = "descriptions:.*,aliases"
    val p_desc: Pattern = Pattern.compile(pattStr_desc)
    val m_desc: Matcher = p_desc.matcher(inputDescription)
    while ((m_desc.find())) {
      val repdesc: String = m_desc.group()
      val cleaned_value1 = repdesc.replace("descriptions:", "").trim()
      val cleaned_value2 = cleaned_value1.replace(",aliases", "").trim()
      if (cleaned_value2 == "[]") {
        list_pairs_fromDescrip.add("NA")

      } else {
        list_pairs_fromDescrip.add(cleaned_value2)
      }
    }
    list_pairs_fromDescrip
  }
  // extract Aliases from Json String
  def Get_Alias_FromJson(str: String): ArrayList[String] = {

    val list_pairs_fromAliases: ArrayList[String] = new ArrayList[String]()
    val inputDescription: CharSequence = str
    val pattStr_desc: String = "aliases:.*,claims"
    val p_desc: Pattern = Pattern.compile(pattStr_desc)
    val m_desc: Matcher = p_desc.matcher(inputDescription)
    while ((m_desc.find())) {
      val repdesc: String = m_desc.group()
      val cleaned_value1 = repdesc.replace("aliases:", "").trim()
      val cleaned_value2 = cleaned_value1.replace(",claims", "").trim()

      if (cleaned_value2 == "[]") {
        list_pairs_fromAliases.add("NA")

      } else {
        list_pairs_fromAliases.add(cleaned_value2)
      }

    }

    list_pairs_fromAliases

  }

  def Get_Contributor(str: String): String = {
    val reg_contributer = "<contributor><username>.*</username><id>.*</id></contributor>"
    val startIndex3 = str.indexOf("<contributor>");
    val endIndex3 = str.indexOf("</contributor>");
    val result_contributor = str.substring(startIndex3 + 1, endIndex3);
    if (result_contributor == null || result_contributor.isEmpty()) {
      val error = "Error1 in String for  Contributor is Empty or Null"
      error
    } else {
      val contributor_Revision = result_contributor.substring(12).trim()
      contributor_Revision
    }
  }
  def Get_User_NameofRevisionFromcontributor(str: String): String = {
    // user name Revision
    val startIndex4 = str.indexOf("<username>");
    val endIndex4 = str.indexOf("</username>");
    val result_contributor_UserName = str.substring(startIndex4 + 1, endIndex4);
    if (result_contributor_UserName == null || result_contributor_UserName.isEmpty()) {
      val error = "Error1 in String for UseName Contributor is Empty or Null"
      error
    } else {
      val UserName_Revision = result_contributor_UserName.substring(9).trim()
      UserName_Revision
    }

  }
  def Get_Id_UserName_OfRevisionFromContributor(str: String): String = {

    val startIndex5 = str.indexOf("<id>");
    val endIndex5 = str.indexOf("</id>");
    val result_Id_UserName = str.substring(startIndex5 + 1, endIndex5);

    if (result_Id_UserName == null || result_Id_UserName.isEmpty()) {
      val error = "Error1 in String for Id User of revision is Empty or Null"
      error
    } else {

      val Id_user_Revision = result_Id_UserName.substring(3).trim()
      Id_user_Revision
    }
  }
  def Get_Comment(str: String): ArrayList[String] = {

    val comment: ArrayList[String] = new ArrayList[String]()
    val inputComment: CharSequence = str
    val pattStr_comment: String = "<comment>.*</comment>"
    val p_comment: Pattern = Pattern.compile(pattStr_comment)
    val m_comment: Matcher = p_comment.matcher(inputComment)
    while ((m_comment.find())) {
      val repcomment: String = m_comment.group()
      val cleaned_value1 = repcomment.replace("<comment>", "").trim()
      val cleaned_value2 = cleaned_value1.replace("</comment>", "").trim()

      comment.add(cleaned_value2)

    }
    comment
  }

  // FlatMap the content of arraylist in multi string lines :
  def AbendArrayListinOneString(arrList: ArrayList[String]): String = {

    var temp: String = ""
    for (j <- 0 until arrList.size()) {
      temp = temp + " " + arrList.get(j).toString().trim()
    }
    temp
  }
  // Build Map revision that includes all info of Revision
  def build_Revision_Map(obj: String): ArrayList[String] = {

    val Map_Revision: ArrayList[String] = new ArrayList[String]()
    val Message: ArrayList[String] = new ArrayList[String]()

    // Id Revision
    val IdRevision = Get_ID_Revision(obj)
    val ID = IdRevision.get(0).toString()
    //=============Start:======= extract information from the json string
    //Json Revision :
    val JsonStr = Get_Json_Revision(obj)
    val Json = JsonStr.get(0).toString()

    //1. ID Item
    val IdItemArray = Get_Item_ID_FromJson(Json)
    //      val IdItem = IdItemArray.get(0).toString().trim()
    val element = ID + "::" + "id_Item" + "::" + IdItemArray
    Map_Revision.add(element)
    //2. Labels Array becuase the lables sometime take multi value with  multi languages
    val Labels: ArrayList[String] = Get_Labels_FromJson(Json)
    if (Labels.size() > 0) {
      val groupLabels = Labels.get(0).toString()
      val inputlabels2: CharSequence = groupLabels
      val pattStr_labels2: String = "(language:[A-Za-z]+,value:[A-Za-z]+)"
      val p_labels2: Pattern = Pattern.compile(pattStr_labels2)
      val m_labels2: Matcher = p_labels2.matcher(inputlabels2)
      var i = 1
      while ((m_labels2.find())) {
        val replabels2: String = m_labels2.group()
        val parts: Array[String] = replabels2.split(":", 2)
        val pairs = parts(1)
        val pairValue: Array[String] = pairs.split(":", 2)
        val finalvalue = pairValue(1)
        val element = ID + "::" + "labels_Value" + (i) + "::" + finalvalue
        Map_Revision.add(element)
        i = i + 1
      }
    } else {

      val element = ID + "::" + "labels_Value" + "::" + "NA"
      Map_Revision.add(element)

    }
    //3. descriptions
    val Description: ArrayList[String] = Get_Descri_FromJson(Json)
    if (Description.size() > 0) {
      if (Description.get(0) != "NA") {
        val groupDescription = Description.get(0).toString()
        val inputdescrip: CharSequence = groupDescription
        val pattStr_Descrip: String = "(language:[A-Za-z]+,value:[A-Za-z]+)"
        val p_Description: Pattern = Pattern.compile(pattStr_Descrip)
        val m_Description: Matcher = p_Description.matcher(inputdescrip)
        var i = 1
        while ((m_Description.find())) {
          val replabels2: String = m_Description.group()
          val parts: Array[String] = replabels2.split(":", 2)
          val pairs = parts(1)
          val pairValue: Array[String] = pairs.split(":", 2)
          val finalvalue = pairValue(1)
          val element = ID + "::" + "descriptions_Value" + (i) + "::" + finalvalue
          Map_Revision.add(element)
          i = i + 1;
        }
      } else {
        val element = ID + "::" + "descriptions_Value" + "::" + "NA"
        Map_Revision.add(element)
      }

    }
    //4. aliases:
    val Aliases: ArrayList[String] = Get_Alias_FromJson(Json)
    if (Aliases.size() > 0) {
      if (Aliases.get(0) != "NA") {
        val groupAlias = Aliases.get(0).toString()
        val inputAliases: CharSequence = groupAlias
        val pattStr_Aliases: String = "(language:[A-Za-z]+,value:[A-Za-z]+)"
        val p_Aliases: Pattern = Pattern.compile(pattStr_Aliases)
        val m_Aliases: Matcher = p_Aliases.matcher(inputAliases)
        var i = 1
        while ((m_Aliases.find())) {
          val replabels2: String = m_Aliases.group()
          val parts: Array[String] = replabels2.split(":", 2)
          val pairs = parts(1)
          val pairValue: Array[String] = pairs.split(":", 2)
          val finalvalue = pairValue(1)
          val element = ID + "::" + "aliases_Value" + (i) + "::" + finalvalue
          Map_Revision.add(element)
          i = i + 1
        }
      } else {
        val element = ID + "::" + "aliases_Value" + "::" + "NA"
        Map_Revision.add(element)
      }
    }
    
    //5.Claims

    //6.Sitelink:

    //.7 Comments :
    val commentarray = Get_Comment(obj)
    val comment = commentarray.get(0)
    val elementcomment = ID + "::" + "comment" + "::" + comment
    Map_Revision.add(elementcomment)

    Map_Revision
   
  }
  //============= Under testing =========================================================
  //    //8. info contributer:
  //    val contributor = Get_Contributor(obj)
  //
  //    if (contributor == null || contributor.isEmpty()) {
  //      val error = "Warning about Empty or Null String (contributor) in Build Revision Map Function"
  //    } else {
  //
  //      //9. usename contributor:
  //      val username = Get_User_NameofRevisionFromcontributor(contributor)
  //      //10. Id User in contributor
  //      val ID_contributor = Get_Id_UserName_OfRevisionFromContributor(contributor)
  //    }
  //
  //    //    val startIndex8 = obj.indexOf("</format>");
  //    //    val endIndex8 = obj.indexOf("<sha1>");
  //    //    val result_json = obj.substring(startIndex8 + 1, endIndex8);
  //    //    val Json_In_Revision = result_json.substring(34).trim();
  //    //    // delete all quotes from the string
  //    //    val withoutQuotes_Json_In_Revision = Json_In_Revision.replace("\"", "");
  //
  //    // id Item  Page...Data Model7
  //    //    val startIndex9 = withoutQuotes_Json_In_Revision.indexOf("id");
  //    //    val endIndex9 = withoutQuotes_Json_In_Revision.indexOf(",labels");
  //    //    val id_Item = withoutQuotes_Json_In_Revision.substring(startIndex9 + 1, endIndex9).substring(2).trim();
  //    //    val Item_Identifier = id_Item;
  //
  //    //TimeStamp Revision...Data Model2
  //    //    val timestamp = Get_TIMEStamp(obj)
  //
  //    //=============================
  //    //contributor revision :
  //    //    val reg_contributer = "<contributor><username>.*</username><id>.*</id></contributor>"
  //    //    val startIndex3 = obj.indexOf("<contributor>");
  //    //    val endIndex3 = obj.indexOf("</contributor>");
  //    //    val result_contributor = obj.substring(startIndex3 + 1, endIndex3);
  //    //    val contributor_Revision = result_contributor.substring(12).trim()
  //    //
  //    //    // user name Revision...Data Model3
  //    //    val startIndex4 = contributor_Revision.indexOf("<username>");
  //    //    val endIndex4 = contributor_Revision.indexOf("</username>");
  //    //    val result_contributor_UserName = contributor_Revision.substring(startIndex4 + 1, endIndex4);
  //    //    val UserName_Revision = result_contributor_UserName.substring(9).trim()
  //    //
  //    //    // Id User Revision......Data Model4
  //    //    val startIndex5 = contributor_Revision.indexOf("<id>");
  //    //    val endIndex5 = contributor_Revision.indexOf("</id>");
  //    //    val result_Id_UserName = contributor_Revision.substring(startIndex5 + 1, endIndex5);
  //    //    val Id_user_Revision = result_Id_UserName.substring(3).trim()
  //    //===================================
  //    //    // Comment Revision ...Data Model5
  //    //    val startIndex6 = obj.indexOf("<comment>");
  //    //    val endIndex6 = obj.indexOf("</comment>");
  //    //    val result_comment = obj.substring(startIndex6 + 1, endIndex6);
  //    //    val comment_In_Revision = result_comment.substring(8).trim()
  //
  //    // Model Revision...Data Model6
  //    val startIndex7 = obj.indexOf("<model>");
  //    val endIndex7 = obj.indexOf("</model>");
  //    val result_model = obj.substring(startIndex7 + 1, endIndex7);
  //    val model_In_Revision = result_model.substring(6).trim()
  //
  //    //======================
  //
  //    //======================
  //

  //
  //    //    //claims...Data Model11
  //    //    val startIndex13 = withoutQuotes_Json_In_Revision.indexOf("claims");
  //    //    val endIndex13 = withoutQuotes_Json_In_Revision.indexOf(",sitelinks");
  //    //    val claims = withoutQuotes_Json_In_Revision.substring(startIndex13, endIndex13);
  //    //
  //    //    //site links...Data Model12
  //    //    val startIndex14 = withoutQuotes_Json_In_Revision.indexOf("sitelinks");
  //    //    val endIndex14 = withoutQuotes_Json_In_Revision.indexOf(getLast(withoutQuotes_Json_In_Revision));
  //    //    val sitelinks = withoutQuotes_Json_In_Revision.substring(startIndex14, endIndex14);
  //
  //    //=============End:======= extract information from the json string
  //    //=============Header Item : ID, Label, Description, Aliases:

  // for extract the sitelink
  def getLast(str: String): Char = {
    val len = str.length();
    val result = str.charAt(len - 1);
    result
  }
  // make the revision as one string
  def abendRevision(str: String): String = {
    val st = str.replaceAll("\\s", "")
    st

  }

  // def clean string from {, } , ""

  def cleaner(str: String): String = {

    val cleaned_value1 = str.replace("{", "").trim()
    val cleaned_value2 = str.replace("}", "").trim()
    val cleaned_value3 = cleaned_value2.replace("\"", "");

    cleaned_value3
  }

}