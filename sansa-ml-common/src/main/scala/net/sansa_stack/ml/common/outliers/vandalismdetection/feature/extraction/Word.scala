package net.sansa_stack.ml.common.outliers.vandalismdetection.feature.extraction

import java.util.{ ArrayList, Arrays, List }
import java.util.regex.{ Matcher, Pattern }

import org.apache.commons.lang3.StringUtils

import net.sansa_stack.ml.common.outliers.vandalismdetection.feature.Utils._

class Word extends Serializable {

  def wordFeatures(StrValue: String): Array[Double] = {

    var RatioValues = new Array[Double](17)
    // 1. Double for LanguageWord Ratio
    val languageWord = languageWordRatioCharacter(StrValue)
    if (!languageWord.isNaN()) {
      RatioValues(0) = roundDouble(languageWord)
    } else {
      RatioValues(0) = 0.0
    }
    // 2. Boolean --> Double for Contain language word  (1 Boolean)
    val isContainLanguageWord = containLanguageWord(StrValue)
    if (isContainLanguageWord == true) {
      RatioValues(1) = 1.0
    } else if (isContainLanguageWord == false) {
      RatioValues(1) = 0.0
    } else {
      RatioValues(1) = 0.0
    }
    // 3.Double for LowerCaseWord Ratio
    val lowerCaseWord = lowercaseWordRatio(StrValue)
    if (!lowerCaseWord.isNaN()) {
      RatioValues(2) = roundDouble(lowerCaseWord)
    } else {
      RatioValues(2) = 0.0
    }
    // 4.Integer --> to Double for LongestWord (1 Integer)
    val longWord = longestWord(StrValue)
    if (longWord != null) {
      val castedValue = longWord.toDouble
      RatioValues(3) = castedValue
    } else {
      RatioValues(3) = 0.0
    }
    // 5.Boolean --> Double for word Contain URL (2 boolean)
    val isWordContainURL = containURLWord(StrValue)
    if (isWordContainURL == true) {
      RatioValues(4) = 1.0
    } else if (isWordContainURL == false) {
      RatioValues(4) = 0.0
    } else {
      RatioValues(4) = 0.0
    }
    // 6.Double for  Bad Word Ratio
    val badWord = badWordRatio(StrValue)
    if (!badWord.isNaN()) {
      RatioValues(5) = roundDouble(badWord)
    } else {
      RatioValues(5) = 0.0
    }
    // 7. Double for UppercaseWord Ratio
    val upperCaseWord = uppercaseWordRatio(StrValue)
    if (!upperCaseWord.isNaN()) {
      RatioValues(6) = roundDouble(upperCaseWord)
    } else {
      RatioValues(6) = 0.0
    }
    // 8.Double for Ban Word Ratio
    val banWord = banWordRatio(StrValue)
    if (!banWord.isNaN()) {
      RatioValues(7) = roundDouble(banWord)
    } else {
      RatioValues(7) = 0.0
    }

    // 9.Boolean Femal FirstName (3 Boolean )
    val isFemalFirstName = femaleNameWord(StrValue)
    if (isFemalFirstName == true) {
      RatioValues(8) = 1.0
    } else if (isFemalFirstName == false) {
      RatioValues(8) = 0.0
    } else {
      RatioValues(8) = 0.0
    }

    // 10. Boolean Male FirstName (4 Boolean)
    val isMaleFirstName = maleNameWord(StrValue)
    if (isMaleFirstName == true) {
      RatioValues(9) = 1.0
    } else if (isMaleFirstName == false) {
      RatioValues(9) = 0.0
    } else {
      RatioValues(9) = 0.0
    }

    // 11. Boolean containBadWord_word (5 Boolean )
    val isContainBad_Word = containBadWord(StrValue)
    if (isContainBad_Word == true) {
      RatioValues(10) = 1.0
    } else if (isContainBad_Word == false) {
      RatioValues(10) = 0.0
    } else {
      RatioValues(10) = 0.0
    }

    // 12. Boolean containBanWord_word (6 Boolean)
    val isContainBan_Word = banBuilderWordListWord(StrValue)
    if (isContainBan_Word == true) {
      RatioValues(11) = 1.0
    } else if (isContainBan_Word == false) {
      RatioValues(11) = 0.0
    } else {
      RatioValues(11) = 0.0
    }
    RatioValues
  }

  def wordRatio(str: String, pattern: Pattern): Double = {
    val pattern_splitting: Pattern = Pattern.compile("\\s+")
    var wordRatio: Double = 0.0
    val words: Array[String] = pattern_splitting.split(str.trim())
    if (words.length > 0) {
      wordRatio = 0
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

  // 1.Language Words Ratio :
  val regex_LanguageWordRatio: String = """(a(frikaa?ns|lbanian?|lemanha|ng(lais|ol)|ra?b(e?|[ei]c|ian?|isc?h)
    |rmenian?|ssamese|azeri|z[e\\u0259]rba(ijani?|ycan(ca)?|yjan)|\\u043d\\u0433\\u043b\\u0438\\u0439\\u0441
    \\u043a\\u0438\\u0439)|b(ahasa( (indonesia|jawa|malaysia|melayu))?|angla|as(k|qu)e|[aeo]ng[ao]?li|elarusian?
    |okm\\u00e5l|osanski|ra[sz]il(ian?)?|ritish( kannada)?|ulgarian?)|c(ebuano|hina|hinese( simplified)?|zech
    |roat([eo]|ian?)|atal[a\\u00e0]n?|\\u0440\\u043f\\u0441\\u043a\\u0438|antonese)|[c\\u010d](esky|e[s
    \\u0161]tina)\r\n|d(an(isc?h|sk)|e?uts?ch)|e(esti|ll[hi]nika|ng(els|le(ski|za)|lisc?h)|spa(g?[n\\u00f1]
    h?i?ol|nisc?h)|speranto|stonian|usk[ae]ra)|f(ilipino|innish|ran[c\\u00e7](ais|e|ez[ao])|ren[cs]h|arsi|rancese)
    |g(al(ego|ician)|uja?rati|ree(ce|k)|eorgian|erman[ay]?|ilaki)|h(ayeren|ebrew|indi|rvatski|ungar(y|ian))
    |i(celandic|ndian?|ndonesian?|ngl[e\\u00ea]se?|ngilizce|tali(ano?|en(isch)?))|ja(pan(ese)?|vanese)
    |k(a(nn?ada|zakh)|hmer|o(rean?|sova)|urd[i\\u00ee])|l(at(in[ao]?|vi(an?|e[s\\u0161]u))|ietuvi[u\\u0173]
    |ithuanian?)|m(a[ck]edon(ian?|ski)|agyar|alay(alam?|sian?)?|altese|andarin|arathi|elayu|ontenegro
    |ongol(ian?)|yanmar)|n(e(d|th)erlands?|epali|orw(ay|egian)|orsk( bokm[a\\u00e5]l)?|ynorsk)|o(landese|dia)
    |p(ashto|ersi?an?|ol(n?isc?h|ski)|or?tugu?[e\\u00ea]se?(( d[eo])? brasil(eiro)?| ?\\(brasil\\))?|unjabi)
    |r(om[a\\u00e2i]ni?[a\\u0103]n?|um(ano|\\u00e4nisch)|ussi([ao]n?|sch))|s(anskrit|erbian|imple english
    |inha?la|lov(ak(ian?)?|en\\u0161?[c\\u010d]ina|en(e|ij?an?)|uomi)|erbisch|pagnolo?|panisc?h|rbeska|rpski
    |venska|c?wedisc?h|hqip)|t(a(galog|mil)|elugu|hai(land)?|i[e\\u1ebf]ng vi[e\\u1ec7]t|[u\\u00fc]rk([c\\u00e7]e
    |isc?h|i\\u015f|ey))|u(rdu|zbek)|v(alencia(no?)?|ietnamese)|welsh|(\\u0430\\u043d\\u0433\\u043b\\u0438
    \\u0438\\u0441|[k\\u043a]\\u0430\\u043b\\u043c\\u044b\\u043a\\u0441|[k\\u043a]\\u0430\\u0437\\u0430
    \\u0445\\u0441|\\u043d\\u0435\\u043c\\u0435\\u0446|[p\\u0440]\\u0443\\u0441\\u0441|[y\\u0443]\\u0437
    \\u0431\\u0435\\u043a\\u0441)\\u043a\\u0438\\u0439( \\u044f\\u0437\\u044b\\u043a)??|\\u05e2\\u05d1
    \\u05e8\\u05d9\\u05ea|[k\\u043a\\u049b](\\u0430\\u0437\\u0430[\\u043a\\u049b]\\u0448\\u0430|\\u044b
    \\u0440\\u0433\\u044b\\u0437\\u0447\\u0430|\\u0438\\u0440\\u0438\\u043b\\u043b)|\\u0443\\u043a
    \\u0440\\u0430\\u0457\\u043d\\u0441\\u044c\\u043a(\\u0430|\\u043e\\u044e)|\\u0431(\\u0435\\u043b
    \\u0430\\u0440\\u0443\\u0441\\u043a\\u0430\\u044f|\\u044a\\u043b\\u0433\\u0430\\u0440\\u0441\\u043a
    \\u0438( \\u0435\\u0437\\u0438\\u043a)?)|\\u03b5\\u03bb\\u03bb[\\u03b7\\u03b9]\\u03bd\\u03b9\\u03ba
    (\\u03ac|\\u03b1)|\\u10e5\\u10d0\\u10e0\\u10d7\\u10e3\\u10da\\u10d8|\\u0939\\u093f\\u0928\\u094d
    \\u0926\\u0940|\\u0e44\\u0e17\\u0e22|[m\\u043c]\\u043e\\u043d\\u0433\\u043e\\u043b(\\u0438\\u0430)?
    |([c\\u0441]\\u0440\\u043f|[m\\u043c]\\u0430\\u043a\\u0435\\u0434\\u043e\\u043d)\\u0441\\u043a\\u0438
    |\\u0627\\u0644\\u0639\\u0631\\u0628\\u064a\\u0629|\\u65e5\\u672c\\u8a9e|\\ud55c\\uad6d(\\ub9d0|\\uc5b4)
    |\\u200c\\u0939\\u093f\\u0928\\u0926\\u093c\\u093f|\\u09ac\\u09be\\u0982\\u09b2\\u09be|\\u0a2a\\u0a70
    \\u0a1c\\u0a3e\\u0a2c\\u0a40|\\u092e\\u0930\\u093e\\u0920\\u0940|\\u0c95\\u0ca8\\u0ccd\\u0ca8\\u0ca1|
    \\u0627\\u064f\\u0631\\u062f\\u064f\\u0648|\\u0ba4\\u0bae\\u0bbf\\u0bb4\\u0bcd|\\u0c24\\u0c46\\u0c32
    \\u0c41\\u0c17\\u0c41|\\u0a97\\u0ac1\\u0a9c\\u0ab0\\u0abe\\u0aa4\\u0ac0|\\u0641\\u0627\\u0631\\u0633
    \\u06cc|\\u067e\\u0627\\u0631\\u0633\\u06cc|\\u0d2e\\u0d32\\u0d2f\\u0d3e\\u0d33\\u0d02|\\u067e\\u069a
    \\u062a\\u0648|\\u1019\\u103c\\u1014\\u103a\\u1019\\u102c\\u1018\\u102c\\u101e\\u102c|\\u4e2d\\u6587
    (\\u7b80\\u4f53|\\u7e41\\u9ad4)?|\\u4e2d\\u6587\\uff08(\\u7b80\\u4f53?|\\u7e41\\u9ad4)\\uff09|\\u7b80
    \\u4f53|\\u7e41\\u9ad4)"""
  val pattern_LanguageWordRatio: Pattern = Pattern.compile(regex_LanguageWordRatio)

  def languageWordRatioCharacter(str: String): Double = {
    val result: Double = wordRatio(str, pattern_LanguageWordRatio)
    result
  }
  // 2. Contain language word :
  val regex_ContainLanguageWord: String = """(^|\\n)([ei]n )??(a(frikaa?ns|lbanian?|lemanha|ng(lais|ol)|ra?b(e?
    |[ei]c|ian?|isc?h)|rmenian?|ssamese|azeri|z[e\\u0259]rba(ijani?|ycan(ca)?|yjan)|\\u043d\\u0433\\u043b\\u0438
    \\u0439\\u0441\\u043a\\u0438\\u0439)|b(ahasa( (indonesia|jawa|malaysia|melayu))?|angla|as(k|qu)e|[aeo]ng[ao]
    ?li|elarusian?|okm\\u00e5l|osanski|ra[sz]il(ian?)?|ritish( kannada)?|ulgarian?)|c(ebuano|hina|hinese(
    simplified)?|zech|roat([eo]|ian?)|atal[a\\u00e0]n?|\\u0440\\u043f\\u0441\\u043a\\u0438|antonese)|[c\\u010d]
    (esky|e[s\\u0161]tina)\r\n|d(an(isc?h|sk)|e?uts?ch)|e(esti|ll[hi]nika|ng(els|le(ski|za)|lisc?h)|spa(g?[n
    \\u00f1]h?i?ol|nisc?h)|speranto|stonian|usk[ae]ra)|f(ilipino|innish|ran[c\\u00e7](ais|e|ez[ao])|ren[cs]h
    |arsi|rancese)|g(al(ego|ician)|uja?rati|ree(ce|k)|eorgian|erman[ay]?|ilaki)|h(ayeren|ebrew|indi|rvatski
    |ungar(y|ian))|i(celandic|ndian?|ndonesian?|ngl[e\\u00ea]se?|ngilizce|tali(ano?|en(isch)?))|ja(pan(ese)?
    |vanese)|k(a(nn?ada|zakh)|hmer|o(rean?|sova)|urd[i\\u00ee])|l(at(in[ao]?|vi(an?|e[s\\u0161]u))|ietuvi[u\\u0173]
    |ithuanian?)|m(a[ck]edon(ian?|ski)|agyar|alay(alam?|sian?)?|altese|andarin|arathi|elayu|ontenegro|ongol(ian?)
    |yanmar)|n(e(d|th)erlands?|epali|orw(ay|egian)|orsk( bokm[a\\u00e5]l)?|ynorsk)|o(landese|dia)|p(ashto|ersi?an?
    |ol(n?isc?h|ski)|or?tugu?[e\\u00ea]se?(( d[eo])? brasil(eiro)?| ?\\(brasil\\))?|unjabi)|r(om[a\\u00e2i]ni?
    [a\\u0103]n?|um(ano|\\u00e4nisch)|ussi([ao]n?|sch))|s(anskrit|erbian|imple english|inha?la|lov(ak(ian?)?
    |en\\u0161?[c\\u010d]ina|en(e|ij?an?)|uomi)|erbisch|pagnolo?|panisc?h|rbeska|rpski|venska|c?wedisc?h|hqip)
    |t(a(galog|mil)|elugu|hai(land)?|i[e\\u1ebf]ng vi[e\\u1ec7]t|[u\\u00fc]rk([c\\u00e7]e|isc?h|i\\u015f|ey))
    |u(rdu|zbek)|v(alencia(no?)?|ietnamese)|welsh|(\\u0430\\u043d\\u0433\\u043b\\u0438\\u0438\\u0441|[k\\u043a]
    \\u0430\\u043b\\u043c\\u044b\\u043a\\u0441|[k\\u043a]\\u0430\\u0437\\u0430\\u0445\\u0441|\\u043d\\u0435\\u043c
    \\u0435\\u0446|[p\\u0440]\\u0443\\u0441\\u0441|[y\\u0443]\\u0437\\u0431\\u0435\\u043a\\u0441)\\u043a\\u0438
    \\u0439( \\u044f\\u0437\\u044b\\u043a)??|\\u05e2\\u05d1\\u05e8\\u05d9\\u05ea|[k\\u043a\\u049b](\\u0430\\u0437
    \\u0430[\\u043a\\u049b]\\u0448\\u0430|\\u044b\\u0440\\u0433\\u044b\\u0437\\u0447\\u0430|\\u0438\\u0440\\u0438
    \\u043b\\u043b)|\\u0443\\u043a\\u0440\\u0430\\u0457\\u043d\\u0441\\u044c\\u043a(\\u0430|\\u043e\\u044e)|
    \\u0431(\\u0435\\u043b\\u0430\\u0440\\u0443\\u0441\\u043a\\u0430\\u044f|\\u044a\\u043b\\u0433\\u0430\\u0440
    \\u0441\\u043a\\u0438( \\u0435\\u0437\\u0438\\u043a)?)|\\u03b5\\u03bb\\u03bb[\\u03b7\\u03b9]\\u03bd\\u03b9
    \\u03ba(\\u03ac|\\u03b1)|\\u10e5\\u10d0\\u10e0\\u10d7\\u10e3\\u10da\\u10d8|\\u0939\\u093f\\u0928\\u094d
    \\u0926\\u0940|\\u0e44\\u0e17\\u0e22|[m\\u043c]\\u043e\\u043d\\u0433\\u043e\\u043b(\\u0438\\u0430)?|([c
    \\u0441]\\u0440\\u043f|[m\\u043c]\\u0430\\u043a\\u0435\\u0434\\u043e\\u043d)\\u0441\\u043a\\u0438|\\u0627
    \\u0644\\u0639\\u0631\\u0628\\u064a\\u0629|\\u65e5\\u672c\\u8a9e|\\ud55c\\uad6d(\\ub9d0|\\uc5b4)|\\u200c
    \\u0939\\u093f\\u0928\\u0926\\u093c\\u093f|\\u09ac\\u09be\\u0982\\u09b2\\u09be|\\u0a2a\\u0a70\\u0a1c\\u0a3e
    \\u0a2c\\u0a40|\\u092e\\u0930\\u093e\\u0920\\u0940|\\u0c95\\u0ca8\\u0ccd\\u0ca8\\u0ca1|\\u0627\\u064f\\u0631
    \\u062f\\u064f\\u0648|\\u0ba4\\u0bae\\u0bbf\\u0bb4\\u0bcd|\\u0c24\\u0c46\\u0c32\\u0c41\\u0c17\\u0c41|\\u0a97
    \\u0ac1\\u0a9c\\u0ab0\\u0abe\\u0aa4\\u0ac0|\\u0641\\u0627\\u0631\\u0633\\u06cc|\\u067e\\u0627\\u0631\\u0633
    \\u06cc|\\u0d2e\\u0d32\\u0d2f\\u0d3e\\u0d33\\u0d02|\\u067e\\u069a\\u062a\\u0648|\\u1019\\u103c\\u1014\\u103a
    \\u1019\\u102c\\u1018\\u102c\\u101e\\u102c|\\u4e2d\\u6587(\\u7b80\\u4f53|\\u7e41\\u9ad4)?|\\u4e2d\\u6587
    \\uff08(\\u7b80\\u4f53?|\\u7e41\\u9ad4)\\uff09|\\u7b80\\u4f53|\\u7e41\\u9ad4)( language)??($|\\n)"""
  val pattern_ContainLanguageWord: Pattern = Pattern.compile(regex_ContainLanguageWord)
  val matcher_ContainLanguageWord: Matcher = pattern_ContainLanguageWord.matcher("")

  def containLanguageWord(str: String): Boolean = {
    var text: String = str
    var result: Boolean = false
    if (text != null) {
      text = text.trim()
      text = text.toLowerCase()
      result = matcher_ContainLanguageWord.reset(text).matches()
    }
    result
  }

  // 3. Upper case word Ratio:
  def uppercaseWordRatio(str: String): Double = {
    val pattern: Pattern = Pattern.compile("\\p{Lu}.*")
    val result: Double = wordRatio(str, pattern)
    result
  }

  // 4.  Lower case word Ratio:
  def lowercaseWordRatio(str: String): Double = {
    val pattern: Pattern = Pattern.compile("[\\p{L}&&[^\\p{Lu}]].*")
    val result: Double = wordRatio(str, pattern)
    result
  }
  // 5.word Contain URL :
  val pattern_WordContainURL: Pattern = Pattern.compile("\\b(https?:\\/\\/|www\\.)\\S{10}.*", Pattern.CASE_INSENSITIVE
    | Pattern.UNICODE_CASE | Pattern.DOTALL | Pattern.CANON_EQ)
  val matcher_WordContainURL: Matcher = pattern_WordContainURL.matcher("")

  def containURLWord(str: String): Boolean = {
    var text: String = str
    var result: Boolean = false
    if (text != null) {
      text = text.trim()
      text = text.toLowerCase()
      result = matcher_WordContainURL.reset(text).matches()
    }
    result
  }

  // 6. Longest Word
  val pattern_longestWord: Pattern = Pattern.compile("\\p{IsAlphabetic}+", Pattern.CASE_INSENSITIVE
    | Pattern.UNICODE_CASE | Pattern.DOTALL | Pattern.CANON_EQ)
  val matcher_longestWord: Matcher = pattern_WordContainURL.matcher("")

  def longestWord(str: String): Integer = {
    var max: Integer = null
    var text: String = str
    if (text != null) {
      max = 0
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
  // 7. Bad Word : It is Ok
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

  def badWordRatio(str: String): Double = {
    var results: Double = 0.0
    var text: String = str
    if (text != null) {
      text = text.toLowerCase()
      results = wordRatio(text, pattern_badWord)
    }
    results
  }

  // 8. Contain Bad Word:It is ok
  val tokens_containbadword: List[String] = new ArrayList[String](Arrays.asList(luisVonAhnWordlist: _*))
  val patternString_containBadword: String = ".*\\b(" + StringUtils.join(tokens_containbadword, "|") + ")\\b.*"
  val pattern_containBadword: Pattern = Pattern.compile(patternString, Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE | Pattern.DOTALL | Pattern.CANON_EQ)
  val matcher_ContainBadWord: Matcher = pattern_containBadword.matcher("")

  def containBadWord(str: String): Boolean = {
    var results: Boolean = false
    var text: String = str
    if (text != null) {
      results = matcher_ContainBadWord.reset(text).matches()
    }
    results
  }

  // 9.Ban Builder Word:It is OK
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
  val matcher_BanBuilder: Matcher = pattern_banBuilder.matcher("")

  def banBuilderWordListWord(str: String): Boolean = {
    var results: Boolean = false
    var text: String = str
    if (text != null) {
      results = matcher_BanBuilder.reset(text).matches()
    }
    results
  }
  // 10 Ban word Ratio:
  val tokens_ban: List[String] = new ArrayList[String](Arrays.asList(BanBuilderWordlist: _*))
  val patternString_ban: String = StringUtils.join(tokens_ban, "|")
  val pattern_banWord: Pattern = Pattern.compile(patternString_ban)

  def banWordRatio(str: String): Double = {
    var results: Double = 0.0
    var text: String = str
    if (text != null) {
      text = text.toLowerCase()
      results = wordRatio(text, pattern_banWord)
    }
    results

  }

  // 11.Contain language word:It is ok
  val regex_containLanguageWord: String = """.*(a(frikaa?ns|lbanian?|lemanha|ng(lais|ol)|ra?b(e?|[ei]c|ian?
    |isc?h)|rmenian?|ssamese|azeri|z[e\\u0259]rba(ijani?|ycan(ca)?|yjan)|\\u043d\\u0433\\u043b\\u0438\\u0439
    \\u0441\\u043a\\u0438\\u0439)|b(ahasa( (indonesia|jawa|malaysia|melayu))?|angla|as(k|qu)e|[aeo]ng[ao]?li
    |elarusian?|okm\\u00e5l|osanski|ra[sz]il(ian?)?|ritish( kannada)?|ulgarian?)|c(ebuano|hina|hinese( simplified)?
    |zech|roat([eo]|ian?)|atal[a\\u00e0]n?|\\u0440\\u043f\\u0441\\u043a\\u0438|antonese)|[c\\u010d](esky|e[s\\u0161]tina)
    \r\n|d(an(isc?h|sk)|e?uts?ch)|e(esti|ll[hi]nika|ng(els|le(ski|za)|lisc?h)|spa(g?[n\\u00f1]h?i?ol|nisc?h)|speranto
    |stonian|usk[ae]ra)|f(ilipino|innish|ran[c\\u00e7](ais|e|ez[ao])|ren[cs]h|arsi|rancese)|g(al(ego|ician)
    |uja?rati|ree(ce|k)|eorgian|erman[ay]?|ilaki)|h(ayeren|ebrew|indi|rvatski|ungar(y|ian))|i(celandic|ndian?
    |ndonesian?|ngl[e\\u00ea]se?|ngilizce|tali(ano?|en(isch)?))|ja(pan(ese)?|vanese)|k(a(nn?ada|zakh)|hmer|o(rean?
    |sova)|urd[i\\u00ee])|l(at(in[ao]?|vi(an?|e[s\\u0161]u))|ietuvi[u\\u0173]|ithuanian?)|m(a[ck]edon(ian?|ski)
    |agyar|alay(alam?|sian?)?|altese|andarin|arathi|elayu|ontenegro|ongol(ian?)|yanmar)|n(e(d|th)erlands?|epali
    |orw(ay|egian)|orsk( bokm[a\\u00e5]l)?|ynorsk)|o(landese|dia)|p(ashto|ersi?an?|ol(n?isc?h|ski)|or?tugu?[e\\u00ea]se?
    (( d[eo])? brasil(eiro)?| ?\\(brasil\\))?|unjabi)|r(om[a\\u00e2i]ni?[a\\u0103]n?|um(ano|\\u00e4nisch)|ussi([ao]n?
    |sch))|s(anskrit|erbian|imple english|inha?la|lov(ak(ian?)?|en\\u0161?[c\\u010d]ina|en(e|ij?an?)|uomi)|erbisch
    |pagnolo?|panisc?h|rbeska|rpski|venska|c?wedisc?h|hqip)|t(a(galog|mil)|elugu|hai(land)?|i[e\\u1ebf]ng vi[e\\u1ec7]t
    |[u\\u00fc]rk([c\\u00e7]e|isc?h|i\\u015f|ey))|u(rdu|zbek)|v(alencia(no?)?|ietnamese)|welsh|(\\u0430\\u043d\\u0433
    \\u043b\\u0438\\u0438\\u0441|[k\\u043a]\\u0430\\u043b\\u043c\\u044b\\u043a\\u0441|[k\\u043a]\\u0430\\u0437\\u0430
    \\u0445\\u0441|\\u043d\\u0435\\u043c\\u0435\\u0446|[p\\u0440]\\u0443\\u0441\\u0441|[y\\u0443]\\u0437\\u0431\\u0435
    \\u043a\\u0441)\\u043a\\u0438\\u0439( \\u044f\\u0437\\u044b\\u043a)??|\\u05e2\\u05d1\\u05e8\\u05d9\\u05ea|[k\\u043a
    \\u049b](\\u0430\\u0437\\u0430[\\u043a\\u049b]\\u0448\\u0430|\\u044b\\u0440\\u0433\\u044b\\u0437\\u0447\\u0430|
    \\u0438\\u0440\\u0438\\u043b\\u043b)|\\u0443\\u043a\\u0440\\u0430\\u0457\\u043d\\u0441\\u044c\\u043a(\\u0430|
    \\u043e\\u044e)|\\u0431(\\u0435\\u043b\\u0430\\u0440\\u0443\\u0441\\u043a\\u0430\\u044f|\\u044a\\u043b\\u0433
    \\u0430\\u0440\\u0441\\u043a\\u0438( \\u0435\\u0437\\u0438\\u043a)?)|\\u03b5\\u03bb\\u03bb[\\u03b7\\u03b9]
    \\u03bd\\u03b9\\u03ba(\\u03ac|\\u03b1)|\\u10e5\\u10d0\\u10e0\\u10d7\\u10e3\\u10da\\u10d8|\\u0939\\u093f\\u0928
    \\u094d\\u0926\\u0940|\\u0e44\\u0e17\\u0e22|[m\\u043c]\\u043e\\u043d\\u0433\\u043e\\u043b(\\u0438\\u0430)?|([c
    \\u0441]\\u0440\\u043f|[m\\u043c]\\u0430\\u043a\\u0435\\u0434\\u043e\\u043d)\\u0441\\u043a\\u0438|\\u0627\\u0644
    \\u0639\\u0631\\u0628\\u064a\\u0629|\\u65e5\\u672c\\u8a9e|\\ud55c\\uad6d(\\ub9d0|\\uc5b4)|\\u200c\\u0939\\u093f
    \\u0928\\u0926\\u093c\\u093f|\\u09ac\\u09be\\u0982\\u09b2\\u09be|\\u0a2a\\u0a70\\u0a1c\\u0a3e\\u0a2c\\u0a40|
    \\u092e\\u0930\\u093e\\u0920\\u0940|\\u0c95\\u0ca8\\u0ccd\\u0ca8\\u0ca1|\\u0627\\u064f\\u0631\\u062f\\u064f\\u0648
    |\\u0ba4\\u0bae\\u0bbf\\u0bb4\\u0bcd|\\u0c24\\u0c46\\u0c32\\u0c41\\u0c17\\u0c41|\\u0a97\\u0ac1\\u0a9c\\u0ab0\\u0abe
    \\u0aa4\\u0ac0|\\u0641\\u0627\\u0631\\u0633\\u06cc|\\u067e\\u0627\\u0631\\u0633\\u06cc|\\u0d2e\\u0d32\\u0d2f\\u0d3e
    \\u0d33\\u0d02|\\u067e\\u069a\\u062a\\u0648|\\u1019\\u103c\\u1014\\u103a\\u1019\\u102c\\u1018\\u102c\\u101e\\u102c
    |\\u4e2d\\u6587(\\u7b80\\u4f53|\\u7e41\\u9ad4)?|\\u4e2d\\u6587\\uff08(\\u7b80\\u4f53?|\\u7e41\\u9ad4)\\uff09
    |\\u7b80\\u4f53|\\u7e41\\u9ad4).*"""
  val pattern_forContainLanguageWord: Pattern = Pattern.compile(regex_containLanguageWord)
  val matcher_containLanguageWord: Matcher = pattern_forContainLanguageWord.matcher("")

  def containLanguageBadWordWord(str: String): Boolean = {
    var results: Boolean = false
    var text: String = str
    if (text != null) {
      text = text.trim()
      text = text.toLowerCase()
      results = matcher_containLanguageWord.reset(text).matches()
    }
    results
  }

  // 12. Male Names
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
  val pattern_MaleName: Pattern = Pattern.compile(patternString_MaleName, Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE | Pattern.DOTALL | Pattern.CANON_EQ)
  val matcher_MaleName: Matcher = pattern_MaleName.matcher("")

  def maleNameWord(str: String): Boolean = {
    var results: Boolean = false
    var text: String = str
    if (text != null) {
      results = matcher_MaleName.reset(text).matches()
    }
    results
  }

  // 13. Female Names
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
  val pattern_FeMaleName: Pattern = Pattern.compile(patternString_FemaleName, Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE | Pattern.DOTALL | Pattern.CANON_EQ)
  val matcher_FeMaleName: Matcher = pattern_FeMaleName.matcher("")

  def femaleNameWord(str: String): Boolean = {
    var results: Boolean = false
    var text: String = str
    if (text != null) {
      results = matcher_FeMaleName.reset(text).matches()
    }
    results
  }

  def currentPreviousCommentTialNumberSharingWords(str_current: String, Str_Prev: String): Integer = {
    var results = 0
    var pattern: Pattern = null
    pattern = Pattern.compile("\\s+")

    var current_CommentTail = str_current
    var Prev_commentTail = Str_Prev

    if (current_CommentTail != "") {
      var Words: Array[String] = pattern.split(current_CommentTail.trim())
      if (Words.length > 0) {
        results = 0
      }
      for (word <- Words) {
        if (word.trim().!=("") && Prev_commentTail.contains(word)) {
          results = results + 1
        }
      }
    }
    results
  }

  val StopWord: Array[String] = Array("a's", "able", "about",
    "above", "according", "accordingly", "across", "actually", "after",
    "afterwards", "again", "against", "ain't", "all", "allow",
    "allows", "almost", "alone", "along", "already", "also",
    "although", "always", "am", "among", "amongst", "an", "and",
    "another", "any", "anybody", "anyhow", "anyone", "anything",
    "anyway", "anyways", "anywhere", "apart", "appear", "appreciate",
    "appropriate", "are", "aren't", "around", "as", "aside", "ask",
    "asking", "associated", "at", "available", "away", "awfully", "be",
    "became", "because", "become", "becomes", "becoming", "been",
    "before", "beforehand", "behind", "being", "believe", "below",
    "beside", "besides", "best", "better", "between", "beyond", "both",
    "brief", "but", "by", "c'mon", "c's", "came", "can", "can't",
    "cannot", "cant", "cause", "causes", "certain", "certainly",
    "changes", "clearly", "co", "com", "come", "comes", "concerning",
    "consequently", "consider", "considering", "contain", "containing",
    "contains", "corresponding", "could", "couldn't", "course",
    "currently", "definitely", "described", "despite", "did", "didn't",
    "different", "do", "does", "doesn't", "doing", "don't", "done",
    "down", "downwards", "during", "each", "edu", "eg", "eight",
    "either", "else", "elsewhere", "enough", "entirely", "especially",
    "et", "etc", "even", "ever", "every", "everybody", "everyone",
    "everything", "everywhere", "ex", "exactly", "example", "except",
    "far", "few", "fifth", "first", "five", "followed", "following",
    "follows", "for", "former", "formerly", "forth", "four", "from",
    "further", "furthermore", "get", "gets", "getting", "given",
    "gives", "go", "goes", "going", "gone", "got", "gotten",
    "greetings", "had", "hadn't", "happens", "hardly", "has", "hasn't",
    "have", "haven't", "having", "he", "he's", "hello", "help",
    "hence", "her", "here", "here's", "hereafter", "hereby", "herein",
    "hereupon", "hers", "herself", "hi", "him", "himself", "his",
    "hither", "hopefully", "how", "howbeit", "however", "i'd", "i'll",
    "i'm", "i've", "ie", "if", "ignored", "immediate", "in",
    "inasmuch", "inc", "indeed", "indicate", "indicated", "indicates",
    "inner", "insofar", "instead", "into", "inward", "is", "isn't",
    "it", "it'd", "it'll", "it's", "its", "itself", "just", "keep",
    "keeps", "kept", "know", "knows", "known", "last", "lately",
    "later", "latter", "latterly", "least", "less", "lest", "let",
    "let's", "like", "liked", "likely", "little", "look", "looking",
    "looks", "ltd", "mainly", "many", "may", "maybe", "me", "mean",
    "meanwhile", "merely", "might", "more", "moreover", "most",
    "mostly", "much", "must", "my", "myself", "name", "namely", "nd",
    "near", "nearly", "necessary", "need", "needs", "neither", "never",
    "nevertheless", "new", "next", "nine", "no", "nobody", "non",
    "none", "noone", "nor", "normally", "not", "nothing", "novel",
    "now", "nowhere", "obviously", "of", "off", "often", "oh", "ok",
    "okay", "old", "on", "once", "one", "ones", "only", "onto", "or",
    "other", "others", "otherwise", "ought", "our", "ours",
    "ourselves", "out", "outside", "over", "overall", "own",
    "particular", "particularly", "per", "perhaps", "placed", "please",
    "plus", "possible", "presumably", "probably", "provides", "que",
    "quite", "qv", "rather", "rd", "re", "really", "reasonably",
    "regarding", "regardless", "regards", "relatively", "respectively",
    "right", "said", "same", "saw", "say", "saying", "says", "second",
    "secondly", "see", "seeing", "seem", "seemed", "seeming", "seems",
    "seen", "self", "selves", "sensible", "sent", "serious",
    "seriously", "seven", "several", "shall", "she", "should",
    "shouldn't", "since", "six", "so", "some", "somebody", "somehow",
    "someone", "something", "sometime", "sometimes", "somewhat",
    "somewhere", "soon", "sorry", "specified", "specify", "specifying",
    "still", "sub", "such", "sup", "sure", "t's", "take", "taken",
    "tell", "tends", "th", "than", "thank", "thanks", "thanx", "that",
    "that's", "thats", "the", "their", "theirs", "them", "themselves",
    "then", "thence", "there", "there's", "thereafter", "thereby",
    "therefore", "therein", "theres", "thereupon", "these", "they",
    "they'd", "they'll", "they're", "they've", "think", "third",
    "this", "thorough", "thoroughly", "those", "though", "three",
    "through", "throughout", "thru", "thus", "to", "together", "too",
    "took", "toward", "towards", "tried", "tries", "truly", "try",
    "trying", "twice", "two", "un", "under", "unfortunately", "unless",
    "unlikely", "until", "unto", "up", "upon", "us", "use", "used",
    "useful", "uses", "using", "usually", "value", "various", "very",
    "via", "viz", "vs", "want", "wants", "was", "wasn't", "way", "we",
    "we'd", "we'll", "we're", "we've", "welcome", "well", "went",
    "were", "weren't", "what", "what's", "whatever", "when", "whence",
    "whenever", "where", "where's", "whereafter", "whereas", "whereby",
    "wherein", "whereupon", "wherever", "whether", "which", "while",
    "whither", "who", "who's", "whoever", "whole", "whom", "whose",
    "why", "will", "willing", "wish", "with", "within", "without",
    "won't", "wonder", "would", "would", "wouldn't", "yes", "yet",
    "you", "you'd", "you'll", "you're", "you've", "your", "yours",
    "yourself", "yourselves", "zero", "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m",
    "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z")

  val tokens_StopWords: List[String] = new ArrayList[String](Arrays.asList(StopWord: _*))
  val patternString_stopword: String = ".*\\b(" + StringUtils.join(tokens_StopWords, "|") + ")\\b.*"
  val pattern_stopwords: Pattern = Pattern.compile(patternString_stopword, Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE | Pattern.DOTALL | Pattern.CANON_EQ)
  val matcher_stopword: Matcher = pattern_stopwords.matcher("")

  def currentPreviousCommentTialNumberSharingWordsWithoutStopWords(str_current: String, Str_Prev: String): Integer = {
    var results = 0
    var pattern: Pattern = null
    pattern = Pattern.compile("\\s+")
    var current_CommentTail = str_current
    var Prev_commentTail = Str_Prev
    if (current_CommentTail != "") {
      var Words: Array[String] = pattern.split(current_CommentTail.trim())
      if (Words.length > 0) {
        results = 0
      }
      for (word <- Words) {
        var flag: Boolean = false
        var text: String = word
        if (text != null) {
          flag = matcher_stopword.reset(text).matches()
          if (word.trim().!=("") && flag != true && Prev_commentTail.contains(word)) {
            results = results + 1
          }
        }
      }
    }
    results
  }
  def getNumberOfLinks(str: String): Double =
    stringMatchValue(str, "https?:\\/\\/|www\\.")

  val RegexStr = "(a(frikaa?ns|lbanian?|lemanha|ng(lais|ol)|ra?b(e?|" +
    "[ei]c|ian?|isc?h)|rmenian?|ssamese|azeri|z[eə]rba" +
    "(ijani?|ycan(ca)?|yjan)|нглийский)|b(ahasa( (indonesia|" +
    "jawa|malaysia|melayu))?|angla|as(k|qu)e|[aeo]ng[ao]?li|" +
    "elarusian?|okmål|osanski|ra[sz]il(ian?)?|ritish( " +
    "kannada)?|ulgarian?)|c(ebuano|hina|hinese( simplified)?" +
    "|zech|roat([eo]|ian?)|atal[aà]n?|рпски|antonese)|[cč]" +
    "(esky|e[sš]tina)|d(an(isc?h|sk)|e?uts?ch)|e(esti|ll[hi]" +
    "nika|ng(els|le(ski|za)|lisc?h)|spa(g?[nñ]h?i?ol|nisc?h)" +
    "|speranto|stonian|usk[ae]ra)|f(ilipino|innish|ran[cç]" +
    "(ais|e|ez[ao])|ren[cs]h|arsi|rancese)|g(al(ego|ician)|" +
    "uja?rati|ree(ce|k)|eorgian|erman[ay]?|ilaki)|h(ayeren|" +
    "ebrew|indi|rvatski|ungar(y|ian))|i(celandic|ndian?|" +
    "ndonesian?|ngl[eê]se?|ngilizce|tali(ano?|en(isch)?))|" +
    "ja(pan(ese)?|vanese)|k(a(nn?ada|zakh)|hmer|o(rean?|" +
    "sova)|urd[iî])|l(at(in[ao]?|vi(an?|e[sš]u))|ietuvi[uų]" +
    "|ithuanian?)|m(a[ck]edon(ian?|ski)|agyar|alay(alam?|" +
    "sian?)?|altese|andarin|arathi|elayu|ontenegro|ongol" +
    "(ian?)|yanmar)|n(e(d|th)erlands?|epali|orw(ay|egian)|" +
    "orsk( bokm[aå]l)?|ynorsk)|o(landese|dia)|p(ashto|" +
    "ersi?an?|ol(n?isc?h|ski)|or?tugu?[eê]se?(( d[eo])?" +
    "brasil(eiro)?| ?\\(brasil\\))?|unjabi)|r(om[aâi]ni?[aă]n?" +
    "|um(ano|änisch)|ussi([ao]n?|sch))|s(anskrit|erbian|" +
    "imple english|inha?la|lov(ak(ian?)?|enš?[cč]ina|" +
    "en(e|ij?an?)|uomi)|erbisch|pagnolo?|panisc?h|rbeska|" +
    "rpski|venska|c?wedisc?h|hqip)|t(a(galog|mil)|elugu|" +
    "hai(land)?|i[eế]ng vi[eệ]t|[uü]rk([cç]e|isc?h|iş|ey))|" +
    "u(rdu|zbek)|v(alencia(no?)?|ietnamese)|welsh|(англиис|" +
    "[kк]алмыкс|[kк]азахс|немец|[pр]усс|[yу]збекс|" +
    "татарс)кий( язык)??|עברית|[kкқ](аза[кқ]ша|ыргызча|" +
    "ирилл)|українськ(а|ою)|б(еларуская|" +
    "ългарски( език)?)|ελλ[ηι]" + "νικ(ά|α)|ქართული|हिन्दी|ไทย|[mм]онгол(иа)?|([cс]рп|" +
    "[mм]акедон)ски|العربية|日本語|한국(말|어)|‌हिनद़ि| " +
    " বাংলা|ਪੰਜਾਬੀ|मराठी|ಕನ್ನಡ|اُردُو|தமிழ்|తెలుగు|ગુજરાતી|" +
    "فارسی|پارسی|മലയാളം|پښتو|မြန်မာဘာသာ|中文(简体|繁體)?|" +
    "中文（(简体?|繁體)）|简体|繁體)"

  def getNumberOfLanguageWord(str: String): Double =
    stringMatchValue(str, RegexStr)

  def getNumberOfQId(str: String): Double =
    stringMatchValue(str, "Q\\d{1,8}")

  def proportion(oldCount: Double, newCount: Double): Float = {
    val result: Double = (newCount - oldCount) / (newCount + 1.0)
    result.toFloat
  }
}
