package net.sansa_stack.ml.common.outliers.vandalismdetection.feature.extraction

import java.util.regex.{ Matcher, Pattern }

import net.sansa_stack.ml.common.outliers.vandalismdetection.feature.Utils._
import net.sansa_stack.ml.common.outliers.vandalismdetection.feature.extraction.Comment._

object Revision extends Serializable {

  // Contain language Latin :
  val latinRegexStr: String = """(af|ak|an|ang|ast|ay|az|bar|bcl|bi|bm|br|bs|ca|cbk-zam
    |ceb|ch|chm|cho|chy|co|crh-latn|cs|csb|cv|cy|da|de|diq|dsb|ee|eml|en|eo|es|et|eu|ff
    |fi|fj|fo|fr|frp|frr|fur|fy|ga|gd|gl|gn|gsw|gv|ha|haw|ho|hr|hsb|ht|hu|hz|id|ie|ig|ik
    |ilo|io|is|it|jbo|jv|kab|kg|ki|kj|kl|kr|ksh|ku(?!-arab\b)|kw|la|lad|lb|lg|li|lij|lmo
    |ln|lt|lv|map-bms|mg|mh|min?|ms|mt|mus|mwl|na|nah|nan|nap|nb|nds|nds-nl|ng|nl|nn|nov
    |nrm|nv|ny|oc|om|pag|pam|pap|pcd|pdc|pih|pl|pms|pt|qu|rm|rn|ro|roa-tara|rup|rw|sc|scn
    |sco|se|sg|sgs|sk|sl|sm|sn|so|sq|sr-el|ss|st|stq|su|sv|sw|szl|tet|tk|tl|tn|to|tpi|tr
    |ts|tum|tw|ty|uz|ve|vec|vi|vls|vo|vro|wa|war|wo|xh|yo|za|zea|zu)"""
  val containLanguageLatin = stringMatch(latinRegexStr, "")

  val nonlatinRegexStr: String = """(ab|am|arc|ar|arz|as|ba|be|be-tarask|bg|bh|bn|bo
    |bpy|bxr|chr|ckb|cr|cv|dv|dz|el|fa|gan|glk|got|gu|hak|he|hi|hy|ii|iu|ja|ka|kbd|kk
    |km|kn|ko|koi|krc|ks|ku-arab|kv|ky|lbe|lez|lo|mai|mdf|mhr|mk|ml|mn|mo|mr|mrj|my|myv
    |mzn|ne|new|or|os|pa|pnb|pnt|ps|ru|rue|sa|sah|sd|si|sr|ta|te|tg|th|ti|tt|tyv|udm|ug
    |uk|ur|wuu|xmf|yi|zh|zh-classical|zh-hans|zh-hant|zh-tw|zh-cn|zh-hk|zh-sg)"""
  val containLanguage_NonLatin = stringMatch(nonlatinRegexStr, "")

  def checkContainLanguageLatinNonLatin(str: String): Boolean = {
    var result = false
    var text: String = str
    var result_isLatin: Boolean = false
    var result_isNonLatin: Boolean = false

    if (text != null) {
      text = text.trim()
      text = text.toLowerCase()
      result_isLatin = containLanguageLatin.reset(text).matches()
      result_isNonLatin = containLanguage_NonLatin.reset(text).matches()
    }
    if (result_isLatin == true) { // is matched
      result = true
    } else {
      result = false

    }
    result
  }

  // For conentType:
  def getContentType(action: String): String = {
    var result: String = null
    if (action == null) {
      result = "MISC"
    } else {
      action match {
        case "wbcreateclaim" | "wbsetclaim" | "wbremoveclaims" |
          "wbsetclaimvalue" | "wbsetreference" | "wbremovereferences" |
          "wbsetqualifier" | "wbremovequalifiers" =>
          result = "STATEMENT"
        case "wbsetsitelink" | "wbcreateredirect" | "clientsitelink" |
          "wblinktitles" =>
          result = "SITELINK"
        case "wbsetaliases" | "wbsetdescription" | "wbsetlabel" =>
          result = "TEXT"
        case "wbeditentity" | "wbsetentity" | "special" | "wbcreate" |
          "wbmergeitems" | "rollback" | "undo" | "restore" | "pageCreation" |
          "emptyComment" | "setPageProtection" | "changePageProtection" |
          "removePageProtection" | "unknownCommentType" | "null" | "" =>
          result = "MISC"
        case _ =>
          result = "MISC"
      }
    }
    result
  }

  def extractRevisionLanguage(comment: String): String = {

    var langeType = "NA"
    val flag = checkCommentNormalOrNot(comment)

    if (flag == true) { // it is normal comment

      val start_point: Int = comment.indexOf("|")
      val end_point: Int = comment.indexOf("*/")
      if (start_point != -1 && end_point != -1) {
        val language = comment.substring(start_point + 1, end_point)
        if (language.nonEmpty) {
          langeType = language.trim()
        } else {
          langeType = "NA"
        }
      } else {
        langeType = "NA"
      }
    } else {
      langeType = "NA"
    }
    langeType.trim()
  }
}
