package net.sansa_stack.ml.common.outliers.vandalismdetection.feature.extraction

import net.sansa_stack.ml.common.outliers.vandalismdetection.feature.extraction.Comment._

object Sentence extends Serializable {

  // 1.comment tail Lenght  Action subaction param+ tail
  def commentTailLenght(comment: String): Integer =
    extractCommentTail(comment).length()

  // similarity  between the comment ( suffix of the comment = Tail ) where the comment is normal comment /* .........*/ or  /* .........
  // e.g This comment includes wb...sitelink
  // 1-we have to be sure the comment is normal comment take the form /* ........./*
  // 2-Next step: we check the Action part if it includes a sitelink word or not.
  // 3-we compare the suffix in this case to  site link with pay attention to  the same language.

  // we check the type of Normal comment if it contains Aliases  .
  def extractCommentAliasesLanguageType(comment: String): String = {
    var langeType = ""
    var suffix = ""

    val flag = Comment.checkCommentNormalOrNot(comment)

    if (flag == true) { // it is normal comment

      val sitelink_Word = comment.contains("aliases")

      if (sitelink_Word == true) { // language class is between | and */( e.g | enwiki */)

        val start_point: Int = comment.indexOf("|")
        val end_point: Int = comment.indexOf("*/")
        if (start_point != -1 && end_point != -1) {

          val language = comment.substring(start_point + 1, end_point)
          if (language.nonEmpty) {
            langeType = language.trim()
          } else {
            langeType = "NA"
          }
        }

        val suffixComment = Comment.extractCommentTail(comment)

        if (suffixComment != "NA") {
          suffix = suffixComment.trim()
        }
      }
    }
    suffix.trim() + "_" + langeType.trim()
  }

  // we check the type of Normal comment if it contains Description  .
  def extractCommentDescriptionLanguageType(comment: String): String = {

    var langeType = ""
    var suffix = ""

    val flag = Comment.checkCommentNormalOrNot(comment)

    if (flag == true) { // it is normal comment

      val sitelink_Word = comment.contains("description")

      if (sitelink_Word == true) { // language class is between | and */( e.g | enwiki */)

        val start_point: Int = comment.indexOf("|")
        val end_point: Int = comment.indexOf("*/")
        if (start_point != -1 && end_point != -1) {

          val language = comment.substring(start_point + 1, end_point)
          if (language.nonEmpty) {
            langeType = language.trim()
          } else {
            langeType = "NA"
          }
        }

        val suffixComment = Comment.extractCommentTail(comment)

        if (suffixComment != "NA") {
          suffix = suffixComment.trim()
        }
      }
    }
    suffix.trim() + "_" + langeType.trim()
  }

  // we check the type of Normal comment if it contains label  .
  def extractCommentLabelLanguageType(comment: String): String = {

    var langeType = ""
    var suffix = ""

    val flag = Comment.checkCommentNormalOrNot(comment)

    if (flag == true) { // it is normal comment

      val sitelink_Word = comment.contains("label")

      if (sitelink_Word == true) { // language class is between | and */( e.g | en */)

        val start_point: Int = comment.indexOf("|")
        val end_point: Int = comment.indexOf("*/")
        if (start_point != -1 && end_point != -1) {

          val language = comment.substring(start_point + 1, end_point)
          if (language.nonEmpty) {
            langeType = language.trim()
          } else {
            langeType = "NA"
          }
        }

        val suffixComment = Comment.extractCommentTail(comment)

        if (suffixComment != "NA") {
          suffix = suffixComment.trim()
        }
      }
    }
    langeType.trim()
  }

  // we check the type of Normal comment if it contains Sitelink
  def extractCommentSiteLinkLanguageType(comment: String): String = {

    var langeType = ""
    val flag = Comment.checkCommentNormalOrNot(comment)

    if (flag == true) { // it is normal comment

      val sitelink_Word = comment.contains("sitelink")
      if (sitelink_Word == true) { // language class is between | and */( e.g | enwiki */)
        val start_point: Int = comment.indexOf("|")
        val end_point: Int = comment.indexOf("*/")
        if (start_point != -1 && end_point != -1) {
          val language = comment.substring(start_point + 1, end_point)
          if (language.nonEmpty) {
            langeType = language.trim()
          } else {
            langeType = "NA"
          }
        }
      }
    }
    langeType.trim()
  }
}
