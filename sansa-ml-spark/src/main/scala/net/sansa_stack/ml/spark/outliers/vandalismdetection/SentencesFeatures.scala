package net.sansa_stack.ml.spark.outliers.vandalismdetection

class SentencesFeatures extends Serializable {

  def RoundDouble(va: Double): Double = {

    //    number = Math.round(number * 100)
    //    number = number / 100

    val rounded: Double = Math.round(va * 10000).toDouble / 10000

    rounded

  }

  //1.comment tail Lenght  Action subaction param+ tail
  def CommentTailLenght(Full_Comment_Str: String): Integer = {
    val parsedCommment_OBJ = new CommentProcessor()
    val commentTail_Str = parsedCommment_OBJ.Extract_CommentTail(Full_Comment_Str)
    val commentTail_Length = commentTail_Str.length()
    commentTail_Length

  }
  // similarity  between the comment ( suffix of the comment = Tail ) where the comment is normal comment /* .........*/ or  /* .........
  // e.g This comment includes wb...sitelink
  //1-we have to be sure the comment is normal comment take the form /* ........./*
  //2-Next step: we check the Action part if it includes a sitelink word or not.
  //3-we compare the suffix in this case to  site link with pay attention to  the same language.

  // we check the type of Normal comment if it contains Aliases  .
  def extract_CommentAliases_LanguageType(Full_Comment_Str: String): String = {

    var langeType = ""
    var suffix = ""

    val parsedCommment_OBJ = new CommentProcessor()
    val flag = parsedCommment_OBJ.Check_CommentNormal_Or_Not(Full_Comment_Str)

    if (flag == true) { // it is normal comment

      val sitelink_Word = Full_Comment_Str.contains("aliases")

      if (sitelink_Word == true) { // language class is between | and */( e.g | enwiki */)

        val start_point: Int = Full_Comment_Str.indexOf("|")
        val end_point: Int = Full_Comment_Str.indexOf("*/")
        if (start_point != -1 && end_point != -1) {

          val language = Full_Comment_Str.substring(start_point + 1, end_point)
          if (language.nonEmpty) {
            langeType = language.trim()
          } else {
            langeType = "NA"
          }
        }

        val SuffixCommment_OBJ = new CommentProcessor()
        val suffixComment = SuffixCommment_OBJ.Extract_CommentTail(Full_Comment_Str)

        if (suffixComment != "NA") {
          suffix = suffixComment.trim()

        }

      }

    }

    suffix.trim() + "_" + langeType.trim()

  }

  // we check the type of Normal comment if it contains Description  .
  def extract_CommentDescription_LanguageType(Full_Comment_Str: String): String = {

    var langeType = ""
    var suffix = ""

    val parsedCommment_OBJ = new CommentProcessor()
    val flag = parsedCommment_OBJ.Check_CommentNormal_Or_Not(Full_Comment_Str)

    if (flag == true) { // it is normal comment

      val sitelink_Word = Full_Comment_Str.contains("description")

      if (sitelink_Word == true) { // language class is between | and */( e.g | enwiki */)

        val start_point: Int = Full_Comment_Str.indexOf("|")
        val end_point: Int = Full_Comment_Str.indexOf("*/")
        if (start_point != -1 && end_point != -1) {

          val language = Full_Comment_Str.substring(start_point + 1, end_point)
          if (language.nonEmpty) {
            langeType = language.trim()
          } else {
            langeType = "NA"
          }
        }

        val SuffixCommment_OBJ = new CommentProcessor()
        val suffixComment = SuffixCommment_OBJ.Extract_CommentTail(Full_Comment_Str)

        if (suffixComment != "NA") {
          suffix = suffixComment.trim()

        }

      }

    }

    suffix.trim() + "_" + langeType.trim()

  }

  // we check the type of Normal comment if it contains label  .
  def extract_CommentLabel_LanguageType(Full_Comment_Str: String): String = {

    var langeType = ""
    var suffix = ""

    val parsedCommment_OBJ = new CommentProcessor()
    val flag = parsedCommment_OBJ.Check_CommentNormal_Or_Not(Full_Comment_Str)

    if (flag == true) { // it is normal comment

      val sitelink_Word = Full_Comment_Str.contains("label")

      if (sitelink_Word == true) { // language class is between | and */( e.g | en */)

        val start_point: Int = Full_Comment_Str.indexOf("|")
        val end_point: Int = Full_Comment_Str.indexOf("*/")
        if (start_point != -1 && end_point != -1) {

          val language = Full_Comment_Str.substring(start_point + 1, end_point)
          if (language.nonEmpty) {
            langeType = language.trim()
          } else {
            langeType = "NA"
          }
        }

        val SuffixCommment_OBJ = new CommentProcessor()
        val suffixComment = SuffixCommment_OBJ.Extract_CommentTail(Full_Comment_Str)

        if (suffixComment != "NA") {
          suffix = suffixComment.trim()

        }

      }

    }

    langeType.trim()

  }

  // we check the type of Normal comment if it contains Sitelink
  def extract_CommentSiteLink_LanguageType(Full_Comment_Str: String): String = {

    var langeType = ""
    val parsedCommment_OBJ = new CommentProcessor()
    val flag = parsedCommment_OBJ.Check_CommentNormal_Or_Not(Full_Comment_Str)

    if (flag == true) { // it is normal comment

      val sitelink_Word = Full_Comment_Str.contains("sitelink")
      if (sitelink_Word == true) { // language class is between | and */( e.g | enwiki */)
        val start_point: Int = Full_Comment_Str.indexOf("|")
        val end_point: Int = Full_Comment_Str.indexOf("*/")
        if (start_point != -1 && end_point != -1) {
          val language = Full_Comment_Str.substring(start_point + 1, end_point)
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