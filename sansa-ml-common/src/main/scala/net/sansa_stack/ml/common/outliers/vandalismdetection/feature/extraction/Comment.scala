package net.sansa_stack.ml.common.outliers.vandalismdetection.feature.extraction

import java.util.regex.{ Matcher, Pattern }

import org.slf4j.{ Logger, LoggerFactory }

object Comment extends Serializable {

  val logger: Logger = LoggerFactory.getLogger(getClass)

  // Some Operations related to  Comment Parsing : "Parsed Comments"
  val ROBUST_ROLLBACK_PATTERN: Pattern = Pattern.compile(
    ".*\\bReverted\\s*edits\\s*by\\s*\\[\\[Special:Contributions\\/([^\\|\\]]*)\\|.*",
    Pattern.CASE_INSENSITIVE | Pattern.DOTALL)

  val PRECISE_ROLLBACK_PATTERN: Pattern = Pattern.compile(
    "^Reverted edits by \\[\\[Special:Contributions\\/([^\\|\\]]*)\\|\\1\\]\\] \\(\\[\\[User talk:\\1\\|talk\\]\\]\\) to last revision by \\[\\[User:([^\\|\\]]*)\\|\\2\\]\\]$")
  val ROBUST_UNDO_PATTERN: Pattern = Pattern.compile(
    ".*\\b(Undo|Undid)\\b.*revision\\s*(\\d+).*",
    Pattern.CASE_INSENSITIVE | Pattern.DOTALL)

  val PRECISE_UNDO_PATTERN: Pattern = Pattern.compile(
    ".*\\b(Undo|Undid) revision (\\d+) by \\[\\[Special:Contributions\\/([^|]*)\\|\\3\\]\\] \\(\\[\\[User talk:\\3\\|talk\\]\\]\\).*")

  val ROBUST_RESTORE_PATTERN: Pattern = Pattern.compile(
    ".*\\bRestored?\\b.*revision\\s*(\\d+).*",
    Pattern.CASE_INSENSITIVE | Pattern.DOTALL)

  val PRECISE_RESTORE_PATTERN: Pattern = Pattern.compile(
    ".*\\bRestored? revision (\\d+) by \\[\\[Special:Contributions\\/([^|]*)\\|\\2\\]\\].*")
  var text: String = ""
  var action1: String = ""
  var action2: String = ""
  var parameters: Array[String] = Array.ofDim[String](0)
  var suffixComment: String = ""
  var property: String = ""
  var dataValue: String = ""
  var itemValue: String = ""

  def parseComment(comment: String): String = {
    this.text = comment
    if (comment != null) {
      if (isRollback(comment)) {
        action1 = "rollback"
      } else if (isUndo(comment)) {
        action1 = "undo"
      } else if (isRestore(comment)) {
        action1 = "restore"
      } else if (isPageCreation(comment)) {
        action1 = "pageCreation"
      } else if ("" == comment) {
        action1 = "emptyComment"
      } else if (isSetPageProtection(comment)) {
        action1 = "setPageProtection"
      } else if (isChangePageProtection(comment)) {
        action1 = "changePageProtection"
      } else if (isRemovePageProtection(comment)) {
        action1 = "removePageProtection"
      } else {
        val result: Boolean = parseNormalComment(comment)
        if (result == false) {
          action1 = "unknownCommentType"
          logger.debug("unknown comment type: " + comment)
        }
      }
    }

    action1
  }

  // Used in  Revision Action - SubAction Features
  def extractActionsFromComments(comment: String): String = {

    var actions = ""
    if (comment != null) {
      if (isRollback(comment)) {
        actions = "rollback" + "_" + "NA"
      } else if (isUndo(comment)) {
        actions = "undo" + "_" + "NA"
      } else if (isRestore(comment)) {
        actions = "restore" + "_" + "NA"
      } else if (isPageCreation(comment)) {
        actions = "pageCreation" + "_" + "NA"
      } else if ("" == comment) {
        actions = "emptyComment" + "_" + "NA"
      } else if (isSetPageProtection(comment)) {
        actions = "setPageProtection" + "_" + "NA"
      } else if (isChangePageProtection(comment)) {
        actions = "changePageProtection" + "_" + "NA"
      } else if (isRemovePageProtection(comment)) {
        actions = "removePageProtection" + "_" + "NA"
      } else {

        actions = extractActionsOfNormalComment(comment)

      }
    }
    actions
  }

  // Helper for Revision Features:  extract Action- subaction from comment:
  def extractActionsOfNormalComment(comment: String): String = {

    var result: Boolean = false
    var result_Str = ""
    var suffixComment = ""
    var Action1 = ""
    var Action2 = ""
    var Param = ""
    var parameters: Array[String] = Array.ofDim[String](0)
    var asterisk_Start = 0 // == /*
    var asterisk_End = 0 // == */
    var colon = 0
    if (comment != null) {
      val check_asterisk_Start = comment.contains("/*")
      if (check_asterisk_Start == true) {

        // place the symbol(/*) in the string comment
        asterisk_Start = comment.indexOf("/*")

        val check_asterisk_End = comment.contains("*/")

        if (check_asterisk_End == true) {

          asterisk_End = comment.indexOf("*/")

          // we add 2 to avoid print /*
          var ActionsParams_string = comment.substring(asterisk_Start + 2, asterisk_End)
          result_Str = ActionsParams_string
        } else {
          // we add 2 to avoid print /*
          asterisk_End = comment.length()
          var ActionsParams_string = comment.substring(asterisk_Start + 2, asterisk_End)
          result_Str = ActionsParams_string

        }

        val str_colon: Boolean = comment.contains(":")

        if (str_colon == true) {
          colon = comment.indexOf(":")

          // denotes the end of action1 or action2 respectively
          var actionsEnd: Int = 0
          if (colon != -1 && colon < asterisk_End) {

            actionsEnd = colon
          } else { actionsEnd = asterisk_End }

          val str_hyphenPos = comment.contains("-")
          val hyphenPos: Int = comment.indexOf("-")

          if (str_hyphenPos == true) {

            // Does the action consist of two parts?
            if (hyphenPos > -1 && hyphenPos < actionsEnd) {
              Action1 = comment.substring(asterisk_Start + 3, hyphenPos).trim()
              Action2 = comment.substring(hyphenPos + 1, actionsEnd).trim()

            }

          } else {

            Action1 = comment.substring(asterisk_Start + 3, colon).trim()
            Action2 = "NA"

          }
        }
      }
    }
    Action1 + "_" + Action2
  }

  // Using in character , word and sentences Features :  suffix =  CommentTail from Comment
  def extractCommentTail(comment: String): String = {
    var suffixComment = "NA"

    var asterisk_Start = 0 // == /*
    var asterisk_End = 0 // == */
    var colon = 0

    if (comment != null) {
      val check_asterisk_Start = comment.contains("/*")
      if (check_asterisk_Start == true) {
        // place the symbol(/*) in the string comment
        asterisk_Start = comment.indexOf("/*")

        val check_asterisk_End = comment.contains("*/")
        if (check_asterisk_End == true) {

          asterisk_End = comment.indexOf("*/")
          val Checkempty_suffixComment = comment.substring(asterisk_End + 2)
          if (Checkempty_suffixComment != "") {
            suffixComment = Checkempty_suffixComment

          } else {
            suffixComment = "NA".trim()
          }

          var ActionsParams_string = comment.substring(asterisk_Start + 2, asterisk_End)
        } else {
          asterisk_End = comment.length()
          var ActionsParams_string = comment.substring(asterisk_Start + 2, asterisk_End)
          suffixComment = comment.trim() // instead of "NA"
        }

        val str_colon: Boolean = comment.contains(":")
        if (str_colon == true) {
          colon = comment.indexOf(":")

          // denotes the end of action1 or action2 respectively
          var actionsEnd: Int = 0
          if (colon != -1 && colon < asterisk_End) {

            actionsEnd = colon
          } else { actionsEnd = asterisk_End }

        }

      }
    } else { suffixComment = "NA" }

    suffixComment
  }

  // extract Action2 from Comment:
  def extractAction2(comment: String): String = {

    var result: Boolean = false
    var result_Str = ""
    var suffixComment = ""
    var Action1 = ""
    var Action2 = ""
    var Param = ""
    var parameters: Array[String] = Array.ofDim[String](0)
    var asterisk_Start = 0 // == /*
    var asterisk_End = 0 // == */
    var colon = 0
    if (comment != null) {
      val check_asterisk_Start = comment.contains("/*")
      if (check_asterisk_Start == true) {

        // place the symbol(/*) in the string comment
        asterisk_Start = comment.indexOf("/*")

        val check_asterisk_End = comment.contains("*/")

        if (check_asterisk_End == true) { // for end

          asterisk_End = comment.indexOf("*/")

          // we add 2 to avoid print /*
          var ActionsParams_string = comment.substring(asterisk_Start + 2, asterisk_End)
          result_Str = ActionsParams_string
        } else {
          // we add 2 to avoid print /*
          asterisk_End = comment.length()
          var ActionsParams_string = comment.substring(asterisk_Start + 2, asterisk_End)
          result_Str = ActionsParams_string

        }

        val str_colon: Boolean = comment.contains(":")

        if (str_colon == true) {
          colon = comment.indexOf(":")

          // denotes the end of action1 or action2 respectively
          var actionsEnd: Int = 0
          if (colon != -1 && colon < asterisk_End) {

            actionsEnd = colon
          } else { actionsEnd = asterisk_End }

          val str_hyphenPos = comment.contains("-")
          val hyphenPos: Int = comment.indexOf("-")

          if (str_hyphenPos == true) {

            // Does the action consist of two parts?
            if (hyphenPos > -1 && hyphenPos < actionsEnd) {
              Action1 = comment.substring(asterisk_Start + 3, hyphenPos).trim()
              Action2 = comment.substring(hyphenPos + 1, actionsEnd).trim()

            }

          } else {

            Action1 = comment.substring(asterisk_Start + 3, colon).trim()
            Action2 = "NA"
          }
        }
      }
    }
    Action2
  }

  // extract Action- subaction from comment:
  def extractActionsRevisionFromNormalComment(comment: String): String = {

    var result: Boolean = false
    var result_Str = ""
    var suffixComment = ""
    var Action1 = ""
    var Action2 = ""
    var Param = ""
    var parameters: Array[String] = Array.ofDim[String](0)
    var asterisk_Start = 0 // == /*
    var asterisk_End = 0 // == */
    var colon = 0
    if (comment != null) {
      val check_asterisk_Start = comment.contains("/*")
      if (check_asterisk_Start == true) {

        // place the symbol(/*) in the string comment
        asterisk_Start = comment.indexOf("/*")

        val check_asterisk_End = comment.contains("*/")

        if (check_asterisk_End == true) { // for end

          asterisk_End = comment.indexOf("*/")

          // we add 2 to avoid print /*
          var ActionsParams_string = comment.substring(asterisk_Start + 2, asterisk_End)
          result_Str = ActionsParams_string
        } else {
          // we add 2 to avoid print /*
          asterisk_End = comment.length()
          var ActionsParams_string = comment.substring(asterisk_Start + 2, asterisk_End)
          result_Str = ActionsParams_string

        }

        val str_colon: Boolean = comment.contains(":")

        if (str_colon == true) {
          colon = comment.indexOf(":")

          // denotes the end of action1 or action2 respectively
          var actionsEnd: Int = 0
          if (colon != -1 && colon < asterisk_End) {

            actionsEnd = colon
          } else { actionsEnd = asterisk_End }

          val str_hyphenPos = comment.contains("-")
          val hyphenPos: Int = comment.indexOf("-")

          if (str_hyphenPos == true) {

            // Does the action consist of two parts?
            if (hyphenPos > -1 && hyphenPos < actionsEnd) {
              Action1 = comment.substring(asterisk_Start + 3, hyphenPos).trim()
              Action2 = comment.substring(hyphenPos + 1, actionsEnd).trim()

            }

          } else {

            Action1 = comment.substring(asterisk_Start + 3, colon).trim()
            Action2 = "NA"
          }
        }
      }
    }
    Action1 + "-" + Action2
  }

  // extract the Params from Comment :
  def actionParamSuffixNormalCommentMap(comment: String): String = {

    var result: Boolean = false
    var result_Str = ""
    var suffixComment = ""
    var Action1 = ""
    var Action2 = ""
    var Param = ""
    var parameters: Array[String] = Array.ofDim[String](0)
    var asterisk_Start = 0 // == /*
    var asterisk_End = 0 // == */
    var colon = 0
    if (comment != null) {
      val check_asterisk_Start = comment.contains("/*")
      if (check_asterisk_Start == true) {

        // place the symbol(/*) in the string comment
        asterisk_Start = comment.indexOf("/*")

        val check_asterisk_End = comment.contains("*/")

        if (check_asterisk_End == true) { // for end

          asterisk_End = comment.indexOf("*/")

          // we add 2 to avoid print /*
          var ActionsParams_string = comment.substring(asterisk_Start + 2, asterisk_End)
          result_Str = ActionsParams_string
        } else {
          // we add 2 to avoid print /*
          asterisk_End = comment.length()
          var ActionsParams_string = comment.substring(asterisk_Start + 2, asterisk_End)
          result_Str = ActionsParams_string
        }

        val str_colon: Boolean = comment.contains(":")

        if (str_colon == true) {
          colon = comment.indexOf(":")

          // denotes the end of action1 or action2 respectively
          var actionsEnd: Int = 0
          if (colon != -1 && colon < asterisk_End) {

            actionsEnd = colon
          } else { actionsEnd = asterisk_End }

          val str_hyphenPos = comment.contains("-")
          val hyphenPos: Int = comment.indexOf("-")

          if (str_hyphenPos == true) {

            // Does the action consist of two parts?
            if (hyphenPos > -1 && hyphenPos < actionsEnd) {
              Action1 = comment.substring(asterisk_Start + 3, hyphenPos).trim()
              Action2 = comment.substring(hyphenPos + 1, actionsEnd).trim()
            }

          } else {
            Action1 = comment.substring(asterisk_Start + 3, colon).trim()
          }
        }
      }
    }
    suffixComment
  }

  def extractParams(comment: String): String = {

    var result: Boolean = false
    var result_Str = ""
    var suffixComment = ""
    var Action1 = ""
    var Action2 = ""
    var Param = ""
    var parameters: Array[String] = Array.ofDim[String](0)

    var asterisk_Start = 0
    var asterisk_End = 0
    var colon = 0
    if (comment != null) {

      val check_asterisk_Start = comment.contains("/*")
      if (check_asterisk_Start == true) {

        // place the symbol(/*) in the string comment
        asterisk_Start = comment.indexOf("/*")

        val check_asterisk_End = comment.contains("*/")

        if (check_asterisk_End == true) { // for end

          asterisk_End = comment.indexOf("*/")

          var ActionsParams_string = comment.substring(asterisk_Start + 2, asterisk_End)
          result_Str = ActionsParams_string

        } else {

          asterisk_End = comment.length()
          var ActionsParams_string = comment.substring(asterisk_Start + 2, asterisk_End)
          result_Str = ActionsParams_string

        }

        val str_colon: Boolean = comment.contains(":")

        if (str_colon == true) {
          colon = comment.indexOf(":")

          // denotes the end of action1 or action2 respectively
          var actionsEnd: Int = 0
          if (colon != -1 && colon < asterisk_End) {

            actionsEnd = colon
          } else { actionsEnd = asterisk_End }

          val str_hyphenPos = comment.contains("-")
          val hyphenPos: Int = comment.indexOf("-")

          if (str_hyphenPos == true) {

            // Does the action consist of two parts?
            if (hyphenPos > -1 && hyphenPos < actionsEnd) {
              Action1 = comment.substring(asterisk_Start + 3, hyphenPos).trim()
              Action2 = comment.substring(hyphenPos + 1, actionsEnd).trim()
            }

          } else {
            Action1 = comment.substring(asterisk_Start + 3, colon).trim()
          }
        }

        if (suffixComment != "") {

        }
        if (colon != -1 && colon < asterisk_End) {
          var tmp: String = comment.substring(colon + 1, asterisk_End)
          tmp = tmp.trim()
          parameters = tmp.split("\\|")
          var counter = 0
          for (x <- parameters) {
            result_Str = result_Str + "Param" + counter + "::" + x
            counter = counter + 1
          }

        } else {

        }
      }
      for (i <- 0 until parameters.length) {
        parameters(i) = trim(parameters(i))
      }

    }
    parameters(0)
  }

  // "Thecommentis" + result_Str + "&&&" + "Ac1:" + Action1 + "&&&" + "Ac2 :" + Action2 + "&&&" + "SF:" + suffixComment
  def isRollback(comment: String): Boolean = {
    var result: Boolean = false
    if (comment != null) {
      val tmp: String = comment.trim()
      result = ROBUST_ROLLBACK_PATTERN.matcher(tmp).matches()
      if (result != PRECISE_ROLLBACK_PATTERN.matcher(tmp).matches()) {
        logger.debug("Robust but not precise rollback match (result = " + result + ") : " + tmp)
      }
    }
    result
  }

  def isUndo(comment: String): Boolean = {
    var result: Boolean = false
    if (comment != null) {
      val tmp: String = comment.trim()
      result = ROBUST_UNDO_PATTERN.matcher(comment).matches()
      if (logger.isDebugEnabled) {
        if (result != PRECISE_UNDO_PATTERN.matcher(tmp).matches()) {
          logger.debug(
            "Robust but not precise undo match (result = " + result +
              ") : " +
              tmp)
        }
      }
    }
    result
  }

  def isRestore(comment: String): Boolean = {
    var result: Boolean = false
    if (comment != null) {
      val tmp: String = comment.trim()
      result = ROBUST_RESTORE_PATTERN.matcher(tmp).matches()
      if (logger.isDebugEnabled) {
        if (result != PRECISE_RESTORE_PATTERN.matcher(tmp).matches()) {
          logger.debug(
            "Robust but not precise restore match (result = " + result +
              ") : " +
              tmp)
        }
      }
    }
    result
  }

  def isPageCreation(comment: String): Boolean = {
    var result: Boolean = false
    if (comment != null) {
      val tmp: String = comment.trim()
      result = (tmp.startsWith("Created page"))
    }
    result
  }

  def isSetPageProtection(comment: String): Boolean = {
    var result: Boolean = false
    if (comment != null) {
      val tmp: String = comment.trim()
      result = tmp.startsWith("Protected")
    }
    result
  }

  def isChangePageProtection(comment: String): Boolean = {
    var result: Boolean = false
    if (comment != null) {
      val tmp: String = comment.trim()
      result = tmp.startsWith("Changed protection")
    }
    result
  }

  def isRemovePageProtection(comment: String): Boolean = {
    var result: Boolean = false
    if (comment != null) {
      val tmp: String = comment.trim()
      result = tmp.startsWith("Removed protection")
    }
    result
  }

  def getRevertedContributor(comment: String): String = {
    var origResult: String = null
    val pattern: String = "[[Special:Contributions/"
    val startIndex: Int = comment.indexOf(pattern)
    val endIndex: Int = comment.indexOf("|")
    if (endIndex > startIndex) {
      origResult = comment.substring(startIndex + pattern.length, endIndex)
    }
    var result: String = "null"
    val matcher: Matcher = ROBUST_ROLLBACK_PATTERN.matcher(comment)
    if (matcher.matches()) {
      result = matcher.group(1)
    }
    if (result != origResult) {
      logger.warn("Difference to original contributor: " + comment)
    }
    result
  }

  def getUndoneRevisionId(comment: String): Long = {
    var result: Long = 0L
    val matcher: Matcher = ROBUST_UNDO_PATTERN.matcher(comment)
    if (matcher.matches()) {
      val str: String = matcher.group(2)
      result = java.lang.Long.parseLong(str)
    } else {
      result = -1
    }
    result
  }

  def getRestoredRevisionId(comment: String): Long = {
    var result: Long = 0L
    val matcher: Matcher = ROBUST_RESTORE_PATTERN.matcher(comment)
    if (matcher.matches()) {
      val str: String = matcher.group(1)
      result = java.lang.Long.parseLong(str)
    } else {
      result = -1
    }
    result
  }

  // Using in Extract_Revision_Language function in Revision Features class
  def checkCommentNormalOrNot(comment: String): Boolean = {

    var result: Boolean = false

    val asterisk_Start: Int = comment.indexOf("/*")
    // Is there something of the form /* ... */?
    if (asterisk_Start != -1) {
      result = true

      var asterisk_End: Int = comment.indexOf("*/", asterisk_Start)
      // Is the closing ... */ missing? (The comment was shortened because it was too long)
      if (asterisk_End == -1) {
        asterisk_End = comment.length
      }

    } else {
      // There is NOT something of the form /* ... */
      suffixComment = comment
      result = false
    }
    result
  }

  //   Parse a comment of the form /* action1-action2: param1, param2, ... */ value
  //   or of the form              /* action1 */ value
  //   @param comment
  //   returns whether it is a normal comment, i.e., it contains /* ...*/
  private def parseNormalComment(comment: String): Boolean = {

    var result: Boolean = false

    val asterisk_Start: Int = comment.indexOf("/*")
    // Is there something of the form /* ... */?
    if (asterisk_Start != -1) {
      result = true

      var asterisk_End: Int = comment.indexOf("*/", asterisk_Start)

      // Is the closing ... */ missing? (The comment was shortened because it was too long)
      if (asterisk_End == -1) {
        asterisk_End = comment.length

        suffixComment = ""
      } else {
        suffixComment = comment.substring(asterisk_End + 2).trim()
        println("lolo2 " + suffixComment)

      }
      val colon: Int = comment.indexOf(":")

      // denotes the end of action1 or action2 respectively
      var actionsEnd: Int = 0
      actionsEnd =
        if (colon != -1 && colon < asterisk_End) colon else asterisk_End
      val hyphenPos: Int = comment.indexOf("-")

      // Does the action consist of two parts?
      if (hyphenPos > -1 && hyphenPos < actionsEnd) {
        action1 = comment.substring(asterisk_Start + 3, hyphenPos)
        action2 = comment.substring(hyphenPos + 1, actionsEnd)
      } else {
        action1 = comment.substring(asterisk_Start + 3, actionsEnd)
      }
      // Are there parameters?
      if (colon != -1 && colon < asterisk_End) {
        var tmp: String = comment.substring(colon + 1, asterisk_End)
        tmp = tmp.trim()
        parameters = tmp.split("\\|")
      }
    } else {
      // There is NOT something of the form /* ... */
      suffixComment = comment
    }
    property = Statement.getProperty(suffixComment)
    dataValue = Statement.getDataValue(suffixComment)
    itemValue = Statement.getItemValue(suffixComment)
    action1 = trim(action1)
    action2 = trim(action2)
    for (i <- 0 until parameters.length) {
      parameters(i) = trim(parameters(i))
    }
    result
  }

  private def trim(str: String): String = {
    var result: String = str
    if (str != null) {
      result = str.trim()
    }
    result
  }

  def getText(): String = text

  def getAction1(): String = action1

  def getAction2(): String = action2

  def getParameters(): Array[String] = parameters

  def getSuffixComment(str: String): String = suffixComment

  def getProperty(): String = property

  def getDataValue(): String = dataValue

  def getItemValue(): String = itemValue

}
