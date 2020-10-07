package net.sansa_stack.ml.common.outliers.vandalismdetection.feature.extraction

import org.scalatest.FunSuite

import net.sansa_stack.ml.common.outliers.vandalismdetection.feature.extraction.Comment._

class CommentTests extends FunSuite {

  val comment1 = """Created page with "Country in Northern Europe""""
  val comment2 = "/* wbsetdescription-set:1|de */ Ein Kontinent"

  test("getting action while parsing the comment should match") {
    val action = parseComment(comment1)
    assert(action == "pageCreation")
  }

  test("extracting actions while parsing the comment should match") {
    val action = extractActionsFromComments(comment1)
    assert(action == "pageCreation_NA")
  }

  test("extracting actions while parsing a normal comment should match") {
    val action = extractActionsOfNormalComment(comment2)
    assert(action == "wbsetdescription_set")
  }

  test("extracting tail while parsing the comment should match") {
    val tail = extractCommentTail(comment2)
    assert(tail.trim() == "Ein Kontinent")
  }

  test("extracting action2 while parsing the comment should match") {
    val action2 = extractAction2(comment2)
    assert(action2 == "set")
  }

  test("extracting revision while parsing a normal comment should match") {
    val revision = extractActionsRevisionFromNormalComment(comment2)
    assert(revision == "wbsetdescription-set")
  }

  test("extracting param suffix while parsing a normal comment should match") {
    val paramSuffix = actionParamSuffixNormalCommentMap(comment2)
    assert(paramSuffix == "")
  }

  test("extracting params while parsing the comment should match") {
    val params = extractParams(comment2)
    assert(params == "1")
  }

  test("checking if the action is `rollback` while parsing the comment should match") {
    val action = isRollback(comment1)
    assert(action == false)
  }

  test("checking if the action is `undo` while parsing the comment should match") {
    val action = isUndo(comment1)
    assert(action == false)
  }

  test("checking if the action is `restore` while parsing the comment should match") {
    val action = isRestore(comment1)
    assert(action == false)
  }

  test("checking if the action is `page creation` while parsing the comment should match") {
    val action = isPageCreation(comment1)
    assert(action == true)
  }

  test("checking if the action is `page protection` while parsing the comment should match") {
    val action = isSetPageProtection(comment1)
    assert(action == false)
  }

  test("checking if the action is `change page protection` while parsing the comment should match") {
    val action = isChangePageProtection(comment1)
    assert(action == false)
  }

  test("checking if the action is `remove page protection` while parsing the comment should match") {
    val action = isRemovePageProtection(comment1)
    assert(action == false)
  }

  test("getting a reverted contributor while parsing the comment should match") {
    val action = getRevertedContributor(comment1)
    assert(action == "null")
  }

  test("getting a undo revision id while parsing the comment should match") {
    val action = getUndoneRevisionId(comment1)
    assert(action == -1)
  }

  test("getting a restore revision id while parsing the comment should match") {
    val action = getRestoredRevisionId(comment1)
    assert(action == -1)
  }

  test("checking if a comment is a normal one or not while parsing the comment should match") {
    val action = checkCommentNormalOrNot(comment2)
    assert(action == true)
  }
}
