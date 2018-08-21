package net.sansa_stack.ml.spark.outliers.vandalismdetection

class StatementFeatures extends Serializable {

  def getProperty(comment: String): String = {
    var result: String = null
    if (comment != null) {
      val pattern: String = "[[Property:"
      val index1: Int = comment.indexOf(pattern)

      val index2: Int = comment.indexOf("]]", index1 + pattern.length)

      if (index1 != -1 && index2 != -1) {
        result = comment.substring(index1 + pattern.length, index2)
      }
    }
    result
  }
  def getDataValue(comment: String): String = {
    var result: String = null
    if (comment != null) {
      val antiPattern: String = "]]: [[Q"
      if (!comment.contains(antiPattern)) {
        val pattern: String = "]]: "
        val index1: Int = comment.indexOf(pattern)
        if (index1 != -1) {
          result = comment.substring(index1 + pattern.length)
        }
      }
    }
    result
  }
  def getItemValue(comment: String): String = {
    var result: String = null
    if (comment != null) {
      val pattern: String = "]]: [[Q"
      val index1: Int = comment.indexOf(pattern)
      val index2: Int = comment.indexOf("]]", index1 + pattern.length)
      if (index1 != -1 && index2 != -1) {
        result = comment.substring(index1 + pattern.length, index2)
      }
    }
    result
  }
}
