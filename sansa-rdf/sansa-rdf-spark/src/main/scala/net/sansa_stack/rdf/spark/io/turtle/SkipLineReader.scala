package net.sansa_stack.rdf.spark.io.turtle

import java.io.InputStream

import net.sansa_stack.rdf.common.annotation.Experimental
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, SplitLineReader}
import org.apache.hadoop.util.LineReader

/**
  *
  * @author Lorenz Buehmann
  */
@Experimental
private[turtle] class SkipLineReader(in: FSDataInputStream, conf: Configuration,
                     recordDelimiterBytes: Array[Byte], splitLength: Long)
  extends SplitLineReader(in, conf, recordDelimiterBytes) {

  private var needAdditionalRecord = false
  /** Total bytes read from the input stream. */
  private var totalBytesRead = 0
  private var finished = false
  private var usingCRLF = false

  override def readLine(str: Text, maxLineLength: Int, maxBytesToConsume: Int): Int = {
    var bytesRead = 0
    if (!finished) { // only allow at most one more record to be read after the stream
      // reports the split ended
      if (totalBytesRead > splitLength) finished = true
      bytesRead = super.readLine(str, maxLineLength, maxBytesToConsume)
    }
    bytesRead
  }

  def skipComments(in: InputStream): Unit = {
    var i = 0
    var break = false
    while (!break && {i = in.read; i != -1}) { // Reading begin
      println(s"Char: ${i.asInstanceOf[Char]}")

      if (i == '\n') { // If a line in .txt file ends.
        break = true
        println("BREAK")
      }
      if (i.asInstanceOf[Char] == '#') { // || (char) i == '0')
        println("COMMENT")
      }
      totalBytesRead += 1
    }
    println("RETURN")
  }

  override def fillBuffer(in: InputStream, buffer: Array[Byte], inDelimiter: Boolean): Int = {
    var maxBytesToRead = buffer.length
    if (totalBytesRead < splitLength) {
      val leftBytesForSplit = splitLength - totalBytesRead
      // check if leftBytesForSplit exceed Integer.MAX_VALUE
      if (leftBytesForSplit <= Integer.MAX_VALUE) maxBytesToRead = Math.min(maxBytesToRead, leftBytesForSplit.toInt)
    }
    var firstChar = in.read()
    if(firstChar == '#') {
//      println("COMMENT")
      var break = false
      var i = 0
      while (!break && {i = in.read; i != -1}) {
//        println(s"${i.asInstanceOf[Char]}")
        if (i == '\n') {
          break = true
//          println("BREAK")
        }
      }
      firstChar = in.read()
    }
    buffer(0) = firstChar.toByte
    val bytesRead = in.read(buffer, 1, maxBytesToRead - 1)

//    skipComments(in)
//    val bytesRead = in.read(buffer, 0, maxBytesToRead)

    // If the split ended in the middle of a record delimiter then we need
    // to read one additional record, as the consumer of the next split will
    // not recognize the partial delimiter as a record.
    // However if using the default delimiter and the next character is a
    // linefeed then next split will treat it as a delimiter all by itself
    // and the additional record read should not be performed.
    if (totalBytesRead == splitLength && inDelimiter && bytesRead > 0) if (usingCRLF) needAdditionalRecord = buffer(0) != '\n'
    else needAdditionalRecord = true
    if (bytesRead > 0) totalBytesRead += bytesRead

    bytesRead
  }

  override def needAdditionalRecordAfterSplit(): Boolean = !finished && needAdditionalRecord

 // override def unsetNeedAdditionalRecordAfterSplit(): Unit = {needAdditionalRecord = false}
}
