package net.sansa_stack.rdf.spark.io.turtle

import net.sansa_stack.rdf.common.annotation.Experimental
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, FSDataInputStream, Path, Seekable }
import org.apache.hadoop.io.{ LongWritable, Text }
import org.apache.hadoop.io.compress._
import org.apache.hadoop.mapreduce.{ InputSplit, RecordReader, TaskAttemptContext }
import org.apache.hadoop.mapreduce.lib.input.{ CompressedSplitLineReader, FileInputFormat, FileSplit, SplitLineReader }
import org.apache.hadoop.util.LineReader
import org.apache.log4j.Logger
import org.slf4j.LoggerFactory
import util.control.Breaks._

/**
 * @author Lorenz Buehmann
 */
@Experimental
class TurtleRecordReader(val recordDelimiterBytes: Array[Byte]) extends RecordReader[LongWritable, Text] {

  val LOG = LoggerFactory.getLogger(classOf[TurtleRecordReader])

  val MAX_LINE_LENGTH = "mapreduce.input.linerecordreader.line.maxlength"

  var start, end, pos = 0L
  var reader: SplitLineReader = null
  var key = new LongWritable
  var value = new Text

  private var fileIn: FSDataInputStream = null
  private var filePosition: Seekable = null

  private var maxLineLength = 0

  private var isCompressedInput = false
  private var decompressor: Decompressor = null

  override def initialize(inputSplit: InputSplit, context: TaskAttemptContext): Unit = {
    // split position in data (start one byte earlier to detect if
    // the split starts in the middle of a previous record)
    val split = inputSplit.asInstanceOf[FileSplit]
    val job = context.getConfiguration
    maxLineLength = job.getInt(MAX_LINE_LENGTH, Integer.MAX_VALUE)
    start = split.getStart
    end = start + split.getLength

    val file = split.getPath

    // open the file and seek to the start of the split
    val fs = file.getFileSystem(job)
    fileIn = fs.open(file)

    val codec = new CompressionCodecFactory(job).getCodec(file)
    if (null != codec) {
      isCompressedInput = true
      decompressor = CodecPool.getDecompressor(codec)
      if (codec.isInstanceOf[SplittableCompressionCodec]) {
        val cIn = codec.asInstanceOf[SplittableCompressionCodec].createInputStream(fileIn, decompressor, start, end, SplittableCompressionCodec.READ_MODE.BYBLOCK)
        reader = new CompressedSplitLineReader(cIn, job, recordDelimiterBytes)
        start = cIn.getAdjustedStart
        end = cIn.getAdjustedEnd
        filePosition = cIn
      } else {
        reader = new SplitLineReader(codec.createInputStream(fileIn, decompressor), job, recordDelimiterBytes)
        filePosition = fileIn
      }
    } else {
      fileIn.seek(start)
      reader = new SkipLineReader(fileIn, job, recordDelimiterBytes, split.getLength)
      filePosition = fileIn
    }

    // If this is not the first split, we always throw away first record
    // because we always (except the last split) read one extra line in
    // next() method.
    if (start != 0) start += reader.readLine(new Text, 0, maxBytesToConsume(start))
    pos = start
  }

  private def maxBytesToConsume(pos: Long) =
    if (isCompressedInput) Integer.MAX_VALUE
    else Math.max(Math.min(Integer.MAX_VALUE, end - pos), maxLineLength).toInt

  private def getFilePosition: Long = {
    if (isCompressedInput && null != filePosition) filePosition.getPos
    else pos
  }

  override def nextKeyValue(): Boolean = {
    if (key == null) key = new LongWritable
    key.set(pos)
    if (value == null) value = new Text
    var newSize = 0
    // We always read one extra line, which lies outside the upper
    // split limit i.e. (end - 1)
    var break = false
    while (!break && (getFilePosition <= end || reader.needAdditionalRecordAfterSplit)) {
      //      breakable {
      if (pos == 0) {
        newSize = skipUtfByteOrderMark
      } else {
        newSize = reader.readLine(value, maxLineLength, maxBytesToConsume(pos))
        pos += newSize
      }
      //        if(value.toString.startsWith("#")) println("Comment: " + value.toString)
      if ((newSize == 0) || (newSize < maxLineLength)) break = true
      // line too long. try again
      //        LOG.warn("Skipped line of size " + newSize + " at pos " + (pos - newSize))
      //      }
    }
    val ret = if (newSize == 0) {
      key = null
      value = null
      false
    } else {
      true
    }
    ret
  }

  override def getCurrentKey: LongWritable = key

  override def getProgress: Float =
    if (start == end) {
      0.0f
    } else Math.min(1.0f, (pos - start) / (end - start).asInstanceOf[Float])

  override def getCurrentValue: Text = value

  override def close(): Unit = reader.close()

  private def skipUtfByteOrderMark = { // Strip BOM(Byte Order Mark)
    // Text only support UTF-8, we only need to check UTF-8 BOM
    // (0xEF,0xBB,0xBF) at the start of the text stream.
    val newMaxLineLength = Math.min(3L + maxLineLength.toLong, Integer.MAX_VALUE).toInt
    var newSize = reader.readLine(value, newMaxLineLength, maxBytesToConsume(pos))
    // Even we read 3 extra bytes for the first line,
    // we won't alter existing behavior (no backwards incompat issue).
    // Because the newSize is less than maxLineLength and
    // the number of bytes copied to Text is always no more than newSize.
    // If the return size from readLine is not less than maxLineLength,
    // we will discard the current line and read the next line.
    pos += newSize
    var textLength = value.getLength
    var textBytes = value.getBytes
    if ((textLength >= 3) && (textBytes(0) == 0xEF.toByte) && (textBytes(1) == 0xBB.toByte) && (textBytes(2) == 0xBF.toByte)) { // find UTF-8 BOM, strip it.
      LOG.info("Found UTF-8 BOM and skipped it")
      textLength -= 3
      newSize -= 3
      if (textLength > 0) { // It may work to use the same buffer and not do the copyBytes
        textBytes = value.copyBytes
        value.set(textBytes, 3, textLength)
      } else value.clear()
    }
    newSize
  }
}
