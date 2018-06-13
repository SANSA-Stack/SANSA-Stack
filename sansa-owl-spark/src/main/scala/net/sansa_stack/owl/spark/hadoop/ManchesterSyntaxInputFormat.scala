package net.sansa_stack.owl.spark.hadoop

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.{FileSplit, InputSplit, JobConf, RecordReader, Reporter, TextInputFormat}
import org.apache.hadoop.util.LineReader
import org.semanticweb.owlapi.manchestersyntax.parser.ManchesterOWLSyntax


class ManchesterSyntaxRecordReader(job: Configuration, split: FileSplit) extends RecordReader[LongWritable, Text] {
  private val file: Path = split.getPath
  private val fs: FileSystem = file.getFileSystem(job)
  private val fileIn: FSDataInputStream = fs.open(file)
  private var pos: Long = split.getStart
  private val start: Long = split.getStart
  private val end: Long = start + split.getLength
  private var currentRecord: String = null
  fileIn.seek(start)
  private val lineReader: LineReader = new LineReader(fileIn)
  private var readAhead: String = null
  private var bytesReadAhead = 0
  private var firstRecord = true

  val sectionKeywords: Array[String] = Array(
    ManchesterOWLSyntax.CLASS,
    ManchesterOWLSyntax.OBJECT_PROPERTY,
    ManchesterOWLSyntax.DATA_PROPERTY,
    ManchesterOWLSyntax.ANNOTATION_PROPERTY,
    ManchesterOWLSyntax.INDIVIDUAL,
    ManchesterOWLSyntax.EQUIVALENT_CLASSES,
    ManchesterOWLSyntax.DISJOINT_CLASSES,
    ManchesterOWLSyntax.EQUIVALENT_PROPERTIES,
    ManchesterOWLSyntax.DISJOINT_PROPERTIES,
    ManchesterOWLSyntax.SAME_INDIVIDUAL,
    ManchesterOWLSyntax.DIFFERENT_INDIVIDUALS,
    ManchesterOWLSyntax.DATATYPE,
    ManchesterOWLSyntax.ONTOLOGY,
    ManchesterOWLSyntax.PREFIX,
    ManchesterOWLSyntax.IMPORT
  ).map(_.toString)

  override def next(key: LongWritable, value: Text): Boolean = {
    key.set(pos)

    currentRecord = readNextRecord
    if (currentRecord == null) {
      value.set("")
    } else {
      value.set(currentRecord)
    }

    currentRecord != null
  }

  override def getProgress: Float = {
    if (start == end) 0.0f
    else Math.min(1.0f, (pos - start) / (end - start).toFloat)
  }

  override def getPos: Long = pos

  override def createKey(): LongWritable = new LongWritable()

  override def close(): Unit = {
    fileIn.close()
  }

  override def createValue(): Text = new Text()

  private def readNextRecord: String = {
    val tmp = new Text()
    var lines = Array[String]()

    if (firstRecord) {
      firstRecord = false
      var bytesRead = lineReader.readLine(tmp)
      pos += bytesRead

      lines = lines :+ tmp.toString
      tmp.clear()

      while (!isBeginningOfSection(readAhead) && pos < end) {
        bytesRead = lineReader.readLine(tmp)
        pos += bytesReadAhead
        bytesReadAhead = bytesRead
        if (readAhead != null) lines = lines :+ readAhead
        readAhead = tmp.toString
        tmp.clear()
      }

      /* If we're not in the first split and started reading, what's in lines
       * now might be something like
       * [
       *   "ain:",
       *   "      <http://ex.com/bar#Cls1>",
       *   "",
       *   "  Range:",
       *   "      <http://ex.com/bar#Cls2>"
       * ]
       *
       * i.e. we're somewhere inside a frame. Thus it is next checked whether
       * the first line actually starts with a valid section keyword. If so,
       * lines will be converted to a string and returned. If not, lines will
       * be discarded and we will take the next if-else path.
       */
      if (isBeginningOfSection(lines(0))) return lines.mkString("\n")
      else lines = Array[String]()
    }

    if (pos >= end) {
      null

    } else {
      var bytesRead = 0
      do {
        bytesRead = lineReader.readLine(tmp)
        pos += bytesReadAhead
        bytesReadAhead = bytesRead
        lines = lines :+ readAhead
        readAhead = tmp.toString
        tmp.clear()
      } while (!isBeginningOfSection(readAhead) && (bytesRead > 0))

      lines.mkString("\n")
    }
  }

  private def isBeginningOfSection(line: String): Boolean = {
    if (line == null || line.isEmpty) return false

    for (kw <- sectionKeywords) {
      if (line.trim.startsWith(kw)) return true
    }
    false
  }
}


/**
  * Same as TextInputFormat, except that objects of this class will call a
  * different record reader (ManchesterSyntaxRecordReader).
  */
class ManchesterSyntaxInputFormat extends TextInputFormat {
  override def getRecordReader(
                                split: InputSplit, job: JobConf, reporter: Reporter): RecordReader[LongWritable, Text] = {

    new ManchesterSyntaxRecordReader(job, split.asInstanceOf[FileSplit])
  }
}