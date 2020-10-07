package net.sansa_stack.owl.flink.hadoop

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FSDataInputStream, Path}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.{FileSplit, InputSplit, JobConf, RecordReader, Reporter, TextInputFormat}
import org.apache.hadoop.util.LineReader


/**
  * A RecordReader implementation which takes care of reading whole OWL axiom
  * expressions in functional syntax from a given file split.
  * The main functionality that distinguishes a FunctionalSyntaxRecordReader
  * from a LineRecordReader is that it checks whether a read line contains the
  * beginning of a multi-line literal like, for example
  *
  *   Annotation(:description "A longer
  *   description running over
  *   several lines")
  *
  */
class FunctionalSyntaxRecordReader(
  job: Configuration, split: FileSplit) extends RecordReader[LongWritable, Text] {

  private val file: Path = split.getPath
  private val fs: FileSystem = file.getFileSystem(job)
  private val fileIn: FSDataInputStream = fs.open(file)
  private var pos: Long = split.getStart
  fileIn.seek(pos)
  private val start: Long = split.getStart
  private val end: Long = start + split.getLength
  private var currentRecord: String = null
  private val lineReader: LineReader = new LineReader(fileIn)
  private var firstLine = true
  private val sectionKeywords: Array[String] = Array(
    "#",
    // ")",  TODO
    "Ontology", "Import", "SubClassOf", "EquivalentClasses", "DisjointClasses",
    "DisjointUnion", "Annotation", "AnnotationAssertion",
    "SubAnnotationPropertyOf", "AnnotationPropertyDomain",
    "AnnotationPropertyRange", "HasKey", "Declaration",
    "InverseObjectProperties", "SubObjectPropertyOf",
    "EquivalentObjectProperties", "DisjointObjectProperties",
    "ObjectPropertyDomain", "ObjectPropertyRange", "FunctionalObjectProperty",
    "InverseFunctionalObjectProperty", "ReflexiveObjectProperty",
    "IrreflexiveObjectProperty", "SymmetricObjectProperty",
    "AsymmetricObjectProperty", "TransitiveObjectProperty", "SubDataPropertyOf",
    "EquivalentDataProperties", "DisjointDataProperties", "DataPropertyDomain",
    "DataPropertyRange", "FunctionalDataProperty", "SameIndividual",
    "DifferentIndividuals", "ClassAssertion", "ObjectPropertyAssertion",
    "NegativeObjectPropertyAssertion", "DataPropertyAssertion",
    "NegativeDataPropertyAssertion", "Prefix"
  )

  /**
    * @return Boolean which determines whether an OWL axiom expression could be
    *         read (true) or not (false)
    */
  override def next(key: LongWritable, value: Text): Boolean = {
    currentRecord = readNextRecord
    key.set(pos)
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

  override def createValue(): Text = new Text()

  override def close(): Unit = {
    fileIn.close()
  }

  /**
    * Reads a new line from this.lineReader and checks whether the quotes in
    * this line are balanced, i.e. whether a string started with a quote
    * character also ended with a quote character. If this is not the case
    * the read line contains a multi-line literal as e.g. in
    *
    *   Annotation(:description "A longer
    *   description running over
    *   several lines")
    *
    * In this case the next lines are read from this.lineReader until the
    * closing quote was found.
    *
    * @return A string containing a single axioms in functional OWL syntax
    */
  private def readNextRecord: String = {
    val record = new Text()

    /* In case this.split is somewhere in the middle of the file, the beginning
     * of the split might not be the beginning of an OWL functional syntax
     * expression. Thus, this case is handled separately checking whether the
     * beginning of the split is a valid beginning ('sectionKeyword') w.r.t.
     * the OWL functional syntax specification.
     *
     * It has to be noted here that this does not cover the case of the last
     * closing parenthesis which would be a valid beginning in some sense but
     * cannot be distinguished from a remainder of an arbitrary line, e.g.
     *                   --- split (n-1) --->|<-- split n ---
     *   "ClassAssertion(bar:Cls1 foo:indivA" ")"
     *
     * But this should not matter since the last closing parenthesis will be
     * discarded in later processing steps anyway.
     */
    if (firstLine) {
      val bytesRead = lineReader.readLine(record)
      firstLine = false
      pos += bytesRead
      var skip = true

      var cntr = 0
      while (skip && cntr < sectionKeywords.length) {
        if (record.toString.trim.startsWith(sectionKeywords(cntr))) skip = false
        cntr += 1
      }

      if (skip) record.clear()  // discard what's read so far and read next line
      else return record.toString  // return what's read
    }

    if (pos >= end) {
      null

    } else {
      var bytesRead: Int = lineReader.readLine(record)
      pos += bytesRead

      var line = record.toString

      while (!quotesAreBalanced(line)) {
        record.clear()
        bytesRead = lineReader.readLine(record)
        pos += bytesRead

        line += "\n"
        line += record.toString
      }

      line
    }
  }

  private def quotesAreBalanced(line: String): Boolean = line.count(_ == '"') % 2 == 0
}


/**
  * An InputFormat class behaves like a TextInputFormat, except
  * that it returns a FunctionalSyntaxRecordReader instead of a
  * LineRecordReader.
  */
class FunctionalSyntaxInputFormat extends TextInputFormat {
  override def getRecordReader(
      split: InputSplit, job: JobConf, reporter: Reporter): RecordReader[LongWritable, Text] = {

    new FunctionalSyntaxRecordReader(job, split.asInstanceOf[FileSplit])
  }
}
