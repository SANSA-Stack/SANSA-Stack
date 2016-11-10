package net.sansa_stack.owl.common.parsing

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{BlockLocation, FSDataInputStream, FileStatus,
  FileSystem, LocatedFileStatus, Path}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.{FileInputFormat, FileSplit, InputSplit,
  JobConf, JobConfigurable, RecordReader, Reporter}
import org.apache.hadoop.net.NetworkTopology
import org.apache.hadoop.util.LineReader


/**
  * A RecordReader implementations which takes care of reading whole OWL axiom
  * expressions in functional syntax from a given file split.
  * The main functionality that distinguishes a FunctionalSyntaxRecordReader
  * from a LineRecordReader is that it checks whether a read line contains the
  * beginning of a multi-line literal like, for example
  *
  *   Annotation(:description "A longer
  *   description runnig over
  *   several lines")
  *
  */
class FunctionalSyntaxRecordReader(
    job: Configuration, split: FileSplit) extends RecordReader[LongWritable, Text] {

  private val file: Path = split.getPath
  private val fs: FileSystem = file.getFileSystem(job)
  private val fileIn: FSDataInputStream = fs.open(file)
  private var pos: Long = 0
  private val start: Long = split.getStart
  private val end: Long = start + split.getLength
  private var currentRecord: String = null
  private val lineReader: LineReader = new LineReader(fileIn)

  /**
    * @return Boolean which determines whether an OWL axiom expression could be
    *         read (true) or not (false)
    */
  override def next(key: LongWritable, value: Text): Boolean = {
    key.set(pos)

    currentRecord = readNextRecord
    if (currentRecord == null)
      value.set("")
    else
      value.set(currentRecord)

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
    * this line are balanced, i.e. whether a sting started with a quote
    * character also ended with a quote character. If this is not the case
    * the read line contains a multi-line literal as e.g. in
    *
    *   Annotation(:description "A longer
    *   description runnig over
    *   several lines")
    *
    * In this case the next lines are read from this.lineReader until the
    * closing quote was found.
    *
    * @return A string containing a single axioms in functional OWL syntax
    */
  private def readNextRecord: String = {
    val record = new Text()

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
  * An InputFormat class which mimics the behaviour of TextInputFormat, except
  * that it returns a FunctionalSyntaxRecordReader instead of a
  * LineRecordReader.
  */
class FunctionalSyntaxInputFormat extends FileInputFormat[LongWritable, Text]
    with JobConfigurable {

  var minSplitSize: Long = 1
  var SPLIT_SLOP = 1.1

  /**
    * Generates and returns an array of FileInput splits based on the
    * configured number of splits.
    * The actual code of this method was mainly copied over from
    * TextInputFormat and slightly adapted.
    */
  override def getSplits(job: JobConf, numSplits: Int): Array[InputSplit] = {
    val files: Array[FileStatus] = listStatus(job)
    val numFiles = files.length
    job.setLong(FileInputFormat.NUM_INPUT_FILES, files.length)

    var totalSize: Long = 0
    for (file <- files) {
      if (file.isDirectory) throw new Exception("Not a file: " + file)

      totalSize += file.getLen
    }

    val goalSize = totalSize / (if(numSplits == 0) 1 else numSplits)
    val minSize = Math.max(
      job.getLong(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.SPLIT_MINSIZE, 1),
      minSplitSize)
    val splits = new Array[InputSplit](numFiles)
    val clusterMap = new NetworkTopology

    for (i <- 0 until numFiles) {
      val file = files(i)
      val path = file.getPath
      val length = file.getLen

      if (length != 0) {
        val fs = path.getFileSystem(job)
        var blockLocations: Array[BlockLocation] = null

        file match {
          case file: LocatedFileStatus =>
            blockLocations = file.asInstanceOf[LocatedFileStatus].getBlockLocations
          case _ => blockLocations = fs.getFileBlockLocations(file, 0, length)
        }

        if (isSplitable(fs, path)) {
          val blockSize = file.getBlockSize
          val splitSize = Math.max(minSize, Math.min(goalSize, blockSize))

          var bytesRemaining = length

          while (bytesRemaining.asInstanceOf[Double] / splitSize > SPLIT_SLOP) {
            val splitHosts = getSplitHosts(blockLocations,
              length-bytesRemaining, splitSize, clusterMap)
            splits(i) = new FileSplit(path, length-bytesRemaining,
              bytesRemaining, splitHosts)

            bytesRemaining -= splitSize
          }

          if (bytesRemaining != 0) {
            val splitHosts: Array[String] = getSplitHosts(blockLocations,
              length - bytesRemaining, bytesRemaining, clusterMap)
            splits(i) = new FileSplit(path, length-bytesRemaining,
              bytesRemaining, splitHosts)
          }

        } else {
          val splitHosts = getSplitHosts(blockLocations, 0, length, clusterMap)
          splits(i) = new FileSplit(path, 0, length, splitHosts)
        }

      } else {
        splits(i) = new FileSplit(path, 0, length, new Array[String](0))
      }
    }

    splits
  }

  override def getRecordReader(
      split: InputSplit, job: JobConf, reporter: Reporter): RecordReader[LongWritable, Text] = {

    new FunctionalSyntaxRecordReader(job, split.asInstanceOf[FileSplit])
  }

  override def configure(job: JobConf): Unit = {
    // ...well, seems there is nothing to configure, yet
  }
}
