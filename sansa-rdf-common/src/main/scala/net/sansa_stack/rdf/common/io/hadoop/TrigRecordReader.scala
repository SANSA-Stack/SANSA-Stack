package net.sansa_stack.rdf.common.io.hadoop

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream, SequenceInputStream}
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.util
import java.util.concurrent.atomic.AtomicLong
import java.util.regex.Pattern

import org.aksw.jena_sparql_api.common.DefaultPrefixes
import org.aksw.jena_sparql_api.io.binseach.{CharSequenceFromSeekable, PageManagerForByteBuffer, PageNavigator, ReverseCharSequenceFromSeekable}
import org.aksw.jena_sparql_api.rx.RDFDataMgrRx
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import org.apache.jena.ext.com.google.common.primitives.Ints
import org.apache.jena.query.{Dataset, DatasetFactory}
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.apache.jena.riot.{Lang, RDFDataMgr, RDFFormat}

/**
 * A record reader for Trig RDF files.
 *
 * @author Lorenz Buehmann
 * @author Claus Stadler
 */
class TrigRecordReader(prefixMapping: Model = ModelFactory.createDefaultModel().setNsPrefixes(DefaultPrefixes.prefixes))
  extends RecordReader[LongWritable, Dataset] {

  private val trigFwdPattern: Pattern = Pattern.compile("@base|@prefix|(graph)?\\s*(<[^>]*>|_:[^-\\s]+)\\s*\\{", Pattern.CASE_INSENSITIVE)
  private val trigBwdPattern: Pattern = Pattern.compile("esab@|xiferp@|\\{\\s*(>[^<]*<|[^-\\s]+:_)\\s*(hparg)?", Pattern.CASE_INSENSITIVE)

  private var start, end, position = 0L

  private val currentKey = new AtomicLong
  private var currentValue: Dataset = DatasetFactory.create()

  private var datasetFlow: util.Iterator[Dataset] = _

  override def initialize(inputSplit: InputSplit, context: TaskAttemptContext): Unit = {
    // split position in data (start one byte earlier to detect if
    // the split starts in the middle of a previous record)
    val split = inputSplit.asInstanceOf[FileSplit]
    start = 0L.max(split.getStart - 1)
    end = start + split.getLength

    // open a stream to the data, pointing to the start of the split
    val stream = split.getPath.getFileSystem(context.getConfiguration)
      .open(split.getPath)


    val bufferSize = inputSplit.getLength.toInt
    val buffer = new Array[Byte](bufferSize)
    stream.readFully(0, buffer)

    val pageManager = new PageManagerForByteBuffer(ByteBuffer.wrap(buffer))
    val nav = new PageNavigator(pageManager)

    val isFwd = true

    // Lets start from this position
    nav.setPos(0)
    val absMatcherStartPos = nav.getPos

    // The charSequence has a clone of nav so it has independent relative positioning
    val charSequence = new CharSequenceFromSeekable(nav.clone)
    val fwdMatcher = trigFwdPattern.matcher(charSequence)

    val reverseCharSequence = new ReverseCharSequenceFromSeekable(nav.clone)
    val bwdMatcher = trigBwdPattern.matcher(reverseCharSequence)

    val m = if (isFwd) fwdMatcher else bwdMatcher

    val availableRegionLength = if (isFwd) Ints.saturatedCast(pageManager.getEndPos)
    else Ints.saturatedCast(absMatcherStartPos + 1)

    m.region(0, availableRegionLength)

    // we have to prepend prefixes to help the parser as there is no other way to make it aware of those
    val baos = new ByteArrayOutputStream()
    RDFDataMgr.write(baos, prefixMapping, RDFFormat.TURTLE_PRETTY)
    val prefixBytes = baos.toByteArray

    var matchCount = 0
    while (m.find && matchCount < 10) {
      val start = m.start
      val end = m.end
      // The matcher yields absolute byte positions from the beginning of the byte sequence
      val matchPos = if (isFwd) start else -end + 1
      val absPos = (absMatcherStartPos + matchPos).asInstanceOf[Int]
      // Artificially create errors
      // absPos += 5;
      nav.setPos(absPos)
      println(s"Attempting pos: $absPos")
      val navClone = nav.clone
      val maxQuadCount = 3

      val task = new java.util.concurrent.Callable[InputStream]() {
        def call(): InputStream = new SequenceInputStream(new ByteArrayInputStream(prefixBytes), Channels.newInputStream(navClone.clone))
      }
      val quadCount = RDFDataMgrRx.createFlowableQuads(task, Lang.TRIG, null)
        .limit(maxQuadCount)
        .count
        .onErrorReturnItem(-1L)
        .blockingGet

      // if success, parse to Dataset
      if (quadCount != 0) {
        matchCount += 1
        println(s"Candidate start pos $absPos yield $quadCount / $maxQuadCount quads")

        datasetFlow = RDFDataMgrRx.createFlowableDatasets(task, Lang.TRIG, null)
          .blockingIterable()
          .iterator()

        return
      }
    }

  }

  override def nextKeyValue(): Boolean = {
    if (datasetFlow == null || !datasetFlow.hasNext) false
    else {
      currentValue = datasetFlow.next()
      currentKey.incrementAndGet
      currentValue != null
    }
  }

  override def getCurrentKey: LongWritable = if (currentValue == null) null else new LongWritable(currentKey.get)

  override def getCurrentValue: Dataset = currentValue

  override def getProgress: Float = 0

  override def close(): Unit = {
    if (datasetFlow != null) {
      datasetFlow = null
    }
  }
}
