package net.sansa_stack.hadoop.jena.rdf.turtle;

import net.sansa_stack.hadoop.jena.rdf.base.FileInputFormatRdfBase;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.Lang;

public class FileInputFormatTurtleTriple
        extends FileInputFormatRdfBase<Triple>
{
    public FileInputFormatTurtleTriple() {
        super(Lang.TURTLE, RecordReaderTurtleTriple.PREFIXES_MAXLENGTH_KEY);
    }

    @Override
    public RecordReader<LongWritable, Triple> createRecordReaderActual(InputSplit inputSplit, TaskAttemptContext context) {
        return new RecordReaderTurtleTriple();
    }
}
