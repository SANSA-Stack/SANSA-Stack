package net.sansa_stack.hadoop.format.jena.ntriples;

import net.sansa_stack.hadoop.format.jena.base.FileInputFormatRdfBase;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.Lang;

public class FileInputFormatRdfNTriples
        extends FileInputFormatRdfBase<Triple>
{
    public FileInputFormatRdfNTriples() {
        super(Lang.NTRIPLES, null);
    }

    @Override
    public RecordReader<LongWritable, Triple> createRecordReaderActual(InputSplit inputSplit, TaskAttemptContext context) {
        return new RecordReaderRdfNTriples();
    }
}
