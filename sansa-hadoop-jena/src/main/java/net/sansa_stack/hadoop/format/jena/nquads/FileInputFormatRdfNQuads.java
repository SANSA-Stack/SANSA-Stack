package net.sansa_stack.hadoop.format.jena.nquads;

import net.sansa_stack.hadoop.format.jena.base.FileInputFormatRdfBase;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.jena.riot.Lang;
import org.apache.jena.sparql.core.Quad;

public class FileInputFormatRdfNQuads
        extends FileInputFormatRdfBase<Quad>
{
    public FileInputFormatRdfNQuads() {
        super(Lang.NQUADS, null);
    }

    @Override
    public RecordReader<LongWritable, Quad> createRecordReaderActual(InputSplit inputSplit, TaskAttemptContext context) {
        return new RecordReaderRdfNQuads();
    }
}
