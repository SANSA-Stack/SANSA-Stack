package net.sansa_stack.hadoop.format.jena.trig;

import net.sansa_stack.hadoop.format.jena.base.FileInputFormatRdfBase;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.jena.riot.Lang;
import org.apache.jena.sparql.core.Quad;

public class FileInputFormatRdfTrigQuad
        extends FileInputFormatRdfBase<Quad> {
    public FileInputFormatRdfTrigQuad() {
        super(Lang.TRIG, RecordReaderRdfTrigQuad.PREFIXES_MAXLENGTH_KEY);
    }

    @Override
    public RecordReader<LongWritable, Quad> createRecordReaderActual(InputSplit inputSplit, TaskAttemptContext context) {
        return new RecordReaderRdfTrigQuad();
    }
}