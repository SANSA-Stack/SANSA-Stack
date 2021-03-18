package net.sansa_stack.hadoop.jena.rdf.trig;

import net.sansa_stack.hadoop.jena.rdf.base.FileInputFormatRdfBase;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.jena.riot.Lang;
import org.apache.jena.sparql.core.Quad;

public class FileInputFormatTrigQuad
        extends FileInputFormatRdfBase<Quad> {
    public FileInputFormatTrigQuad() {
        super(Lang.TRIG, RecordReaderTrigQuad.PREFIXES_MAXLENGTH_KEY);
    }

    @Override
    public RecordReader<LongWritable, Quad> createRecordReaderActual(InputSplit inputSplit, TaskAttemptContext context) {
        return new RecordReaderTrigQuad();
    }
}