package net.sansa_stack.hadoop.jena.rdf.trig;

import net.sansa_stack.hadoop.jena.rdf.base.FileInputFormatRdfBase;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.jena.query.Dataset;
import org.apache.jena.riot.Lang;

public class FileInputFormatTrigDataset
    extends FileInputFormatRdfBase<Dataset>
{
    public FileInputFormatTrigDataset() {
        super(Lang.TRIG, RecordReaderTrigDataset.PREFIXES_MAXLENGTH_KEY);
    }

    @Override
    public RecordReader<LongWritable, Dataset> createRecordReaderActual(InputSplit inputSplit, TaskAttemptContext context) {
        return new RecordReaderTrigDataset();
    }
}
