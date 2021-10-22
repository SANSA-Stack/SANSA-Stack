package net.sansa_stack.hadoop.format.jena.trig;

import net.sansa_stack.hadoop.format.jena.base.FileInputFormatRdfBase;
import org.aksw.jenax.arq.dataset.api.DatasetOneNg;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.jena.riot.Lang;

public class FileInputFormatRdfTrigDataset
    extends FileInputFormatRdfBase<DatasetOneNg>
{
    public FileInputFormatRdfTrigDataset() {
        super(Lang.TRIG, RecordReaderRdfTrigDataset.PREFIXES_MAXLENGTH_KEY);
    }

    @Override
    public RecordReader<LongWritable, DatasetOneNg> createRecordReaderActual(InputSplit inputSplit, TaskAttemptContext context) {
        return new RecordReaderRdfTrigDataset();
    }
}
