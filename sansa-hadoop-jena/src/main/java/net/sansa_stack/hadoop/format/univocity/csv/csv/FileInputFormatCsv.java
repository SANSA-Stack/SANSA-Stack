package net.sansa_stack.hadoop.format.univocity.csv.csv;

import net.sansa_stack.hadoop.util.ConfigurationUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.util.List;

public class FileInputFormatCsv
        extends FileInputFormat<LongWritable, String[]> {

    public FileInputFormatCsv() {
    }

    @Override
    public boolean isSplitable(JobContext context, Path file) {
        CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
        // If there is no codec - indicated by codec equals null - then the input is considered to be splittable
        boolean result = codec == null || codec instanceof SplittableCompressionCodec;
        return result;
    }

    @Override
    public RecordReader<LongWritable, String[]> createRecordReader(InputSplit inputSplit, TaskAttemptContext context) {
        return new RecordReaderCsv();
    }
}