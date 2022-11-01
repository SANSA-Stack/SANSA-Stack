package net.sansa_stack.hadoop.core;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class FileInputFormatStats
    extends FileInputFormat<LongWritable, Stats>
{
    protected InputFormat<?, ?> decoratee;

    public FileInputFormatStats(InputFormat<?, ?> decoratee) {
        super();
        this.decoratee = decoratee;
    }

    @Override
    public RecordReader<LongWritable, Stats> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        RecordReader<?, ?> reader = decoratee.createRecordReader(split, context);
        RecordReaderGenericBase<?, ?, ?, ?> tmp = (RecordReaderGenericBase<?, ?, ?, ?>) reader;

        return new RecordReaderGenericBaseStatsWrapper(tmp);
    }
}
