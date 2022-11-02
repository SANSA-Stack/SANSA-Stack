package net.sansa_stack.hadoop.core;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.google.common.base.Preconditions;

public class InputFormatStats
    extends InputFormat<LongWritable, Stats>
{
    protected InputFormat<?, ?> decoratee;

    public InputFormatStats() {
        this(null);
    }

    public InputFormatStats(InputFormat<?, ?> decoratee) {
        super();
        this.decoratee = decoratee;
    }

    public void ensureInit(Configuration hc) {
        if (decoratee == null) {
            String className = hc.get("delegate");
            Preconditions.checkNotNull(className, "Delegate class not set - must be a fully qualified class name that is a subclass of " + RecordReaderGenericBase.class.getName());

            Class<?> clz;
            try {
                clz = Class.forName(className);
                decoratee = (InputFormat<?, ?>) clz.getDeclaredConstructor().newInstance();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public RecordReader<LongWritable, Stats> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        ensureInit(context.getConfiguration());
        RecordReader<?, ?> reader = decoratee.createRecordReader(split, context);
        RecordReaderGenericBase<?, ?, ?, ?> tmp = (RecordReaderGenericBase<?, ?, ?, ?>) reader;

        return new RecordReaderGenericBaseStatsWrapper(tmp);
    }

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        ensureInit(context.getConfiguration());
        List<InputSplit> result = decoratee.getSplits(context);
        return result;
    }
}
