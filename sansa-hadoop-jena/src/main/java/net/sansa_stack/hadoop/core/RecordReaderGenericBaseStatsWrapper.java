package net.sansa_stack.hadoop.core;

import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class RecordReaderGenericBaseStatsWrapper
    extends RecordReader<LongWritable, Stats>
{
    protected Stats stats = null;
    protected RecordReaderGenericBase<?, ?, ?, ?> decoratee;
    protected boolean closeRequested = false;
    protected boolean finished = false;

    public RecordReaderGenericBaseStatsWrapper(RecordReaderGenericBase<?, ?, ?, ?> decoratee) {
        super();
        this.decoratee = decoratee;
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        decoratee.initialize(split, context);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        boolean result = false;
        if (!finished) {
            boolean wasInterrupted;
            while (!(wasInterrupted = Thread.interrupted()) && decoratee.nextKeyValue()) {
                decoratee.nextKeyValue();
            }
            finished = true;

            stats = decoratee.getStats();
            result = true;
        }

        return result;
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return new LongWritable(0);
    }

    @Override
    public Stats getCurrentValue() throws IOException, InterruptedException {
        return stats;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return decoratee.getProgress();
    }

    @Override
    public void close() throws IOException {
        decoratee.close();
    }
}
