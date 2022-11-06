package net.sansa_stack.hadoop.core;

import java.io.IOException;
import java.util.Objects;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.jena.rdf.model.Resource;

import com.google.common.base.Throwables;

public class RecordReaderGenericBaseStatsWrapper
    extends RecordReader<LongWritable, Resource> // Returned as Resource in order to reuse Sansa's default serialization of ResourceImpl
{
    protected Stats2 stats = null;
    protected RecordReaderGenericBase<?, ?, ?, ?> decoratee;

    public RecordReaderGenericBaseStatsWrapper(RecordReaderGenericBase<?, ?, ?, ?> decoratee) {
        super();
        Objects.requireNonNull(decoratee);
        this.decoratee = decoratee;
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        decoratee.initialize(split, context);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        boolean result = false;
        if (stats == null) {
            Throwable throwable = null;
            StopWatch sw = StopWatch.createStarted();
            try {
                boolean wasInterrupted;
                while (!(wasInterrupted = Thread.interrupted()) && decoratee.nextKeyValue()) {
                    decoratee.nextKeyValue();
                }
            } catch (Throwable t) {
                throwable = t;
            } finally {
                stats = decoratee.getStats();
                stats.setTotalTime(sw.getNanoTime() * 1e-9);
                stats.setErrorMessage(throwable == null ? null : Throwables.getStackTraceAsString(throwable));
            }
            result = true;
        }

        return result;
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return stats == null ? null : new LongWritable(0);
    }

    @Override
    public Resource getCurrentValue() throws IOException, InterruptedException {
        return stats.asResource();
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
