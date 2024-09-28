package net.sansa_stack.hadoop.output.jena.base;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.system.PrefixMap;
import org.apache.jena.riot.system.StreamRDF;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

public class RecordWriterStreamRDF<T>
    extends RecordWriter<Long, T>
{
    protected StreamRDF streamRdf;
    protected BiConsumer<StreamRDF, T> sendRecordToStreamRdf;
    protected AutoCloseable closeAction;

    public RecordWriterStreamRDF(StreamRDF streamRdf, BiConsumer<StreamRDF, T> sendRecordToStreamRdf, AutoCloseable closeAction) {
        this.streamRdf = streamRdf;
        this.sendRecordToStreamRdf = sendRecordToStreamRdf;
        this.closeAction = closeAction;

        // streamRdf.start();
    }

    @Override
    public void write(Long aLong, T record) throws IOException, InterruptedException {
        sendRecordToStreamRdf.accept(streamRdf, record);
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        try {
            streamRdf.finish();
            closeAction.close();
        } catch (InterruptedException e) {
            throw e;
        } catch (IOException e) {
            throw new IOException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
