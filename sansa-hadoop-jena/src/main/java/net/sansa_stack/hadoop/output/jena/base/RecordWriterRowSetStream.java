package net.sansa_stack.hadoop.output.jena.base;

import org.aksw.jenax.io.rowset.core.RowSetStreamWriter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.jena.sparql.engine.binding.Binding;

import java.io.IOException;

public class RecordWriterRowSetStream
        extends RecordWriter<Long, Binding> {
    protected RowSetStreamWriter writer;
    protected FragmentOutputSpec fragmentOutputSpec;

    boolean hasPreviousBinding;

    public RecordWriterRowSetStream(RowSetStreamWriter writer, FragmentOutputSpec fragmentOutputSpec) throws IOException {
        this.writer = writer;
        this.fragmentOutputSpec = fragmentOutputSpec;
        // this.closeAction = closeAction;

        // streamRdf.start();
        this.hasPreviousBinding = RowSetStreamWriterUtils.init(writer, fragmentOutputSpec);
    }

    @Override
    public void write(Long aLong, Binding record) throws IOException, InterruptedException {
        if (hasPreviousBinding) {
            writer.writeBindingSeparator();
        }

        writer.writeBinding(record);
        hasPreviousBinding = true;
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        try {
            if (fragmentOutputSpec.isEmitTail()) {
                writer.endBindings();
                writer.writeFooter();
            }
            writer.close();
            // streamRdf.finish();
            // closeAction.close();
        } catch (IOException e) {
            throw new IOException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}