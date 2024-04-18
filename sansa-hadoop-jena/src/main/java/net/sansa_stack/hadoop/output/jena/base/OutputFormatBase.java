package net.sansa_stack.hadoop.output.jena.base;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public abstract class OutputFormatBase<T>
        extends FileOutputFormat<Long, T>
{


    @Override
    public RecordWriter<Long, T> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        Configuration conf = job.getConfiguration();

//        if (!conf.getBoolean(SansaHadoopConstants.OUTPUT_VALIDATE_DISABLE, false)) {
//            checkOutputSpecs(job);
//        }

        int splitCount = OutputUtils.getSplitCount(conf);

        TaskAttemptID taskAttemptId = job.getTaskAttemptID();
        TaskID taskId = taskAttemptId.getTaskID();
        int splitId = taskId.getId();

        boolean isCompressed = getCompressOutput(job);
        CompressionCodec codec = null;
        String extension = "";
        if (isCompressed) {
            Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(job, GzipCodec.class);
            codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
            extension = codec.getDefaultExtension();
        }

        Path file = this.getDefaultWorkFile(job, extension);
        FileSystem fs = file.getFileSystem(conf);
        boolean overwrite = false;
        OutputStream out = fs.create(file, overwrite);
        if (isCompressed) {
            out = new DataOutputStream(codec.createOutputStream(out));
        }

        FragmentOutputSpec fragmentOutputSpec = FragmentOutputSpec.create(splitCount, splitId);

        return getRecordWriter(conf, out, fragmentOutputSpec);
    }

    protected abstract RecordWriter<Long, T> getRecordWriter(Configuration conf, OutputStream out, FragmentOutputSpec fragmentOutputSpec);
}