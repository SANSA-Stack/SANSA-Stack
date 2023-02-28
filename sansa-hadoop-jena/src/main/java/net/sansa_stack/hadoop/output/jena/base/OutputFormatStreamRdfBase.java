package net.sansa_stack.hadoop.output.jena.base;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.function.Function;

import org.aksw.jenax.arq.util.prefix.PrefixMapTrie;
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
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.system.PrefixMap;
import org.apache.jena.riot.system.PrefixMapStd;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.shared.PrefixMapping;

public abstract class OutputFormatStreamRdfBase<T>
    extends FileOutputFormat<Long, T>
{
    protected abstract void sendRecordToStreamRdf(StreamRDF streamRdf, T record);

    protected abstract RDFFormat getDefaultRdfFormat();

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

        RDFFormat rdfFormat = getDefaultRdfFormat();
        rdfFormat = RdfOutputUtils.getRdfFormat(conf, rdfFormat);

        PrefixMapping prefixMap = RdfOutputUtils.getPrefixes(conf);
        boolean mapQuadsToTriplesForTripleLangs = RdfOutputUtils.getMapQuadsToTriplesForTripleLangs(conf);

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
        // PrefixMap prefixes = new PrefixMapAdapter(prefixMap);
        PrefixMap p = new PrefixMapTrie(); //PrefixMapStd(); // PrefixMapFactory.createForOutput(prefixMap);
        p.putAll(prefixMap);

        Function<OutputStream, StreamRDF> mapper = StreamRDFUtils.createStreamRDFFactory(rdfFormat, mapQuadsToTriplesForTripleLangs, p, fragmentOutputSpec);
        StreamRDF streamRdf = mapper.apply(out);

        RecordWriterStreamRDF result = new RecordWriterStreamRDF<>(streamRdf, this::sendRecordToStreamRdf, out::close);
        return result;
    }
}
