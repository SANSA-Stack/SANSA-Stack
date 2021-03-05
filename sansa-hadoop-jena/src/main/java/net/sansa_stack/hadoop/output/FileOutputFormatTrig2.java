package net.sansa_stack.hadoop.output;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.jena.hadoop.rdf.io.output.AbstractStreamRdfNodeTupleOutputFormat;
import org.apache.jena.hadoop.rdf.io.output.writers.StreamRdfQuadWriter;
import org.apache.jena.hadoop.rdf.types.QuadWritable;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.writer.WriterStreamRDFBlocks;
import org.apache.jena.sparql.core.Quad;

import java.io.Writer;

/** Not yet used; the idea is to provide an improved version of elepha's TrigOutputFormat */
public class FileOutputFormatTrig2<TKey> extends AbstractStreamRdfNodeTupleOutputFormat<TKey, Quad, QuadWritable> {

    @Override
    protected RecordWriter<TKey, QuadWritable> getRecordWriter(StreamRDF stream, Writer writer, Configuration config) {
        return new StreamRdfQuadWriter<TKey>(stream, writer);
    }

    @Override
    protected StreamRDF getStream(Writer writer, Configuration config) {
        return new WriterStreamRDFBlocks(writer, null);
    }

    @Override
    protected String getFileExtension() {
        return ".trig";
    }

}
