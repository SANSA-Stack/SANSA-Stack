package net.sansa_stack.hadoop.output.jena.base;

import org.aksw.jenax.arq.util.prefix.PrefixMapTrie;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.system.PrefixMap;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.shared.PrefixMapping;

import java.io.OutputStream;
import java.util.function.Function;

public abstract class OutputFormatStreamRdfBase<T>
    extends OutputFormatBase<T>
{
    protected abstract void sendRecordToStreamRdf(StreamRDF streamRdf, T record);

    protected abstract RDFFormat getDefaultRdfFormat();

    @Override
    protected RecordWriter<Long, T> getRecordWriter(Configuration conf, OutputStream out, FragmentOutputSpec fragmentOutputSpec) {
        RDFFormat rdfFormat = getDefaultRdfFormat();
        rdfFormat = RdfOutputUtils.getRdfFormat(conf, rdfFormat);

        PrefixMapping prefixMap = RdfOutputUtils.getPrefixes(conf);
        boolean mapQuadsToTriplesForTripleLangs = RdfOutputUtils.getMapQuadsToTriplesForTripleLangs(conf);

        // PrefixMap prefixes = new PrefixMapAdapter(prefixMap);
        PrefixMap p = new PrefixMapTrie(); //PrefixMapStd(); // PrefixMapFactory.createForOutput(prefixMap);
        p.putAll(prefixMap);

        Function<OutputStream, StreamRDF> mapper = StreamRDFUtils.createStreamRDFFactory(rdfFormat, mapQuadsToTriplesForTripleLangs, p, fragmentOutputSpec);
        StreamRDF streamRdf = mapper.apply(out);

        RecordWriterStreamRDF result = new RecordWriterStreamRDF<>(streamRdf, this::sendRecordToStreamRdf, out::close);
        return result;
    }
}
