package net.sansa_stack.hadoop.output.jena.base;

import org.apache.jena.graph.Triple;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.system.StreamRDF;

public class OutputFormatStreamRdfTriple
    extends OutputFormatStreamRdfBase<Triple>
{
    @Override
    protected void sendRecordToStreamRdf(StreamRDF streamRdf, Triple record) {
        streamRdf.triple(record);
    }

    @Override
    protected RDFFormat getDefaultRdfFormat() {
        return RDFFormat.NTRIPLES;
    }
}
