package net.sansa_stack.hadoop.output.jena.base;

import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.sparql.core.Quad;

public class OutputFormatStreamRdfQuad
    extends OutputFormatStreamRdfBase<Quad>
{
    @Override
    protected void sendRecordToStreamRdf(StreamRDF streamRdf, Quad record) {
        streamRdf.quad(record);
    }

    @Override
    protected RDFFormat getDefaultRdfFormat() {
        return RDFFormat.NQUADS;
    }
}
