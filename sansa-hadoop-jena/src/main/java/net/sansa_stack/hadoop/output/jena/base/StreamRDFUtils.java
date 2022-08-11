package net.sansa_stack.hadoop.output.jena.base;

import org.aksw.jenax.arq.util.prefix.PrefixMapAdapter;
import org.aksw.jenax.arq.util.prefix.PrefixMappingTrie;
import org.aksw.jenax.arq.util.streamrdf.WriterStreamRDFBaseUtils;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.system.*;
import org.apache.jena.riot.writer.WriterStreamRDFBase;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.sparql.core.Quad;

import java.io.OutputStream;
import java.util.function.Function;

public class StreamRDFUtils {
    /**
     * Create a function that can create a StreamRDF instance that is backed by the given
     * OutputStream.
     *
     * @param rdfFormat
     * @param prefixMap
     * @return
     */
    public static Function<OutputStream, StreamRDF> createStreamRDFFactory(
            RDFFormat rdfFormat,
            boolean mapQuadsToTriplesForTripleLangs,
            PrefixMap prefixMap,
            FragmentOutputSpec fragmentOutputSpec) {

        return out -> {

            StreamRDF rawWriter = StreamRDFWriter.getWriterStream(out, rdfFormat, null);

            StreamRDF coreWriter = org.aksw.jenax.arq.util.streamrdf.StreamRDFUtils.unwrap(rawWriter);

            // Retain blank nodes as given
            if (coreWriter instanceof WriterStreamRDFBase) {
                WriterStreamRDFBase tmp = (WriterStreamRDFBase)coreWriter;
                WriterStreamRDFBaseUtils.setNodeToLabel(tmp, SyntaxLabels.createNodeToLabelAsGiven());

                if (prefixMap != null && !fragmentOutputSpec.isEmitHead()) {
                    // Inject the trie-based prefix mapping rather than using the default
                    WriterStreamRDFBaseUtils.setPrefixMap(tmp, prefixMap);
                    /*
                    PrefixMap pm = WriterStreamRDFBaseUtils.getPrefixMap(tmp);
                    for (Map.Entry<String, String> e : prefixMapping.getNsPrefixMap().entrySet()) {
                        pm.add(e.getKey(), e.getValue());
                    }
                    */
                } else {
                    WriterStreamRDFBaseUtils.setPrefixMap(tmp, new PrefixMapAdapter(new PrefixMappingTrie()));
                }
                WriterStreamRDFBaseUtils.updateFormatter(tmp);
            }

            if (RDFLanguages.isTriples(rdfFormat.getLang()) && mapQuadsToTriplesForTripleLangs) {
                rawWriter = new StreamRDFWrapper(rawWriter) {
                    @Override
                    public void quad(Quad quad) {
                        super.triple(quad.asTriple());
                    }
                };
            }

            rawWriter.start();

            if (fragmentOutputSpec.isEmitHead()) {
                StreamRDFOps.sendPrefixesToStream(prefixMap, rawWriter);
            }

            rawWriter = org.aksw.jenax.arq.util.streamrdf.StreamRDFUtils.wrapWithoutPrefixDelegation(rawWriter);

            return rawWriter;
        };
    }
}
