package net.sansa_stack.hadoop.output.jena.base;

import org.aksw.jena_sparql_api.rx.RDFLanguagesEx;
import org.aksw.jenax.arq.util.prefix.PrefixMappingTrie;
import org.aksw.jenax.arq.util.prefix.PrefixUtils;
import org.aksw.jenax.arq.util.triple.ModelUtils;
import org.aksw.jenax.sparql.query.rx.RDFDataMgrEx;
import org.apache.hadoop.conf.Configuration;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.shared.PrefixMapping;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

public class RdfOutputUtils<T> {
    public static final String RDF_FORMAT = "mapreduce.output.rdf.format";
    public static final String RDF_PREFIXES = "mapreduce.output.rdf.prefixes";
    public static final String RDF_DOWNGRADE_QUADS = "mapreduce.output.rdf.quads.downgrade";

    public static Configuration setPrefixes(Configuration conf, PrefixMapping prefixMap) {
        String str = PrefixUtils.toString(prefixMap, RDFFormat.TURTLE_PRETTY);
        conf.set(RDF_PREFIXES, str);
        return conf;
    }

    public static Configuration setRdfFormat(Configuration conf, RDFFormat rdfFormat) {
        conf.set(RDF_FORMAT, rdfFormat == null ? null : rdfFormat.toString());
        return conf;
    }

    public static Configuration setMapQuadsToTriplesForTripleLangs(Configuration conf, boolean value) {
        if (!value) {
            conf.unset(RDF_DOWNGRADE_QUADS);
        } else {
            conf.setBoolean(RDF_DOWNGRADE_QUADS, true);
        }
        return conf;
    }

    public static PrefixMapping getPrefixes(Configuration conf) {
        String str = conf.get(RDF_PREFIXES, "");
        Model tmp = ModelFactory.createDefaultModel();
        RDFDataMgr.read(tmp, new ByteArrayInputStream(str.getBytes(StandardCharsets.UTF_8)), Lang.TURTLE);

        PrefixMapping result = new PrefixMappingTrie();
        result.setNsPrefixes(tmp.getNsPrefixMap());

        return result;
    }

    public static boolean getMapQuadsToTriplesForTripleLangs(Configuration conf) {
        boolean result = conf.getBoolean(RDF_DOWNGRADE_QUADS, false);
        return result;
    }

    public static RDFFormat getRdfFormat(Configuration conf, RDFFormat defaultFormat) {
        String str = conf.get(RDF_FORMAT);
        RDFFormat result = str == null ? defaultFormat : RDFLanguagesEx.findRdfFormat(str);
        return result;
    }
}
