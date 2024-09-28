package net.sansa_stack.hadoop.output.jena.base;

import org.aksw.jenax.arq.util.lang.RDFLanguagesEx;
import org.aksw.jenax.arq.util.prefix.PrefixMappingTrie;
import org.aksw.jenax.arq.util.prefix.PrefixUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.resultset.ResultSetLang;
import org.apache.jena.riot.resultset.ResultSetWriterRegistry;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.sparql.core.Var;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class RdfOutputUtils<T> {
    public static final String RDF_FORMAT = "mapreduce.output.rdf.format";
    public static final String RDF_PREFIXES = "mapreduce.output.rdf.prefixes";
    public static final String RDF_DOWNGRADE_QUADS = "mapreduce.output.rdf.quads.downgrade";

    // XXX Move methods for row sets to a separate class?

    public static final String RS_LANG = "mapreduce.output.rs.lang";

    public static final String RS_VARS = "mapreduce.output.rs.vars";

    public static Configuration setPrefixes(Configuration conf, PrefixMapping prefixMap) {
        String str = PrefixUtils.toString(prefixMap, RDFFormat.TURTLE_PRETTY);
        conf.set(RDF_PREFIXES, str);
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

    public static Configuration setRdfFormat(Configuration conf, RDFFormat rdfFormat) {
        conf.set(RDF_FORMAT, rdfFormat == null ? null : rdfFormat.toString());
        return conf;
    }

    public static RDFFormat getRdfFormat(Configuration conf, RDFFormat defaultFormat) {
        String str = conf.get(RDF_FORMAT);
        RDFFormat result = str == null ? defaultFormat : RDFLanguagesEx.findRdfFormat(str);
        return result;
    }

    public static Configuration setMapQuadsToTriplesForTripleLangs(Configuration conf, boolean value) {
        if (!value) {
            conf.unset(RDF_DOWNGRADE_QUADS);
        } else {
            conf.setBoolean(RDF_DOWNGRADE_QUADS, true);
        }
        return conf;
    }

    public static boolean getMapQuadsToTriplesForTripleLangs(Configuration conf) {
        boolean result = conf.getBoolean(RDF_DOWNGRADE_QUADS, false);
        return result;
    }

    public static Configuration setLang(Configuration conf, Lang lang) {
        conf.set(RS_LANG, lang == null ? null : lang.getName());
        return conf;
    }

    public static Lang getLang(Configuration conf, Lang defaultLang) {
        String str = conf.get(RS_LANG);
        // XXX Not ideal to use only ResultSetWriterRegistry.registered() because there are other registries as well
        Lang result = str == null ? defaultLang : RDFLanguagesEx.findLang(str, ResultSetWriterRegistry.registered());
        return result;
    }

    public static Configuration setVars(Configuration conf, List<Var> vars) {
        String[] varNames = vars.stream().map(Var::getName).collect(Collectors.toList())
                .toArray(new String[0]);
        conf.setStrings(RS_VARS, varNames);
        return conf;
    }

    public static List<Var> getVars(Configuration conf) {
        String[] arr = conf.getStrings(RS_VARS);
        List<Var> result = arr == null
                ? null
                : Arrays.asList(arr).stream().map(Var::alloc).collect(Collectors.toList());
        return result;
    }
}
