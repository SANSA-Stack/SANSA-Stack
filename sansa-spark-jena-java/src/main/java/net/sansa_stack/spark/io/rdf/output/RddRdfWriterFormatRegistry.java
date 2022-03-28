package net.sansa_stack.spark.io.rdf.output;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.jena.hadoop.rdf.io.output.jsonld.JsonLDQuadOutputFormat;
import org.apache.jena.hadoop.rdf.io.output.nquads.NQuadsOutputFormat;
import org.apache.jena.hadoop.rdf.io.output.ntriples.NTriplesOutputFormat;
import org.apache.jena.hadoop.rdf.io.output.rdfjson.RdfJsonOutputFormat;
import org.apache.jena.hadoop.rdf.io.output.rdfxml.RdfXmlOutputFormat;
import org.apache.jena.hadoop.rdf.io.output.thrift.ThriftQuadOutputFormat;
import org.apache.jena.hadoop.rdf.io.output.trig.TriGOutputFormat;
import org.apache.jena.hadoop.rdf.io.output.trix.TriXOutputFormat;
import org.apache.jena.hadoop.rdf.io.output.turtle.TurtleOutputFormat;
import org.apache.jena.hadoop.rdf.types.QuadWritable;
import org.apache.jena.hadoop.rdf.types.TripleWritable;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFFormat;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Registry for mapping between jena's {@link RDFFormat} and
 * hadoop's {@link org.apache.hadoop.mapreduce.OutputFormat}.
 */
public class RddRdfWriterFormatRegistry {
    private static transient RddRdfWriterFormatRegistry INSTANCE = null;

    public static RddRdfWriterFormatRegistry getInstance() {
        if (INSTANCE == null) {
            synchronized (RddRdfWriterFormatRegistry.class) {
                if (INSTANCE == null) {
                    INSTANCE = RddRdfWriterFormatRegistry.createDefault();
                }
            }
        }
        return INSTANCE;
    }


    protected Map<Lang, FormatEntry> registry = new LinkedHashMap<>();

    public RddRdfWriterFormatRegistry register(Lang lang, FormatEntry entry) {
        registry.put(lang, entry);

        return this;
    }

    public FormatEntry get(Lang lang) {
        return registry.get(lang);
    }

    public static class FormatEntry {
        protected Class<?> keyClass;
        protected Class<?> valueClass;
        protected Class<? extends OutputFormat> outputFormatClass;

        public FormatEntry(Class<?> keyClass, Class<?> valueClass, Class<? extends OutputFormat> outputFormatClass) {
            this.keyClass = keyClass;
            this.valueClass = valueClass;
            this.outputFormatClass = outputFormatClass;
        }

        public Class<?> getKeyClass() {
            return keyClass;
        }

        public Class<?> getValueClass() {
            return valueClass;
        }

        public Class<? extends OutputFormat> getOutputFormatClass() {
            return outputFormatClass;
        }
    }

    /**
     * The default registry for elephas output formats based on {@link Lang}.
     * It seems at present it is not possible to request a specific {@link RDFFormat} variant
     * via the hadoop configuration.
     */
    public static Map<Lang, FormatEntry> getDefaults() {
        Map<Lang, FormatEntry> result = new LinkedHashMap<>();
        result.put(Lang.NTRIPLES, new FormatEntry(LongWritable.class, TripleWritable.class, NTriplesOutputFormat.class));
        result.put(Lang.TURTLE, new FormatEntry(LongWritable.class, TripleWritable.class, TurtleOutputFormat.class));
        result.put(Lang.RDFJSON, new FormatEntry(LongWritable.class, TripleWritable.class, RdfJsonOutputFormat.class));
        result.put(Lang.RDFXML, new FormatEntry(LongWritable.class, TripleWritable.class, RdfXmlOutputFormat.class));

        result.put(Lang.NQUADS, new FormatEntry(LongWritable.class, QuadWritable.class, NQuadsOutputFormat.class));
        result.put(Lang.TRIG, new FormatEntry(LongWritable.class, QuadWritable.class, TriGOutputFormat.class));
        result.put(Lang.RDFTHRIFT, new FormatEntry(LongWritable.class, QuadWritable.class, ThriftQuadOutputFormat.class));
        result.put(Lang.TRIX, new FormatEntry(LongWritable.class, QuadWritable.class, TriXOutputFormat.class));
        result.put(Lang.JSONLD, new FormatEntry(LongWritable.class, QuadWritable.class, JsonLDQuadOutputFormat.class));
        return result;
    }

    public static RddRdfWriterFormatRegistry createDefault() {
        RddRdfWriterFormatRegistry result = new RddRdfWriterFormatRegistry();
        Map<Lang, FormatEntry> map = getDefaults();

        for (Map.Entry<Lang, FormatEntry> e : map.entrySet()) {
            result.register(e.getKey(), e.getValue());
        }

        return result;
    }
}
