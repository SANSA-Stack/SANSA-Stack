package net.sansa_stack.spark.io.rdf.output;

import net.sansa_stack.spark.io.common.HadoopOutputFormat;
import org.apache.hadoop.io.LongWritable;
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


    protected Map<Lang, HadoopOutputFormat> registry = new LinkedHashMap<>();

    public RddRdfWriterFormatRegistry register(Lang lang, HadoopOutputFormat entry) {
        registry.put(lang, entry);

        return this;
    }

    public HadoopOutputFormat get(Lang lang) {
        return registry.get(lang);
    }

    /**
     * The default registry for elephas output formats based on {@link Lang}.
     * It seems at present it is not possible to request a specific {@link RDFFormat} variant
     * via the hadoop configuration.
     */
    public static Map<Lang, HadoopOutputFormat> getDefaults() {
        Map<Lang, HadoopOutputFormat> result = new LinkedHashMap<>();
        result.put(Lang.NTRIPLES, HadoopOutputFormat.of(LongWritable.class, TripleWritable.class, NTriplesOutputFormat.class));
        result.put(Lang.TURTLE, HadoopOutputFormat.of(LongWritable.class, TripleWritable.class, TurtleOutputFormat.class));
        result.put(Lang.RDFJSON, HadoopOutputFormat.of(LongWritable.class, TripleWritable.class, RdfJsonOutputFormat.class));
        result.put(Lang.RDFXML, HadoopOutputFormat.of(LongWritable.class, TripleWritable.class, RdfXmlOutputFormat.class));

        result.put(Lang.NQUADS, HadoopOutputFormat.of(LongWritable.class, QuadWritable.class, NQuadsOutputFormat.class));
        result.put(Lang.TRIG, HadoopOutputFormat.of(LongWritable.class, QuadWritable.class, TriGOutputFormat.class));
        result.put(Lang.RDFTHRIFT, HadoopOutputFormat.of(LongWritable.class, QuadWritable.class, ThriftQuadOutputFormat.class));
        result.put(Lang.TRIX, HadoopOutputFormat.of(LongWritable.class, QuadWritable.class, TriXOutputFormat.class));
        result.put(Lang.JSONLD, HadoopOutputFormat.of(LongWritable.class, QuadWritable.class, JsonLDQuadOutputFormat.class));
        return result;
    }

    public static RddRdfWriterFormatRegistry createDefault() {
        RddRdfWriterFormatRegistry result = new RddRdfWriterFormatRegistry();
        Map<Lang, HadoopOutputFormat> map = getDefaults();

        for (Map.Entry<Lang, HadoopOutputFormat> e : map.entrySet()) {
            result.register(e.getKey(), e.getValue());
        }

        return result;
    }
}
