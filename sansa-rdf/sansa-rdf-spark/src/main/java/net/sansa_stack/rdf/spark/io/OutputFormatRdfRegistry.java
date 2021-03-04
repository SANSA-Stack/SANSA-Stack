package net.sansa_stack.rdf.spark.io;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.jena.hadoop.rdf.io.output.trig.TriGOutputFormat;
import org.apache.jena.hadoop.rdf.types.QuadWritable;
import org.apache.jena.riot.Lang;

import java.util.LinkedHashMap;
import java.util.Map;

public class OutputFormatRdfRegistry {
    private static transient OutputFormatRdfRegistry INSTANCE = null;

    public static OutputFormatRdfRegistry getInstance() {
        if (INSTANCE == null) {
            synchronized (OutputFormatRdfRegistry.class) {
                if (INSTANCE == null) {
                    INSTANCE = OutputFormatRdfRegistry.createDefault();
                }
            }
        }
        return INSTANCE;
    }


    protected Map<Lang, Enty> registry = new LinkedHashMap<>();

    public OutputFormatRdfRegistry register(Lang lang, Enty entry) {
        registry.put(lang, entry);

        return this;
    }

    public Enty get(Lang lang) {
        return registry.get(lang);
    }

    public static class Enty {
        protected Class<?> keyClass;
        protected Class<?> valueClass;
        protected Class<? extends OutputFormat> outputFormatClass;

        public Enty(Class<?> keyClass, Class<?> valueClass, Class<? extends OutputFormat> outputFormatClass) {
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

    public static Map<Lang, Enty> getDefaults() {
        Map<Lang, Enty> result = new LinkedHashMap<>();
        result.put(Lang.TRIG, new Enty(LongWritable.class, QuadWritable.class, TriGOutputFormat.class));
        return result;
    }

    public static OutputFormatRdfRegistry createDefault() {
        OutputFormatRdfRegistry result = new OutputFormatRdfRegistry();
        Map<Lang, Enty> map = getDefaults();

        for (Map.Entry<Lang, Enty> e : map.entrySet()) {
            result.register(e.getKey(), e.getValue());
        }

        return result;
    }
}
