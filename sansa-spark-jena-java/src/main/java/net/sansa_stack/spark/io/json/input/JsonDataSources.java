package net.sansa_stack.spark.io.json.input;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import net.sansa_stack.hadoop.format.gson.json.FileInputFormatJsonArray;
import net.sansa_stack.hadoop.format.gson.json.FileInputFormatJsonSequence;
import net.sansa_stack.hadoop.format.gson.json.JsonElementArrayIterator;
import net.sansa_stack.hadoop.format.gson.json.JsonElementSequenceIterator;
import org.aksw.jena_sparql_api.sparql.ext.json.JenaJsonUtils;
import org.aksw.jena_sparql_api.sparql.ext.json.RDFDatatypeJson;
import org.apache.commons.io.input.CloseShieldReader;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

public class JsonDataSources {

    public static JavaRDD<Binding> createRddFromJson(JavaSparkContext javaSparkContext, String filename, int probeCount, Var outputVar) {
        JavaRDD<JsonElement> rdd = null;
        try {
            rdd = createRddFromJson(javaSparkContext, filename, probeCount);
        } catch (IOException e) {
            throw new RuntimeException("Failed to probe JSON content of '" + filename + "'", e);
        }
        JavaRDD<Binding> result = toBindings(rdd, outputVar);
        return result;
    }

    public static JavaRDD<JsonElement> createRddFromJson(JavaSparkContext javaSparkContext, String filename, int probeCount) throws IOException {
        FileSystem hadoopFs = FileSystem.get(javaSparkContext.hadoopConfiguration());
        Path path = new Path(filename);
        JsonProbeResult probeResult;
        try (Reader reader = new BufferedReader(new InputStreamReader(hadoopFs.open(path), StandardCharsets.UTF_8))) {
            probeResult = probeJsonFormat(reader, RDFDatatypeJson.get().getGson(), probeCount);
        }

        JavaRDD<JsonElement> result;
        switch (probeResult.getDetectedType()) {
            case ARRAY:
                result = createRddFromJsonArray(javaSparkContext, filename);
                break;
            case SEQUENCE:
                result = createRddFromJsonSequence(javaSparkContext, filename);
                break;
            case UNKNOWN:
            default:
                throw new RuntimeException("Failed to determine JSON format (only array or sequences supported): " + probeResult);
        }
        return result;
    }

    public static JavaRDD<JsonElement> createRddFromJsonArray(JavaSparkContext javaSparkContext, String filename) {
        JavaRDD<JsonElement> rdd = javaSparkContext.newAPIHadoopFile(filename, FileInputFormatJsonArray.class, LongWritable.class,
                JsonElement.class, javaSparkContext.hadoopConfiguration()).map(t -> t._2);
        return rdd;
    }

    public static JavaRDD<Binding> createRddFromJsonArray(JavaSparkContext javaSparkContext, String filename, Var outputVar) {
        JavaRDD<JsonElement> rdd = createRddFromJsonArray(javaSparkContext, filename);
        return toBindings(rdd, outputVar);
    }

    public static JavaRDD<JsonElement> createRddFromJsonSequence(JavaSparkContext javaSparkContext, String filename) {
        JavaRDD<JsonElement> rdd = javaSparkContext.newAPIHadoopFile(filename, FileInputFormatJsonSequence.class, LongWritable.class,
                JsonElement.class, javaSparkContext.hadoopConfiguration()).map(t -> t._2);
        return rdd;
    }

    public static JavaRDD<Binding> createRddFromJsonSequence(JavaSparkContext javaSparkContext, String filename, Var outputVar) {
        JavaRDD<JsonElement> rdd = createRddFromJsonSequence(javaSparkContext, filename);
        return toBindings(rdd, outputVar);
    }

    /**
     * Convert a JavaRDD&gt;JsonElement&lt; into a JavaRDD&gt;Binding&lt; by means of
     * converting JSON elements into Nodes (primitive JSON will become native RDF!)
     * and adding them to bindings with the given outputVar.
     */
    public static JavaRDD<Binding> toBindings(JavaRDD<JsonElement> rdd, Var outputVar) {
        String varName = outputVar.getName();
        return rdd.mapPartitions(it -> {
            Var var = Var.alloc(varName);
            return Iter.iter(it).map(json -> {
                Node node = JenaJsonUtils.convertJsonToNode(json, RDFDatatypeJson.get().getGson(), RDFDatatypeJson.get());
                Binding r = BindingFactory.binding(var, node);
                return r;
            });
        });
    }

    public enum JsonSourceType {
        UNKNOWN, // No known format succeeded to parse
        // INVALID, Distinguish between "not-parsed-yet" and "failed to parse"?
        ARRAY,
        SEQUENCE,
    }

    public static class JsonProbeResult {
        protected JsonSourceType detectedType;
        protected Map<JsonSourceType, Throwable> exceptions;

        public JsonProbeResult(JsonSourceType detectedType, Map<JsonSourceType, Throwable> exceptions) {
            this.detectedType = detectedType;
            this.exceptions = exceptions;
        }

        public JsonSourceType getDetectedType() {
            return detectedType;
        }

        public Map<JsonSourceType, Throwable> getExceptions() {
            return exceptions;
        }

        @Override
        public String toString() {
            return "JsonProbeResult{" +
                    "detectedType=" + detectedType +
                    ", exceptions=" + exceptions +
                    '}';
        }
    }

    /**
     * Detect whether input is...
     * (a) A JSON array; identified by a starting open bracket [
     * (b) A sequence of JSON elements (with no special separator)
     *
     * @param reader A reader with mark support. The mark will be reset upon returning from this function.
     * @param gson The Gson instance
     * @param probeCount The number of json elements to read from the input stream for probing
     */
    public static JsonProbeResult probeJsonFormat(Reader reader, Gson gson, int probeCount) throws IOException {
        JsonSourceType detectedType = null;
        Map<JsonSourceType, Throwable> exceptions = new LinkedHashMap<>();
        if (!reader.markSupported()) {
            throw new IllegalArgumentException("InputStream must support marks");
        }
        int readLimit = 1 * 1024 * 1024 * 1024;

        if (detectedType == null) {
            reader.mark(readLimit);
            try (JsonElementArrayIterator it = new JsonElementArrayIterator(gson, gson.newJsonReader(new CloseShieldReader(reader)))) {
                for (int i = 0; i < probeCount && it.hasNext(); ++i) {
                    it.next();
                }
                detectedType = JsonSourceType.ARRAY;
            } catch (Throwable e) {
                exceptions.put(JsonSourceType.ARRAY, e);
            }
            reader.reset();
        }

        if (detectedType == null) {
            reader.mark(readLimit);
            try (JsonElementSequenceIterator it = new JsonElementSequenceIterator(gson, gson.newJsonReader(new CloseShieldReader(reader)))) {
                for (int i = 0; i < probeCount && it.hasNext(); ++i) {
                    it.next();
                    // System.out.println(it.next());
                }
                detectedType = JsonSourceType.SEQUENCE;
            } catch (Throwable e) {
                exceptions.put(JsonSourceType.SEQUENCE, e);
            }
            reader.reset();
        }

        if (detectedType == null) {
            detectedType = JsonSourceType.UNKNOWN;
        }

        JsonProbeResult result = new JsonProbeResult(detectedType, exceptions);
        return result;
    }

    /*
    public static void main(String[] args) throws IOException {
        Path path = Path.of("/home/raven/Datasets/json/test.json");
        Gson gson = new GsonBuilder().setLenient().create();
        try (Reader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
          JsonProbeResult probeResult = probeJsonFormat(reader, gson, 10);
          System.out.println(probeResult);
        }
    }
    */
}
