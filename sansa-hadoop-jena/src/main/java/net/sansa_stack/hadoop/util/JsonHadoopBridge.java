package net.sansa_stack.hadoop.util;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.univocity.parsers.csv.CsvParserSettings;
import net.sansa_stack.hadoop.format.univocity.conf.UnivocityHadoopConf;
import net.sansa_stack.hadoop.format.univocity.csv.csv.UnivocityUtils;
import org.aksw.commons.model.csvw.domain.api.Dialect;
import org.aksw.commons.path.core.Path;
import org.aksw.commons.path.core.PathOpsStr;
import org.apache.hadoop.conf.Configuration;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * Jackson-based mapper that can read/write java beans from/to a hadoop configuration object
 * (essentially a Map&lt;String, String&gt;). The java bean is first converted to a (possibly
 * nested) json object structure - only strings and objects allowed! Afterwards, the json
 * keys are converted to strings and appended to a given prefix.
 * For example, a java bean with a json serialization of { "foo": {"bar": "baz" }}
 * and a prefix my.hadoop.configuration.prefix leads to the setting:
 * my.hadoop.configuration.prefix.foo.bar = baz
 */
public class JsonHadoopBridge {

    protected Path<String> basePath;
    protected JsonNode prototype;

    public JsonHadoopBridge(Path<String> basePath, JsonNode prototype) {
        this.basePath = basePath;
        this.prototype = prototype;
    }

    public <T> T read(Configuration conf, T bean) {
        T result;
        try {
            JsonNode node = read(conf);
            result = read(node, bean);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    /** Merge the state of src into dst via json serialization */
    public static <T> T merge(T dst, Object src) {
        JsonNode state = write(src);
        read(state, dst);
        return dst;
    }

    public static <T> T read(JsonNode node, T bean) {
        T result;
        try {
            result = new ObjectMapper()
                    .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                    .readerForUpdating(bean)
                    .readValue(node);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    public JsonNode read(Configuration conf) {

        Function<Path<String>, String> getter = p -> {
            String key = String.join(".", p.getSegments());
            String r = conf.get(key);
            return r;
        };

        JsonNode dst = prototype.deepCopy();

        JsonNode result = readRecursively(prototype, dst, basePath, getter);
        return result;
    }

    public static JsonNode write(Object bean) {
        JsonNode src = new ObjectMapper()
                .setSerializationInclusion(JsonInclude.Include.NON_NULL)
                .valueToTree(bean);
        return src;
    }


    public void write(Configuration conf, Object bean) {
        JsonNode src = write(bean);
        write(conf, src);
    }

    public void write(Configuration conf, JsonNode src) {
        BiConsumer<Path<String>, String> setter = (p, v) -> {
            String key = String.join(".", p.getSegments());
            conf.set(key, v);
        };

        writeRecursively(prototype, src, basePath, setter);
    }


    /** If dst is non-null, then the return value will be dst; otherwise, the return
     * value is either and object or textual node matching the prototype
     */
    public static JsonNode readRecursively(JsonNode prototype, JsonNode dst,
                                Path<String> path, Function<Path<String>, String> getter) {
        JsonNode result;
        if (prototype == null) {
            result = null;
        } if (prototype.isObject()) {
            ObjectNode protoObj = (ObjectNode)prototype;

            ObjectNode dstObj;
            if (dst == null) {
                dstObj = JsonNodeFactory.instance.objectNode();
            } else if (dst.isObject()) {
                dstObj = (ObjectNode)dst;
            } else {
                throw new RuntimeException(String.format("Cannot read object %s into non object %s", protoObj, dst));
            }

            result = dstObj;

            Iterator<Map.Entry<String, JsonNode>> it = protoObj.fields();
            while (it.hasNext()) {
                Map.Entry<String, JsonNode> e = it.next();
                String protoName = e.getKey();
                Path<String> childPath = path.resolve(protoName);

                JsonNode protoValue = e.getValue();
                if (protoValue.isTextual() || protoValue.isNull()) {
                    String val = getter.apply(childPath);
                    if (val != null) {
                        dstObj.set(protoName, JsonNodeFactory.instance.textNode(val));
                    }
                }
                else if (protoValue.isObject()) {
                    JsonNode childNode = readRecursively(protoValue, dstObj, childPath, getter);
                    if (childNode != null) {
                        dstObj.set(protoName, childNode);
                    }
                }
            }
        } else {
            result = null;
        }
        return result;
    }

    public static void writeRecursively(JsonNode prototype, JsonNode src, Path<String> path, BiConsumer<Path<String>, String> setter) {
        if (prototype == null) {
        } if (prototype.isObject()) {
            ObjectNode protoObj = (ObjectNode)prototype;

            ObjectNode srcObj = (ObjectNode)src;

            Iterator<Map.Entry<String, JsonNode>> it = protoObj.fields();
            while (it.hasNext()) {
                Map.Entry<String, JsonNode> e = it.next();
                String protoName = e.getKey();
                Path<String> childPath = path.resolve(protoName);

                JsonNode protoValue = e.getValue();
                JsonNode srcValue = srcObj.get(protoName);

                if (srcValue != null && !srcValue.isNull()) {
                    if (protoValue != null && protoValue.isObject()) {
                        writeRecursively(protoValue, srcValue, childPath, setter);
                    } else {
                        if (!Objects.equals(protoValue, srcValue)) {
                            setter.accept(childPath, srcValue.textValue());
                        }
                    }
                }
            }
        }
    }

    /** This method takes as argument an object that serves as a prototype:
     * It is converted to json (null values retained) and the obtained (nested) keys
     * are used as attributes
     */
    public static JsonHadoopBridge createFromPrototype(Object o, String prefix) {
        Path<String> path = PathOpsStr.newRelativePath(prefix.split("\\."));

        JsonNode prototype = new ObjectMapper()
                .valueToTree(o);

        return new JsonHadoopBridge(path, prototype);
    }

    public static void main(String[] args) throws JsonProcessingException {
        CsvParserSettings settings = new CsvParserSettings();

        UnivocityHadoopConf conf = new UnivocityHadoopConf();
        Dialect dialect = conf.getDialect()
                .setEncoding(StandardCharsets.ISO_8859_1.name())
                .setCommentPrefix(".")
                // .setDelimiter("\n")
                .setSkipBlankRows(false);

        // UnivocityUtils.configureFromDialect(settings, dialect);

        System.out.println(settings);


        /*
        for (int i = 0; i < 1054; ++i) {
            // System.out.println(CsvUtils.excelHeadLabel(i));
        }

        UnivocityHadoopConf protoBean = new UnivocityHadoopConf();
        JsonHadoopBridge adapter = JsonHadoopBridge.createFromPrototype(protoBean, "org.foo.bar");

        Configuration conf = new Configuration();

        UnivocityHadoopConf input = new UnivocityHadoopConf();
        input.getDialect().setEncoding(StandardCharsets.ISO_8859_1.name());
        adapter.write(conf, input);

        System.out.println("raw access: " + conf.get("org.foo.bar.dialect.encoding"));

        UnivocityHadoopConf output = adapter.read(conf, new UnivocityHadoopConf());
        System.out.println(output.getDialect().getEncoding());
*/

        /*
        DialectMutable protoBean = new DialectMutableForwardingJacksonString<>(new DialectMutableImpl());
        ConfigurationJson adapter = ConfigurationJson.createFromPrototype(protoBean, "org.foo.bar");

        DialectMutable dialect = new DialectMutableForwardingJacksonString<>(new DialectMutableImpl())
                .setEncoding(StandardCharsets.ISO_8859_1.name())
                .setCommentPrefix("foobar")
                .setDelimiter("xxx")
                .setSkipBlankRows(true);

        Configuration conf = new Configuration();
        adapter.write(conf, dialect);


        DialectMutable bean = adapter.read(conf, new DialectMutableForwardingJacksonString<>(new DialectMutableImpl()));
        System.out.println(bean.getEncoding());

         */
        // System.out.println(conf);
/*
        DialectMutable dialect = new DialectMutableForwardingJacksonString<>(new DialectMutableImpl())
                .setEncoding(StandardCharsets.ISO_8859_1.name())
                .setCommentPrefix("foobar")
                .setDelimiter("xxx")
                .setSkipBlankRows(true);



        ObjectMapper om = new ObjectMapper()
                .setSerializationInclusion(JsonInclude.Include.NON_NULL);

        String str = om.writeValueAsString(dialect);
        System.out.println(str);
        str = str.replace("true", "false");

        Object o = om.readerForUpdating(dialect)
                .readValue(str);

        System.out.println(dialect.getSkipBlankRows());
        //System.out.println(o);

        // DialectMutable core = new DialectMutableImpl();
        // core.setEncoding(StandardCharsets.ISO_8859_1.name());
        UnivocityHadoopConf conf = new UnivocityHadoopConf();
        conf.getDialect()
                .setEncoding(StandardCharsets.ISO_8859_1.name())
                .setCommentPrefix("foobar")
                .setDelimiter("xxx")
                .setSkipBlankRows(true);

        str = new ObjectMapper()
                .setSerializationInclusion(JsonInclude.Include.NON_NULL)
                .writeValueAsString(conf);
        System.out.println(str);

        // conf.getClass("name")
 */
    }
}
